"""Construct App."""

import os
from typing import Any

from aws_cdk import (
    App,
    Aspects,
    CfnOutput,
    Duration,
    Stack,
    Tags,
    aws_lambda,
)
from aws_cdk import aws_apigatewayv2 as apigw
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cloudwatch_actions
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subscriptions
from aws_cdk.aws_apigatewayv2_integrations import HttpLambdaIntegration
from constructs import Construct

from titiler.cmr import __version__ as titiler_cmr_version
from titiler.cmr.settings import ApiSettings, EarthdataSettings, StackSettings

from .permissions_boundary.construct import PermissionsBoundaryAspect

stack_settings = StackSettings()
api_settings = ApiSettings()
earthdata_settings = EarthdataSettings()

DEFAULT_ENV = {
    "AWS_LAMBDA_LOG_FORMAT": "JSON",
    "AWS_LAMBDA_LOG_LEVEL": "INFO",
    "GDAL_CACHEMAX": "200",  # 200 mb
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    "GDAL_INGESTED_BYTES_AT_OPEN": "32768",  # get more bytes when opening the files.
    "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES",
    "GDAL_HTTP_MULTIPLEX": "YES",
    "GDAL_HTTP_VERSION": "2",
    "PYTHONWARNINGS": "ignore",
    "VSI_CACHE": "TRUE",
    "VSI_CACHE_SIZE": "5000000",  # 5 MB (per file-handle)
}


class LambdaStack(Stack):
    """Lambda Stack"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        stack_settings: StackSettings,
        api_settings: ApiSettings,
        earthdata_settings: EarthdataSettings,
        context_dir: str = "../../",
        **kwargs: Any,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, *kwargs)

        if stack_settings.permissions_boundary_policy_name:
            permissions_boundary_policy = iam.ManagedPolicy.from_managed_policy_name(
                self,
                "permissions-boundary",
                stack_settings.permissions_boundary_policy_name,
            )
            iam.PermissionsBoundary.of(self).apply(permissions_boundary_policy)
            Aspects.of(self).add(PermissionsBoundaryAspect(permissions_boundary_policy))

        lambda_env = {
            **DEFAULT_ENV,
            "TITILER_CMR_ROOT_PATH": api_settings.root_path,
            "TITILER_CMR_EARTHDATA_USERNAME": earthdata_settings.username,
            "TITILER_CMR_EARTHDATA_PASSWORD": earthdata_settings.password,
            "TITILER_CMR_EARTHDATA_S3_DIRECT_ACCESS": str(
                earthdata_settings.s3_direct_access
            ).upper(),
        }

        if api_settings.telemetry_enabled:
            lambda_env.update(
                {
                    "TITILER_CMR_TELEMETRY_ENABLED": "TRUE",
                    "OTEL_SERVICE_NAME": api_settings.name,
                }
            )

        if api_settings.aws_request_payer:
            lambda_env["AWS_REQUEST_PAYER"] = api_settings.aws_request_payer

        if api_settings.client_id:
            lambda_env["TITILER_CMR_CLIENT_ID"] = api_settings.client_id

        lambda_function = aws_lambda.Function(
            self,
            f"{id}-lambda",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=aws_lambda.Code.from_docker_build(
                path=os.path.abspath(context_dir),
                file="infrastructure/aws/lambda/Dockerfile",
                platform="linux/amd64",
                build_args={"APP_VERSION": titiler_cmr_version},
            ),
            memory_size=stack_settings.memory,
            reserved_concurrent_executions=stack_settings.max_concurrent,
            timeout=Duration.seconds(stack_settings.timeout),
            environment=lambda_env,
            log_retention=logs.RetentionDays.ONE_WEEK,
            tracing=(
                aws_lambda.Tracing.ACTIVE
                if api_settings.telemetry_enabled
                else aws_lambda.Tracing.DISABLED
            ),
            snap_start=aws_lambda.SnapStartConf.ON_PUBLISHED_VERSIONS,
        )

        # SnapStart only activates on published versions. Create a version and
        # alias so that API Gateway integrates with a versioned function rather
        # than $LATEST, which would bypass the snapshot entirely.
        live_alias = aws_lambda.Alias(
            self,
            f"{id}-live",
            alias_name="live",
            version=lambda_function.current_version,
        )

        # xray:PutSpans/PutSpansForIndexing are required for the X-Ray native OTLP
        # ingestion endpoint. Tracing.ACTIVE only grants PutTraceSegments and
        # PutTelemetryRecords (X-Ray SDK path). PutTraceSegments is included here
        # as well since AWS documentation is inconsistent about which action the
        # OTLP endpoint uses.
        if api_settings.telemetry_enabled:
            lambda_function.add_to_role_policy(
                iam.PolicyStatement(
                    actions=[
                        "xray:PutSpans",
                        "xray:PutSpansForIndexing",
                        "xray:PutTraceSegments",
                        "xray:PutTelemetryRecords",
                    ],
                    resources=["*"],
                )
            )

        api = apigw.HttpApi(
            self,
            f"{id}-endpoint",
            default_integration=HttpLambdaIntegration(
                f"{id}-integration",
                live_alias,
                parameter_mapping=apigw.ParameterMapping().overwrite_header(
                    "host",
                    apigw.MappingValue(stack_settings.veda_custom_host),
                )
                if stack_settings.veda_custom_host
                else None,
            ),
        )

        # Create an SNS Topic
        if stack_settings.alarm_email:
            topic = sns.Topic(
                self,
                f"{id}-500-Errors",
                display_name=f"{id} Gateway 500 Errors",
                topic_name=f"{id}-Gateway-500-Errors",
            )
            # Subscribe email to the topic
            topic.add_subscription(
                subscriptions.EmailSubscription(stack_settings.alarm_email),
            )

            # Create CloudWatch Alarm
            alarm = cloudwatch.Alarm(
                self,
                "MyAlarm",
                metric=cloudwatch.Metric(
                    namespace="AWS/ApiGateway",
                    metric_name="5XXError",
                    dimensions_map={"ApiName": f"{id}-endpoint"},
                    period=Duration.minutes(1),
                ),
                evaluation_periods=1,
                threshold=1,
                alarm_description="Alarm if 500 errors are detected",
                alarm_name=f"{id}-ApiGateway500Alarm",
                actions_enabled=True,
            )
            alarm.add_alarm_action(cloudwatch_actions.SnsAction(topic))

        CfnOutput(self, "Endpoint", value=api.url)


app = App()
if stack_settings.bootstrap_qualifier:
    app.node.set_context(
        "@aws-cdk/core:bootstrapQualifier", stack_settings.bootstrap_qualifier
    )

lambda_stack = LambdaStack(
    app,
    f"{stack_settings.name}-{stack_settings.stage}",
    stack_settings=stack_settings,
    api_settings=api_settings,
    earthdata_settings=earthdata_settings,
)
# Tag infrastructure
for key, value in {
    "Project": stack_settings.name,
    "Stack": stack_settings.stage,
    "Owner": stack_settings.owner,
    "Client": stack_settings.client,
}.items():
    if value:
        Tags.of(lambda_stack).add(key, value)


app.synth()
