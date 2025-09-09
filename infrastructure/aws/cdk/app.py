"""Construct App."""

import os
from typing import Any, List, Optional

from aws_cdk import App, CfnOutput, Duration, Stack, Tags, aws_lambda
from aws_cdk import aws_apigatewayv2 as apigw
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cloudwatch_actions
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subscriptions
from aws_cdk.aws_apigatewayv2_integrations import HttpLambdaIntegration
from config import AppSettings, StackSettings
from constructs import Construct

stack_settings, app_settings = StackSettings(), AppSettings()

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
        memory: int = 1024,
        timeout: int = 30,
        runtime: aws_lambda.Runtime = aws_lambda.Runtime.PYTHON_3_12,
        concurrent: Optional[int] = None,
        permissions: Optional[List[iam.PolicyStatement]] = None,
        role_arn: Optional[str] = None,
        context_dir: str = "../../",
        **kwargs: Any,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, *kwargs)

        permissions = permissions or []

        iam_reader_role = None
        if role_arn:
            iam_reader_role = iam.Role.from_role_arn(
                self,
                "veda-reader-dev-role",
                role_arn=role_arn,
            )

        lambda_env = {
            **DEFAULT_ENV,
            "TITILER_CMR_ROOT_PATH": app_settings.root_path,
            "TITILER_CMR_S3_AUTH_STRATEGY": app_settings.s3_auth_strategy,
        }

        if app_settings.aws_request_payer:
            lambda_env["AWS_REQUEST_PAYER"] = app_settings.aws_request_payer

        lambda_function = aws_lambda.Function(
            self,
            f"{id}-lambda",
            runtime=runtime,
            code=aws_lambda.Code.from_docker_build(
                path=os.path.abspath(context_dir),
                file="infrastructure/aws/lambda/Dockerfile",
                platform="linux/amd64",
            ),
            handler="handler.handler",
            memory_size=memory,
            reserved_concurrent_executions=concurrent,
            timeout=Duration.seconds(timeout),
            environment=lambda_env,
            log_retention=logs.RetentionDays.ONE_WEEK,
            role=iam_reader_role,
            # layers=[
            #     aws_lambda.LayerVersion.from_layer_version_arn(
            #         self,
            #         f"{id}-lambda-python-otel-layer",
            #         layer_version_arn=f"arn:aws:lambda:{self.region}:901920570463:layer:aws-otel-python-amd64-ver-1-32-0:2",
            #         # layer_version_arn=f"arn:aws:lambda:{self.region}:184161586896:layer:opentelemetry-python-0_16_0:1",
            #     )
            # ],
            tracing=aws_lambda.Tracing.ACTIVE,
        )

        for perm in permissions:
            lambda_function.add_to_role_policy(perm)

        api = apigw.HttpApi(
            self,
            f"{id}-endpoint",
            default_integration=HttpLambdaIntegration(
                f"{id}-integration",
                lambda_function,
                parameter_mapping=apigw.ParameterMapping().overwrite_header(
                    "host",
                    apigw.MappingValue(stack_settings.veda_custom_host),
                )
                if stack_settings.veda_custom_host
                else None,
            ),
        )

        # Create an SNS Topic
        if app_settings.alarm_email:
            topic = sns.Topic(
                self,
                f"{id}-500-Errors",
                display_name=f"{id} Gateway 500 Errors",
                topic_name=f"{id}-Gateway-500-Errors",
            )
            # Subscribe email to the topic
            topic.add_subscription(
                subscriptions.EmailSubscription(app_settings.alarm_email),
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

perms = []
if app_settings.buckets:
    perms.append(
        iam.PolicyStatement(
            actions=["s3:GetObject"],
            resources=[f"arn:aws:s3:::{bucket}*" for bucket in app_settings.buckets],
        )
    )

lambda_stack = LambdaStack(
    app,
    f"{app_settings.name}-{stack_settings.stage}",
    memory=app_settings.memory,
    timeout=app_settings.timeout,
    concurrent=app_settings.max_concurrent,
    role_arn=app_settings.role_arn,
    permissions=perms,
)
# Tag infrastructure
for key, value in {
    "Project": app_settings.name,
    "Stack": stack_settings.stage,
    "Owner": app_settings.owner,
    "Client": app_settings.client,
}.items():
    if value:
        Tags.of(lambda_stack).add(key, value)


app.synth()
