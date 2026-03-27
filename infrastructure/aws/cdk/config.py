"""STACK Configs."""

from typing import List

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class StackSettings(BaseSettings):
    """Stack settings"""

    veda_custom_host: str | None = None
    stage: str = "production"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    bootstrap_qualifier: str | None = Field(
        None,
        description="Custom bootstrap qualifier override if not using a default installation of AWS CDK Toolkit to synthesize app.",
    )

    permissions_boundary_policy_name: str | None = Field(
        None,
        description="Name of IAM policy to define stack permissions boundary",
    )


class AppSettings(BaseSettings):
    """Application settings"""

    name: str = "titiler-cmr"

    owner: str | None = None
    client: str | None = None
    project: str | None = None

    # S3 bucket names where TiTiler could do HEAD and GET Requests
    # specific private and public buckets MUST be added if you want to use s3:// urls
    # You can whitelist all bucket by setting `*`.
    # ref: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-arn-format.html
    buckets: List = []

    # S3 key pattern to limit the access to specific items (e.g: "my_data/*.tif")
    key: str = "*"

    timeout: int = 30
    memory: int = 10240

    role_arn: str | None = None

    # The maximum of concurrent executions you want to reserve for the function.
    # Default: - No specific limit - account limit.
    max_concurrent: int | None = None
    alarm_email: str | None = None
    root_path: str | None = None
    earthdata_username: str | None = Field(None, validation_alias="EARTHDATA_USERNAME")
    earthdata_password: str | None = Field(None, validation_alias="EARTHDATA_PASSWORD")
    earthdata_s3_direct_access: bool = False

    aws_request_payer: str | None = None
    telemetry_enabled: bool = True

    model_config = SettingsConfigDict(
        env_prefix="TITILER_CMR_", env_file=".env", extra="ignore"
    )

    @model_validator(mode="after")
    def validate_earthdata_creds(self):
        """Validate that Earthdata credentials are provided."""
        if not (self.earthdata_username and self.earthdata_password):
            raise ValueError(
                "EARTHDATA_USERNAME and EARTHDATA_PASSWORD must be provided"
            )
        return self
