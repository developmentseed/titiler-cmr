"""STACK Configs."""

from typing import Dict, List, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class StackSettings(BaseSettings):
    """Application settings"""

    name: str = "titiler-cmr"
    stage: str = "production"

    owner: Optional[str] = None
    client: Optional[str] = None
    project: Optional[str] = None

    additional_env: Dict = {}

    # S3 bucket names where TiTiler could do HEAD and GET Requests
    # specific private and public buckets MUST be added if you want to use s3:// urls
    # You can whitelist all bucket by setting `*`.
    # ref: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-arn-format.html
    buckets: List = []

    # S3 key pattern to limit the access to specific items (e.g: "my_data/*.tif")
    key: str = "*"

    timeout: int = 30
    memory: int = 3009

    role_arn: Optional[str] = None

    # The maximum of concurrent executions you want to reserve for the function.
    # Default: - No specific limit - account limit.
    max_concurrent: Optional[int] = None
    alarm_email: Optional[str] = None

    model_config = SettingsConfigDict(
        env_prefix="STACK_", env_file=".env", extra="ignore"
    )
