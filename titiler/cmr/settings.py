"""API settings."""

from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings
from typing_extensions import Annotated


class ApiSettings(BaseSettings):
    """API settings"""

    name: str = "titiler-cmr"
    cors_origins: str = "*"
    cachecontrol: str = "public, max-age=3600"
    root_path: str = ""
    debug: bool = False

    model_config = {
        "env_prefix": "TITILER_CMR_API_",
        "env_file": ".env",
        "extra": "ignore",
    }

    @field_validator("cors_origins")
    def parse_cors_origin(cls, v):
        """Parse CORS origins."""
        return [origin.strip() for origin in v.split(",")]


class CacheSettings(BaseSettings):
    """Cache settings"""

    # TTL of the cache in seconds
    ttl: int = 300

    # Maximum size of the cache in Number of element
    maxsize: int = 512

    # Whether or not caching is enabled
    disable: bool = False

    model_config = {"env_prefix": "TITILER_CMR_CACHE_", "env_file": ".env"}

    @model_validator(mode="after")
    def check_enable(self):
        """Check if cache is disabled."""
        if self.disable:
            self.ttl = 0
            self.maxsize = 0

        return self


class RetrySettings(BaseSettings):
    """Retry settings"""

    retry: Annotated[int, Field(ge=0)] = 3
    delay: Annotated[float, Field(ge=0.0)] = 0.0

    model_config = {
        "env_prefix": "TITILER_CMR_API_",
        "env_file": ".env",
        "extra": "ignore",
    }


class AuthSettings(BaseSettings):
    """AWS credential settings."""

    strategy: Literal["environment", "iam"] = "environment"

    model_config = {
        "env_prefix": "TITILER_CMR_S3_AUTH_",
        "env_file": ".env",
    }
