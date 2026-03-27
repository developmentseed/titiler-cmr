"""API settings."""

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Annotated


class EarthdataSettings(BaseSettings):
    """Earthdata Settings"""

    model_config = SettingsConfigDict(
        env_prefix="TITILER_CMR_",
        env_file=".env",
        extra="ignore",
    )

    earthdata_username: str | None = None
    earthdata_password: str | None = None
    earthdata_s3_direct_access: bool = False


class ApiSettings(BaseSettings):
    """API settings"""

    name: str = "titiler-cmr"
    cors_origins: str = "*"
    cachecontrol: str = "public, max-age=3600"
    root_path: str = ""
    time_series_max_requests: int = 995
    time_series_max_image_size: float = 5.625e7
    time_series_statistics_max_total_size: float = 1.5e10
    time_series_image_max_total_size: float = 1e8
    telemetry_enabled: bool = False
    debug: bool = False
    cmr_timeout: float = 10.0
    cmr_client_id: str | None = None

    model_config = {
        "env_prefix": "TITILER_CMR_",
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

    model_config = {
        "env_prefix": "TITILER_CMR_CACHE_",
        "env_file": ".env",
        "extra": "ignore",
    }

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
        "env_prefix": "TITILER_CMR_",
        "env_file": ".env",
        "extra": "ignore",
    }
