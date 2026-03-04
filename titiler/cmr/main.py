"""TiTiler+CMR FastAPI application."""

import threading
import typing as t
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import TypedDict

import cachetools
from fastapi import FastAPI
from httpx import Client, HTTPError
from starlette.middleware.cors import CORSMiddleware
from titiler.core.dependencies import AssetsExprParams
from titiler.core.dependencies import DatasetParams as RasterioDatasetParams
from titiler.core.errors import DEFAULT_STATUS_CODES, add_exception_handlers
from titiler.core.middleware import CacheControlMiddleware, LoggerMiddleware
from titiler.mosaic.errors import MOSAIC_STATUS_CODES
from titiler.xarray.dependencies import DatasetParams as XarrayDatasetParams

from titiler.cmr import __version__ as titiler_cmr_version
from titiler.cmr.dependencies import CMRAssetsParams, XarrayReaderOptions
from titiler.cmr.factory import CMRTilerFactory
from titiler.cmr.logger import configure_logging, logger
from titiler.cmr.query import CMR_GRANULE_SEARCH_API
from titiler.cmr.reader import GranuleReader
from titiler.cmr.settings import ApiSettings, EarthdataSettings
from titiler.cmr.utils import retry

configure_logging()

settings = ApiSettings()
earthdata_settings = EarthdataSettings()


class AWSCredentials(TypedDict, total=True):
    """AWS S3 temporary credentials."""

    accessKeyId: str
    secretAccessKey: str
    sessionToken: str
    expiration: str


def _fetch_earthdata_token(username: str, password: str) -> str:
    """Fetch an Earthdata Login bearer token via find-or-create."""
    with Client() as client:
        response = client.post(
            "https://urs.earthdata.nasa.gov/api/users/find_or_create_token",
            auth=(username, password),
            headers={"Accept": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()["access_token"]


def make_get_s3_credentials(auth_token: str) -> Callable[[str], AWSCredentials]:
    """Create a function that returns temporary S3 credentials for an endpoint.

    Wraps an httpx request with a TTL-based cache to limit calls for temporary
    S3 credentials while keeping them fresh.

    Args:
        auth_token: Earthdata Login bearer token used to authenticate requests.

    Returns:
        A callable that accepts an S3 credentials endpoint URL and returns
        temporary AWS S3 credentials.
    """

    @cachetools.cached(
        cachetools.TTLCache(maxsize=100, ttl=50 * 60),  # Expire in 50 minutes
        condition=threading.Condition(),  # Prevent race conditions
    )
    @retry(5, HTTPError, 1)
    def get_s3_credentials(endpoint: str) -> AWSCredentials:
        logger.info("Fetching temporary S3 credentials from %s", endpoint)

        with Client() as client:
            response = client.get(
                endpoint,
                headers={"Authorization": f"Bearer {auth_token}"},
                timeout=10,
            )

        response.raise_for_status()
        creds = response.json()

        logger.info(
            "Fetched temporary S3 credentials from %s, expiring at %s.",
            endpoint,
            creds.get("expiration", "an unknown time"),
        )

        return t.cast(AWSCredentials, creds)

    return get_s3_credentials


def startup(app: FastAPI) -> None:
    """Perform application startup.

    Called directly by the Lambda handler (which bypasses the lifespan) and
    also from within the lifespan context manager for non-Lambda deployments.
    """
    app.state.client = Client(base_url=CMR_GRANULE_SEARCH_API)

    app.state.s3_access = earthdata_settings.earthdata_s3_direct_access
    logger.info("S3 direct access: %s", app.state.s3_access)

    app.state.earthdata_token = None
    app.state.get_s3_credentials = None

    if earthdata_settings.earthdata_username and earthdata_settings.earthdata_password:
        logger.info("Fetching earthdata token")
        app.state.earthdata_token = _fetch_earthdata_token(
            earthdata_settings.earthdata_username,
            earthdata_settings.earthdata_password,
        )
        logger.info("Earthdata bearer token acquired")

        if app.state.s3_access:
            # TODO: wire app.state.get_s3_credentials into GranuleReader for S3 direct access
            app.state.get_s3_credentials = make_get_s3_credentials(
                app.state.earthdata_token
            )
    else:
        logger.warning(
            "EARTHDATA_USERNAME/EARTHDATA_PASSWORD not set; authenticated access unavailable"
        )


def shutdown(app: FastAPI) -> None:
    """Perform application shutdown."""
    app.state.client.close()
    logger.info("Shutting down")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI Lifespan."""
    startup(app)
    yield
    shutdown(app)


description = """A TiTiler-based dynamic tiling application for the Common Metadata Repository (CMR).

---

**Documentation**: <a href="https://developmentseed.org/titiler-cmr/" target="_blank">https://developmentseed.org/titiler-cmr/</a>

**Source Code**: <a href="https://github.com/developmentseed/titiler-cmr" target="_blank">https://github.com/developmentseed/titiler-cmr</a>

---

This API allows you to interact with data in CMR using many of the familiar TiTiler functions.
Users can specify a CMR query for a specific concept id (e.g. C123456-LPDAAC_ECS) and datetime
and get a TileJSON, XYZ tile image, statistics report (for a GeoJSON) and more.
"""


tags_metadata = [
    {
        "name": "Xarray Backend",
    },
    {
        "name": "Rasterio Backend",
    },
    # TODO: re-implement timeseries endpoints
]

app = FastAPI(
    title=settings.name,
    openapi_url="/api",
    docs_url="/api.html",
    description=description,
    version=titiler_cmr_version,
    root_path=settings.root_path,
    lifespan=lifespan,
    openapi_tags=tags_metadata,
)

app.state.get_s3_credentials = None

add_exception_handlers(app, DEFAULT_STATUS_CODES)
add_exception_handlers(app, MOSAIC_STATUS_CODES)

# Set all CORS enabled origins
if settings.cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

app.add_middleware(CacheControlMiddleware, cachecontrol=settings.cachecontrol)
app.add_middleware(LoggerMiddleware)

if settings.telemetry_enabled:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app)

###############################################################################
# application endpoints

xarray = CMRTilerFactory(
    router_prefix="/xarray",
    dataset_reader=GranuleReader,
    reader_dependency=XarrayReaderOptions,
    dataset_dependency=XarrayDatasetParams,
    add_statistics=True,
    add_viewer=True,
    add_part=True,
    add_ogc_maps=False,
)
app.include_router(xarray.router, tags=["Xarray Backend"], prefix="/xarray")

rasterio = CMRTilerFactory(
    router_prefix="/rasterio",
    reader_dependency=CMRAssetsParams,
    dataset_dependency=RasterioDatasetParams,
    layer_dependency=AssetsExprParams,
    add_statistics=True,
    add_viewer=True,
    add_part=True,
    add_ogc_maps=False,
)
app.include_router(rasterio.router, tags=["Rasterio Backend"], prefix="/rasterio")
