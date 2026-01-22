"""TiTiler+cmr FastAPI application."""

import os
import threading
import typing as t
from collections.abc import Callable
from contextlib import asynccontextmanager

import cachetools
import earthaccess
import jinja2
import requests
from fastapi import FastAPI, HTTPException
from starlette.middleware.cors import CORSMiddleware
from starlette.templating import Jinja2Templates
from titiler.core.errors import DEFAULT_STATUS_CODES, add_exception_handlers
from titiler.core.middleware import CacheControlMiddleware, LoggerMiddleware
from titiler.mosaic.errors import MOSAIC_STATUS_CODES

from titiler.cmr import __version__ as titiler_cmr_version
from titiler.cmr.backend import AWSCredentials
from titiler.cmr.errors import DEFAULT_STATUS_CODES as CMR_STATUS_CODES
from titiler.cmr.factory import Endpoints
from titiler.cmr.logger import configure_logging, logger
from titiler.cmr.settings import ApiSettings, AuthSettings
from titiler.cmr.timeseries import TimeseriesExtension
from titiler.cmr.utils import retry

# Configure logging at application startup
configure_logging()

jinja2_env = jinja2.Environment(
    loader=jinja2.ChoiceLoader(
        [
            jinja2.PackageLoader("titiler.cmr"),
            jinja2.PackageLoader("titiler.core"),
        ]
    ),
)
templates = Jinja2Templates(env=jinja2_env)

settings = ApiSettings()
auth_config = AuthSettings()
auth: earthaccess.Auth | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI Lifespan."""

    global auth

    logger.info("Starting up")

    if os.environ.get("PYTEST_VERSION"):
        # Avoid earthaccess login during testing
        app.state.auth = None
        app.state.get_s3_credentials = None
    elif auth is None:
        # Set global auth instance only if not already set.  This allows for
        # Lambda functions to avoid repeated logins during warm starts, also
        # maintaining the s3 credential cache across warm starts.
        app.state.auth = (auth := earthaccess.login(strategy="environment"))
        app.state.get_s3_credentials = (
            make_get_s3_credentials(auth) if auth_config.access == "direct" else None
        )

    yield

    logger.info("Shutting down")


def make_get_s3_credentials(auth: earthaccess.Auth) -> Callable[[str], AWSCredentials]:
    """Create a function that returns temporary S3 credentials for an endpoint.

    Wraps an authenticated earthaccess client with a TTL-based cache to limit
    calls for temporary S3 credentials while keeping them fresh.

    Args:
        auth: Authenticated earthaccess client used to request S3 credentials.

    Returns:
        A callable that accepts an (HTTPS) endpoint and returns temporary S3
        credentials from the endpoint, via the ``auth`` object.
    """

    @cachetools.cached(
        cachetools.TTLCache(maxsize=100, ttl=50 * 60),  # Expire in 50 minutes
        condition=threading.Condition(),  # Prevent race conditions
    )
    @retry(5, requests.RequestException, 1)
    def get_s3_credentials(endpoint: str) -> AWSCredentials:
        logger.info("Fetching temporary S3 credentials from %s", endpoint)

        # NOTE: Frustratingly, Auth.get_s3_credentials simply returns an empty
        # dict if any sort of request fails, rather than raising an error.
        # Therefore, we are forced to check the result and raise our own error
        # if the result is empty.
        if not (creds := auth.get_s3_credentials(endpoint=endpoint)):
            logger.error("Failed to fetch temporary S3 credentials from %s", endpoint)
            # We cannot tell what the underlying exception was, since it was
            # swallowed by earthaccess, so we're just making one up.
            raise HTTPException(500, "earthaccess failed to retrieve S3 credentials")

        logger.info(
            "Fetched temporary S3 credentials from %s, expiring at %s.",
            endpoint,
            creds.get("expiration", "an unknown time"),
        )

        return t.cast(AWSCredentials, creds)

    return get_s3_credentials


description = """A TiTiler-based dynamic tiling application for the Common Metadata Repository (CMR).

---

**Documentation**: <a href="https://developmentseed.org/titiler-cmr/" target="_blank">https://developmentseed.org/titiler-cmr/</a>

**Source Code**: <a href="https://github.com/developmentseed/titiler-cmr" target="_blank">https://github.com/developmentseed/titiler-cmr</a>

---

This API allows you to interact with data in CMR using many of the familiar TiTiler functions.
Users can specify a CMR query for a specific concept id (e.g. C123456-LPDAAC_ECS) and datetime
and get a TileJSON, XYZ tile image, statistics report (for a GeoJSON) and more.

## Timeseries
The Timeseries Extension provides endpoints for requesting results for all points or intervals
along a timeseries. The [/timeseries family of endpoints](#/Timeseries) works by converting
the provided timeseries parameters (`datetime`, `step`, and `temporal_mode`) into a set of
`datetime` query parameters for the corresponding lower-level endpoint, running asynchronous
requests to the lower-level endpoint, then collecting the results and formatting them in a
coherent format for the user.

The timeseries structure is defined by the `datetime`, `step`, and `temporal_mode` parameters.

The `temporal_mode` mode parameter controls whether or not CMR is queried for a particular
point-in-time (`temporal_mode=point`) or over an entire interval (`temporal_mode=interval`).
In general, it is best to use `temporal_mode=point` for datasets where granules overlap completely
in space (e.g. daily sea surface temperature predictions) because the /timeseries endpoints will
create a mosaic of all assets returned by the query and the first asset to cover a pixel will
be used. For datasets where it requires granules from multiple timestamps to fully cover an AOI,
`temporal_mode=interval` is appropriate. For example, you can get weekly composites of satellite
imagery for visualization purposes with `step=P1W & temporal_mode=interval`.

To get a timeseries for all granules between two datetimes, you can simply specify
`datetime={start}/{end}` and a query will be sent to CMR to identify all of the granule timestamps
between the provided `start` and `end` datetimes.

To get a weekly sample of granules you can specify `datetime={start}/{end}`, `step=P1W`, and
`temporal_mode=point`.
"""


tags_metadata = [
    {
        "name": "Raster Tiles",
    },
    {
        "name": "TileJSON",
    },
    {
        "name": "Map",
    },
    {
        "name": "Statistics",
    },
    {
        "name": "Images",
    },
    {
        "name": "Timeseries",
        "description": "A family of endpoints for timeseries analysis and visualization.",
    },
    {
        "name": "Tiling Schemes",
    },
    {
        "name": "Landing Page",
    },
    {
        "name": "Conformance",
    },
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

add_exception_handlers(app, DEFAULT_STATUS_CODES)
add_exception_handlers(app, MOSAIC_STATUS_CODES)
add_exception_handlers(app, CMR_STATUS_CODES)

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

###############################################################################
# application endpoints
endpoints = Endpoints(
    title=settings.name,
    templates=templates,
    extensions=[TimeseriesExtension()],
    enable_telemetry=settings.telemetry_enabled,
)
app.include_router(endpoints.router)
