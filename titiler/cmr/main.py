"""TiTiler+cmr FastAPI application."""

from contextlib import asynccontextmanager

import earthaccess
import jinja2
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from starlette.templating import Jinja2Templates

from titiler.cmr import __version__ as titiler_cmr_version
from titiler.cmr.errors import DEFAULT_STATUS_CODES as CMR_STATUS_CODES
from titiler.cmr.factory import Endpoints
from titiler.cmr.logger import configure_logging
from titiler.cmr.settings import ApiSettings, AuthSettings
from titiler.cmr.timeseries import TimeseriesExtension
from titiler.core.errors import DEFAULT_STATUS_CODES, add_exception_handlers
from titiler.core.middleware import CacheControlMiddleware, LoggerMiddleware
from titiler.mosaic.errors import MOSAIC_STATUS_CODES

# Configure logging at application startup
configure_logging()

jinja2_env = jinja2.Environment(
    loader=jinja2.ChoiceLoader(
        [
            jinja2.PackageLoader(__package__, "templates"),
            jinja2.PackageLoader("titiler.core"),
        ]
    ),
)
templates = Jinja2Templates(env=jinja2_env)

settings = ApiSettings()
auth_config = AuthSettings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI Lifespan."""
    if auth_config.strategy == "environment" and auth_config.access == "direct":
        app.state.cmr_auth = earthaccess.login(strategy="environment")
    else:
        app.state.cmr_auth = None

    yield


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
