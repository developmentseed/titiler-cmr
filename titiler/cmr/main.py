"""TiTiler+CMR FastAPI application."""

import threading
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Annotated, Literal

import cachetools
import jinja2
from fastapi import FastAPI, Query, Request
from httpx import Client
from starlette.middleware.cors import CORSMiddleware
from starlette.templating import Jinja2Templates
from titiler.core.dependencies import AssetsExprParams
from titiler.core.dependencies import DatasetParams as RasterioDatasetParams
from titiler.core.errors import DEFAULT_STATUS_CODES, add_exception_handlers
from titiler.core.middleware import CacheControlMiddleware, LoggerMiddleware
from titiler.core.models.OGC import Conformance, Landing
from titiler.core.resources.enums import MediaType
from titiler.core.utils import accept_media_type, create_html_response
from titiler.mosaic.errors import MOSAIC_STATUS_CODES
from titiler.xarray.dependencies import (
    DatasetParams as XarrayDatasetParams,
)
from titiler.xarray.dependencies import XarrayParams

from titiler.cmr import __version__ as titiler_cmr_version
from titiler.cmr.compatibility import router as compatibility_router
from titiler.cmr.credentials import EarthdataS3CredentialProvider
from titiler.cmr.dependencies import CMRAssetsParams
from titiler.cmr.factory import CMRTilerFactory
from titiler.cmr.logger import configure_logging, logger
from titiler.cmr.query import CMR_GRANULE_SEARCH_API
from titiler.cmr.reader import MultiBaseGranuleReader, XarrayGranuleReader
from titiler.cmr.settings import ApiSettings, EarthdataSettings
from titiler.cmr.timeseries import TimeseriesExtension, timeseries_router

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
earthdata_settings = EarthdataSettings()


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


def make_get_s3_credentials(
    auth_token: str,
) -> Callable[[str], EarthdataS3CredentialProvider]:
    """Create a factory that returns an S3 credential provider for an endpoint.

    Wraps provider creation with a TTL-based cache so the same provider instance
    (with its own internal credential cache) is reused across requests for the
    same endpoint.

    Args:
        auth_token: Earthdata Login bearer token used to authenticate requests.

    Returns:
        A callable that accepts an S3 credentials endpoint URL and returns
        an EarthdataS3CredentialProvider instance.
    """

    @cachetools.cached(
        cachetools.TTLCache(maxsize=100, ttl=50 * 60),  # Expire in 50 minutes
        condition=threading.Condition(),  # Prevent race conditions
    )
    def get_s3_credentials(endpoint: str) -> EarthdataS3CredentialProvider:
        return EarthdataS3CredentialProvider(endpoint, auth_token)

    return get_s3_credentials


def startup(app: FastAPI) -> None:
    """Perform application startup.

    Called directly by the Lambda handler (which bypasses the lifespan) and
    also from within the lifespan context manager for non-Lambda deployments.
    """
    app.state.client = Client(
        base_url=CMR_GRANULE_SEARCH_API, timeout=settings.cmr_timeout
    )

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


@app.get(
    "/",
    response_model=Landing,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "text/html": {},
                "application/json": {},
            }
        },
    },
    tags=["OGC Common"],
)
def landing(
    request: Request,
    f: Annotated[
        Literal["html", "json"] | None,
        Query(
            description="Response MediaType. Defaults to endpoint's default or value defined in `accept` header."
        ),
    ] = None,
):
    """TiTiler landing page."""
    data = {
        "title": "titiler-cmr",
        "links": [
            {
                "title": "Landing Page",
                "href": str(request.url_for("landing")),
                "type": MediaType.html,
                "rel": "self",
            },
            {
                "title": "the API definition (JSON)",
                "href": str(request.url_for("openapi")),
                "type": MediaType.openapi30_json,
                "rel": "service-desc",
            },
            {
                "title": "the API documentation",
                "href": str(request.url_for("swagger_ui_html")),
                "type": MediaType.html,
                "rel": "service-doc",
            },
            {
                "title": "Conformance",
                "href": str(request.url_for("conformance")),
                "type": MediaType.json,
                "rel": "conformance",
            },
            {
                "title": "TiTiler-CMR Documentation (external link)",
                "href": "https://developmentseed.org/titiler-cmr/",
                "type": MediaType.html,
                "rel": "doc",
            },
            {
                "title": "TiTiler-CMR source code (external link)",
                "href": "https://github.com/developmentseed/titiler-cmr",
                "type": MediaType.html,
                "rel": "doc",
            },
        ],
    }

    if f:
        output_type = MediaType[f]
    else:
        accepted_media = [MediaType.html, MediaType.json]
        output_type = (
            accept_media_type(request.headers.get("accept", ""), accepted_media)
            or MediaType.json
        )

    if output_type == MediaType.html:
        return create_html_response(
            request,
            data,
            title="TiTiler",
            template_name="landing",
            templates=templates,
        )

    return data


@app.get(
    "/conformance",
    response_model=Conformance,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "text/html": {},
                "application/json": {},
            }
        },
    },
    tags=["OGC Common"],
)
def conformance(
    request: Request,
    f: Annotated[
        Literal["html", "json"] | None,
        Query(
            description="Response MediaType. Defaults to endpoint's default or value defined in `accept` header."
        ),
    ] = None,
):
    """Conformance classes.

    Called with `GET /conformance`.

    Returns:
        Conformance classes which the server conforms to.

    """
    data = {
        "conformsTo": sorted(
            [
                "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
                "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/landing-page",
                "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/json",
                "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/html",
                "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/oas30",
                "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/core",
                "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/oas30",
                "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tileset",
                "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tilesets-list",
            ]
        )
    }

    if f:
        output_type = MediaType[f]
    else:
        accepted_media = [MediaType.html, MediaType.json]
        output_type = (
            accept_media_type(request.headers.get("accept", ""), accepted_media)
            or MediaType.json
        )

    if output_type == MediaType.html:
        return create_html_response(
            request,
            data,
            title="Conformance",
            template_name="conformance",
            templates=templates,
        )

    return data


if settings.telemetry_enabled:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app)

###############################################################################
# application endpoints

xarray = CMRTilerFactory(
    router_prefix="/xarray",
    dataset_reader=XarrayGranuleReader,
    reader_dependency=XarrayParams,
    dataset_dependency=XarrayDatasetParams,
    extensions=[TimeseriesExtension()],
    add_statistics=True,
    add_viewer=True,
    add_part=True,
    add_ogc_maps=False,
    templates=templates,
)
app.include_router(xarray.router, tags=["Xarray Backend"], prefix="/xarray")

rasterio = CMRTilerFactory(
    router_prefix="/rasterio",
    dataset_reader=MultiBaseGranuleReader,
    reader_dependency=CMRAssetsParams,
    dataset_dependency=RasterioDatasetParams,
    layer_dependency=AssetsExprParams,
    extensions=[TimeseriesExtension()],
    add_statistics=True,
    add_viewer=True,
    add_part=True,
    add_ogc_maps=False,
    templates=templates,
)
app.include_router(rasterio.router, tags=["Rasterio Backend"], prefix="/rasterio")
app.include_router(compatibility_router, tags=["Compatibility"])
app.include_router(timeseries_router, tags=["Timeseries"])
