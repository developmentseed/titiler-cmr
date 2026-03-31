"""TiTiler+CMR FastAPI application."""

from contextlib import asynccontextmanager
from typing import Annotated, Literal

import jinja2
from fastapi import FastAPI, Query, Request
from fastapi.responses import RedirectResponse
from httpx import Client
from starlette.middleware.cors import CORSMiddleware
from starlette.templating import Jinja2Templates
from titiler.core.dependencies import DatasetParams as RasterioDatasetParams
from titiler.core.dependencies import ExpressionParams
from titiler.core.errors import DEFAULT_STATUS_CODES, add_exception_handlers
from titiler.core.factory import (
    AlgorithmFactory,
    ColorMapFactory,
    TMSFactory,
)
from titiler.core.middleware import CacheControlMiddleware, LoggerMiddleware
from titiler.core.models.OGC import Conformance, Landing
from titiler.core.resources.enums import MediaType
from titiler.core.utils import accept_media_type, create_html_response
from titiler.mosaic.errors import MOSAIC_STATUS_CODES
from titiler.xarray.dependencies import (
    DatasetParams as XarrayDatasetParams,
)

from titiler.cmr import __version__ as titiler_cmr_version
from titiler.cmr.compatibility import router as compatibility_router
from titiler.cmr.credentials import EarthdataTokenProvider, GetS3Credentials
from titiler.cmr.dependencies import (
    CMRAssetsExprParams,
    CMRAssetsParams,
    RasterioGranuleSearchBackendParams,
    interpolated_xarray_ds_params,
)
from titiler.cmr.errors import CMRQueryTimeout
from titiler.cmr.factory import CMRTilerFactory, granules_router
from titiler.cmr.legacy import legacy_router
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

TITILER_CONFORMS_TO = {
    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/landing-page",
    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/oas30",
    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/html",
    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/json",
}


def startup(app: FastAPI) -> None:
    """Perform application startup.

    Called directly by the Lambda handler (which bypasses the lifespan) and
    also from within the lifespan context manager for non-Lambda deployments.
    """
    cmr_headers = {"client-id": settings.client_id} if settings.client_id else {}
    app.state.client = Client(
        base_url=CMR_GRANULE_SEARCH_API,
        timeout=settings.cmr_timeout,
        headers=cmr_headers,
    )

    app.state.s3_access = earthdata_settings.s3_direct_access
    logger.info("S3 direct access: %s", app.state.s3_access)

    token_provider = EarthdataTokenProvider(
        earthdata_settings.username,
        earthdata_settings.password,
    )
    app.state.earthdata_token_provider = token_provider
    app.state.get_s3_credentials = None

    if app.state.s3_access:
        get_s3_credentials = GetS3Credentials(token_provider)
        app.state.get_s3_credentials = get_s3_credentials
        token_provider.register_refresh_callback(get_s3_credentials.clear)


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

This API renders dynamic tile-based visualizations of geospatial assets discovered through
[NASA's Common Metadata Repository (CMR)](https://cmr.earthdata.nasa.gov). Users specify a
CMR collection (`collection_concept_id`) along with optional CMR search filters — such as
`temporal`, `bounding_box`, or `cloud_cover` — and the API will query CMR for matching
granules, fetch their assets, and render tiles, statistics, or images on demand. Query
parameters accepted by this API are forwarded directly to the
[CMR Granule Search API](https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html).

Two backends are available:

- **Xarray** (`/xarray`): For multi-dimensional array datasets (e.g. NetCDF, HDF5). Supports
  selecting specific variables and dimensions.
- **Rasterio** (`/rasterio`): For raster file formats readable by GDAL/rasterio
  (e.g. GeoTIFF, COG).

## Timeseries
The Timeseries Extension provides endpoints for requesting results for all points or intervals
along a timeseries. The [/timeseries family of endpoints](#/Timeseries) works by converting
the provided timeseries parameters (`temporal`, `step`, and `temporal_mode`) into a set of
`temporal` query parameters for the corresponding lower-level endpoint, running asynchronous
requests to the lower-level endpoint, then collecting the results and formatting them in a
coherent format for the user.

The timeseries structure is defined by the `temporal`, `step`, and `temporal_mode` parameters.

The `temporal_mode` parameter controls whether CMR is queried for a particular point-in-time
(`temporal_mode=point`) or over an entire interval (`temporal_mode=interval`). In general,
use `temporal_mode=point` for datasets where granules overlap completely in space (e.g. daily
sea surface temperature predictions). For datasets requiring granules from multiple timestamps
to fully cover an AOI, `temporal_mode=interval` is appropriate — for example, weekly composites
of satellite imagery with `step=P1W&temporal_mode=interval`.

To get a timeseries for all granules between two datetimes, specify
`temporal={start}/{end}` and a query will be sent to CMR to identify all of the granule
timestamps between the provided `start` and `end` datetimes.

To get a weekly sample of granules you can specify `temporal={start}/{end}`, `step=P1W`, and
`temporal_mode=point`.
"""


tags_metadata = [
    {
        "name": "OGC Common",
        "description": "OGC API Common endpoints for the landing page and conformance declaration.",
    },
    {
        "name": "Compatibility",
        "description": "Endpoint for evaluating a collection_concept_ids compatibility with TiTiler-CMR.",
    },
    {
        "name": "Xarray Backend",
        "description": "Tile, statistics, and image endpoints backed by the Xarray reader. "
        "Suitable for multi-dimensional array datasets such as NetCDF and HDF5.",
    },
    {
        "name": "Rasterio Backend",
        "description": "Tile, statistics, and image endpoints backed by the GDAL/rasterio reader. "
        "Suitable for raster file formats such as GeoTIFF and Cloud-Optimized GeoTIFF (COG).",
    },
    {
        "name": "Granules",
        "description": "Backend-independent endpoints for querying CMR granule metadata. "
        "These endpoints return matching granules without reading any data.",
    },
    {
        "name": "Timeseries",
        "description": "Endpoints for timeseries analysis and visualization. These endpoints "
        "expand a `temporal` range into individual CMR queries and aggregate the results.",
    },
    {
        "name": "Tiling Schemes",
        "description": "Available OGC Tile Matrix Sets (tiling schemes).",
    },
    {
        "name": "Algorithms",
        "description": "Available post-processing algorithms that can be applied to tile data.",
    },
    {
        "name": "ColorMaps",
        "description": "Available colormaps for single-band data visualization.",
    },
    {
        "name": "Legacy (Deprecated)",
        "description": "Deprecated redirect routes maintained for backwards compatibility. "
        "These routes will be removed in a future version.",
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
add_exception_handlers(app, {CMRQueryTimeout: 504})

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


@app.get("/docs", include_in_schema=False)
def docs_redirect() -> RedirectResponse:
    """Redirect /docs to /api.html (Swagger UI)."""
    return RedirectResponse(url="/api.html")


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


###############################################################################
# application endpoints

xarray = CMRTilerFactory(
    router_prefix="/xarray",
    dataset_reader=XarrayGranuleReader,
    reader_dependency=interpolated_xarray_ds_params,
    dataset_dependency=XarrayDatasetParams,
    layer_dependency=ExpressionParams,
    extensions=[TimeseriesExtension()],
    add_statistics=True,
    add_viewer=True,
    add_part=True,
    add_ogc_maps=False,
    templates=templates,
    enable_telemetry=settings.telemetry_enabled,
)
app.include_router(xarray.router, tags=["Xarray Backend"], prefix="/xarray")

TITILER_CONFORMS_TO.update(xarray.conforms_to)

rasterio = CMRTilerFactory(
    router_prefix="/rasterio",
    dataset_reader=MultiBaseGranuleReader,
    reader_dependency=CMRAssetsParams,
    dataset_dependency=RasterioDatasetParams,
    layer_dependency=CMRAssetsExprParams,
    assets_accessor_dependency=RasterioGranuleSearchBackendParams,
    extensions=[TimeseriesExtension()],
    add_statistics=True,
    add_viewer=True,
    add_part=True,
    add_ogc_maps=False,
    templates=templates,
    enable_telemetry=settings.telemetry_enabled,
)
app.include_router(rasterio.router, tags=["Rasterio Backend"], prefix="/rasterio")

TITILER_CONFORMS_TO.update(rasterio.conforms_to)

app.include_router(granules_router, tags=["Granules"])
app.include_router(compatibility_router, tags=["Compatibility"])
app.include_router(timeseries_router, tags=["Timeseries"])

###############################################################################
# TileMatrixSets endpoints
tms = TMSFactory(templates=templates)
app.include_router(
    tms.router,
    tags=["Tiling Schemes"],
)
TITILER_CONFORMS_TO.update(tms.conforms_to)

###############################################################################
# Algorithms endpoints
algorithms = AlgorithmFactory(templates=templates)
app.include_router(
    algorithms.router,
    tags=["Algorithms"],
)
TITILER_CONFORMS_TO.update(algorithms.conforms_to)

###############################################################################
# Colormaps endpoints
cmaps = ColorMapFactory(templates=templates)
app.include_router(
    cmaps.router,
    tags=["ColorMaps"],
)
TITILER_CONFORMS_TO.update(cmaps.conforms_to)

###############################################################################
# Legacy backwards-compatibility redirect routes (must be last so new routes take priority)
app.include_router(
    legacy_router,
    tags=["Legacy (Deprecated)"],
    include_in_schema=True,
)
