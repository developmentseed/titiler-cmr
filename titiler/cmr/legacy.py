"""titiler.cmr.legacy: backwards-compatibility redirect routes.

Redirects old root-level routes (pre-restructure) to the new /{backend}/... paths.
- GET routes use 301 (Moved Permanently)
- POST routes use 308 (Permanent Redirect) to preserve method and request body

Parameter renames applied transparently:
- concept_id         → collection_concept_id
- datetime           → temporal
- bands_regex        → assets_regex
- ?backend=rasterio  → /rasterio/... (default when absent)
- ?backend=xarray    → /xarray/...
"""

from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

PARAM_RENAMES = {
    "concept_id": "collection_concept_id",
    "datetime": "temporal",
    "bands_regex": "assets_regex",
}


def _legacy_redirect(
    request: Request, backend: str, new_path: str, status_code: int
) -> RedirectResponse:
    params = dict(request.query_params)
    params.pop("backend", None)
    for old, new in PARAM_RENAMES.items():
        if old in params:
            if new not in params:
                params[new] = params.pop(old)
            else:
                # New name already present — drop the redundant old name
                params.pop(old)
    base = str(request.base_url).rstrip("/")
    url = f"{base}/{backend}{new_path}"
    if params:
        url += f"?{urlencode(params, doseq=True)}"
    return RedirectResponse(url, status_code=status_code)


legacy_router = APIRouter()

# ---------------------------------------------------------------------------
# Tile / TileJSON / Map routes (GET → 301)
# ---------------------------------------------------------------------------


@legacy_router.get("/{tileMatrixSetId}/tilejson.json")
def legacy_tilejson(request: Request, tileMatrixSetId: str):
    """Redirect to new tilejson endpoint."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(request, backend, f"/{tileMatrixSetId}/tilejson.json", 301)


@legacy_router.get("/{tileMatrixSetId}/map.html")
def legacy_map(request: Request, tileMatrixSetId: str):
    """Redirect to new map endpoint."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(request, backend, f"/{tileMatrixSetId}/map.html", 301)


# Register more specific tile routes (with format/scale) before the generic ones
# so that Starlette matches them first (routes are evaluated in registration order).


@legacy_router.get("/tiles/{tileMatrixSetId}/{z}/{x}/{y}@{scale}x.{format}")
def legacy_tile_scale_format(
    request: Request,
    tileMatrixSetId: str,
    z: int,
    x: int,
    y: int,
    scale: int,
    format: str,
):
    """Redirect to new tile endpoint with scale and format."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(
        request,
        backend,
        f"/tiles/{tileMatrixSetId}/{z}/{x}/{y}@{scale}x.{format}",
        301,
    )


@legacy_router.get("/tiles/{tileMatrixSetId}/{z}/{x}/{y}@{scale}x")
def legacy_tile_scale(
    request: Request, tileMatrixSetId: str, z: int, x: int, y: int, scale: int
):
    """Redirect to new tile endpoint with scale."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(
        request, backend, f"/tiles/{tileMatrixSetId}/{z}/{x}/{y}@{scale}x", 301
    )


@legacy_router.get("/tiles/{tileMatrixSetId}/{z}/{x}/{y}.{format}")
def legacy_tile_format(
    request: Request, tileMatrixSetId: str, z: int, x: int, y: int, format: str
):
    """Redirect to new tile endpoint with format."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(
        request, backend, f"/tiles/{tileMatrixSetId}/{z}/{x}/{y}.{format}", 301
    )


@legacy_router.get("/tiles/{tileMatrixSetId}/{z}/{x}/{y}")
def legacy_tile(request: Request, tileMatrixSetId: str, z: int, x: int, y: int):
    """Redirect to new tile endpoint."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(
        request, backend, f"/tiles/{tileMatrixSetId}/{z}/{x}/{y}", 301
    )


@legacy_router.get("/preview")
def legacy_preview(request: Request):
    """Redirect to new preview endpoint."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(request, backend, "/preview", 301)


@legacy_router.get("/bbox/{minx},{miny},{maxx},{maxy}.{format}")
def legacy_bbox(
    request: Request,
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    format: str,
):
    """Redirect to new bbox endpoint."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(
        request, backend, f"/bbox/{minx},{miny},{maxx},{maxy}.{format}", 301
    )


@legacy_router.get("/bbox/{minx},{miny},{maxx},{maxy}/{width}x{height}.{format}")
def legacy_bbox_size(
    request: Request,
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    width: int,
    height: int,
    format: str,
):
    """Redirect to new bbox endpoint with explicit width/height."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(
        request,
        backend,
        f"/bbox/{minx},{miny},{maxx},{maxy}/{width}x{height}.{format}",
        301,
    )


# ---------------------------------------------------------------------------
# POST routes (308 preserves method + body through redirect)
# ---------------------------------------------------------------------------


@legacy_router.post("/statistics")
def legacy_statistics(request: Request):
    """Redirect to new statistics endpoint."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(request, backend, "/statistics", 308)


@legacy_router.post("/part")
def legacy_part(request: Request):
    """Redirect to new part endpoint."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(request, backend, "/part", 308)


@legacy_router.post("/feature")
def legacy_feature(request: Request):
    """Redirect to new feature endpoint."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(request, backend, "/feature", 308)


@legacy_router.post("/feature.{format}")
def legacy_feature_format(request: Request, format: str):
    """Redirect to new feature endpoint with format."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(request, backend, f"/feature.{format}", 308)


@legacy_router.post("/feature/{width}x{height}.{format}")
def legacy_feature_size(request: Request, width: int, height: int, format: str):
    """Redirect to new feature endpoint with explicit width/height."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(
        request, backend, f"/feature/{width}x{height}.{format}", 308
    )


# ---------------------------------------------------------------------------
# Timeseries legacy routes
# ---------------------------------------------------------------------------


@legacy_router.get("/timeseries")
def legacy_timeseries(request: Request):
    """Redirect to new timeseries endpoint."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(request, backend, "/timeseries", 301)


@legacy_router.post("/timeseries/statistics")
def legacy_timeseries_statistics(request: Request):
    """Redirect to new timeseries statistics endpoint."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(request, backend, "/timeseries/statistics", 308)


@legacy_router.get("/timeseries/{tileMatrixSetId}/tilejson.json")
def legacy_timeseries_tilejson(request: Request, tileMatrixSetId: str):
    """Redirect to new timeseries tilejson endpoint."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(
        request, backend, f"/timeseries/{tileMatrixSetId}/tilejson.json", 301
    )


@legacy_router.get("/timeseries/bbox/{minx},{miny},{maxx},{maxy}.{format}")
def legacy_timeseries_bbox(
    request: Request,
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    format: str,
):
    """Redirect to new timeseries bbox endpoint."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(
        request, backend, f"/timeseries/bbox/{minx},{miny},{maxx},{maxy}.{format}", 301
    )


@legacy_router.get(
    "/timeseries/bbox/{minx},{miny},{maxx},{maxy}/{width}x{height}.{format}"
)
def legacy_timeseries_bbox_size(
    request: Request,
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    width: int,
    height: int,
    format: str,
):
    """Redirect to new timeseries bbox endpoint with explicit width/height."""
    backend = request.query_params.get("backend", "rasterio")
    return _legacy_redirect(
        request,
        backend,
        f"/timeseries/bbox/{minx},{miny},{maxx},{maxy}/{width}x{height}.{format}",
        301,
    )
