"""CMR granule and collection search functions."""

from typing import Any, Generator

import shapely
from fastapi import HTTPException
from geojson_pydantic.geometries import Geometry, Point
from httpx import Client, HTTPStatusError
from shapely.geometry import shape

from titiler.cmr.logger import logger
from titiler.cmr.models import (
    Collection,
    CollectionSearchResponse,
    Granule,
    GranuleSearch,
    GranuleSearchResponse,
)

CMR_GRANULE_SEARCH_API = "https://cmr.earthdata.nasa.gov/search"


def _combine_bbox_and_geometry(bounding_box: str, geometry: Geometry) -> str:
    """Intersect a bounding_box string with a geometry.

    bounding_box format: "minx,miny,maxx,maxy"
    Raises NoAssetFoundError if the two do not overlap.
    Returns a new bounding_box string for their intersection.
    """
    from rio_tiler.errors import NoAssetFoundError

    minx, miny, maxx, maxy = [float(v) for v in bounding_box.split(",")]
    bbox_shape = shapely.geometry.box(minx, miny, maxx, maxy)
    geom_shape = shapely.geometry.shape(geometry.model_dump())

    intersection = bbox_shape.intersection(geom_shape)
    if intersection.is_empty:
        raise NoAssetFoundError("Provided bounding_box and geometry do not overlap")

    ix_minx, ix_miny, ix_maxx, ix_maxy = intersection.bounds
    return f"{ix_minx},{ix_miny},{ix_maxx},{ix_maxy}"


def _geometry_to_cmr_params(geometry: Geometry) -> dict[str, str]:
    """Convert a GeoJSON geometry to CMR spatial search parameters.

    Points map to CMR's `point` parameter (lon,lat).
    All other geometry types map to CMR's `bounding_box` parameter
    derived from the geometry's own bounds (minx,miny,maxx,maxy).
    """
    if isinstance(geometry, Point):
        lon, lat, *_ = geometry.coordinates
        return {"point": f"{lon},{lat}"}

    minx, miny, maxx, maxy = shape(geometry.model_dump()).bounds
    return {"bounding_box": f"{minx},{miny},{maxx},{maxy}"}


def get_granules(
    search_params: GranuleSearch,
    client: Client,
    geometry: Geometry | None = None,
    page_size: int = 10,
    limit: int = 100,
    exitwhenfull: bool = False,
) -> Generator[Granule, None, None]:
    """Run a granule search.

    If exitwhenfull is True and a geometry is provided, stops early once the
    union of returned granule geometries fully covers the search geometry.
    """
    dumped = search_params.model_dump(exclude_none=True)
    sort_keys = dumped.pop("sort_key", None)
    params: dict[str, Any] = {
        "page_size": page_size,
        **dumped,
    }
    if sort_keys:
        params["sort_key[]"] = sort_keys
    if geometry:
        if search_params.bounding_box:
            params["bounding_box"] = _combine_bbox_and_geometry(
                search_params.bounding_box, geometry
            )
        else:
            params.update(_geometry_to_cmr_params(geometry))

    search_shape = (
        shapely.geometry.shape(geometry.model_dump())
        if exitwhenfull and geometry
        else None
    )
    covered: shapely.Geometry = shapely.GeometryCollection()

    headers: dict[str, str] = {}
    count = 0
    while count <= limit:
        response = client.get(
            "granules.umm_json",
            params=params,
            headers=headers,
        )
        logger.info("Querying CMR: %s with headers %s", response.url, headers)

        try:
            response.raise_for_status()
        except HTTPStatusError as e:
            raise HTTPException(response.status_code, response.text) from e

        result = GranuleSearchResponse(**response.json())
        logger.debug(f"Found {len(result.items)} granules")

        for granule in result.items:
            count += 1
            yield granule
            if search_shape is not None and granule.geometry is not None:
                covered = covered.union(shapely.geometry.shape(granule.geometry))
                if search_shape.covered_by(covered):
                    logger.info("Search geometry fully covered, stopping early")
                    return

        if not (cmr_search_after := response.headers.get("cmr-search-after")):
            break

        headers["cmr-search-after"] = cmr_search_after


def get_collection(concept_id: str, client: Client) -> Collection:
    """Fetch UMM metadata for a CMR collection by concept_id.

    Raises:
        HTTPException: 404 if the collection is not found; otherwise the CMR
            HTTP status code.
    """
    response = client.get(
        "collections.umm_json",
        params={"concept_id": concept_id},
    )

    try:
        response.raise_for_status()
    except HTTPStatusError as e:
        raise HTTPException(response.status_code, response.text) from e

    result = CollectionSearchResponse(**response.json())
    if not result.items:
        raise HTTPException(404, f"concept_id {concept_id} not found")

    return result.items[0].umm
