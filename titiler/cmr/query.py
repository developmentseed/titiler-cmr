"""CMR granule and collection search functions."""

from typing import Any, Generator, Literal

import shapely
from fastapi import HTTPException
from geojson_pydantic.geometries import Geometry, Point
from httpx2 import (
    Client,
    ConnectTimeout,
    DecodingError,
    NetworkError,
    ReadTimeout,
    RemoteProtocolError,
    Request,
    Response,
    WriteTimeout,
)
from pydantic import BaseModel, ValidationError
from shapely.geometry import shape

from titiler.cmr.errors import CMROutage, CMRQueryTimeout
from titiler.cmr.logger import logger
from titiler.cmr.models import (
    Collection,
    CollectionSearchResponse,
    Granule,
    GranuleSearch,
    GranuleSearchResponse,
)

CMR_GRANULE_SEARCH_API = "https://cmr.earthdata.nasa.gov/search"

SearchKind = Literal["collection", "granule"]


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


def _send_cmr_search(client: Client, request: Request, kind: SearchKind) -> Response:
    """Send a CMR search request and return its successful response.

    Only failures on CMR's side become CMRQueryTimeout or CMROutage. Any other
    httpx error (a full connection pool, a malformed header, a proxy setting)
    is titiler-cmr's own fault, so it propagates and becomes a 500.

    Raises:
        CMRQueryTimeout: CMR did not answer in time.
        CMROutage: CMR could not be reached, sent a body that could not be
            decoded, or answered with a 3xx or 5xx, so the failure is CMR's,
            not the request's.
        HTTPException: CMR answered with a 4xx. The status is kept, and the
            detail is CMR's error messages as plain text, or the body as is
            when it does not carry CMR's messages.
    """
    try:
        response = client.send(request)
    except (ConnectTimeout, ReadTimeout, WriteTimeout) as e:
        raise CMRQueryTimeout(
            f"CMR {kind} search timed out after {client.timeout.read} seconds"
        ) from e
    except (NetworkError, RemoteProtocolError, DecodingError) as e:
        raise CMROutage(
            f"CMR {kind} search failed: {str(e) or type(e).__name__}"
        ) from e

    if response.is_success:
        return response

    if not response.is_client_error:
        raise CMROutage(
            f"CMR {kind} search failed with {response.status_code} "
            f"({response.reason_phrase})"
        )

    try:
        errors = response.json()["errors"]
        detail = (
            "; ".join(errors) if isinstance(errors, list) and errors else response.text
        )
    except (ValueError, KeyError, TypeError):
        detail = response.text

    raise HTTPException(response.status_code, detail)


def _parse_cmr_response[M: BaseModel](
    response: Response, model: type[M], kind: SearchKind
) -> M:
    """Parse a successful CMR search response into its model.

    Raises:
        CMROutage: the body is not JSON, or not the shape of a search result,
            so CMR answered with something other than a search result.
    """
    try:
        return model.model_validate_json(response.content)
    except ValidationError as e:
        raise CMROutage(f"CMR {kind} search returned an unreadable response") from e


def _build_granule_params(
    search_params: GranuleSearch,
    geometry: Geometry | None,
    page_size: int,
) -> dict[str, Any]:
    """Build the CMR query params dict from search parameters and optional geometry."""
    dumped = search_params.model_dump(exclude_none=True)
    sort_keys = dumped.pop("sort_key", None)
    attributes = dumped.pop("attribute", None)
    params: dict[str, Any] = {"page_size": page_size, **dumped}
    if sort_keys:
        params["sort_key[]"] = sort_keys
    if attributes:
        params["attribute[]"] = attributes
    if geometry:
        if search_params.bounding_box:
            params["bounding_box"] = _combine_bbox_and_geometry(
                search_params.bounding_box, geometry
            )
        else:
            params.update(_geometry_to_cmr_params(geometry))
    return params


def _is_fully_covered(
    granule: Granule,
    search_shape: shapely.Geometry | None,
    covered: shapely.Geometry,
    coverage_tolerance: float = 0.0,
) -> tuple[bool, shapely.Geometry]:
    """Update covered area with granule geometry and return (done, covered).

    Args:
        granule: The granule whose geometry is unioned into covered.
        search_shape: The search geometry to check coverage against.
        covered: Accumulated union of granule geometries so far.
        coverage_tolerance: Buffer (in degrees) applied to search_shape before the
            covered_by check. Requires granule polygons to extend this far beyond
            the tile edge before declaring full coverage, compensating for CMR
            polygon overshoot relative to actual raster data boundaries.

    Returns:
        Tuple of (fully_covered, updated_covered).
    """
    if search_shape is None or granule.geometry is None:
        return False, covered
    covered = covered.union(shapely.geometry.shape(granule.geometry))
    check = (
        search_shape.buffer(coverage_tolerance) if coverage_tolerance else search_shape
    )
    return check.covered_by(covered), covered


def get_granules(
    search_params: GranuleSearch,
    client: Client,
    geometry: Geometry | None = None,
    page_size: int = 20,
    limit: int = 100,
    exitwhenfull: bool = False,
    coverage_tolerance: float = 0.0,
    skipcovered: bool = False,
) -> Generator[Granule, None, None]:
    """Run a granule search.

    If exitwhenfull is True and a geometry is provided, stops early once the
    union of returned granule geometries fully covers the search geometry.

    If skipcovered is True, granules whose geometry is identical to a previously
    yielded granule geometry are skipped (deduplicates repeat-pass coverage).

    Args:
        search_params: CMR granule search parameters.
        client: HTTP client to use for CMR requests.
        geometry: Optional spatial filter geometry.
        page_size: Number of granules to fetch per CMR page.
        limit: Maximum total number of granules to return.
        exitwhenfull: Stop early once the search geometry is fully covered.
        coverage_tolerance: Buffer (in degrees) applied when checking full
            coverage. A small positive value (e.g. 1e-4) reduces slivers caused
            by imprecise CMR polygon edges.
        skipcovered: Skip any granule whose geometry equals a geometry already
            yielded in this search.

    Any httpx error on titiler-cmr's side (e.g., a full connection pool)
    propagates as is.

    Raises:
        CMRQueryTimeout: CMR did not answer in time.
        CMROutage: CMR could not be reached, did not answer with a search
            result, or answered with a 3xx or 5xx.
        HTTPException: CMR answered with a 4xx, with its status and messages.
    """
    params = _build_granule_params(search_params, geometry, page_size)
    search_shape = (
        shapely.geometry.shape(geometry.model_dump())
        if exitwhenfull and geometry
        else None
    )
    covered: shapely.Geometry = shapely.GeometryCollection()
    seen_geometries: list[shapely.Geometry] = []

    headers: dict[str, str] = {}
    count = 0
    while count < limit:
        request = client.build_request(
            method="GET", url="granules.umm_json", params=params, headers=headers
        )
        logger.info("Querying CMR: %s with headers %s", request.url, headers)
        response = _send_cmr_search(client, request, "granule")
        result = _parse_cmr_response(response, GranuleSearchResponse, "granule")
        logger.debug("Found %d granules", len(result.items))

        for granule in result.items:
            if skipcovered and granule.geometry is not None:
                granule_shape = shapely.geometry.shape(granule.geometry)
                if any(granule_shape.equals(seen) for seen in seen_geometries):
                    logger.debug("Skipping granule with duplicate geometry")
                    continue
                seen_geometries.append(granule_shape)

            count += 1
            yield granule

            if count >= limit:
                return

            done, covered = _is_fully_covered(
                granule, search_shape, covered, coverage_tolerance
            )
            if done:
                logger.info("Search geometry fully covered, stopping early")
                return

        if not (cmr_search_after := response.headers.get("cmr-search-after")):
            break

        headers["cmr-search-after"] = cmr_search_after


def get_collection(concept_id: str, client: Client) -> Collection:
    """Fetch UMM metadata for a CMR collection by concept_id.

    Any httpx error on titiler-cmr's side (e.g., a full connection pool)
    propagates as is.

    Raises:
        CMRQueryTimeout: CMR did not answer in time.
        CMROutage: CMR could not be reached, did not answer with a search
            result, or answered with a 3xx or 5xx.
        HTTPException: 404 if the collection is not found, otherwise CMR's 4xx
            status and messages.
    """
    request = client.build_request(
        method="GET", url="collections.umm_json", params={"concept_id": concept_id}
    )
    response = _send_cmr_search(client, request, "collection")
    result = _parse_cmr_response(response, CollectionSearchResponse, "collection")

    if not result.items:
        raise HTTPException(404, f"concept_id {concept_id} not found")

    return result.items[0].umm
