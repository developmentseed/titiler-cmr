"""titiler-cmr dependencies."""

from typing import List, Literal, Optional, get_args

from ciso8601 import parse_rfc3339
from fastapi import Depends, HTTPException, Path, Query
from starlette.requests import Request
from typing_extensions import Annotated

from titiler.cmr.enums import MediaType
from titiler.cmr.errors import InvalidBBox, MissingCollectionCatalog
from titiler.cmr.models import Catalog, Collection, CollectionList

ResponseType = Literal["json", "html"]


def s_intersects(bbox: List[float], spatial_extent: List[float]) -> bool:
    """Check if bbox intersects with spatial extent."""
    return (
        (bbox[0] < spatial_extent[2])
        and (bbox[2] > spatial_extent[0])
        and (bbox[3] > spatial_extent[1])
        and (bbox[1] < spatial_extent[3])
    )


def t_intersects(interval: List[str], temporal_extent: List[Optional[str]]) -> bool:
    """Check if dates intersect with temporal extent."""
    if len(interval) == 1:
        start = end = parse_rfc3339(interval[0])

    else:
        start = parse_rfc3339(interval[0]) if interval[0] not in ["..", ""] else None
        end = parse_rfc3339(interval[1]) if interval[1] not in ["..", ""] else None

    mint, maxt = temporal_extent
    min_ext = parse_rfc3339(mint) if mint is not None else None
    max_ext = parse_rfc3339(maxt) if maxt is not None else None

    if len(interval) == 1:
        if start == min_ext or start == max_ext:
            return True

    if not start:
        return max_ext <= end or min_ext <= end

    elif not end:
        return min_ext >= start or max_ext >= start

    else:
        return min_ext >= start and max_ext <= end

    return False


def accept_media_type(accept: str, mediatypes: List[MediaType]) -> Optional[MediaType]:
    """Return MediaType based on accept header and available mediatype.

    Links:
    - https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
    - https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Accept

    """
    accept_values = {}
    for m in accept.replace(" ", "").split(","):
        values = m.split(";")
        if len(values) == 1:
            name = values[0]
            quality = 1.0
        else:
            name = values[0]
            groups = dict([param.split("=") for param in values[1:]])  # type: ignore
            try:
                q = groups.get("q")
                quality = float(q) if q else 1.0
            except ValueError:
                quality = 0

        # if quality is 0 we ignore encoding
        if quality:
            accept_values[name] = quality

    # Create Preference matrix
    media_preference = {
        v: [n for (n, q) in accept_values.items() if q == v]
        for v in sorted(set(accept_values.values()), reverse=True)
    }

    # Loop through available compression and encoding preference
    for _, pref in media_preference.items():
        for media in mediatypes:
            if media.value in pref:
                return media

    # If no specified encoding is supported but "*" is accepted,
    # take one of the available compressions.
    if "*" in accept_values and mediatypes:
        return mediatypes[0]

    return None


def OutputType(
    request: Request,
    f: Annotated[
        Optional[ResponseType],
        Query(
            description="Response MediaType. Defaults to endpoint's default or value defined in `accept` header."
        ),
    ] = None,
) -> Optional[MediaType]:
    """Output MediaType: json or html."""
    if f:
        return MediaType[f]

    accepted_media = [MediaType[v] for v in get_args(ResponseType)]
    return accept_media_type(request.headers.get("accept", ""), accepted_media)


def bbox_query(
    bbox: Annotated[
        Optional[str],
        Query(
            description="A bounding box, expressed in WGS84 (westLong,southLat,eastLong,northLat) or WGS84h (westLong,southLat,minHeight,eastLong,northLat,maxHeight) CRS, by which to filter out all collections whose spatial extent does not intersect with the bounding box.",
            openapi_examples={
                "simple": {"value": "160.6,-55.95,-170,-25.89"},
            },
        ),
    ] = None
) -> Optional[List[float]]:
    """BBox dependency."""
    if bbox:
        bounds = list(map(float, bbox.split(",")))
        if len(bounds) == 4:
            if abs(bounds[0]) > 180 or abs(bounds[2]) > 180:
                raise InvalidBBox(f"Invalid longitude in bbox: {bounds}")
            if abs(bounds[1]) > 90 or abs(bounds[3]) > 90:
                raise InvalidBBox(f"Invalid latitude in bbox: {bounds}")

        elif len(bounds) == 6:
            if abs(bounds[0]) > 180 or abs(bounds[3]) > 180:
                raise InvalidBBox(f"Invalid longitude in bbox: {bounds}")
            if abs(bounds[1]) > 90 or abs(bounds[4]) > 90:
                raise InvalidBBox(f"Invalid latitude in bbox: {bounds}")

        else:
            raise InvalidBBox(f"Invalid bbox: {bounds}")

        return bounds

    return None


def datetime_query(
    datetime: Annotated[
        Optional[str],
        Query(
            description="Either a date-time or an interval. Date and time expressions adhere to [RFC 3339](https://www.rfc-editor.org/rfc/rfc3339). Intervals may be bounded or half-bounded (double-dots at start or end).",
            openapi_examples={
                "A date-time": {"value": "2018-02-12T23:20:50Z"},
                "A bounded interval": {
                    "value": "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
                },
                "Half-bounded intervals (start)": {"value": "2018-02-12T00:00:00Z/.."},
                "Half-bounded intervals (end)": {"value": "../2018-03-18T12:31:12Z"},
            },
        ),
    ] = None,
) -> Optional[List[str]]:
    """Datetime dependency."""
    if datetime:
        dt = datetime.split("/")
        if len(dt) > 2:
            raise HTTPException(status_code=422, detail="Invalid datetime: {datetime}")

        return dt

    return None


def CollectionParams(
    request: Request,
    collectionId: Annotated[str, Path(description="Local identifier of a collection")],
) -> Collection:
    """Return Layer Object."""
    catalog: Optional[Catalog] = getattr(request.app.state, "collection_catalog", None)
    if not catalog:
        raise MissingCollectionCatalog("Could not find collections catalog.")

    if collectionId in catalog["collections"]:
        return catalog["collections"][collectionId].copy()

    raise HTTPException(
        status_code=404, detail=f"Collection '{collectionId}' not found."
    )


def CollectionsParams(
    request: Request,
    bbox_filter: Annotated[Optional[List[float]], Depends(bbox_query)],
    datetime_filter: Annotated[Optional[List[str]], Depends(datetime_query)],
    limit: Annotated[
        Optional[int],
        Query(
            ge=0,
            le=1000,
            description="Limits the number of collection in the response.",
        ),
    ] = None,
    offset: Annotated[
        Optional[int],
        Query(
            ge=0,
            description="Starts the response at an offset.",
        ),
    ] = None,
) -> CollectionList:
    """Return Collections Catalog."""
    limit = limit or 0
    offset = offset or 0

    # NOTE:
    # For now, we are using a static catalog which is loaded at startup but
    # we could use an external STAC API to get the list of collection / CMR collection_concept_id
    catalog: Optional[Catalog] = getattr(request.app.state, "collection_catalog", None)
    if not catalog:
        raise MissingCollectionCatalog("Could not find collections catalog.")

    collections_list = list(catalog["collections"].values())

    # bbox filter
    if bbox_filter is not None:
        collections_list = [
            collection
            for collection in collections_list
            if collection.bounds is not None
            and s_intersects(bbox_filter, collection.bounds)
        ]

    # datetime filter
    if datetime_filter is not None:
        collections_list = [
            collection
            for collection in collections_list
            if collection.dt_bounds is not None
            and t_intersects(datetime_filter, collection.dt_bounds)
        ]

    matched = len(collections_list)

    if limit:
        collections_list = collections_list[offset : offset + limit]
    else:
        collections_list = collections_list[offset:]

    returned = len(collections_list)

    return CollectionList(
        collections=collections_list,
        matched=matched,
        next=offset + returned if matched - returned > offset else None,
        prev=max(offset - limit, 0) if offset else None,
    )
