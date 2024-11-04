"""titiler-cmr dependencies."""

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Union, get_args

from fastapi import Query
from rio_tiler.types import RIOResampling, WarpResampling
from starlette.requests import Request
from typing_extensions import Annotated

from titiler.cmr.enums import MediaType
from titiler.cmr.utils import parse_datetime
from titiler.core.dependencies import DefaultDependency

ResponseType = Literal["json", "html"]


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


ConceptID = Annotated[
    str,
    Query(
        description="A CMR concept id, in the format <concept-type-prefix> <unique-number> '-' <provider-id>"
    ),
]


def cmr_query(
    concept_id: ConceptID,
    datetime: Annotated[
        Optional[str],
        Query(
            description="Either a date-time or an interval. Date and time expressions adhere to rfc3339 ('2020-06-01T09:00:00Z') format. Intervals may be bounded or half-bounded (double-dots at start or end).",
            openapi_examples={
                "A date-time": {"value": "2018-02-12T09:00:00Z"},
                "A bounded interval": {
                    "value": "2018-02-12T09:00:00Z/2018-03-18T09:00:00Z"
                },
                "Half-bounded intervals (start)": {"value": "2018-02-12T09:00:00Z/.."},
                "Half-bounded intervals (end)": {"value": "../2018-03-18T09:00:00Z"},
            },
        ),
    ] = None,
) -> Dict:
    """CMR Query options."""
    query: Dict[str, Any] = {"concept_id": concept_id}

    if datetime:
        datetime_, start, end = parse_datetime(datetime)
        query["temporal"] = datetime_ if datetime_ else (start, end)

    return query


@dataclass
class RasterioParams(DefaultDependency):
    """Rasterio backend parameters"""

    indexes: Annotated[
        Optional[List[int]],
        Query(
            title="Band indexes",
            alias="bidx",
            description="Dataset band indexes",
        ),
    ] = None
    expression: Annotated[
        Optional[str],
        Query(
            title="Band Math expression",
            description="rio-tiler's band math expression",
        ),
    ] = None
    bands: Annotated[
        Optional[List[str]],
        Query(
            title="Band names",
            description="Band names.",
        ),
    ] = None
    bands_regex: Annotated[
        Optional[str],
        Query(
            title="Regex expression to parse dataset links",
            description="Regex expression to parse dataset links.",
        ),
    ] = None
    unscale: Annotated[
        Optional[bool],
        Query(
            title="Apply internal Scale/Offset",
            description="Apply internal Scale/Offset. Defaults to `False`.",
        ),
    ] = None
    resampling_method: Annotated[
        Optional[RIOResampling],
        Query(
            alias="resampling",
            description="RasterIO resampling algorithm. Defaults to `nearest`.",
        ),
    ] = None


@dataclass
class ZarrParams(DefaultDependency):
    """Zarr backend parameters"""

    variable: Annotated[
        Optional[str],
        Query(description="Xarray Variable"),
    ] = None
    drop_dim: Annotated[
        Optional[str],
        Query(description="Dimension to drop"),
    ] = None
    time_slice: Annotated[
        Optional[str], Query(description="Slice of time to read (if available)")
    ] = None
    decode_times: Annotated[
        Optional[bool],
        Query(
            title="decode_times",
            description="Whether to decode times",
        ),
    ] = None


@dataclass
class ReaderParams(DefaultDependency):
    """Reader parameters"""

    backend: Annotated[
        Literal["rasterio", "xarray"],
        Query(description="Backend to read the CMR dataset"),
    ] = "rasterio"
    nodata: Annotated[
        Optional[Union[str, int, float]],
        Query(
            title="Nodata value",
            description="Overwrite internal Nodata value",
        ),
    ] = None
    reproject_method: Annotated[
        Optional[WarpResampling],
        Query(
            alias="reproject",
            description="WarpKernel resampling algorithm (only used when doing re-projection). Defaults to `nearest`.",
        ),
    ] = None
