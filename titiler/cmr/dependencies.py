"""titiler-cmr dependencies."""

from datetime import datetime
from typing import Dict, List, Literal, Optional, get_args

from fastapi import HTTPException, Query
from starlette.requests import Request
from typing_extensions import Annotated

from titiler.cmr.enums import MediaType
from titiler.cmr.errors import InvalidDatetime

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


def cmr_query(
    temporal: Annotated[
        Optional[str],
        Query(
            description="Either a date-time or an interval. Date and time expressions adhere to 'YYYY-MM-DD' format. Intervals may be bounded or half-bounded (double-dots at start or end).",
            openapi_examples={
                "A date-time": {"value": "2018-02-12"},
                "A bounded interval": {"value": "2018-02-12/2018-03-18"},
                "Half-bounded intervals (start)": {"value": "2018-02-12/.."},
                "Half-bounded intervals (end)": {"value": "../2018-03-18"},
            },
        ),
    ] = None,
) -> Dict:
    """CMR Query options."""
    query = {}
    if temporal:
        dt = temporal.split("/")
        if len(dt) > 2:
            raise HTTPException(status_code=422, detail="Invalid temporal: {temporal}")

        start: Optional[str]
        end: Optional[str]

        if len(dt) == 1:
            start = end = dt[0]

        else:
            start = dt[0] if dt[0] not in ["..", ""] else None
            end = dt[1] if dt[1] not in ["..", ""] else None

        if start:
            try:
                datetime.strptime(
                    start, "%Y-%m-%d"
                ), f"Start datetime {start} not in form of 'YYYY-MM-DD'"
            except ValueError as e:
                raise InvalidDatetime(
                    f"Start datetime {start} not in form of 'YYYY-MM-DD'"
                ) from e

        if end:
            try:
                datetime.strptime(
                    end, "%Y-%m-%d"
                ), f"Start datetime {start} not in form of 'YYYY-MM-DD'"
            except ValueError as e:
                raise InvalidDatetime(
                    f"End datetime {end} not in form of 'YYYY-MM-DD'"
                ) from e

        query["temporal"] = (start, end)

    return query
