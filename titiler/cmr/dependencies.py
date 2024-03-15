"""titiler-cmr dependencies."""

import datetime as python_datetime
from typing import Any, Dict, List, Literal, Optional, get_args

from ciso8601 import parse_rfc3339
from fastapi import Query
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


def _parse_date(date: str) -> python_datetime.datetime:
    try:
        return parse_rfc3339(date[0])
    except Exception as e:
        raise InvalidDatetime(f"Invalid datetime {date[0]}") from e


def cmr_query(
    concept_id: Annotated[
        str,
        Query(
            description="A CMR concept id, in the format <concept-type-prefix> <unique-number> '-' <provider-id>"
        ),
    ],
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
        dt = datetime.split("/")
        if len(dt) == 1:
            start_datetime = _parse_date(dt[0])
            end_datetime = start_datetime + python_datetime.timedelta(days=1)
            query["temporal"] = (
                start_datetime.strftime("%Y-%m-%d"),
                end_datetime.strftime("%Y-%m-%d"),
            )

        elif len(dt) == 2:
            dates: List[Optional[str]] = [None, None]
            dates[0] = dt[0] if dt[0] not in ["..", ""] else None
            dates[1] = dt[1] if dt[1] not in ["..", ""] else None

            # TODO: once https://github.com/nsidc/earthaccess/pull/451 is publish
            # we can move to Datetime object instead of String
            start: Optional[str] = None
            end: Optional[str] = None

            if dates[0]:
                start = _parse_date(dates[0]).strftime("%Y-%m-%d")

            if dates[1]:
                end = _parse_date(dates[1]).strftime("%Y-%m-%d")

            query["temporal"] = (start, end)
        else:
            raise InvalidDatetime("Invalid datetime: {datetime}")

    return query
