"""titiler.cmr utilities.

Code from titiler.pgstac, MIT License.

"""

import time
from datetime import datetime
from typing import Any, List, Optional, Sequence, Tuple, Type, Union

from isodate import parse_datetime as _parse_datetime

from titiler.cmr.errors import InvalidDatetime


def retry(
    tries: int,
    exceptions: Union[Type[Exception], Sequence[Type[Exception]]] = Exception,
    delay: float = 0.0,
):
    """Retry Decorator"""

    def _decorator(func: Any):
        def _newfn(*args: Any, **kwargs: Any):
            attempt = 0
            while attempt < tries:
                try:
                    return func(*args, **kwargs)

                except exceptions:  # type: ignore
                    attempt += 1
                    time.sleep(delay)

            return func(*args, **kwargs)

        return _newfn

    return _decorator


def _parse_date(date: str) -> datetime:
    try:
        return _parse_datetime(date)
    except Exception as e:
        raise InvalidDatetime(f"Invalid datetime {date}") from e


def parse_datetime(
    datetime_str: str,
) -> Tuple[Optional[datetime], Optional[datetime], Optional[datetime]]:
    """Parse datetime string input into datetime objects"""
    datetime_, start, end = None, None, None
    dt = datetime_str.split("/")
    if len(dt) == 1:
        datetime_ = _parse_date(dt[0])

    elif len(dt) == 2:
        dates: List[Optional[str]] = [None, None]
        dates[0] = dt[0] if dt[0] not in ["..", ""] else None
        dates[1] = dt[1] if dt[1] not in ["..", ""] else None

        if dates[0]:
            start = _parse_date(dates[0])

        if dates[1]:
            end = _parse_date(dates[1])

    else:
        raise InvalidDatetime("Invalid datetime: {datetime}")

    return datetime_, start, end
