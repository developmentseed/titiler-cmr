"""Test timeseries module"""

from collections.abc import Callable
from datetime import datetime
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi import HTTPException
from freezegun import freeze_time
from httpx import Client
from starlette.requests import Request

from titiler.cmr.models import GranuleSearch
from titiler.cmr.query import CMR_GRANULE_SEARCH_API
from titiler.cmr.timeseries import (
    TemporalMode,
    TimeseriesParams,
    build_request_urls,
    generate_datetime_ranges,
    timeseries_cmr_query,
)


def test_generate_datetime_ranges():
    """Test datetime ranges"""
    start = datetime(2023, 1, 1)
    end = datetime(2023, 12, 31, 23, 59, 59)

    # Test yearly ranges
    yearly_ranges = generate_datetime_ranges(start, end, "P1Y")
    assert len(yearly_ranges) == 1
    assert yearly_ranges[0] == (
        datetime(2023, 1, 1),
        datetime(2023, 12, 31, 23, 59, 59),
    )

    # Test monthly ranges
    monthly_ranges = generate_datetime_ranges(start, end, "P1M")
    assert len(monthly_ranges) == 12
    assert monthly_ranges[0] == (
        datetime(2023, 1, 1),
        datetime(2023, 1, 31, 23, 59, 59),
    )
    assert monthly_ranges[-1] == (
        datetime(2023, 12, 1),
        datetime(2023, 12, 31, 23, 59, 59),
    )

    # Test daily ranges
    daily_ranges = generate_datetime_ranges(
        start, datetime(2023, 1, 5, 23, 59, 59), "P1D"
    )
    assert len(daily_ranges) == 5
    assert daily_ranges[0] == (datetime(2023, 1, 1), datetime(2023, 1, 1, 23, 59, 59))
    assert daily_ranges[-1] == (datetime(2023, 1, 5), datetime(2023, 1, 5, 23, 59, 59))

    # Test hourly ranges
    hourly_ranges = generate_datetime_ranges(start, datetime(2023, 1, 1, 5), "PT1H")
    assert len(hourly_ranges) == 5
    assert hourly_ranges[0] == (
        datetime(2023, 1, 1, 0),
        datetime(2023, 1, 1, 0, 59, 59),
    )
    assert hourly_ranges[-1] == (datetime(2023, 1, 1, 4), datetime(2023, 1, 1, 5))

    # Test when start and end are the same
    same_time_ranges = generate_datetime_ranges(start, start, "P1D")
    assert len(same_time_ranges) == 1
    assert same_time_ranges[0] == (start, start)

    # Test when step is larger than the range
    large_step_ranges = generate_datetime_ranges(start, datetime(2023, 1, 2), "P1Y")
    assert len(large_step_ranges) == 1
    assert large_step_ranges[0] == (start, datetime(2023, 1, 2))

    # Test point-in-time mode
    exact_end_datetime = datetime(2023, 5, 1)
    exact_ranges = generate_datetime_ranges(
        start, exact_end_datetime, "P1M", temporal_mode=TemporalMode.point
    )
    assert len(exact_ranges) == 5
    assert exact_ranges[-1] == (exact_end_datetime,)

    exact_end_datetime = datetime(2023, 10, 25)
    exact_ranges = generate_datetime_ranges(
        start, exact_end_datetime, "P1W", temporal_mode=TemporalMode.point
    )
    assert len(exact_ranges) == 43


def test_generate_datetime_ranges_edge_cases():
    """Test datetime edge cases"""
    start = datetime(2023, 1, 1)
    end = datetime(2023, 12, 31, 23, 59, 59)

    # Test with a very small step
    small_step_ranges = generate_datetime_ranges(
        start, datetime(2023, 1, 1, 0, 0, 1, 999999), "PT1S"
    )
    assert len(small_step_ranges) == 2
    assert small_step_ranges[0] == (
        datetime(2023, 1, 1, 0, 0, 0),
        datetime(2023, 1, 1, 0, 0, 0, 999999),
    )
    assert small_step_ranges[1] == (
        datetime(2023, 1, 1, 0, 0, 1),
        datetime(2023, 1, 1, 0, 0, 1, 999999),
    )

    # Test with a step that doesn't divide evenly into the range
    uneven_ranges = generate_datetime_ranges(start, datetime(2023, 1, 11), "P3D")
    assert len(uneven_ranges) == 4
    assert uneven_ranges[-1] == (
        datetime(2023, 1, 10),
        datetime(2023, 1, 11),
    )

    # Test with a very large step
    large_step_ranges = generate_datetime_ranges(start, end, "P10Y")
    assert len(large_step_ranges) == 1
    assert large_step_ranges[0] == (start, end)


def test_generate_datetime_ranges_small_timesteps():
    """Test small datetime steps"""
    start = datetime(2023, 1, 1, 0, 0, 0)

    # Test with 1-second step
    one_second_ranges = generate_datetime_ranges(
        start, datetime(2023, 1, 1, 0, 0, 1, 999999), "PT1S"
    )
    assert len(one_second_ranges) == 2
    assert one_second_ranges[0] == (
        datetime(2023, 1, 1, 0, 0, 0),
        datetime(2023, 1, 1, 0, 0, 0, 999999),  # 999000 microseconds = 999 milliseconds
    )
    assert one_second_ranges[1] == (
        datetime(2023, 1, 1, 0, 0, 1),
        datetime(2023, 1, 1, 0, 0, 1, 999999),
    )

    # Test with 500-millisecond step
    half_second_ranges = generate_datetime_ranges(
        start, datetime(2023, 1, 1, 0, 0, 1), "PT0.5S"
    )
    assert len(half_second_ranges) == 2
    assert half_second_ranges[0] == (
        datetime(2023, 1, 1, 0, 0, 0),
        datetime(2023, 1, 1, 0, 0, 0, 499999),  # 499000 microseconds = 499 milliseconds
    )
    assert half_second_ranges[1] == (
        datetime(2023, 1, 1, 0, 0, 0, 500000),  # 500000 microseconds = 500 milliseconds
        datetime(2023, 1, 1, 0, 0, 1),
    )

    # Test with larger step to ensure it still subtracts 1 second
    larger_step_ranges = generate_datetime_ranges(
        start, datetime(2023, 1, 1, 0, 1, 0), "PT30S"
    )
    assert len(larger_step_ranges) == 2
    assert larger_step_ranges[0] == (
        datetime(2023, 1, 1, 0, 0, 0),
        datetime(2023, 1, 1, 0, 0, 29),  # Still subtracting 1 second
    )


def test_timeseries_query(
    xarray_query_params: Callable[..., dict[str, str]],
    arctic_bounds: tuple[float, float, float, float],
) -> None:
    """Test timeseries_query (step-based, no CMR calls needed)"""
    start_temporal, end_temporal = xarray_query_params()["temporal"].split("/")
    mock_request = MagicMock()

    query = timeseries_cmr_query(
        request=mock_request,
        granule_search=GranuleSearch(
            collection_concept_id=xarray_query_params()["collection_concept_id"],
            temporal=xarray_query_params()["temporal"],
        ),
        timeseries_params=TimeseriesParams(
            temporal=xarray_query_params()["temporal"],
            step="P1D",
        ),
    )
    assert len(query) == 1

    query = timeseries_cmr_query(
        request=mock_request,
        granule_search=GranuleSearch(
            collection_concept_id=xarray_query_params()["collection_concept_id"],
            temporal=xarray_query_params()["temporal"],
        ),
        timeseries_params=TimeseriesParams(
            temporal=xarray_query_params()["temporal"],
            step="PT1H",
        ),
    )
    assert len(query) == 24

    query = timeseries_cmr_query(
        request=mock_request,
        granule_search=GranuleSearch(
            collection_concept_id=xarray_query_params()["collection_concept_id"],
        ),
        timeseries_params=TimeseriesParams(
            temporal=f"{start_temporal}/2024-10-31T23:59:59Z",
            step="P1W",
        ),
    )
    assert len(query) == 3


@pytest.mark.vcr
def test_timeseries_query_no_step(
    xarray_query_params: Callable[..., dict[str, str]],
    arctic_bounds: tuple[float, float, float, float],
) -> None:
    """Test timeseries_query when no step is given (CMR granule search path)"""
    mock_request = MagicMock()
    mock_request.app.state.client = Client(base_url=CMR_GRANULE_SEARCH_API)

    # no step parameter will force a CMR query to get unique datetimes from available granules
    query = timeseries_cmr_query(
        request=mock_request,
        granule_search=GranuleSearch(
            collection_concept_id=xarray_query_params()["collection_concept_id"],
        ),
        timeseries_params=TimeseriesParams(
            temporal=xarray_query_params()["temporal"],
        ),
    )
    assert len(query) == 1

    # query CMR to get the actual timesteps from a geographically limited collection
    geographically_limited_concept_id = "C2623694361-GES_DISC"
    query = timeseries_cmr_query(
        request=mock_request,
        granule_search=GranuleSearch(
            collection_concept_id=geographically_limited_concept_id,
        ),
        timeseries_params=TimeseriesParams(
            temporal=xarray_query_params()["temporal"],
        ),
        minx=-100,
        miny=30,
        maxx=-90,
        maxy=40,
    )
    assert len(query) == 8

    # run a bbox query that returns no granules
    query = timeseries_cmr_query(
        request=mock_request,
        granule_search=GranuleSearch(
            collection_concept_id=geographically_limited_concept_id,
        ),
        timeseries_params=TimeseriesParams(
            temporal=xarray_query_params()["temporal"],
        ),
        minx=1,
        miny=1,
        maxx=1,
        maxy=1,
    )
    assert len(query) == 0


@freeze_time("2024-10-01T00:00:00Z")
def test_timeseries_query_unbounded_intervals(
    xarray_query_params: Callable[..., dict[str, str]],
    arctic_bounds: tuple[float, float, float, float],
) -> None:
    """Test unbounded intervals"""
    mock_request = MagicMock()

    # expect an error if an interval is provided with an unbounded start datetime
    with pytest.raises(HTTPException):
        timeseries_cmr_query(
            request=mock_request,
            granule_search=GranuleSearch(
                collection_concept_id=xarray_query_params()["collection_concept_id"],
            ),
            timeseries_params=TimeseriesParams(
                temporal="../2024-01-01T00:00:00Z",
                step="P1W",
            ),
        )

    unbounded_query = timeseries_cmr_query(
        request=mock_request,
        granule_search=GranuleSearch(
            collection_concept_id=xarray_query_params()["collection_concept_id"],
        ),
        timeseries_params=TimeseriesParams(
            temporal="2024-01-01T00:00:00Z/..",
            step="P1W",
        ),
    )

    assert len(unbounded_query) == 40


def test_timeseries_mixed_datetime(
    xarray_query_params: Callable[..., dict[str, str]],
    arctic_bounds: tuple[float, float, float, float],
) -> None:
    """Test comma-separated mixed points and intervals"""
    mixed_query = timeseries_cmr_query(
        request=MagicMock(),
        granule_search=GranuleSearch(
            collection_concept_id=xarray_query_params()["collection_concept_id"],
        ),
        timeseries_params=TimeseriesParams(
            temporal="2023-01-01T00:00:00Z,2024-01-01T00:00:00Z/2024-01-05T00:00:00Z",
            step="P1D",
            temporal_mode=TemporalMode.point,
        ),
    )
    assert len(mixed_query) == 6


def test_build_request_urls_no_duplicate_params() -> None:
    """GranuleSearch fields must not appear twice in sub-request URLs.

    When a caller passes e.g. ?collection_concept_id=C123&bounding_box=...
    those values end up in both the original request's query string AND in the
    GranuleSearch model objects.  build_request_urls must strip them from the
    pass-through params so they are not duplicated.
    """
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/rasterio/timeseries/statistics",
        "query_string": b"collection_concept_id=C123&temporal=2024-01-01T00:00:00Z/2024-06-01T00:00:00Z&step=P1M&max_size=512",
        "headers": [],
    }
    request = Request(scope)

    param_list = [
        GranuleSearch(
            collection_concept_id="C123",
            temporal="2024-01-01T00:00:00Z/2024-01-31T23:59:59Z",
        ),
        GranuleSearch(
            collection_concept_id="C123",
            temporal="2024-02-01T00:00:00Z/2024-02-29T23:59:59Z",
        ),
    ]

    urls = build_request_urls(
        base_url="http://testserver/rasterio/statistics",
        request=request,
        param_list=param_list,
    )

    for url in urls:
        parsed = parse_qs(urlparse(url).query)
        # Each GranuleSearch field must appear exactly once
        assert len(parsed.get("collection_concept_id", [])) == 1
        assert len(parsed.get("temporal", [])) == 1
        # Pass-through params unrelated to GranuleSearch must be preserved
        assert parsed.get("max_size") == ["512"]
        # Timeseries-only params must be stripped entirely
        assert "step" not in parsed

    # List fields (sort_key, attribute) must expand to repeated key=value pairs,
    # not be stringified as Python list literals like "['key1', 'key2']".
    scope_with_lists = {
        "type": "http",
        "method": "GET",
        "path": "/rasterio/timeseries/statistics",
        "query_string": b"collection_concept_id=C123&temporal=2024-01-01T00:00:00Z/2024-06-01T00:00:00Z&step=P1M",
        "headers": [],
    }
    request_with_lists = Request(scope_with_lists)
    param_list_with_sort = [
        GranuleSearch(
            collection_concept_id="C123",
            temporal="2024-01-01T00:00:00Z/2024-01-31T23:59:59Z",
            sort_key=["-start_date", "granule_ur"],
        ),
    ]
    urls_with_sort = build_request_urls(
        base_url="http://testserver/rasterio/statistics",
        request=request_with_lists,
        param_list=param_list_with_sort,
    )
    parsed_sort = parse_qs(urlparse(urls_with_sort[0]).query)
    assert parsed_sort["sort_key"] == ["-start_date", "granule_ur"]


def test_build_request_urls_multi_value_pass_through() -> None:
    """Repeated query params that are *not* GranuleSearch fields must survive unchanged."""
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/rasterio/timeseries/bbox/-100,40,-90,50.png",
        "query_string": b"collection_concept_id=C123&assets=B04&assets=B05&max_size=512&step=P1D",
        "headers": [],
    }
    request = Request(scope)
    param_list = [
        GranuleSearch(
            collection_concept_id="C123",
            temporal="2024-01-01T00:00:00Z/2024-01-01T23:59:59Z",
        ),
    ]

    urls = build_request_urls(
        base_url="http://testserver/rasterio/bbox/-100,40,-90,50.png",
        request=request,
        param_list=param_list,
    )
    parsed = parse_qs(urlparse(urls[0]).query)

    assert parsed["assets"] == ["B04", "B05"]
    assert parsed["max_size"] == ["512"]
    assert "step" not in parsed
    assert parsed["collection_concept_id"] == ["C123"]
    assert parsed["temporal"] == ["2024-01-01T00:00:00Z/2024-01-01T23:59:59Z"]
