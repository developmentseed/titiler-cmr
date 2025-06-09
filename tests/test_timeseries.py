"""Test timeseries module"""

from datetime import datetime
from typing import Dict, Tuple

import pytest
from fastapi import HTTPException
from freezegun import freeze_time

from titiler.cmr.timeseries import (
    TemporalMode,
    TimeseriesParams,
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


@pytest.mark.vcr
def test_timeseries_query(
    xarray_query_params: Dict[str, str],
    arctic_bounds: Tuple[float, float, float, float],
) -> None:
    """Test timeseries_query"""
    start_datetime, end_datetime = xarray_query_params()["datetime"].split("/")
    query = timeseries_cmr_query(
        concept_id=xarray_query_params()["concept_id"],
        timeseries_params=TimeseriesParams(
            datetime=xarray_query_params()["datetime"],
            step="P1D",
        ),
    )
    assert len(query) == 1

    query = timeseries_cmr_query(
        concept_id=xarray_query_params()["concept_id"],
        timeseries_params=TimeseriesParams(
            datetime=xarray_query_params()["datetime"],
            step="PT1H",
        ),
    )
    assert len(query) == 24

    query = timeseries_cmr_query(
        concept_id=xarray_query_params()["concept_id"],
        timeseries_params=TimeseriesParams(
            datetime=f"{start_datetime}/2024-10-31T23:59:59Z",
            step="P1W",
        ),
    )
    assert len(query) == 3

    # no step parameter will force a CMR query to get unique
    # datetimes from available granules
    query = timeseries_cmr_query(
        concept_id=xarray_query_params()["concept_id"],
        timeseries_params=TimeseriesParams(
            datetime=xarray_query_params()["datetime"],
        ),
    )
    assert len(query) == 1

    # query CMR to get the actual timesteps from a collection
    geographically_limited_concept_id = "C2623694361-GES_DISC"
    query = timeseries_cmr_query(
        concept_id=geographically_limited_concept_id,
        timeseries_params=TimeseriesParams(
            datetime=xarray_query_params()["datetime"],
        ),
        minx=-100,
        miny=30,
        maxx=-90,
        maxy=40,
    )
    assert len(query) == 8

    # run a bbox query that returns no granules
    query = timeseries_cmr_query(
        concept_id=geographically_limited_concept_id,
        timeseries_params=TimeseriesParams(
            datetime=xarray_query_params()["datetime"],
        ),
        minx=1,
        miny=1,
        maxx=1,
        maxy=1,
    )
    assert len(query) == 0


@freeze_time("2024-10-01T00:00:00Z")
def test_timeseries_query_unbounded_intervals(
    xarray_query_params: Dict[str, str],
    arctic_bounds: Tuple[float, float, float, float],
) -> None:
    """Test unbounded intervals"""
    # expect an error if an interval is provided with an unbounded start datetime
    with pytest.raises(HTTPException):
        timeseries_cmr_query(
            concept_id=xarray_query_params()["concept_id"],
            timeseries_params=TimeseriesParams(
                datetime="../2024-01-01T00:00:00Z",
                step="P1W",
            ),
        )

    unbounded_query = timeseries_cmr_query(
        concept_id=xarray_query_params()["concept_id"],
        timeseries_params=TimeseriesParams(
            datetime="2024-01-01T00:00:00Z/..",
            step="P1W",
        ),
    )

    assert len(unbounded_query) == 40


def test_timeseries_mixed_datetime(
    xarray_query_params: Dict[str, str],
    arctic_bounds: Tuple[float, float, float, float],
) -> None:
    """Test comma-separated mixed points and intervals"""
    mixed_query = timeseries_cmr_query(
        concept_id=xarray_query_params()["concept_id"],
        timeseries_params=TimeseriesParams(
            datetime="2023-01-01T00:00:00Z,2024-01-01T00:00:00Z/2024-01-05T00:00:00Z",
            step="P1D",
            temporal_mode="point",
        ),
    )
    assert len(mixed_query) == 6
