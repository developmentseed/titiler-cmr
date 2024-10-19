"""Test timeseries module"""

from datetime import datetime

import pytest
from dateutil.relativedelta import relativedelta

# Import your functions here
from titiler.cmr.timeseries import generate_datetime_ranges, parse_duration


def test_parse_duration():
    """Test durations"""
    assert parse_duration("P1Y") == relativedelta(years=1)
    assert parse_duration("P2M") == relativedelta(months=2)
    assert parse_duration("P3D") == relativedelta(days=3)
    assert parse_duration("PT4H") == relativedelta(hours=4)
    assert parse_duration("PT5M") == relativedelta(minutes=5)
    assert parse_duration("PT6S") == relativedelta(seconds=6)
    assert parse_duration("PT1S") == relativedelta(seconds=1)
    assert parse_duration("P1Y2M3DT4H5M6S") == relativedelta(
        years=1, months=2, days=3, hours=4, minutes=5, seconds=6
    )

    with pytest.raises(ValueError):
        parse_duration("invalid")


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
