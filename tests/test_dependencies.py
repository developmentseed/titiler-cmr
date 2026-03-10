"""test titiler-pgstac dependencies."""

from datetime import datetime, timezone


from titiler.cmr import dependencies
from titiler.cmr.dependencies import InterpolatedXarrayParams
from titiler.cmr.models import GranuleSearch


def test_granule_search_temporal_singleton_normalization():
    """Singleton datetimes are converted to closed intervals to avoid CMR open-range behavior."""
    dt = "2024-01-01T00:00:00Z"
    search = GranuleSearch(temporal=dt)
    assert search.temporal == f"{dt}/{dt}"


def test_granule_search_temporal_interval_unchanged():
    """Interval and half-open temporal values pass through unmodified."""
    closed = "2024-01-01T00:00:00Z/2024-02-01T00:00:00Z"
    assert GranuleSearch(temporal=closed).temporal == closed

    half_open = "2024-01-01T00:00:00Z/.."
    assert GranuleSearch(temporal=half_open).temporal == half_open


def test_interpolated_xarray_params_single_datetime():
    """Test InterpolatedXarrayParams with single datetime interpolation."""
    xarray_params = InterpolatedXarrayParams(
        variables=["temperature"], sel=["time={datetime}", "lev=1000"]
    )

    single_datetime = datetime(2025, 9, 23, 0, 0, 0, tzinfo=timezone.utc)
    granule_search = GranuleSearch(
        collection_concept_id="test_concept",
        temporal=single_datetime.isoformat(),
    )

    result = dependencies.interpolated_xarray_ds_params(xarray_params, granule_search)

    assert result.sel == [f"time={single_datetime.isoformat()}", "lev=1000"]
    assert result.variables == ["temperature"]


def test_interpolated_xarray_params_datetime_range():
    """Test InterpolatedXarrayParams with datetime range (uses start datetime)."""
    xarray_params = InterpolatedXarrayParams(
        variables=["temperature"], sel=["time={datetime}"]
    )

    start_datetime = datetime(2025, 9, 23, 0, 0, 0, tzinfo=timezone.utc)
    end_datetime = datetime(2025, 9, 24, 0, 0, 0, tzinfo=timezone.utc)
    granule_search = GranuleSearch(
        collection_concept_id="test_concept",
        temporal=f"{start_datetime.isoformat()}/{end_datetime.isoformat()}",
    )

    result = dependencies.interpolated_xarray_ds_params(xarray_params, granule_search)

    assert result.sel == [f"time={start_datetime.isoformat()}"]


def test_interpolated_xarray_params_no_datetime_template():
    """Test InterpolatedXarrayParams when sel doesn't contain datetime template."""
    xarray_params = InterpolatedXarrayParams(
        variables=["temperature"],
        sel=["time=2025-01-01T00:00:00Z", "lev=1000"],
    )

    single_datetime = datetime(2025, 9, 23, 0, 0, 0, tzinfo=timezone.utc)
    granule_search = GranuleSearch(
        collection_concept_id="test_concept",
        temporal=single_datetime.isoformat(),
    )

    result = dependencies.interpolated_xarray_ds_params(xarray_params, granule_search)

    assert result.sel == ["time=2025-01-01T00:00:00Z", "lev=1000"]


def test_interpolated_xarray_params_no_sel():
    """Test InterpolatedXarrayParams when sel is None or empty."""
    xarray_params = InterpolatedXarrayParams(variables=["temperature"], sel=None)

    single_datetime = datetime(2025, 9, 23, 0, 0, 0, tzinfo=timezone.utc)
    granule_search = GranuleSearch(
        collection_concept_id="test_concept",
        temporal=single_datetime.isoformat(),
    )

    result = dependencies.interpolated_xarray_ds_params(xarray_params, granule_search)

    assert result.sel is None
    assert result.variables == ["temperature"]


def test_interpolated_xarray_params_multiple_templates():
    """Test InterpolatedXarrayParams with multiple datetime templates."""
    xarray_params = InterpolatedXarrayParams(
        variables=["temperature"],
        sel=["time={datetime}", "start_time={datetime}", "lev=1000"],
    )

    single_datetime = datetime(2025, 9, 23, 12, 30, 45, tzinfo=timezone.utc)
    granule_search = GranuleSearch(
        collection_concept_id="test_concept",
        temporal=single_datetime.isoformat(),
    )

    result = dependencies.interpolated_xarray_ds_params(xarray_params, granule_search)

    expected = [
        f"time={single_datetime.isoformat()}",
        f"start_time={single_datetime.isoformat()}",
        "lev=1000",
    ]
    assert result.sel == expected
