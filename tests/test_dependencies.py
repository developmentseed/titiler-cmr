"""test titiler-pgstac dependencies."""

from datetime import datetime, timezone


from titiler.cmr import dependencies
from titiler.cmr.dependencies import (
    CMRAssetsExprParams,
    CMRXarrayExprParams,
    InterpolatedXarrayParams,
)
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


def test_cmr_assets_expr_params_three_assets():
    """Legacy expression with three assets: all detected and mapped in appearance order."""
    params = CMRAssetsExprParams(expression="(NIR-RED)/(NIR+RED+BLUE)")
    assert list(params.assets) == ["NIR", "RED", "BLUE"]
    assert params.expression == "(b1-b2)/(b1+b2+b3)"


def test_cmr_assets_expr_params_substring_asset_names():
    """Asset names that are substrings of each other are distinguished by word boundaries."""
    params = CMRAssetsExprParams(assets=["B4", "B4_mask"], expression="B4 * B4_mask")
    assert params.expression == "b1 * b2"


def test_cmr_assets_expr_params_math_functions():
    """Math function names (sqrt, log) are not treated as asset names."""
    params = CMRAssetsExprParams(expression="sqrt(NIR)/log(RED)")
    assert list(params.assets) == ["NIR", "RED"]
    assert params.expression == "sqrt(b1)/log(b2)"


def test_cmr_assets_expr_params_extra_assets():
    """Assets list with more entries than referenced in expression: order preserved, extras ignored."""
    params = CMRAssetsExprParams(assets=["B03", "B04", "B05"], expression="B04-B05")
    assert list(params.assets) == ["B03", "B04", "B05"]
    assert params.expression == "b2-b3"


def test_cmr_assets_expr_params_b_prefix_asset_names():
    """Asset names starting with 'b' but not new-style (e.g. blue, band1) are treated as legacy."""
    params = CMRAssetsExprParams(expression="(blue-red)/(blue+red)")
    assert list(params.assets) == ["blue", "red"]
    assert params.expression == "(b1-b2)/(b1+b2)"


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


def test_cmr_assets_expr_params_legacy_no_assets():
    """Legacy expression with no assets: auto-detect assets and translate expression."""
    params = CMRAssetsExprParams(expression="(B04-B05)/(B05+B04)")
    assert list(params.assets) == ["B04", "B05"]
    assert params.expression == "(b1-b2)/(b2+b1)"


def test_cmr_assets_expr_params_legacy_with_assets():
    """Legacy expression with assets provided: use assets order for mapping."""
    params = CMRAssetsExprParams(
        assets=["B05", "B04"], expression="(B04-B05)/(B04+B05)"
    )
    assert list(params.assets) == ["B05", "B04"]
    assert params.expression == "(b2-b1)/(b2+b1)"


def test_cmr_assets_expr_params_new_style_passthrough():
    """New-style expression (b1, b2, ...) passes through unchanged."""
    params = CMRAssetsExprParams(assets=["B04", "B05"], expression="(b1-b2)/(b1+b2)")
    assert list(params.assets) == ["B04", "B05"]
    assert params.expression == "(b1-b2)/(b1+b2)"


def test_cmr_assets_expr_params_no_expression():
    """No expression: assets unchanged, no error."""
    params = CMRAssetsExprParams(assets=["B04"])
    assert list(params.assets) == ["B04"]
    assert params.expression is None


def test_cmr_xarray_expr_params_legacy_variable_names():
    """Legacy variable names are translated to positional bN format."""
    params = CMRXarrayExprParams(
        variables=["temperature", "pressure"], expression="temperature/pressure"
    )
    assert params.expression == "b1/b2"


def test_cmr_xarray_expr_params_legacy_partial_match():
    """Legacy NDVI-style expression with partial variable subset."""
    params = CMRXarrayExprParams(
        variables=["nir", "red", "green"], expression="(nir-red)/(nir+red)"
    )
    assert params.expression == "(b1-b2)/(b1+b2)"


def test_cmr_xarray_expr_params_new_style_passthrough():
    """New-style bN expressions pass through unchanged."""
    params = CMRXarrayExprParams(variables=["nir", "red"], expression="(b1-b2)/(b1+b2)")
    assert params.expression == "(b1-b2)/(b1+b2)"


def test_cmr_xarray_expr_params_with_math_functions():
    """Math function names are not treated as variable names."""
    params = CMRXarrayExprParams(
        variables=["nir", "red"], expression="log10(nir)/sqrt(red)"
    )
    assert params.expression == "log10(b1)/sqrt(b2)"


def test_cmr_xarray_expr_params_no_expression():
    """No expression: no error, variables unchanged."""
    params = CMRXarrayExprParams(variables=["nir"], expression=None)
    assert params.expression is None


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
