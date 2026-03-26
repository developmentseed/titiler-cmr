"""Tests for expression.py"""

import numpy as np
import pytest
import xarray as xr
from rio_tiler.errors import InvalidExpression

from titiler.cmr.expression import apply_expression


def _make_da(arrays: list[np.ndarray]) -> xr.DataArray:
    """Build a DataArray with a 'band' dimension from a list of 2D arrays."""
    data = np.stack(arrays, axis=0)
    return xr.DataArray(data, dims=["band", "y", "x"])


class TestApplyExpressionSecurity:
    """Tests that malicious expressions are rejected before eval runs."""

    def test_unknown_name_raises(self) -> None:
        """Names not in the allowed set (band vars + math functions) should raise."""
        da = _make_da([np.ones((2, 2))])
        with pytest.raises(InvalidExpression):
            apply_expression(da, "eval(b1)")

    def test_unknown_name_nested_raises(self) -> None:
        """Unknown names embedded in a larger expression should still be caught."""
        da = _make_da([np.ones((2, 2))])
        with pytest.raises(InvalidExpression):
            apply_expression(da, "b1 + eval(b1)")

    def test_builtin_open_raises(self) -> None:
        """open() is not in the allowed namespace and should raise."""
        da = _make_da([np.ones((2, 2))])
        with pytest.raises(InvalidExpression):
            apply_expression(da, "open('/etc/passwd')")

    def test_dunder_import_raises(self) -> None:
        """__import__ is not in the allowed namespace and should raise."""
        da = _make_da([np.ones((2, 2))])
        with pytest.raises(InvalidExpression):
            apply_expression(da, "__import__('os')")

    def test_attribute_access_raises(self) -> None:
        """Dotted attribute access is blocked at the AST level.

        This covers both file I/O (np.load, xr.open_dataset) and object
        introspection escapes (b1.__class__.__mro__[...]).
        """
        da = _make_da([np.ones((2, 2))])
        with pytest.raises(InvalidExpression):
            apply_expression(da, "b1.__class__")

    def test_mro_introspection_escape_raises(self) -> None:
        """MRO traversal to reach dangerous subclasses should be blocked."""
        da = _make_da([np.ones((2, 2))])
        with pytest.raises(InvalidExpression):
            apply_expression(da, "b1.__class__.__mro__[-1].__subclasses__()")

    def test_method_call_raises(self) -> None:
        """Calls via attribute access (obj.method()) should be blocked."""
        da = _make_da([np.ones((2, 2))])
        with pytest.raises(InvalidExpression):
            apply_expression(da, "b1.mean()")

    def test_np_file_io_raises(self) -> None:
        """np.load() style calls require attribute access and should be blocked."""
        da = _make_da([np.ones((2, 2))])
        with pytest.raises(InvalidExpression):
            apply_expression(da, "np.load('/etc/passwd')")

    def test_lambda_raises(self) -> None:
        """Lambda expressions are not in the allowed AST node set."""
        da = _make_da([np.ones((2, 2))])
        with pytest.raises(InvalidExpression):
            apply_expression(da, "(lambda: 0)()")

    def test_list_comprehension_raises(self) -> None:
        """Comprehensions are not in the allowed AST node set."""
        da = _make_da([np.ones((2, 2))])
        with pytest.raises(InvalidExpression):
            apply_expression(da, "[x for x in b1]")

    def test_syntax_error_raises(self) -> None:
        """Malformed expressions should raise InvalidExpression."""
        da = _make_da([np.ones((2, 2))])
        with pytest.raises(InvalidExpression):
            apply_expression(da, "b1 +* b2")


class TestApplyExpressionMath:
    """Tests for correct band-math evaluation."""

    def test_single_band_identity(self) -> None:
        """b1 returns the first band unchanged."""
        data = np.array([[1.0, 2.0], [3.0, 4.0]])
        da = _make_da([data])
        result = apply_expression(da, "b1")
        np.testing.assert_array_equal(result.values, data)

    def test_two_band_ratio(self) -> None:
        """b1/b2 computes element-wise ratio."""
        b1 = np.array([[4.0, 9.0]])
        b2 = np.array([[2.0, 3.0]])
        da = _make_da([b1, b2])
        result = apply_expression(da, "b1/b2")
        np.testing.assert_array_almost_equal(result.values, [[2.0, 3.0]])

    def test_ndvi_expression(self) -> None:
        """NDVI = (b2-b1)/(b2+b1) should compute correctly."""
        red = np.array([[0.1, 0.2]])
        nir = np.array([[0.5, 0.8]])
        da = _make_da([red, nir])
        result = apply_expression(da, "(b2-b1)/(b2+b1)")
        expected = (nir - red) / (nir + red)
        np.testing.assert_array_almost_equal(result.values, expected)

    def test_math_function_sqrt(self) -> None:
        """sqrt() from the allowed math namespace should work."""
        data = np.array([[4.0, 9.0]])
        da = _make_da([data])
        result = apply_expression(da, "sqrt(b1)")
        np.testing.assert_array_almost_equal(result.values, [[2.0, 3.0]])

    def test_where_function(self) -> None:
        """where() allows conditional masking and should be available."""
        data = np.array([[1.0, -1.0, 2.0]])
        da = _make_da([data])
        result = apply_expression(da, "where(b1 > 0, b1, 0)")
        np.testing.assert_array_almost_equal(result, [[1.0, 0.0, 2.0]])

    def test_numeric_literal_in_expression(self) -> None:
        """Constant numeric literals should be usable in expressions."""
        data = np.array([[2.0, 4.0]])
        da = _make_da([data])
        result = apply_expression(da, "b1 * 0.5")
        np.testing.assert_array_almost_equal(result.values, [[1.0, 2.0]])

    def test_multi_block_expression(self) -> None:
        """Semicolon-separated blocks should produce a DataArray with multiple bands."""
        b1 = np.array([[1.0, 2.0]])
        b2 = np.array([[3.0, 4.0]])
        da = _make_da([b1, b2])
        result = apply_expression(da, "b1;b2")
        assert result.sizes["band"] == 2

    def test_crs_preserved(self) -> None:
        """CRS on the input DataArray should be carried through to the result."""
        data = np.ones((1, 4, 4))
        da = xr.DataArray(data, dims=["band", "y", "x"])
        da = da.rio.write_crs("EPSG:4326")
        result = apply_expression(da, "b1")
        assert result.rio.crs is not None
        assert result.rio.crs.to_epsg() == 4326
