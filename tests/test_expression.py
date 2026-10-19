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
        """where() masks lazily and returns a DataArray that keeps the CRS."""
        data = np.array([[1.0, -1.0, 2.0]])
        da = _make_da([data]).chunk().rio.write_crs("EPSG:4326")
        result = apply_expression(da, "where(b1 > 0, b1, nan)")

        assert isinstance(result, xr.DataArray)
        assert result.chunks is not None  # still lazy
        assert result.rio.crs.to_epsg() == 4326
        np.testing.assert_array_equal(result.values, [[1.0, np.nan, 2.0]])

    def test_combined_conditions(self) -> None:
        """&, |, and ~ combine conditions, and nan marks pixels to drop."""
        b1 = np.array([[1.0, 2.0, 3.0, 4.0]])
        b2 = np.array([[0.0, 0.5, 0.0, np.nan]])
        da = _make_da([b1, b2])
        result = apply_expression(
            da, "where((b2 < 0.15) & ~isnan(b2) | (b1 == 2), b1, nan)"
        )

        np.testing.assert_array_equal(result.values, [[1.0, 2.0, 3.0, np.nan]])

    def test_no_data_in_any_band_is_no_data_in_result(self) -> None:
        """A pixel where any band is nan is nan in the result, even if the test passes."""
        b1 = np.array([[1.0, 2.0, 3.0]])
        b2 = np.array([[0.0, 0.5, np.nan]])
        da = _make_da([b1, b2])
        result = apply_expression(da, "where(b2 != 0.5, b1, nan)")
        np.testing.assert_array_equal(result.values, [[1.0, np.nan, np.nan]])

    def test_integer_bands_keep_their_type(self) -> None:
        """Integer bands cannot hold nan, so the result keeps their integer type."""
        da = _make_da([np.array([[1, 2]]), np.array([[3, 4]])])
        result = apply_expression(da, "b1 + b2")
        assert result.dtype == np.int64

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


class TestApplyExpressionErrors:
    """Tests that expression mistakes are client errors (400), not 500s."""

    @pytest.mark.parametrize(
        "expression",
        [
            "where((b1 > 0) and (b1 < 2), b1, nan)",
            "where(not (b1 > 0), b1, nan)",
            "b1 if b1 > 0 else 0",
            "where(0 < b1 < 2, b1, nan)",
            "where(b1 < 0.15 & b1 < 0.5, b1, nan)",  # parses as chained
        ],
    )
    def test_array_truth_constructs_raise(self, expression: str) -> None:
        """Constructs that need one truth value fail on every array."""
        da = _make_da([np.ones((2, 2))])

        with pytest.raises(InvalidExpression, match="parenthesizing each"):
            apply_expression(da, expression)

    @pytest.mark.parametrize(
        "expression",
        [
            "where((b1 > 0) & b1 == 1, b1, nan)",  # one comparison unparenthesized
            "(b1 & 8) == 0",  # bit test on a float band
            "~b1",  # ~ on a non-boolean band
            "where(b1 > 0, b1)",  # missing argument
            pytest.param(
                "maximum(b1, b1, b1)",  # a third argument is numpy's `out`
                marks=pytest.mark.filterwarnings("ignore::DeprecationWarning"),
            ),
            "b1 + 1 / 0",  # division by zero in constants
            "b1 * 10.0**400",  # float overflow in constants
        ],
    )
    def test_evaluation_errors_raise(self, expression: str) -> None:
        """Expressions that pass validation but fail on the data are 400s."""
        da = _make_da([np.ones((2, 2))]).chunk()

        with pytest.raises(InvalidExpression) as excinfo:
            apply_expression(da, expression)

        # the chained cause shows evaluation failed, not validation
        assert isinstance(
            excinfo.value.__cause__,
            (ArithmeticError, NotImplementedError, TypeError, ValueError),
        )

    @pytest.mark.parametrize(
        "expression",
        [
            "where((b1 > 0) & b1 == 1, b1, nan)",  # one comparison unparenthesized
            "(b1 & 8) == 0",  # bit test on a float band
            "~b1",  # ~ on a non-boolean band
        ],
    )
    def test_logic_operator_errors_include_hint(self, expression: str) -> None:
        """A failed expression that uses &, |, or ~ explains how to use them."""
        da = _make_da([np.ones((2, 2))]).chunk()

        with pytest.raises(InvalidExpression) as excinfo:
            apply_expression(da, expression)

        assert "wrap each comparison in parentheses" in str(excinfo.value)
        assert "use `(bN // code) % 2 == 1`" in str(excinfo.value)

    def test_expression_without_band_raises(self) -> None:
        """A block that references no band cannot produce an image."""
        da = _make_da([np.ones((2, 2))])

        with pytest.raises(InvalidExpression, match="at least one band"):
            apply_expression(da, "nan")
