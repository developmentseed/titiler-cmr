"""Expression evaluation utilities for xarray band math."""

from typing import Any

import numpy as np
import xarray as xr
from rio_tiler.expression import get_expression_blocks

from titiler.cmr.logger import logger

_MATH_FUNCTIONS: dict[str, Any] = {
    "abs": np.abs,
    "ceil": np.ceil,
    "floor": np.floor,
    "round": np.round,
    "trunc": np.trunc,
    "sign": np.sign,
    "sqrt": np.sqrt,
    "exp": np.exp,
    "expm1": np.expm1,
    "log": np.log,
    "log1p": np.log1p,
    "log10": np.log10,
    "log2": np.log2,
    "sin": np.sin,
    "cos": np.cos,
    "tan": np.tan,
    "arcsin": np.arcsin,
    "arccos": np.arccos,
    "arctan": np.arctan,
    "arctan2": np.arctan2,
    "sinh": np.sinh,
    "cosh": np.cosh,
    "tanh": np.tanh,
    "arcsinh": np.arcsinh,
    "arccosh": np.arccosh,
    "arctanh": np.arctanh,
    "isnan": np.isnan,
    "isfinite": np.isfinite,
    "isinf": np.isinf,
    "signbit": np.signbit,
    "fmod": np.fmod,
    "hypot": np.hypot,
    "maximum": np.maximum,
    "minimum": np.minimum,
    "where": np.where,
}


def apply_expression(
    da: xr.DataArray,
    expression: str,
) -> xr.DataArray:
    """Evaluate a band-math expression against a DataArray.

    The DataArray must have a "band" dimension. Each band is exposed as b1, b2, ...
    in the expression namespace, along with numexpr-compatible math functions and
    the full `np` and `xr` namespaces for backwards compatibility.

    Args:
        da: Input DataArray with a "band" dimension.
        expression: Band-math expression string (e.g. "log10(b1)/sqrt(b2)").

    Returns:
        Result DataArray, preserving the CRS if present.
    """
    logger.info(f"applying expression: {expression}")
    pre_expression_crs = da.rio.crs
    expression_blocks = get_expression_blocks(expression)
    band_vars = {
        f"b{i + 1}": da.isel(band=i, drop=True) for i in range(da.sizes["band"])
    }
    namespace = {"np": np, "xr": xr, **_MATH_FUNCTIONS, **band_vars}
    results = [
        eval(block, {"__builtins__": {}}, namespace) for block in expression_blocks
    ]
    result = results[0] if len(results) == 1 else xr.concat(results, dim="band")
    if pre_expression_crs is not None:
        result = result.rio.write_crs(pre_expression_crs)
    return result
