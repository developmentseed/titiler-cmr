"""Safe evaluation of user-provided band-math expressions over lazy xarray DataArrays.

Expressions are evaluated using Python's ``eval()`` with an empty builtins dict and a
restricted namespace containing only named band variables (b1, b2, ...), a whitelist of
NumPy math functions, ``where``, and ``nan``. Because the band variables are
``xr.DataArray`` objects, Python arithmetic operators and the whitelisted NumPy ufuncs
produce new DataArrays rather than triggering computation — the underlying array data is
never loaded into memory during expression evaluation. ``where`` is ``xr.where`` because
``np.where`` is not a ufunc, so it would load the data and return a bare ndarray.

This design rules out ``numexpr`` as an alternative: ``numexpr.evaluate()`` operates on
in-memory NumPy arrays and would force an immediate ``.compute()`` call on any dask-backed
DataArray, defeating lazy loading.

Security is enforced by walking the AST of each expression block and rejecting any node
type not in ``_ALLOWED_NODE_TYPES``. Attribute access (e.g. ``b1.compute()``), subscripts,
imports, and all other constructs that could escape the sandbox are disallowed. Only the
code compiled from that validated tree is evaluated.
"""

import ast
from types import CodeType
from typing import Any

import numpy as np
import xarray as xr
from rio_tiler.errors import InvalidExpression
from rio_tiler.expression import get_expression_blocks

from titiler.cmr.logger import logger

# Functions and constants that expressions may reference besides b1, b2, ...
_NAMESPACE: dict[str, Any] = {
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
    "where": xr.where,
    "nan": np.nan,
}

_ALLOWED_NODE_TYPES = frozenset(
    {
        ast.Expression,
        # arithmetic / unary / comparison
        ast.BinOp,
        ast.UnaryOp,
        ast.Compare,
        # operators
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.FloorDiv,
        ast.Mod,
        ast.Pow,
        ast.USub,
        ast.UAdd,
        ast.Eq,
        ast.NotEq,
        ast.Lt,
        ast.LtE,
        ast.Gt,
        ast.GtE,
        # elementwise logic for combining boolean arrays
        ast.BitAnd,
        ast.BitOr,
        ast.Invert,
        # literals and names / calls (validated separately)
        ast.Constant,
        ast.Name,
        ast.Call,
        ast.Load,
    }
)

_ARRAY_TRUTH_HINT = (
    "`and`, `or`, `not`, `x if c else y`, and chained comparisons such as "
    "`0 < b1 < 1` do not work on arrays. Combine conditions with `&`, `|`, "
    "and `~`, parenthesizing each comparison because `&` and `|` bind tighter "
    "than comparisons, and choose values with `where()`, e.g., "
    "`where((b2 < 0.15) & (b3 < 0.5), b1, nan)`"
)


def _compile_expression_block(block: str, allowed_names: frozenset[str]) -> CodeType:
    """Validate one expression block against the AST whitelist and compile it.

    Only the returned code object is evaluated, so unvalidated text cannot reach eval.

    Raises InvalidExpression if the block contains any disallowed node type,
    unknown name, non-direct function call (e.g., attribute access), or a construct
    that needs a single truth value (``and``, ``or``, ``not``, ``x if c else y``,
    chained comparisons).

    Args:
        block: A single expression string (no semicolons).
        allowed_names: Set of permitted name identifiers (band vars, functions, and
            constants).
    """
    try:
        tree = ast.parse(block, mode="eval")
    except SyntaxError as e:
        raise InvalidExpression(f"Invalid expression syntax: {e}") from e

    for node in ast.walk(tree):
        if isinstance(node, (ast.BoolOp, ast.Not, ast.IfExp)) or (
            isinstance(node, ast.Compare) and len(node.ops) > 1
        ):
            raise InvalidExpression(f"Invalid expression: {_ARRAY_TRUTH_HINT}")
        if type(node) not in _ALLOWED_NODE_TYPES:
            raise InvalidExpression(
                f"Invalid expression: {type(node).__name__} is not allowed"
            )
        if isinstance(node, ast.Name) and node.id not in allowed_names:
            raise InvalidExpression(
                f"Invalid expression: '{node.id}' is not a recognized name"
            )
        if isinstance(node, ast.Call) and not isinstance(node.func, ast.Name):
            raise InvalidExpression("Invalid expression: method calls are not allowed")

    return compile(tree, "<expression>", "eval")


def apply_expression(
    da: xr.DataArray,
    expression: str,
) -> xr.DataArray:
    """Evaluate a band-math expression against a DataArray without loading data.

    The DataArray must have a "band" dimension. Each band is exposed as b1, b2, ...
    in the expression namespace along with the names in ``_NAMESPACE``.
    Arithmetic and ufunc calls on ``xr.DataArray`` objects build a lazy computation
    graph, so no array data is read from disk or network until the result is consumed
    by the caller.

    Each expression block is validated against ``_ALLOWED_NODE_TYPES`` before eval to
    prevent arbitrary code execution. A block that fails to evaluate, or that does not
    reference a band, raises InvalidExpression.

    Args:
        da: Input DataArray with a "band" dimension.
        expression: Band-math expression string (e.g. ``"log10(b1)/sqrt(b2)"``).
            Multiple output bands can be produced with semicolon-separated blocks
            (e.g. ``"(b1-b2)/(b1+b2);(b1+b2)/2"``).

    Returns:
        Result DataArray with the same laziness as the input, preserving the CRS if
        present. A single expression block returns a 2-D DataArray; multiple blocks
        are concatenated along a new "band" dimension.
    """
    logger.info(f"applying expression: {expression}")

    pre_expression_crs = da.rio.crs
    expression_blocks = [b.strip() for b in get_expression_blocks(expression)]
    band_vars = {
        f"b{i + 1}": da.isel(band=i, drop=True) for i in range(da.sizes["band"])
    }
    allowed_names = frozenset(_NAMESPACE) | frozenset(band_vars)
    codes = [_compile_expression_block(b, allowed_names) for b in expression_blocks]

    namespace = {**_NAMESPACE, **band_vars}
    results = []

    for block, code in zip(expression_blocks, codes, strict=True):
        try:
            result = eval(code, {"__builtins__": {}}, namespace)
        except (TypeError, ValueError) as e:
            # Compiling checks syntax, not types, e.g., `&` on a float band, so
            # these errors are revealed only by eval.
            raise InvalidExpression(f"Invalid expression '{block}': {e}") from e

        if not isinstance(result, xr.DataArray):
            raise InvalidExpression(
                f"Invalid expression '{block}': it must reference at least one band"
            )

        results.append(result)

    result = results[0] if len(results) == 1 else xr.concat(results, dim="band")

    if pre_expression_crs is not None:
        result = result.rio.write_crs(pre_expression_crs)

    return result
