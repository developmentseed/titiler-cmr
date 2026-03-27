"""titiler.cmr FastAPI dependencies."""

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, List, Optional

from fastapi import Depends, HTTPException, Query, Request
from httpx import Client
from pydantic import AfterValidator
from titiler.core.dependencies import (
    AssetsExprParams,
    DefaultDependency,
    ExpressionParams,
    _parse_asset,
)
from titiler.xarray.dependencies import SelDimStr, XarrayIOParams

from titiler.cmr.models import (
    AdditionalAttributeFilter,
    BBox,
    CloudCover,
    ConceptID,
    GranuleSearch,
    GranuleUr,
    OrbitNumber,
    SortKey,
    Temporal,
)
from titiler.cmr.utils import parse_datetime


def GranuleSearchParams(
    collection_concept_id: ConceptID | None = None,
    concept_id: ConceptID | None = Query(default=None, include_in_schema=False),
    granule_ur: GranuleUr | None = None,
    temporal: Temporal | None = None,
    datetime_param: Temporal | None = Query(
        alias="datetime", default=None, include_in_schema=False
    ),
    cloud_cover: CloudCover | None = None,
    bounding_box: BBox | None = None,
    sort_key: SortKey = None,
    orbit_number: OrbitNumber = None,
    attribute: AdditionalAttributeFilter = None,
) -> GranuleSearch:
    """Build GranuleSearch parameters from query string inputs.

    Accepts both current and legacy parameter names:
    - concept_id (legacy) → collection_concept_id
    - datetime (legacy)   → temporal
    """
    return GranuleSearch(
        collection_concept_id=collection_concept_id or concept_id,
        granule_ur=granule_ur,
        temporal=temporal or datetime_param,
        cloud_cover=cloud_cover,
        bounding_box=bounding_box,
        sort_key=sort_key,
        orbit_number=orbit_number,
        attribute=attribute,
    )


@dataclass(init=False)
class BackendParams(DefaultDependency):
    """Reader backend parameters sourced from application state.

    Reads the HTTP client, Earthdata auth token, S3 access flag, and S3
    credential provider from the FastAPI app state on each request.
    """

    client: Client = field(init=False)
    auth_token: str | None = field(init=False)
    s3_access: bool = field(init=False)
    get_s3_credentials: Callable | None = field(init=False)

    def __init__(self, request: Request):
        """Read auth state from app.state and resolve the current bearer token.

        Reads ``earthdata_token_provider``, ``s3_access``, and
        ``get_s3_credentials`` from the FastAPI application state.  If a token
        provider is present, calls it to obtain the current bearer token string.
        """
        self.client = request.app.state.client
        token_provider = getattr(request.app.state, "earthdata_token_provider", None)
        self.auth_token = token_provider() if token_provider else None
        self.s3_access = getattr(request.app.state, "s3_access", False)
        self.get_s3_credentials = getattr(request.app.state, "get_s3_credentials", None)


@dataclass
class GranuleSearchBackendParams(DefaultDependency):
    """Backend parameters controlling granule search coverage behaviour."""

    items_limit: Annotated[
        int | None,
        Query(
            description="Return as soon as we have N items per geometry.",
        ),
    ] = None
    exitwhenfull: Annotated[
        bool,
        Query(
            description="Return as soon as the geometry is fully covered.",
        ),
    ] = True
    coverage_tolerance: Annotated[
        float,
        Query(
            description=(
                "Buffer (in degrees) applied to granule polygons when checking full coverage. "
                "A small positive value (e.g. 1e-2) reduces slivers caused by imprecise CMR polygons."
            ),
        ),
    ] = 0.0
    skipcovered: Annotated[
        bool | None,
        Query(
            description="Skip any items that would show up completely under the previous items",
        ),
    ] = None


@dataclass
class RasterioGranuleSearchBackendParams(GranuleSearchBackendParams):
    """GranuleSearchBackendParams with a non-zero default coverage_tolerance.

    Rasterio granule polygons are stored in WGS84 but the underlying data is
    often UTM-aligned, so polygon edges slightly undershoot the actual data
    footprint. A small default tolerance compensates for this, preventing
    coverage slivers along granule boundaries.
    """

    coverage_tolerance: Annotated[
        float,
        Query(
            description=(
                "Buffer (in degrees) applied to granule polygons when checking full coverage. "
                "A small positive value (e.g. 1e-4) reduces slivers caused by imprecise CMR polygons."
            ),
        ),
    ] = 1e-2


@dataclass
class CMRAssetsParams(DefaultDependency):
    """Parameters for filtering CMR assets by filename regex."""

    assets_regex: Annotated[
        str | None, Query(description="regex to extract asset keys from filenames")
    ] = None
    bands_regex: Annotated[
        str | None,
        Query(alias="bands_regex", include_in_schema=False),
    ] = None

    def __post_init__(self):
        """Apply legacy parameter aliases."""
        if self.bands_regex and not self.assets_regex:
            self.assets_regex = self.bands_regex
        # Clear so as_dict() does not leak the deprecated param name
        self.bands_regex = None


def _translate_legacy_expr(expression: str, names: list[str]) -> str:
    """Translate legacy name-based expression to positional bN format.

    If the expression contains identifiers from `names` (not already bN-style,
    not function calls), replaces them with b1, b2, ... based on their position
    in `names`.
    """
    identifiers = re.findall(r"\b([a-zA-Z_]\w*)\b(?!\s*\()", expression)
    new_style = re.compile(r"^b[1-9][0-9]*$", re.IGNORECASE)
    legacy = list(dict.fromkeys(n for n in identifiers if not new_style.match(n)))
    if not legacy:
        return expression
    mapping = {name: f"b{i + 1}" for i, name in enumerate(names)}
    expr = expression
    for name, band_ref in mapping.items():
        expr = re.sub(r"\b" + re.escape(name) + r"\b", band_ref, expr)
    return expr


@dataclass
class CMRAssetsExprParams(AssetsExprParams):
    """AssetsExprParams with backwards-compatible legacy expression translation.

    Detects legacy expressions that reference asset names directly (e.g. B04, NIR)
    and translates them to the new rio-tiler 9.0 positional band format (b1, b2, ...).
    """

    assets: Annotated[
        list[str] | None,
        AfterValidator(_parse_asset),
        Query(
            title="Asset names",
            description="Asset's names.",
        ),
    ] = None

    def __post_init__(self):
        """Translate legacy asset-name expressions to positional bN format.

        If the expression already uses bN references (e.g. b1-b2), it is left
        unchanged. Otherwise, identifiers are matched against the provided or
        auto-detected asset list and substituted with b1, b2, ... in order.
        """
        if not self.expression:
            return

        identifiers = re.findall(r"\b([a-zA-Z_]\w*)\b(?!\s*\()", self.expression)
        new_style_pattern = re.compile(r"^b[1-9][0-9]*$", re.IGNORECASE)
        asset_names = list(
            dict.fromkeys(
                name for name in identifiers if not new_style_pattern.match(name)
            )
        )

        if not asset_names:
            return

        if self.assets:
            ordered_assets = list(self.assets)
        else:
            ordered_assets = asset_names
            self.assets = ordered_assets

        self.expression = _translate_legacy_expr(self.expression, ordered_assets)


@dataclass
class XarrayDsParams(DefaultDependency):
    """Xarray Dataset Options."""

    variables: Annotated[list[str], Query(description="Xarray Variable names.")]

    sel: Annotated[
        list[SelDimStr] | None,
        Query(
            description="Xarray Indexing using dimension names `{dimension}={value}` or `{dimension}={method}::{value}`.",
        ),
    ] = None


@dataclass
class XarrayParams(ExpressionParams, XarrayIOParams, XarrayDsParams):
    """Xarray Reader dependency."""

    pass


@dataclass
class InterpolatedXarrayParams(XarrayParams):
    """Modified version of CompatXarrayParams that describes {datetime} interpolation."""

    sel: Annotated[
        Optional[List[SelDimStr]],
        Query(
            description="Xarray Indexing using dimension names `{dimension}={value}`."
            " If value is {datetime}, it will be interpolated from the temporal query parameter.",
        ),
    ] = None


@dataclass
class CMRXarrayExprParams(InterpolatedXarrayParams):
    """InterpolatedXarrayParams with legacy variable-name expression translation.

    Translates expressions like `temperature/pressure` to `b1/b2` based on the
    order of `variables`.
    """

    def __post_init__(self):
        """Translate legacy variable-name expressions to positional bN format.

        Skipped when expression is already new-style (contains only bN refs) or
        when no expression is provided. Safe to call more than once — already-
        translated expressions are returned unchanged.
        """
        if self.expression and self.variables:
            self.expression = _translate_legacy_expr(self.expression, self.variables)


def interpolated_xarray_ds_params(
    xarray_params: Annotated[CMRXarrayExprParams, Depends(CMRXarrayExprParams)],
    granule_search: Annotated[GranuleSearch, Depends(GranuleSearchParams)],
) -> CMRXarrayExprParams:
    """
    Xarray parameters with string interpolation support for the sel parameter.

    Interpolates {datetime} templates in sel parameter values with the actual
    datetime value from the temporal query parameter in ISO format
    (e.g., 2025-09-23T00:00:00Z).

    Example:
        temporal=2025-09-23T00:00:00Z&sel=time={datetime} → sel=time=2025-09-23T00:00:00Z
    """
    if not xarray_params.sel:
        return xarray_params

    if not granule_search.temporal:
        raise HTTPException(400, "A 'temporal' parameter is required")

    datetime_, start, _ = parse_datetime(granule_search.temporal)
    dt: datetime = datetime_ if datetime_ else start  # type: ignore[assignment]

    interpolated_sel = []
    for sel_item in xarray_params.sel:
        if isinstance(sel_item, str) and "{datetime}" in sel_item:
            interpolated_sel.append(sel_item.format(datetime=dt.isoformat()))
        else:
            interpolated_sel.append(sel_item)

    return CMRXarrayExprParams(
        variables=xarray_params.variables,
        group=xarray_params.group,
        sel=interpolated_sel,
        decode_times=xarray_params.decode_times,
        expression=xarray_params.expression,
    )
