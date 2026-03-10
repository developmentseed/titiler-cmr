"""titiler.cmr FastAPI dependencies."""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, List, Optional

from fastapi import Depends, HTTPException, Query, Request
from httpx import Client
from titiler.core.dependencies import DefaultDependency, ExpressionParams
from titiler.xarray.dependencies import SelDimStr, XarrayIOParams

from titiler.cmr.models import (
    BBox,
    CloudCover,
    ConceptID,
    GranuleSearch,
    GranuleUr,
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
    )


@dataclass(init=False)
class BackendParams(DefaultDependency):
    """backend parameters."""

    client: Client = field(init=False)
    auth_token: str | None = field(init=False)
    s3_access: bool = field(init=False)
    get_s3_credentials: Callable | None = field(init=False)

    def __init__(self, request: Request):
        """Initialize BackendParams"""
        self.client = request.app.state.client
        self.auth_token = getattr(request.app.state, "earthdata_token", None)
        self.s3_access = getattr(request.app.state, "s3_access", False)
        self.get_s3_credentials = getattr(request.app.state, "get_s3_credentials", None)


@dataclass
class GranuleSearchBackendParams(DefaultDependency):
    """PgSTAC parameters."""

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
    skipcovered: Annotated[
        bool | None,
        Query(
            description="Skip any items that would show up completely under the previous items",
        ),
    ] = None


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


def interpolated_xarray_ds_params(
    xarray_params: Annotated[
        InterpolatedXarrayParams, Depends(InterpolatedXarrayParams)
    ],
    granule_search: Annotated[GranuleSearch, Depends(GranuleSearchParams)],
) -> InterpolatedXarrayParams:
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

    return InterpolatedXarrayParams(
        variables=xarray_params.variables,
        group=xarray_params.group,
        sel=interpolated_sel,
        decode_times=xarray_params.decode_times,
    )
