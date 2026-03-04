"""titiler.cmr FastAPI dependencies."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, List, Optional

from fastapi import Depends, HTTPException, Query, Request
from httpx import Client
from titiler.core.dependencies import DefaultDependency
from titiler.xarray.dependencies import CompatXarrayParams, SelDimStr, XarrayParams

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
    granule_ur: GranuleUr | None = None,
    temporal: Temporal | None = None,
    cloud_cover: CloudCover | None = None,
    bounding_box: BBox | None = None,
    sort_key: SortKey = None,
) -> GranuleSearch:
    """Build GranuleSearch parameters from query string inputs."""
    return GranuleSearch(
        collection_concept_id=collection_concept_id,
        granule_ur=granule_ur,
        temporal=temporal,
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

    def __init__(self, request: Request):
        """Initialize BackendParams"""
        self.client = request.app.state.client
        self.auth_token = getattr(request.app.state, "earthdata_token", None)
        self.s3_access = getattr(request.app.state, "s3_access", False)


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
class XarrayReaderParams(DefaultDependency):
    """Xarray reader options wrapper."""

    reader_options: dict


def XarrayReaderOptions(
    xarray_params: Annotated[XarrayParams, Depends()],
) -> XarrayReaderParams:
    """Build XarrayReaderParams from xarray query parameters."""
    return XarrayReaderParams(reader_options=xarray_params.as_dict())


@dataclass
class CMRAssetsParams(DefaultDependency):
    """Parameters for filtering CMR assets by filename regex."""

    assets_regex: Annotated[
        str | None, Query(description="regex to extract asset keys from filenames")
    ] = None


@dataclass
class InterpolatedXarrayParams(CompatXarrayParams):
    """Modified version of CompatXarrayParams that describes {datetime} interpolation."""

    sel: Annotated[
        Optional[List[SelDimStr]],
        Query(
            description="Xarray Indexing using dimension names `{dimension}={value}`."
            " If value is {datetime}, it will be interpolated from the temporal query parameter.",
        ),
    ] = None


def interpolated_xarray_ds_params(
    xarray_params: InterpolatedXarrayParams = Depends(InterpolatedXarrayParams),
    granule_search: GranuleSearch = Depends(GranuleSearchParams),
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
        variable=xarray_params.variable,
        group=xarray_params.group,
        sel=interpolated_sel,
        decode_times=xarray_params.decode_times,
    )
