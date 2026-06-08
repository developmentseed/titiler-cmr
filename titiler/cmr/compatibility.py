"""titiler.cmr.compatibility: Compatibility testing utilities."""

import urllib.parse
from typing import Any, Dict, List, Literal, Optional, TypeAlias, cast

import h5py
import numpy as np
from fastapi import APIRouter, Depends, HTTPException, Query
from obspec_utils.readers import BlockStoreReader
from obstore import store
from pydantic import BaseModel
from rio_tiler.models import Info
from starlette.requests import Request
from titiler.xarray.dependencies import XarrayIOParams

from titiler.cmr.errors import S3CredentialsEndpointMissing
from titiler.cmr.logger import logger
from titiler.cmr.models import GranuleSearch
from titiler.cmr.query import get_collection, get_granules
from titiler.cmr.reader import (
    X_DIM_NAMES,
    Y_DIM_NAMES,
    MultiBaseGranuleReader,
    open_dataset,
)


class VariableInfo(BaseModel):
    """Metadata for a single xarray variable"""

    shape: List[int]
    dtype: str
    min: Optional[float] = None
    max: Optional[float] = None
    mean: Optional[float] = None
    p01: Optional[float] = None
    p05: Optional[float] = None
    p95: Optional[float] = None
    p99: Optional[float] = None


class CoordinateInfo(BaseModel):
    """Metadata for a single xarray coordinate"""

    size: int
    dtype: str
    min: Optional[float] = None
    max: Optional[float] = None


class TemplateLink(BaseModel):
    """A URL template link for a compatibility response."""

    rel: str
    href: str
    title: Optional[str] = None
    type: Optional[str] = None


class CompatibilityResponse(BaseModel):
    """Compatibility endpoint response model"""

    concept_id: str
    backend: Literal["rasterio", "xarray"]
    datetime: List[Dict[str, Any]]
    variables: Optional[Dict[str, VariableInfo]] = None
    dimensions: Optional[Dict[str, int]] = None
    coordinates: Optional[Dict[str, CoordinateInfo]] = None
    example_assets: Optional[Dict[str, str] | str] = None
    sample_asset_raster_info: Optional[Info] = None
    requires_group: Optional[bool] = None
    group_hints: Optional[List[str]] = None
    default_group: Optional[str] = None
    sampled_group: Optional[str] = None
    links: Optional[List[TemplateLink]] = None


def extract_xarray_metadata(
    ds: Any, max_sample_size: float = 100_000.0
) -> Dict[str, Any]:
    """Extract comprehensive metadata from an xarray Dataset.

    For large arrays, uses sampling along each dimension to avoid memory issues.

    Args:
        ds: xarray Dataset instance
        max_sample_size: Maximum number of elements to sample for statistics.
            Arrays larger than this will be sampled. Default: 1,000,000

    Returns:
        Dictionary containing variables, dimensions, and coordinates metadata
    """
    variables = {}
    for var in ds.data_vars:
        var_info: Dict[str, Any] = {
            "shape": list(ds[var].shape),
            "dtype": str(ds[var].dtype),
        }

        if ds[var].dtype.kind in ["i", "f", "u"]:
            try:
                var_data = ds[var]
                total_size = var_data.size

                # Use sampling for large arrays to avoid memory issues
                if total_size > max_sample_size:
                    # Calculate exact sample size per dimension to stay within budget
                    indexers = {}
                    actual_sample_size = 1
                    remaining_budget = max_sample_size

                    for i, dim in enumerate(var_data.dims):
                        dim_size = var_data.sizes[dim]
                        # Distribute budget across remaining dimensions
                        dims_remaining = len(var_data.dims) - i
                        samples_per_dim = int(
                            remaining_budget ** (1.0 / dims_remaining)
                        )
                        sample_size = min(dim_size, max(1, samples_per_dim))

                        # Random sample of indices along this dimension
                        indices = np.sort(
                            np.random.choice(dim_size, size=sample_size, replace=False)
                        )
                        indexers[dim] = indices
                        actual_sample_size *= sample_size
                        remaining_budget = max_sample_size / actual_sample_size

                    # Sample using integer indexing (efficient with chunked data)
                    sampled = var_data.isel(indexers)
                    values = sampled.values

                    logger.info(
                        f"Sampled {actual_sample_size:,} of {total_size:,} elements "
                        f"from variable '{var}' for statistics"
                    )
                else:
                    # Load entire array for smaller datasets
                    values = var_data.values

                var_info["min"] = float(np.nanmin(values))
                var_info["max"] = float(np.nanmax(values))
                var_info["mean"] = float(np.nanmean(values))

                # Calculate multiple percentiles in a single pass, filtering out NaNs
                p01, p05, p95, p99 = np.nanpercentile(values, [1, 5, 95, 99])
                var_info["p01"] = float(p01)
                var_info["p05"] = float(p05)
                var_info["p95"] = float(p95)
                var_info["p99"] = float(p99)
            except Exception:
                # Skip statistics if computation fails (e.g., too large, all NaN values)
                pass

        variables[var] = var_info

    coordinates = {}
    for coord, coord_data in ds.coords.items():
        coord_info = {
            "size": int(coord_data.size),
            "dtype": str(coord_data.dtype),
        }

        if coord_data.dtype.kind in ["i", "f", "u"]:
            try:
                coord_info["min"] = float(coord_data.min())
                coord_info["max"] = float(coord_data.max())
            except Exception:
                pass
        coordinates[coord] = coord_info

    return {
        "variables": variables,
        "dimensions": dict(ds.dims),
        "coordinates": coordinates,
        "backend": "xarray",
    }


def _get_credential_provider(granule: Any, request: Request) -> Any:
    """Return a credential provider for a granule when direct S3 access is enabled."""
    if not request.app.state.s3_access:
        return None

    get_s3_credentials = request.app.state.get_s3_credentials
    if get_s3_credentials is None:
        return None

    try:
        return get_s3_credentials(granule.s3_credentials_endpoint)
    except S3CredentialsEndpointMissing:
        logger.warning(
            "No S3 credentials endpoint found for granule %s; falling back to token auth",
            granule.id,
        )
        return None


def _make_blockstore_reader(
    src_path: str,
    auth_token: str | None = None,
    credential_provider: Any = None,
) -> BlockStoreReader:
    """Create a block-based reader for a remote NetCDF/HDF5 asset."""

    parsed = urllib.parse.urlparse(src_path)
    store_root = f"{parsed.scheme}://{parsed.netloc}"

    if credential_provider is not None:
        object_store = store.from_url(
            store_root,
            credential_provider=credential_provider,
        )
    elif auth_token:
        object_store = store.from_url(
            store_root,
            client_options={
                "default_headers": {"Authorization": f"Bearer {auth_token}"}
            },
        )
    else:
        object_store = store.from_url(store_root)

    return BlockStoreReader(object_store, parsed.path, block_size=8 * 1024**2)


def _dataset_dim_scale_names(dataset: Any) -> set[str]:
    """Return all attached HDF5 dimension-scale names for a dataset."""
    dim_names: set[str] = set()

    for axis in range(dataset.ndim):
        try:
            scales = dataset.dims[axis].values()
        except Exception:
            continue

        for scale in scales:
            scale_name = getattr(scale, "name", "")
            if scale_name:
                dim_names.add(scale_name.rsplit("/", 1)[-1])

    return dim_names


def _group_has_spatial_dims(group: Any) -> bool:
    """Return True when a group contains a dataset with both x and y dimension aliases."""
    x_names = set(X_DIM_NAMES)
    y_names = set(Y_DIM_NAMES)

    for child in group.values():
        if not isinstance(child, h5py.Dataset):
            continue

        dim_names = _dataset_dim_scale_names(child)
        if dim_names & x_names and dim_names & y_names:
            return True

    return False


def _candidate_group_paths(
    src_path: str,
    auth_token: str | None = None,
    credential_provider: Any = None,
) -> list[str]:
    """Return likely xarray group paths, preferring spatial non-metadata groups."""
    reader = _make_blockstore_reader(
        src_path,
        auth_token=auth_token,
        credential_provider=credential_provider,
    )
    try:
        with h5py.File(reader, "r") as file_handle:
            all_group_paths: list[str] = []
            spatial_group_paths: list[str] = []
            spatial_metadata_group_paths: list[str] = []

            def visitor(name: str, obj: Any) -> None:
                if not name or not isinstance(obj, h5py.Group):
                    return

                if not any(isinstance(child, h5py.Dataset) for child in obj.values()):
                    return

                all_group_paths.append(name)

                if not _group_has_spatial_dims(obj):
                    return

                normalized_name = f"/{name.strip('/')}/"
                if "/metadata/" in normalized_name:
                    spatial_metadata_group_paths.append(name)
                else:
                    spatial_group_paths.append(name)

            file_handle.visititems(visitor)

            if spatial_group_paths:
                return spatial_group_paths
            if spatial_metadata_group_paths:
                return spatial_metadata_group_paths
            return all_group_paths
    finally:
        reader.close()


def _group_hints(
    src_path: str,
    auth_token: str | None = None,
    credential_provider: Any = None,
) -> dict[str, Any]:
    """Inspect a hierarchical asset and return lightweight xarray group hints."""
    try:
        group_paths = _candidate_group_paths(
            src_path,
            auth_token=auth_token,
            credential_provider=credential_provider,
        )
    except Exception as exc:
        logger.info("Skipping group inspection for %s: %s", src_path, exc)
        return {
            "requires_group": False,
            "group_hints": [],
            "default_group": None,
            "sampled_group": None,
        }

    group_hints: list[str] = []
    for group_path in group_paths:
        try:
            grouped_dataset = open_dataset(
                src_path,
                group=group_path,
                credential_provider=credential_provider,
                auth_token=auth_token,
            )
        except Exception as exc:
            logger.info(
                "Skipping incompatible group %s for %s: %s",
                group_path,
                src_path,
                exc,
            )
            continue

        if grouped_dataset.data_vars:
            group_hints.append(group_path)

    return {
        "requires_group": False,
        "group_hints": group_hints,
        "default_group": group_hints[0] if len(group_hints) == 1 else None,
        "sampled_group": group_hints[0] if group_hints else None,
    }


def evaluate_xarray_compatibility(
    concept_id: str,
    request: Request,
    group: str | None = None,
) -> Dict[str, Any]:
    """Test XarrayReader compatibility with a concept.

    Args:
        concept_id: CMR concept ID to test
        request: FastAPI request object

    Returns:
        Dictionary with xarray compatibility information

    Raises:
        ValueError: If no assets found or reader incompatible
        HTTPException: If CMR query fails
        OSError: If file access fails
        KeyError: If expected data structure is missing
    """
    logger.info("Testing XarrayReader")

    client = request.app.state.client
    s3_access = request.app.state.s3_access
    token_provider = getattr(request.app.state, "earthdata_token_provider", None)
    auth_token = token_provider() if token_provider else None

    granule = next(
        get_granules(
            search_params=GranuleSearch(collection_concept_id=concept_id),
            client=client,
            page_size=1,
            limit=1,
        ),
        None,
    )

    if granule is None:
        raise ValueError("No assets found for XarrayReader")

    assets = granule.get_assets()
    asset = assets["0"]
    href = asset.direct_href if s3_access else asset.external_href

    credential_provider = _get_credential_provider(granule, request)

    ds = open_dataset(
        href,
        group=group,
        credential_provider=credential_provider,
        auth_token=auth_token,
    )
    result = extract_xarray_metadata(ds)
    group_hints = _group_hints(
        href,
        auth_token=auth_token,
        credential_provider=credential_provider,
    )

    sampled_group = group if group else group_hints.get("sampled_group")
    if not group and not result["variables"] and sampled_group:
        sampled_ds = open_dataset(
            href,
            group=sampled_group,
            credential_provider=credential_provider,
            auth_token=auth_token,
        )
        result = extract_xarray_metadata(sampled_ds)

    group_hints["requires_group"] = (
        bool(group_hints["group_hints"]) and not bool(group) and not bool(ds.data_vars)
    )
    group_hints["sampled_group"] = sampled_group

    result.update(group_hints)
    result["example_assets"] = href
    return result


def evaluate_rasterio_compatibility(
    concept_id: str,
    request: Request,
) -> Dict[str, Any]:
    """Test MultiBaseGranuleReader compatibility with a concept.

    Args:
        concept_id: CMR concept ID to test
        request: FastAPI request object

    Returns:
        Dictionary with rasterio compatibility information

    Raises:
        ValueError: If no assets found or reader incompatible
        HTTPException: If CMR query fails
        OSError: If file access fails
        KeyError: If expected data structure is missing
    """
    logger.info("Testing MultiBaseGranuleReader")

    client = request.app.state.client
    s3_access = request.app.state.s3_access
    token_provider = getattr(request.app.state, "earthdata_token_provider", None)
    auth_token = token_provider() if token_provider else None
    get_s3_credentials = request.app.state.get_s3_credentials

    granule = next(
        get_granules(
            search_params=GranuleSearch(collection_concept_id=concept_id),
            client=client,
            page_size=1,
            limit=1,
        ),
        None,
    )

    if granule is None:
        raise ValueError("No assets found for MultiBaseGranuleReader")

    with MultiBaseGranuleReader(
        granule=granule,
        s3_access=s3_access,
        auth_token=auth_token,
        get_s3_credentials=get_s3_credentials,
    ) as reader:
        asset_name = reader.assets[0]
        info_result = reader.info(assets=[asset_name])
        info = info_result[asset_name]

        asset_info = granule.get_assets()[asset_name]
        href = asset_info.direct_href if s3_access else asset_info.external_href
        example_assets = {asset_name: href}

    return {
        "example_assets": example_assets,
        "sample_asset_raster_info": info,
        "backend": "rasterio",
    }


def _build_links(
    base_url: str,
    concept_id: str,
    backend: str,
    first_var: Optional[str] = None,
    group: Optional[str] = None,
) -> List[TemplateLink]:
    """Build template links for the compatibility response."""
    if backend == "xarray" and first_var:
        prefix = "/xarray"
        extra_params = f"&variables={first_var}"
        if group:
            quoted_group = urllib.parse.quote(group, safe="/")
            extra_params = f"{extra_params}&group={quoted_group}"
    else:
        prefix = "/rasterio"
        extra_params = ""

    id_param = f"?collection_concept_id={concept_id}"
    temporal = "&temporal={temporal}"
    base = f"{base_url}{prefix}"
    qs = f"{id_param}{extra_params}{temporal}"

    return [
        TemplateLink(
            rel="tilejson",
            href=f"{base}/WebMercatorQuad/tilejson.json{qs}",
            title="TileJSON",
            type="application/json",
        ),
        TemplateLink(
            rel="map",
            href=f"{base}/WebMercatorQuad/map.html{qs}",
            title="Map viewer",
            type="text/html",
        ),
        TemplateLink(
            rel="tile",
            href=f"{base}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}{qs}",
            title="Map tile",
            type="image/png",
        ),
    ]


def evaluate_concept_compatibility(
    concept_id: str,
    request: Request,
    group: str | None = None,
) -> CompatibilityResponse:
    """Test which reader backend is compatible with a CMR concept.

    Tries XarrayReader first, falls back to MultiBaseGranuleReader.
    Also fetches and includes CMR metadata in the response.

    Args:
        concept_id: CMR concept ID to test
        request: FastAPI request object

    Returns:
        CompatibilityResponse with backend info, temporal extent, and metadata

    Raises:
        HTTPException: If neither reader is compatible or metadata cannot be fetched
    """
    if not concept_id:
        raise HTTPException(400, "concept_id is required")

    collection = get_collection(concept_id, request.app.state.client)
    temporal_extent = collection.temporal_extents
    base_url = str(request.base_url).rstrip("/")

    # Try xarray first
    xarray_error: Exception | None = None
    try:
        result = evaluate_xarray_compatibility(concept_id, request, group=group)
        first_var = next(iter(result.get("variables") or {}), None)
        links = _build_links(
            base_url,
            concept_id,
            "xarray",
            first_var,
            group or result.get("sampled_group") or result.get("default_group"),
        )
        return CompatibilityResponse(
            concept_id=concept_id,
            datetime=temporal_extent,
            links=links,
            **result,
        )
    except (ValueError, HTTPException, OSError, KeyError) as e:
        xarray_error = e
        logger.warning(f"XarrayReader failed: {e}")

    # Fall back to rasterio
    rasterio_error: Exception | None = None
    try:
        result = evaluate_rasterio_compatibility(concept_id, request)
        links = _build_links(base_url, concept_id, "rasterio")
        return CompatibilityResponse(
            concept_id=concept_id,
            datetime=temporal_extent,
            links=links,
            **result,
        )
    except (ValueError, HTTPException, OSError, KeyError) as e:
        rasterio_error = e
        logger.warning(f"MultiBaseGranuleReader failed: {e}")

    # Both failed
    raise HTTPException(
        400,
        f"Could not open a sample granule for concept_id {concept_id} "
        "with either the rasterio or xarray backends.\n\n "
        f"xarray error: {xarray_error} \n\n rasterio_error: {rasterio_error}",
    )


def _concept_id_param(
    collection_concept_id: Optional[str] = None,
    concept_id: Optional[str] = Query(default=None, include_in_schema=False),
) -> Optional[str]:
    """Accept both collection_concept_id and legacy concept_id."""
    return collection_concept_id or concept_id


XarrayGroupParam: TypeAlias = cast(Any, XarrayIOParams.__annotations__["group"])


router = APIRouter()


@router.get("/compatibility", response_model=CompatibilityResponse)
def compatibility_check(
    request: Request,
    concept_id: Optional[str] = Depends(_concept_id_param),
    group: XarrayGroupParam = None,
) -> CompatibilityResponse:
    """Check which backend is compatible with a CMR collection concept."""
    if concept_id is None:
        raise HTTPException(status_code=400, detail="concept_id is required")
    return evaluate_concept_compatibility(concept_id, request, group=group)
