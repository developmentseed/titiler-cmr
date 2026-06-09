"""titiler.cmr.compatibility: Compatibility testing utilities."""

import urllib.parse
from typing import Any, Literal, TypeAlias, cast

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
    """Metadata for a single xarray variable."""

    shape: list[int]
    dtype: str
    min: float | None = None
    max: float | None = None
    mean: float | None = None
    p01: float | None = None
    p05: float | None = None
    p95: float | None = None
    p99: float | None = None


class CoordinateInfo(BaseModel):
    """Metadata for a single xarray coordinate."""

    size: int
    dtype: str
    min: float | None = None
    max: float | None = None


class TemplateLink(BaseModel):
    """A URL template link for a compatibility response."""

    rel: str
    href: str
    title: str | None = None
    type: str | None = None


class CompatibilityResponse(BaseModel):
    """Compatibility endpoint response model."""

    concept_id: str
    backend: Literal["rasterio", "xarray"]
    datetime: list[dict[str, Any]]
    variables: dict[str, VariableInfo] | None = None
    dimensions: dict[str, int] | None = None
    coordinates: dict[str, CoordinateInfo] | None = None
    compatible_groups: list[str] | None = None
    example_assets: dict[str, str] | str | None = None
    sample_asset_raster_info: Info | None = None
    links: list[TemplateLink] | None = None


def extract_xarray_metadata(
    ds: Any, max_sample_size: float = 100_000.0
) -> dict[str, Any]:
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
        var_info: dict[str, Any] = {
            "shape": list(ds[var].shape),
            "dtype": str(ds[var].dtype),
        }

        if ds[var].dtype.kind in {"i", "f", "u"}:
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

        if coord_data.dtype.kind in {"i", "f", "u"}:
            try:
                coord_info["min"] = float(coord_data.min())
                coord_info["max"] = float(coord_data.max())
            except Exception:
                pass
        coordinates[coord] = coord_info

    return {
        "variables": variables,
        "dimensions": dict(ds.sizes),
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


X_DIM_ALIASES = set(X_DIM_NAMES)
Y_DIM_ALIASES = set(Y_DIM_NAMES)


def _group_has_spatial_dims(group: Any) -> bool:
    """Return True when a group contains a dataset with both x and y dimension aliases."""
    for child in group.values():
        if not isinstance(child, h5py.Dataset):
            continue

        dim_names = _dataset_dim_scale_names(child)
        if dim_names & X_DIM_ALIASES and dim_names & Y_DIM_ALIASES:
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


def _compatible_groups(
    src_path: str,
    auth_token: str | None = None,
    credential_provider: Any = None,
) -> list[str]:
    """Return candidate group paths for hierarchical assets.

    This stays intentionally lightweight. It only scans HDF5 groups for datasets
    with spatial dimension aliases and does not open each candidate with xarray.
    """
    try:
        return _candidate_group_paths(
            src_path,
            auth_token=auth_token,
            credential_provider=credential_provider,
        )
    except Exception as exc:
        logger.info("Skipping group inspection for %s: %s", src_path, exc)
        return []


def _sample_granule(concept_id: str, request: Request) -> Any:
    """Return the first granule for a collection concept."""
    return next(
        get_granules(
            search_params=GranuleSearch(collection_concept_id=concept_id),
            client=request.app.state.client,
            page_size=1,
            limit=1,
        ),
        None,
    )


def _get_auth_token(request: Request) -> str | None:
    """Return an Earthdata auth token when configured."""
    token_provider = getattr(request.app.state, "earthdata_token_provider", None)
    return token_provider() if token_provider else None


def evaluate_xarray_compatibility(
    concept_id: str,
    request: Request,
    group: str | None = None,
) -> dict[str, Any]:
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

    s3_access = request.app.state.s3_access
    auth_token = _get_auth_token(request)
    granule = _sample_granule(concept_id, request)

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

    if not group and not result["variables"]:
        result["compatible_groups"] = _compatible_groups(
            href,
            auth_token=auth_token,
            credential_provider=credential_provider,
        )

    result["example_assets"] = href
    return result


def evaluate_rasterio_compatibility(
    concept_id: str,
    request: Request,
) -> dict[str, Any]:
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

    s3_access = request.app.state.s3_access
    auth_token = _get_auth_token(request)
    get_s3_credentials = request.app.state.get_s3_credentials
    granule = _sample_granule(concept_id, request)

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
    first_var: str | None = None,
    group: str | None = None,
) -> list[TemplateLink]:
    """Build template links for the compatibility response."""
    if backend == "xarray":
        if not first_var:
            return []

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
            group,
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
    collection_concept_id: str | None = None,
    concept_id: str | None = Query(default=None, include_in_schema=False),
) -> str | None:
    """Accept both collection_concept_id and legacy concept_id."""
    return collection_concept_id or concept_id


XarrayGroupParam: TypeAlias = cast(Any, XarrayIOParams.__annotations__["group"])


router = APIRouter()


@router.get("/compatibility", response_model=CompatibilityResponse)
def compatibility_check(
    request: Request,
    concept_id: str | None = Depends(_concept_id_param),
    group: XarrayGroupParam = None,
) -> CompatibilityResponse:
    """Check which backend is compatible with a CMR collection concept."""
    if concept_id is None:
        raise HTTPException(status_code=400, detail="concept_id is required")
    return evaluate_concept_compatibility(concept_id, request, group=group)
