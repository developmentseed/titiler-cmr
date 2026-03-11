"""titiler.cmr.compatibility: Compatibility testing utilities."""

from typing import Any, Dict, List, Literal, Optional

import numpy as np
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from rio_tiler.models import Info
from starlette.requests import Request

from titiler.cmr.errors import S3CredentialsEndpointMissing
from titiler.cmr.logger import logger
from titiler.cmr.models import GranuleSearch
from titiler.cmr.query import get_collection, get_granules
from titiler.cmr.reader import MultiBaseGranuleReader, open_dataset


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


def evaluate_xarray_compatibility(
    concept_id: str,
    request: Request,
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
        raise ValueError("No assets found for XarrayReader")

    assets = granule.get_assets()
    asset = assets["0"]
    href = asset.direct_href if s3_access else asset.external_href

    credential_provider = None
    if s3_access and get_s3_credentials is not None:
        try:
            credential_provider = get_s3_credentials(granule.s3_credentials_endpoint)
        except S3CredentialsEndpointMissing:
            logger.warning(
                "No S3 credentials endpoint found for granule %s; "
                "falling back to token auth",
                granule.id,
            )

    ds = open_dataset(
        href,
        credential_provider=credential_provider,
        auth_token=auth_token,
    )
    result = extract_xarray_metadata(ds)
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
) -> List[TemplateLink]:
    """Build template links for the compatibility response."""
    if backend == "xarray" and first_var:
        prefix = "/xarray"
        extra_params = f"&variable={first_var}"
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
        result = evaluate_xarray_compatibility(concept_id, request)
        first_var = next(iter(result.get("variables") or {}), None)
        links = _build_links(base_url, concept_id, "xarray", first_var)
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


router = APIRouter()


@router.get("/compatibility", response_model=CompatibilityResponse)
def compatibility_check(
    request: Request,
    concept_id: Optional[str] = Depends(_concept_id_param),
) -> CompatibilityResponse:
    """Check which backend is compatible with a CMR collection concept."""
    if concept_id is None:
        raise HTTPException(status_code=400, detail="concept_id is required")
    return evaluate_concept_compatibility(concept_id, request)
