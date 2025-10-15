"""titiler.cmr.compatibility: Compatibility testing utilities."""

from typing import Any, Dict, List, Literal, Optional

from fastapi import HTTPException
from pydantic import BaseModel
from rio_tiler.constants import WEB_MERCATOR_TMS
from rio_tiler.io.rasterio import Reader
from rio_tiler.models import Info
from starlette.requests import Request

from titiler.cmr.backend import CMRBackend
from titiler.cmr.dependencies import ConceptID
from titiler.cmr.logger import logger
from titiler.cmr.reader import xarray_open_dataset
from titiler.cmr.settings import AuthSettings
from titiler.cmr.utils import get_concept_id_umm
from titiler.xarray.io import Reader as XarrayReader


class CompatibilityResponse(BaseModel):
    """Compatibility endpoint response model"""

    concept_id: ConceptID
    backend: Literal["rasterio", "xarray"]
    datetime: List[Dict[str, Any]]
    variables: Optional[Dict[str, Dict[str, Any]]] = None
    dimensions: Optional[Dict[str, int]] = None
    coordinates: Optional[Dict[str, Dict[str, Any]]] = None
    example_assets: Optional[Dict[str, str] | str] = None
    sample_asset_raster_info: Optional[Info] = None


def extract_xarray_metadata(ds: Any) -> Dict[str, Any]:
    """Extract comprehensive metadata from an xarray Dataset.

    Args:
        ds: xarray Dataset instance

    Returns:
        Dictionary containing variables, dimensions, and coordinates metadata
    """
    variables = {
        var: {
            "shape": list(ds[var].shape),
            "dtype": str(ds[var].dtype),
        }
        for var in ds.data_vars
    }

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
    concept_id: ConceptID,
    request: Request,
    s3_auth_config: AuthSettings,
) -> Dict[str, Any]:
    """Test XarrayReader compatibility with a concept.

    Args:
        concept_id: CMR concept ID to test
        request: FastAPI request object
        s3_auth_config: S3 authentication configuration

    Returns:
        Dictionary with xarray compatibility information

    Raises:
        ValueError: If no assets found or reader incompatible
        HTTPException: If CMR query fails
        OSError: If file access fails
        KeyError: If expected data structure is missing
    """
    logger.info("Testing XarrayReader")

    with CMRBackend(
        tms=WEB_MERCATOR_TMS,
        reader=XarrayReader,
        reader_options={},
        auth=request.app.state.cmr_auth,
    ) as src_dst:
        assets = src_dst.assets_for_tile(
            0,
            0,
            0,
            limit=1,
            concept_id=concept_id,
            access=s3_auth_config.access,
        )

        if not assets:
            raise ValueError("No assets found for XarrayReader")

        with xarray_open_dataset(assets[0]["url"]) as ds:
            result = extract_xarray_metadata(ds)
            result["example_assets"] = assets[0]["url"]
            return result


def evaluate_rasterio_compatibility(
    concept_id: ConceptID,
    request: Request,
    s3_auth_config: AuthSettings,
) -> Dict[str, Any]:
    """Test MultiFilesBandsReader compatibility with a concept.

    Args:
        concept_id: CMR concept ID to test
        request: FastAPI request object
        s3_auth_config: S3 authentication configuration

    Returns:
        Dictionary with rasterio compatibility information

    Raises:
        ValueError: If no assets found or reader incompatible
        HTTPException: If CMR query fails
        OSError: If file access fails
        KeyError: If expected data structure is missing
    """
    logger.info("Testing MultiFilesBandsReader")

    with CMRBackend(
        tms=WEB_MERCATOR_TMS,
        reader=Reader,
        reader_options={"bands": [1]},
        auth=request.app.state.cmr_auth,
    ) as src_dst:
        assets = src_dst.assets_for_tile(
            0,
            0,
            0,
            limit=1,
            concept_id=concept_id,
            access=s3_auth_config.access,
            bands_regex=".*",
        )

        if not assets:
            raise ValueError("No assets found for MultiFilesBandsReader")

        example_assets: Dict[str, str] = assets[0]["url"]

        with src_dst.reader(
            input=list(example_assets.values())[0], tms=src_dst.tms
        ) as _src_dst:
            info = _src_dst.info()

        return {
            "example_assets": example_assets,
            "sample_asset_raster_info": info,
            "backend": "rasterio",
        }


def evaluate_concept_compatibility(
    concept_id: ConceptID,
    request: Request,
    s3_auth_config: AuthSettings,
) -> CompatibilityResponse:
    """Test which reader backend is compatible with a CMR concept.

    Tries XarrayReader first, falls back to MultiFilesBandsReader.
    Also fetches and includes CMR metadata in the response.

    Args:
        concept_id: CMR concept ID to test
        request: FastAPI request object
        s3_auth_config: S3 authentication configuration

    Returns:
        CompatibilityResponse with backend info, temporal extent, and metadata

    Raises:
        HTTPException: If neither reader is compatible or metadata cannot be fetched
    """

    metadata = get_concept_id_umm(concept_id)
    temporal_extent = metadata["umm"]["TemporalExtents"]

    # Try xarray first
    try:
        result = evaluate_xarray_compatibility(concept_id, request, s3_auth_config)
        return CompatibilityResponse(
            concept_id=concept_id,
            datetime=temporal_extent,
            **result,
        )
    except (ValueError, HTTPException, OSError, KeyError) as e:
        logger.warning(f"XarrayReader failed: {e}")

    # Fall back to rasterio
    try:
        result = evaluate_rasterio_compatibility(concept_id, request, s3_auth_config)
        return CompatibilityResponse(
            concept_id=concept_id,
            datetime=temporal_extent,
            **result,
        )
    except (ValueError, HTTPException, OSError, KeyError) as e:
        logger.warning(f"MultiFilesBandsReader failed: {e}")

    # Both failed
    raise HTTPException(400, f"cannot parse concept_id {concept_id}")
