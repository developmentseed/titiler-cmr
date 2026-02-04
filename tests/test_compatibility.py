"""Test titiler.cmr.compatibility module."""

from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from fastapi import HTTPException

from titiler.cmr.compatibility import (
    evaluate_concept_compatibility,
    evaluate_rasterio_compatibility,
    evaluate_xarray_compatibility,
    extract_xarray_metadata,
)


class TestExtractXarrayMetadata:
    """Test extract_xarray_metadata function."""

    def test_extract_basic_metadata(self):
        """Test extracting metadata from a simple dataset."""
        # Mock xarray dataset
        mock_ds = MagicMock()

        # Mock data variables
        mock_var = MagicMock()
        mock_var.shape = (365, 1800, 3600)
        mock_var.dtype = np.dtype("float32")
        mock_ds.data_vars = ["temperature"]
        mock_ds.__getitem__ = lambda self, key: mock_var

        # Mock coordinates
        mock_coord = MagicMock()
        mock_coord.size = 365
        mock_coord.dtype = np.dtype("float64")
        mock_coord.min.return_value = 0.0
        mock_coord.max.return_value = 364.0

        # Create a proper mock for coords
        mock_coords = MagicMock()
        mock_coords.__getitem__ = lambda self, key: mock_coord
        mock_coords.items.return_value = [("time", mock_coord)]
        mock_ds.coords = mock_coords

        # Mock dims
        mock_ds.dims = {"time": 365, "lat": 1800, "lon": 3600}

        result = extract_xarray_metadata(mock_ds)

        assert result["backend"] == "xarray"
        assert "temperature" in result["variables"]
        assert result["variables"]["temperature"]["shape"] == [365, 1800, 3600]
        assert result["variables"]["temperature"]["dtype"] == "float32"
        assert result["dimensions"] == {"time": 365, "lat": 1800, "lon": 3600}
        assert "time" in result["coordinates"]
        assert result["coordinates"]["time"]["size"] == 365

    def test_extract_metadata_with_non_numeric_coord(self):
        """Test extracting metadata with non-numeric coordinate."""
        mock_ds = MagicMock()

        # Mock data variables
        mock_var = MagicMock()
        mock_var.shape = (10,)
        mock_var.dtype = np.dtype("float32")
        mock_ds.data_vars = ["data"]
        mock_ds.__getitem__ = lambda self, key: mock_var

        # Mock string coordinate (no min/max)
        mock_coord = MagicMock()
        mock_coord.size = 10
        mock_coord.dtype = np.dtype("U10")  # Unicode string

        # Create a proper mock for coords
        mock_coords = MagicMock()
        mock_coords.__getitem__ = lambda self, key: mock_coord
        mock_coords.items.return_value = [("labels", mock_coord)]
        mock_ds.coords = mock_coords
        mock_ds.dims = {"labels": 10}

        result = extract_xarray_metadata(mock_ds)

        # Non-numeric coordinates shouldn't have min/max
        assert "min" not in result["coordinates"]["labels"]
        assert "max" not in result["coordinates"]["labels"]


class TestXarrayCompatibility:
    """Test evaluate_xarray_compatibility function."""

    @patch("titiler.cmr.compatibility.CMRBackend")
    @patch("titiler.cmr.compatibility.xarray_open_dataset")
    def test_xarray_success(self, mock_xarray_open, mock_backend):
        """Test successful xarray compatibility check."""
        # Mock request
        mock_request = MagicMock()
        mock_request.app.state.get_s3_credentials = None

        # Mock auth config
        mock_auth = MagicMock()
        mock_auth.access = "external"

        # Mock backend and assets
        mock_backend_instance = MagicMock()
        mock_backend.return_value.__enter__ = MagicMock(
            return_value=mock_backend_instance
        )
        mock_backend.return_value.__exit__ = MagicMock(return_value=False)

        mock_backend_instance.assets_for_tile.return_value = [
            {"url": "s3://bucket/file.zarr"}
        ]

        # Mock xarray dataset
        mock_ds = MagicMock()
        mock_var = MagicMock()
        mock_var.shape = (10, 20)
        mock_var.dtype = np.dtype("float32")
        mock_ds.data_vars = ["temp"]
        mock_ds.__getitem__ = lambda self, key: mock_var

        # Create a proper mock for coords
        mock_coords = MagicMock()
        mock_coords.items.return_value = []
        mock_ds.coords = mock_coords
        mock_ds.dims = {"x": 10, "y": 20}

        mock_xarray_open.__enter__ = MagicMock(return_value=mock_ds)
        mock_xarray_open.__exit__ = MagicMock(return_value=False)
        mock_xarray_open.return_value = mock_xarray_open

        result = evaluate_xarray_compatibility("C1234-TEST", mock_request, mock_auth)

        assert result["backend"] == "xarray"
        assert result["example_assets"] == "s3://bucket/file.zarr"
        assert "temp" in result["variables"]

    @patch("titiler.cmr.compatibility.CMRBackend")
    def test_xarray_no_assets(self, mock_backend):
        """Test xarray compatibility with no assets found."""
        mock_request = MagicMock()
        mock_request.app.state.get_s3_credentials = None
        mock_auth = MagicMock()

        # Mock backend returning empty assets
        mock_backend_instance = MagicMock()
        mock_backend.return_value.__enter__ = MagicMock(
            return_value=mock_backend_instance
        )
        mock_backend.return_value.__exit__ = MagicMock(return_value=False)
        mock_backend_instance.assets_for_tile.return_value = []

        with pytest.raises(ValueError, match="No assets found"):
            evaluate_xarray_compatibility("C1234-TEST", mock_request, mock_auth)


class TestRasterioCompatibility:
    """Test evaluate_rasterio_compatibility function."""

    @patch("titiler.cmr.compatibility.CMRBackend")
    def evaluate_rasterio_success(self, mock_backend):
        """Test successful rasterio compatibility check."""
        mock_request = MagicMock()
        mock_request.app.state.get_s3_credentials = None
        mock_auth = MagicMock()
        mock_auth.access = "external"

        # Mock backend and assets
        mock_backend_instance = MagicMock()
        mock_backend.return_value.__enter__ = MagicMock(
            return_value=mock_backend_instance
        )
        mock_backend.return_value.__exit__ = MagicMock(return_value=False)
        mock_backend_instance.assets_for_tile.return_value = [
            {"url": "s3://bucket/file.tif"}
        ]

        result = evaluate_rasterio_compatibility("C1234-TEST", mock_request, mock_auth)

        assert result["backend"] == "rasterio"
        assert result["example_assets"] == "s3://bucket/file.tif"

    @patch("titiler.cmr.compatibility.CMRBackend")
    def evaluate_rasterio_no_assets(self, mock_backend):
        """Test rasterio compatibility with no assets found."""
        mock_request = MagicMock()
        mock_request.app.state.get_s3_credentials = None
        mock_auth = MagicMock()

        # Mock backend returning empty assets
        mock_backend_instance = MagicMock()
        mock_backend.return_value.__enter__ = MagicMock(
            return_value=mock_backend_instance
        )
        mock_backend.return_value.__exit__ = MagicMock(return_value=False)
        mock_backend_instance.assets_for_tile.return_value = []

        with pytest.raises(ValueError, match="No assets found"):
            evaluate_rasterio_compatibility("C1234-TEST", mock_request, mock_auth)


class TestConceptCompatibility:
    """Test evaluate_concept_compatibility function."""

    @patch("titiler.cmr.compatibility.get_concept_id_umm")
    @patch("titiler.cmr.compatibility.evaluate_rasterio_compatibility")
    @patch("titiler.cmr.compatibility.evaluate_xarray_compatibility")
    def evaluate_xarray_succeeds(self, mock_xarray, mock_rasterio, mock_get_umm):
        """Test when xarray compatibility succeeds."""
        mock_request = MagicMock()
        mock_auth = MagicMock()

        # Mock metadata response
        mock_get_umm.return_value = {
            "umm": {
                "TemporalExtents": [
                    {"RangeDateTimes": [{"BeginningDateTime": "2020-01-01T00:00:00Z"}]}
                ]
            }
        }

        mock_xarray.return_value = {
            "backend": "xarray",
            "example_assets": "s3://test.zarr",
        }

        result = evaluate_concept_compatibility("C1234-TEST", mock_request, mock_auth)

        assert result.backend == "xarray"
        assert result.concept_id == "C1234-TEST"
        assert result.datetime is not None
        mock_get_umm.assert_called_once_with("C1234-TEST")
        mock_xarray.assert_called_once()
        mock_rasterio.assert_not_called()

    @patch("titiler.cmr.compatibility.get_concept_id_umm")
    @patch("titiler.cmr.compatibility.evaluate_rasterio_compatibility")
    @patch("titiler.cmr.compatibility.evaluate_xarray_compatibility")
    def test_fallback_to_rasterio(self, mock_xarray, mock_rasterio, mock_get_umm):
        """Test fallback to rasterio when xarray fails."""
        mock_request = MagicMock()
        mock_auth = MagicMock()

        # Mock metadata response
        mock_get_umm.return_value = {
            "umm": {
                "TemporalExtents": [
                    {"RangeDateTimes": [{"BeginningDateTime": "2020-01-01T00:00:00Z"}]}
                ]
            }
        }

        mock_xarray.side_effect = ValueError("No assets found")
        mock_rasterio.return_value = {
            "backend": "rasterio",
            "example_assets": "s3://test.tif",
        }

        result = evaluate_concept_compatibility("C1234-TEST", mock_request, mock_auth)

        assert result.backend == "rasterio"
        assert result.concept_id == "C1234-TEST"
        assert result.datetime is not None
        mock_get_umm.assert_called_once_with("C1234-TEST")
        mock_xarray.assert_called_once()
        mock_rasterio.assert_called_once()

    @patch("titiler.cmr.compatibility.get_concept_id_umm")
    @patch("titiler.cmr.compatibility.evaluate_rasterio_compatibility")
    @patch("titiler.cmr.compatibility.evaluate_xarray_compatibility")
    def test_both_fail(self, mock_xarray, mock_rasterio, mock_get_umm):
        """Test when both readers fail."""
        mock_request = MagicMock()
        mock_auth = MagicMock()

        # Mock metadata response
        mock_get_umm.return_value = {
            "umm": {
                "TemporalExtents": [
                    {"RangeDateTimes": [{"BeginningDateTime": "2020-01-01T00:00:00Z"}]}
                ]
            }
        }

        mock_xarray.side_effect = ValueError("Xarray failed")
        mock_rasterio.side_effect = OSError("Rasterio failed")

        with pytest.raises(HTTPException) as exc_info:
            evaluate_concept_compatibility("C1234-TEST", mock_request, mock_auth)

        assert exc_info.value.status_code == 400
        assert "Could not open a sample granule" in exc_info.value.detail
