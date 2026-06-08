"""Test titiler.cmr.compatibility module."""

from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from fastapi import HTTPException

from titiler.cmr.compatibility import (
    CompatibilityResponse,
    _candidate_group_paths,
    _dataset_dim_scale_names,
    _group_has_spatial_dims,
    evaluate_concept_compatibility,
    evaluate_rasterio_compatibility,
    evaluate_xarray_compatibility,
    extract_xarray_metadata,
)
from titiler.cmr.models import (
    Granule,
    GranuleSpatialExtent,
    RelatedUrl,
)


def _make_request(s3_access=False, auth_token=None, get_s3_credentials=None):
    """Build a mock FastAPI Request with app.state fields set."""
    request = MagicMock()
    request.app.state.client = MagicMock()
    request.app.state.s3_access = s3_access
    request.app.state.earthdata_token_provider = (
        (lambda: auth_token) if auth_token is not None else None
    )
    request.app.state.get_s3_credentials = get_s3_credentials
    request.base_url = "http://testserver/"
    return request


def _make_granule(external_href="https://example.com/file.nc") -> Granule:
    """Create a minimal Granule with one asset."""
    return Granule(
        id="G1234-TEST",
        granule_ur="MOD09A1.A2020001.h12v04.hdf",
        collection_concept_id="C1234-TEST",
        related_urls=[
            RelatedUrl(**{"URL": external_href, "Type": "GET DATA"}),
            RelatedUrl(
                **{"URL": "s3://bucket/file.nc", "Type": "GET DATA VIA DIRECT ACCESS"}
            ),
        ],
        spatial_extent=GranuleSpatialExtent(
            **{
                "HorizontalSpatialDomain": {
                    "Geometry": {
                        "BoundingRectangles": [
                            {
                                "WestBoundingCoordinate": -180,
                                "EastBoundingCoordinate": 180,
                                "NorthBoundingCoordinate": 0,
                                "SouthBoundingCoordinate": 0,
                            }
                        ]
                    }
                }
            }
        ),
    )


class TestExtractXarrayMetadata:
    """Test extract_xarray_metadata function."""

    def test_extract_basic_metadata(self):
        """Test extracting metadata from a simple dataset."""
        mock_ds = MagicMock()

        mock_var = MagicMock()
        mock_var.shape = (365, 1800, 3600)
        mock_var.dtype = np.dtype("float32")
        mock_ds.data_vars = ["temperature"]
        mock_ds.__getitem__ = lambda self, key: mock_var

        mock_coord = MagicMock()
        mock_coord.size = 365
        mock_coord.dtype = np.dtype("float64")
        mock_coord.min.return_value = 0.0
        mock_coord.max.return_value = 364.0

        mock_coords = MagicMock()
        mock_coords.__getitem__ = lambda self, key: mock_coord
        mock_coords.items.return_value = [("time", mock_coord)]
        mock_ds.coords = mock_coords
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

        mock_var = MagicMock()
        mock_var.shape = (10,)
        mock_var.dtype = np.dtype("float32")
        mock_ds.data_vars = ["data"]
        mock_ds.__getitem__ = lambda self, key: mock_var

        mock_coord = MagicMock()
        mock_coord.size = 10
        mock_coord.dtype = np.dtype("U10")  # Unicode string

        mock_coords = MagicMock()
        mock_coords.__getitem__ = lambda self, key: mock_coord
        mock_coords.items.return_value = [("labels", mock_coord)]
        mock_ds.coords = mock_coords
        mock_ds.dims = {"labels": 10}

        result = extract_xarray_metadata(mock_ds)

        assert "min" not in result["coordinates"]["labels"]
        assert "max" not in result["coordinates"]["labels"]


class _FakeScale:
    def __init__(self, name: str):
        self.name = name


class _FakeDim:
    def __init__(self, scale_names: list[str]):
        self._scale_names = scale_names

    def values(self):
        return [_FakeScale(name) for name in self._scale_names]


class _FakeDataset:
    def __init__(self, dim_scale_names: list[list[str]]):
        self.ndim = len(dim_scale_names)
        self.dims = [_FakeDim(names) for names in dim_scale_names]


class _FakeGroup:
    def __init__(self, children):
        self._children = children

    def values(self):
        return self._children.values()

    def items(self):
        return self._children.items()


class TestGroupPruningHelpers:
    """Test HDF5 group pruning helpers."""

    def test_dataset_dim_scale_names(self):
        """Test extraction of attached dimension-scale names."""
        dataset = _FakeDataset([["/science/yCoordinates"], ["/science/xCoordinates"]])

        assert _dataset_dim_scale_names(dataset) == {"yCoordinates", "xCoordinates"}

    @patch("titiler.cmr.compatibility.h5py.Dataset", new=_FakeDataset)
    def test_group_has_spatial_dims(self):
        """Test spatial-dimension detection for a group."""
        group = _FakeGroup(
            {
                "HHHH": _FakeDataset([["yCoordinates"], ["xCoordinates"]]),
                "mask": _FakeDataset([["yCoordinates"], ["xCoordinates"]]),
            }
        )

        assert _group_has_spatial_dims(group) is True

    @patch("titiler.cmr.compatibility.h5py.Dataset", new=_FakeDataset)
    def test_group_has_spatial_dims_false_when_aliases_missing(self):
        """Test non-spatial groups are rejected by the dim-alias filter."""
        group = _FakeGroup(
            {
                "quaternions": _FakeDataset([[], []]),
                "time": _FakeDataset([[]]),
            }
        )

        assert _group_has_spatial_dims(group) is False

    @patch("titiler.cmr.compatibility.h5py.Group", new=_FakeGroup)
    @patch("titiler.cmr.compatibility.h5py.Dataset", new=_FakeDataset)
    @patch("titiler.cmr.compatibility._make_blockstore_reader")
    @patch("titiler.cmr.compatibility.h5py.File")
    def test_candidate_group_paths_prefers_non_metadata_spatial_groups(
        self, mock_h5py_file, mock_reader
    ):
        """Test group pruning prefers non-metadata groups with spatial dims."""
        mock_reader.return_value = MagicMock()

        fake_groups = {
            "science/LSAR/GCOV/grids/frequencyA": _FakeGroup(
                {"HHHH": _FakeDataset([["yCoordinates"], ["xCoordinates"]])}
            ),
            "science/LSAR/GCOV/metadata/radarGrid": _FakeGroup(
                {
                    "slantRange": _FakeDataset(
                        [["heightAboveEllipsoid"], ["yCoordinates"], ["xCoordinates"]]
                    )
                }
            ),
            "science/LSAR/GCOV/metadata/attitude": _FakeGroup(
                {"quaternions": _FakeDataset([[], []])}
            ),
        }

        file_handle = MagicMock()

        def visititems(visitor):
            for name, obj in fake_groups.items():
                visitor(name, obj)

        file_handle.visititems.side_effect = visititems
        mock_h5py_file.return_value.__enter__.return_value = file_handle
        mock_h5py_file.return_value.__exit__.return_value = False

        result = _candidate_group_paths("https://example.com/file.h5")

        assert result == ["science/LSAR/GCOV/grids/frequencyA"]


class TestXarrayCompatibility:
    """Test evaluate_xarray_compatibility function."""

    @patch("titiler.cmr.compatibility._compatible_groups")
    @patch("titiler.cmr.compatibility.open_dataset")
    @patch("titiler.cmr.compatibility.get_granules")
    def test_xarray_success(
        self,
        mock_get_granules,
        mock_open_dataset,
        mock_compatible_groups,
    ):
        """Test successful xarray compatibility check."""
        request = _make_request()
        granule = _make_granule()
        mock_get_granules.return_value = iter([granule])

        mock_ds = MagicMock()
        mock_var = MagicMock()
        mock_var.shape = (10, 20)
        mock_var.dtype = np.dtype("float32")
        mock_ds.data_vars = ["temp"]
        mock_ds.__getitem__ = lambda self, key: mock_var

        mock_coords = MagicMock()
        mock_coords.items.return_value = []
        mock_ds.coords = mock_coords
        mock_ds.dims = {"x": 10, "y": 20}
        mock_open_dataset.return_value = mock_ds
        mock_compatible_groups.return_value = []

        result = evaluate_xarray_compatibility("C1234-TEST", request)

        assert result["backend"] == "xarray"
        assert result["example_assets"] == "https://example.com/file.nc"
        assert result.get("compatible_groups") is None
        assert "temp" in result["variables"]

    @patch("titiler.cmr.compatibility.get_granules")
    def test_xarray_no_assets(self, mock_get_granules):
        """Test xarray compatibility with no granules found."""
        request = _make_request()
        mock_get_granules.return_value = iter([])

        with pytest.raises(ValueError, match="No assets found"):
            evaluate_xarray_compatibility("C1234-TEST", request)

    @patch("titiler.cmr.compatibility._compatible_groups")
    @patch("titiler.cmr.compatibility.open_dataset")
    @patch("titiler.cmr.compatibility.get_granules")
    def test_xarray_uses_direct_href_when_s3_access(
        self,
        mock_get_granules,
        mock_open_dataset,
        mock_compatible_groups,
    ):
        """Test that direct_href is used when s3_access is True."""
        request = _make_request(s3_access=True)
        granule = _make_granule()
        mock_get_granules.return_value = iter([granule])

        mock_ds = MagicMock()
        mock_ds.data_vars = []
        mock_ds.coords = MagicMock()
        mock_ds.coords.items.return_value = []
        mock_ds.dims = {}
        mock_open_dataset.return_value = mock_ds
        mock_compatible_groups.return_value = []

        result = evaluate_xarray_compatibility("C1234-TEST", request)

        assert result["example_assets"] == "s3://bucket/file.nc"

    @patch("titiler.cmr.compatibility._compatible_groups")
    @patch("titiler.cmr.compatibility.open_dataset")
    @patch("titiler.cmr.compatibility.get_granules")
    def test_xarray_lists_compatible_groups_when_root_dataset_is_empty(
        self,
        mock_get_granules,
        mock_open_dataset,
        mock_compatible_groups,
    ):
        """Test grouped xarray compatibility returns group paths without nested inspection."""
        request = _make_request()
        granule = _make_granule()
        mock_get_granules.return_value = iter([granule])

        root_ds = MagicMock()
        root_ds.data_vars = []
        root_ds.coords = MagicMock()
        root_ds.coords.items.return_value = []
        root_ds.dims = {}

        mock_open_dataset.return_value = root_ds
        mock_compatible_groups.return_value = ["science/grids/frequencyA"]

        result = evaluate_xarray_compatibility("C1234-TEST", request)

        assert result["compatible_groups"] == ["science/grids/frequencyA"]
        assert result["variables"] == {}


class TestRasterioCompatibility:
    """Test evaluate_rasterio_compatibility function."""

    @patch("titiler.cmr.compatibility.MultiBaseGranuleReader")
    @patch("titiler.cmr.compatibility.get_granules")
    def test_rasterio_success(self, mock_get_granules, mock_reader_cls):
        """Test successful rasterio compatibility check."""
        request = _make_request()
        granule = _make_granule(external_href="https://example.com/file.tif")
        mock_get_granules.return_value = iter([granule])

        mock_info = MagicMock()
        mock_reader = MagicMock()
        mock_reader.assets = ["0"]
        mock_reader.info.return_value = {"0": mock_info}

        mock_reader_cls.return_value.__enter__ = MagicMock(return_value=mock_reader)
        mock_reader_cls.return_value.__exit__ = MagicMock(return_value=False)

        result = evaluate_rasterio_compatibility("C1234-TEST", request)

        assert result["backend"] == "rasterio"
        assert isinstance(result["example_assets"], dict)
        assert result["sample_asset_raster_info"] is mock_info

    @patch("titiler.cmr.compatibility.get_granules")
    def test_rasterio_no_assets(self, mock_get_granules):
        """Test rasterio compatibility with no granules found."""
        request = _make_request()
        mock_get_granules.return_value = iter([])

        with pytest.raises(ValueError, match="No assets found"):
            evaluate_rasterio_compatibility("C1234-TEST", request)


class TestConceptCompatibility:
    """Test evaluate_concept_compatibility function."""

    @patch("titiler.cmr.compatibility.evaluate_rasterio_compatibility")
    @patch("titiler.cmr.compatibility.evaluate_xarray_compatibility")
    @patch("titiler.cmr.compatibility.get_collection")
    def test_xarray_succeeds(self, mock_get_collection, mock_xarray, mock_rasterio):
        """Test when xarray compatibility succeeds."""
        request = _make_request()

        mock_collection = MagicMock()
        mock_collection.temporal_extents = [{"RangeDateTimes": []}]
        mock_get_collection.return_value = mock_collection

        mock_xarray.return_value = {
            "backend": "xarray",
            "variables": {"temp": {"shape": [10], "dtype": "float32"}},
            "dimensions": {"x": 10},
            "coordinates": {},
            "example_assets": "https://example.com/file.nc",
            "compatible_groups": None,
        }

        result = evaluate_concept_compatibility("C1234-TEST", request)

        assert result.backend == "xarray"
        assert result.concept_id == "C1234-TEST"
        assert result.datetime is not None
        assert result.links is not None
        assert len(result.links) == 3
        mock_xarray.assert_called_once()
        mock_rasterio.assert_not_called()

    @patch("titiler.cmr.compatibility.evaluate_rasterio_compatibility")
    @patch("titiler.cmr.compatibility.evaluate_xarray_compatibility")
    @patch("titiler.cmr.compatibility.get_collection")
    def test_xarray_links_contain_variable(
        self, mock_get_collection, mock_xarray, mock_rasterio
    ):
        """Test that xarray links include the first variable name."""
        request = _make_request()

        mock_collection = MagicMock()
        mock_collection.temporal_extents = []
        mock_get_collection.return_value = mock_collection

        mock_xarray.return_value = {
            "backend": "xarray",
            "variables": {"sea_ice": {"shape": [10], "dtype": "float32"}},
            "dimensions": {},
            "coordinates": {},
            "example_assets": "https://example.com/file.nc",
            "compatible_groups": None,
        }

        result = evaluate_concept_compatibility("C1234-TEST", request)

        tilejson_link = next(link for link in result.links if link.rel == "tilejson")
        assert "variables=sea_ice" in tilejson_link.href
        assert "{temporal}" in tilejson_link.href
        assert "/xarray/" in tilejson_link.href

    @patch("titiler.cmr.compatibility.evaluate_rasterio_compatibility")
    @patch("titiler.cmr.compatibility.evaluate_xarray_compatibility")
    @patch("titiler.cmr.compatibility.get_collection")
    def test_xarray_links_include_explicit_group(
        self, mock_get_collection, mock_xarray, mock_rasterio
    ):
        """Test that xarray links include the explicit group parameter."""
        request = _make_request()

        mock_collection = MagicMock()
        mock_collection.temporal_extents = []
        mock_get_collection.return_value = mock_collection

        mock_xarray.return_value = {
            "backend": "xarray",
            "variables": {"backscatter": {"shape": [10], "dtype": "float32"}},
            "dimensions": {},
            "coordinates": {},
            "example_assets": "https://example.com/file.nc",
            "compatible_groups": None,
        }

        result = evaluate_concept_compatibility(
            "C1234-TEST", request, group="science/grids/frequencyA"
        )

        tilejson_link = next(link for link in result.links if link.rel == "tilejson")
        assert "group=science/grids/frequencyA" in tilejson_link.href
        mock_rasterio.assert_not_called()

    @patch("titiler.cmr.compatibility.evaluate_rasterio_compatibility")
    @patch("titiler.cmr.compatibility.evaluate_xarray_compatibility")
    @patch("titiler.cmr.compatibility.get_collection")
    def test_xarray_returns_no_links_without_variable_inspection(
        self, mock_get_collection, mock_xarray, mock_rasterio
    ):
        """Test grouped root responses do not fabricate xarray links."""
        request = _make_request()

        mock_collection = MagicMock()
        mock_collection.temporal_extents = []
        mock_get_collection.return_value = mock_collection

        mock_xarray.return_value = {
            "backend": "xarray",
            "variables": {},
            "dimensions": {},
            "coordinates": {},
            "example_assets": "https://example.com/file.nc",
            "compatible_groups": ["science/grids/frequencyA"],
        }

        result = evaluate_concept_compatibility("C1234-TEST", request)

        assert result.backend == "xarray"
        assert result.links == []
        mock_rasterio.assert_not_called()

    @patch("titiler.cmr.compatibility.evaluate_rasterio_compatibility")
    @patch("titiler.cmr.compatibility.evaluate_xarray_compatibility")
    @patch("titiler.cmr.compatibility.get_collection")
    def test_fallback_to_rasterio(
        self, mock_get_collection, mock_xarray, mock_rasterio
    ):
        """Test fallback to rasterio when xarray fails."""
        request = _make_request()

        mock_collection = MagicMock()
        mock_collection.temporal_extents = []
        mock_get_collection.return_value = mock_collection

        mock_xarray.side_effect = ValueError("No assets found")
        mock_rasterio.return_value = {
            "backend": "rasterio",
            "example_assets": {"0": "https://example.com/file.tif"},
        }

        result = evaluate_concept_compatibility("C1234-TEST", request)

        assert result.backend == "rasterio"
        assert result.links is not None
        rasterio_link = next(link for link in result.links if link.rel == "tilejson")
        assert "/rasterio/" in rasterio_link.href
        assert "{temporal}" in rasterio_link.href
        mock_xarray.assert_called_once()
        mock_rasterio.assert_called_once()

    @patch("titiler.cmr.compatibility.evaluate_rasterio_compatibility")
    @patch("titiler.cmr.compatibility.evaluate_xarray_compatibility")
    @patch("titiler.cmr.compatibility.get_collection")
    def test_both_fail(self, mock_get_collection, mock_xarray, mock_rasterio):
        """Test when both readers fail."""
        request = _make_request()

        mock_collection = MagicMock()
        mock_collection.temporal_extents = []
        mock_get_collection.return_value = mock_collection

        mock_xarray.side_effect = ValueError("Xarray failed")
        mock_rasterio.side_effect = OSError("Rasterio failed")

        with pytest.raises(HTTPException) as exc_info:
            evaluate_concept_compatibility("C1234-TEST", request)

        assert exc_info.value.status_code == 400
        assert "Could not open a sample granule" in exc_info.value.detail


class TestCompatibilityEndpoint:
    """Test the /compatibility HTTP endpoint."""

    @patch("titiler.cmr.compatibility.evaluate_concept_compatibility")
    def test_endpoint_success(self, mock_evaluate, app):
        """Test the /compatibility endpoint returns a valid response."""
        mock_evaluate.return_value = CompatibilityResponse(
            concept_id="C1234-TEST",
            backend="xarray",
            datetime=[],
            variables={"temp": {"shape": [10], "dtype": "float32"}},
            dimensions={"x": 10},
            coordinates={},
            example_assets="https://example.com/file.nc",
            links=[],
        )

        response = app.get("/compatibility?collection_concept_id=C1234-TEST")

        assert response.status_code == 200
        data = response.json()
        assert data["backend"] == "xarray"
        assert data["concept_id"] == "C1234-TEST"
        assert mock_evaluate.call_args.kwargs["group"] is None

    @patch("titiler.cmr.compatibility.evaluate_concept_compatibility")
    def test_endpoint_accepts_group(self, mock_evaluate, app):
        """Test the /compatibility endpoint forwards the optional group parameter."""
        mock_evaluate.return_value = CompatibilityResponse(
            concept_id="C1234-TEST",
            backend="xarray",
            datetime=[],
            compatible_groups=["science/grids/frequencyA"],
            links=[],
        )

        response = app.get(
            "/compatibility?collection_concept_id=C1234-TEST&group=science/grids/frequencyA"
        )

        assert response.status_code == 200
        assert mock_evaluate.call_args.kwargs["group"] == "science/grids/frequencyA"

    def test_endpoint_openapi_documents_group_like_xarray_routes(self, app):
        """Test the /compatibility endpoint reuses the shared xarray group parameter docs."""
        response = app.get("/api")

        assert response.status_code == 200
        parameters = response.json()["paths"]["/compatibility"]["get"]["parameters"]
        group_param = next(param for param in parameters if param["name"] == "group")
        assert group_param["description"] == (
            "Select a specific zarr group from a zarr hierarchy. "
            "Could be associated with a zoom level or dataset."
        )
