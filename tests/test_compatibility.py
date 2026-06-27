"""Test titiler.cmr.compatibility module."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import h5py
import numpy as np
import pytest
import xarray as xr
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


class _PathReader:
    def __init__(self, path: Path):
        self._path = path

    def __enter__(self) -> "_PathReader":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def __fspath__(self) -> str:
        return str(self._path)

    def close(self) -> None:
        return None


class _FakeRasterReader:
    def __init__(self, info_result):
        self.assets = ["0"]
        self._info_result = info_result

    def info(self, assets: list[str]):
        assert assets == ["0"]
        return {"0": self._info_result}


class _FakeRasterReaderContext:
    def __init__(self, info_result):
        self._reader = _FakeRasterReader(info_result)

    def __enter__(self) -> _FakeRasterReader:
        return self._reader

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def _make_request(s3_access=False, auth_token=None, get_s3_credentials=None):
    """Build a lightweight Request-like object with app.state fields set."""
    return SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                client=object(),
                s3_access=s3_access,
                earthdata_token_provider=(
                    (lambda: auth_token) if auth_token is not None else None
                ),
                get_s3_credentials=get_s3_credentials,
            )
        ),
        base_url="http://testserver/",
    )


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


def _write_test_hdf5(path: Path) -> Path:
    """Create a real HDF5 hierarchy with spatial and metadata groups."""
    with h5py.File(path, "w") as file_handle:
        grids = file_handle.create_group("science/LSAR/GCOV/grids/frequencyA")
        metadata_grid = file_handle.create_group("science/LSAR/GCOV/metadata/radarGrid")
        metadata_attitude = file_handle.create_group(
            "science/LSAR/GCOV/metadata/attitude"
        )

        y = grids.create_dataset("yCoordinates", data=np.arange(2, dtype=np.float32))
        y.make_scale("yCoordinates")
        x = grids.create_dataset("xCoordinates", data=np.arange(3, dtype=np.float32))
        x.make_scale("xCoordinates")
        raster = grids.create_dataset("HHHH", data=np.zeros((2, 3), dtype=np.float32))
        raster.dims[0].attach_scale(y)
        raster.dims[1].attach_scale(x)

        z = metadata_grid.create_dataset(
            "heightAboveEllipsoid", data=np.arange(4, dtype=np.float32)
        )
        z.make_scale("heightAboveEllipsoid")
        metadata_y = metadata_grid.create_dataset(
            "yCoordinates", data=np.arange(2, dtype=np.float32)
        )
        metadata_y.make_scale("yCoordinates")
        metadata_x = metadata_grid.create_dataset(
            "xCoordinates", data=np.arange(3, dtype=np.float32)
        )
        metadata_x.make_scale("xCoordinates")
        slant_range = metadata_grid.create_dataset(
            "slantRange", data=np.zeros((4, 2, 3), dtype=np.float32)
        )
        slant_range.dims[0].attach_scale(z)
        slant_range.dims[1].attach_scale(metadata_y)
        slant_range.dims[2].attach_scale(metadata_x)

        metadata_attitude.create_dataset(
            "quaternions", data=np.zeros((2, 4), dtype=np.float32)
        )

    return path


def _write_test_hdf5_with_unblessed_spatial_names(path: Path) -> Path:
    """Create an HDF5 file whose scales look spatial but use unsupported names."""
    with h5py.File(path, "w") as file_handle:
        grids = file_handle.create_group("science/LSAR/GCOV/grids/frequencyA")

        row = grids.create_dataset(
            "rowCoordinates", data=np.arange(2, dtype=np.float32)
        )
        row.make_scale("rowCoordinates")
        col = grids.create_dataset(
            "columnCoordinates", data=np.arange(3, dtype=np.float32)
        )
        col.make_scale("columnCoordinates")
        raster = grids.create_dataset("HHHH", data=np.zeros((2, 3), dtype=np.float32))
        raster.dims[0].attach_scale(row)
        raster.dims[1].attach_scale(col)

    return path


class TestExtractXarrayMetadata:
    """Test extract_xarray_metadata function."""

    def test_extract_basic_metadata(self):
        """Test extracting metadata from a simple dataset."""
        dataset = xr.Dataset(
            {
                "temperature": (
                    ("time", "lat", "lon"),
                    np.arange(24, dtype=np.float32).reshape(3, 2, 4),
                )
            },
            coords={"time": np.arange(3, dtype=np.float64)},
        )

        result = extract_xarray_metadata(dataset)

        assert result["backend"] == "xarray"
        assert "temperature" in result["variables"]
        assert result["variables"]["temperature"]["shape"] == [3, 2, 4]
        assert result["variables"]["temperature"]["dtype"] == "float32"
        assert result["dimensions"] == {"time": 3, "lat": 2, "lon": 4}
        assert "time" in result["coordinates"]
        assert result["coordinates"]["time"]["size"] == 3

    def test_extract_metadata_with_non_numeric_coord(self):
        """Test extracting metadata with non-numeric coordinate."""
        dataset = xr.Dataset(
            {"data": (("labels",), np.arange(10, dtype=np.float32))},
            coords={"labels": np.array([f"label-{i}" for i in range(10)])},
        )

        result = extract_xarray_metadata(dataset)

        assert "min" not in result["coordinates"]["labels"]
        assert "max" not in result["coordinates"]["labels"]


class TestGroupPruningHelpers:
    """Test HDF5 group pruning helpers."""

    def test_dataset_dim_scale_names(self, tmp_path: Path):
        """Test extraction of attached dimension-scale names."""
        hdf5_path = _write_test_hdf5(tmp_path / "scales.h5")

        with h5py.File(hdf5_path, "r") as file_handle:
            dataset = file_handle["science/LSAR/GCOV/grids/frequencyA/HHHH"]
            assert _dataset_dim_scale_names(dataset) == {"yCoordinates", "xCoordinates"}

    def test_group_has_spatial_dims(self, tmp_path: Path):
        """Test spatial-dimension detection for a group."""
        hdf5_path = _write_test_hdf5(tmp_path / "spatial.h5")

        with h5py.File(hdf5_path, "r") as file_handle:
            group = file_handle["science/LSAR/GCOV/grids/frequencyA"]
            assert _group_has_spatial_dims(group) is True

    def test_group_has_spatial_dims_false_when_aliases_missing(self, tmp_path: Path):
        """Test non-spatial groups are rejected by the dim-alias filter."""
        hdf5_path = _write_test_hdf5(tmp_path / "non-spatial.h5")

        with h5py.File(hdf5_path, "r") as file_handle:
            group = file_handle["science/LSAR/GCOV/metadata/attitude"]
            assert _group_has_spatial_dims(group) is False

    def test_group_has_spatial_dims_false_for_unblessed_spatial_names(
        self, tmp_path: Path
    ):
        """Test spatial-looking scale names are ignored unless they are blessed aliases."""
        hdf5_path = _write_test_hdf5_with_unblessed_spatial_names(
            tmp_path / "unblessed-spatial.h5"
        )

        with h5py.File(hdf5_path, "r") as file_handle:
            group = file_handle["science/LSAR/GCOV/grids/frequencyA"]
            assert _group_has_spatial_dims(group) is False
            dataset = file_handle["science/LSAR/GCOV/grids/frequencyA/HHHH"]
            assert _dataset_dim_scale_names(dataset) == {
                "rowCoordinates",
                "columnCoordinates",
            }

    @patch("titiler.cmr.compatibility._make_blockstore_reader")
    def test_candidate_group_paths_falls_back_to_all_groups_when_aliases_are_unblessed(
        self, mock_reader, tmp_path: Path
    ):
        """Test group discovery falls back when no group's scale names match blessed aliases."""
        hdf5_path = _write_test_hdf5_with_unblessed_spatial_names(
            tmp_path / "unblessed-groups.h5"
        )
        mock_reader.return_value = _PathReader(hdf5_path)

        result = _candidate_group_paths("https://example.com/file.h5")

        assert result == ["science/LSAR/GCOV/grids/frequencyA"]

    @patch("titiler.cmr.compatibility._make_blockstore_reader")
    def test_candidate_group_paths_prefers_non_metadata_spatial_groups(
        self, mock_reader, tmp_path: Path
    ):
        """Test group pruning prefers non-metadata groups with spatial dims."""
        hdf5_path = _write_test_hdf5(tmp_path / "groups.h5")
        mock_reader.return_value = _PathReader(hdf5_path)

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
        mock_open_dataset.return_value = xr.Dataset(
            {"temp": (("x", "y"), np.arange(200, dtype=np.float32).reshape(10, 20))}
        )
        mock_compatible_groups.return_value = []

        result = evaluate_xarray_compatibility("C1234-TEST", request)

        assert result["backend"] == "xarray"
        assert result["example_assets"] == "https://example.com/file.nc"
        assert result["granule_ur"] == "MOD09A1.A2020001.h12v04.hdf"
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
        mock_open_dataset.return_value = xr.Dataset()
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
        mock_open_dataset.return_value = xr.Dataset()
        mock_compatible_groups.return_value = ["science/grids/frequencyA"]

        result = evaluate_xarray_compatibility("C1234-TEST", request)

        assert result["compatible_groups"] == ["science/grids/frequencyA"]
        assert result["variables"] == {}

    @patch("titiler.cmr.compatibility._compatible_groups")
    @patch("titiler.cmr.compatibility.open_dataset")
    @patch("titiler.cmr.compatibility.get_granules")
    def test_xarray_uses_requested_granule_ur(
        self,
        mock_get_granules,
        mock_open_dataset,
        mock_compatible_groups,
    ):
        """Test xarray compatibility forwards granule_ur to CMR search."""
        request = _make_request()
        granule = _make_granule()
        mock_get_granules.return_value = iter([granule])
        mock_open_dataset.return_value = xr.Dataset()
        mock_compatible_groups.return_value = []

        result = evaluate_xarray_compatibility(
            "C1234-TEST",
            request,
            granule_ur="MOD09A1.A2020001.h12v04.hdf",
        )

        search_params = mock_get_granules.call_args.kwargs["search_params"]
        assert search_params.collection_concept_id == "C1234-TEST"
        assert search_params.granule_ur == "MOD09A1.A2020001.h12v04.hdf"
        assert result["granule_ur"] == "MOD09A1.A2020001.h12v04.hdf"


class TestRasterioCompatibility:
    """Test evaluate_rasterio_compatibility function."""

    @patch("titiler.cmr.compatibility.MultiBaseGranuleReader")
    @patch("titiler.cmr.compatibility.get_granules")
    def test_rasterio_success(self, mock_get_granules, mock_reader_cls):
        """Test successful rasterio compatibility check."""
        request = _make_request()
        granule = _make_granule(external_href="https://example.com/file.tif")
        mock_get_granules.return_value = iter([granule])

        info_result = object()
        mock_reader_cls.return_value = _FakeRasterReaderContext(info_result)

        result = evaluate_rasterio_compatibility("C1234-TEST", request)

        assert result["backend"] == "rasterio"
        assert isinstance(result["example_assets"], dict)
        assert result["granule_ur"] == "MOD09A1.A2020001.h12v04.hdf"
        assert result["sample_asset_raster_info"] is info_result

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

        mock_get_collection.return_value = SimpleNamespace(
            temporal_extents=[{"RangeDateTimes": []}]
        )
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

        mock_get_collection.return_value = SimpleNamespace(temporal_extents=[])
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

        mock_get_collection.return_value = SimpleNamespace(temporal_extents=[])
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

        mock_get_collection.return_value = SimpleNamespace(temporal_extents=[])
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

        mock_get_collection.return_value = SimpleNamespace(temporal_extents=[])
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

        mock_get_collection.return_value = SimpleNamespace(temporal_extents=[])
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
        assert mock_evaluate.call_args.kwargs["granule_ur"] is None

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

    @patch("titiler.cmr.compatibility.evaluate_concept_compatibility")
    def test_endpoint_accepts_granule_ur(self, mock_evaluate, app):
        """Test the /compatibility endpoint forwards the optional granule_ur parameter."""
        mock_evaluate.return_value = CompatibilityResponse(
            concept_id="C1234-TEST",
            backend="xarray",
            datetime=[],
            granule_ur="MOD09A1.A2020001.h12v04.hdf",
            links=[],
        )

        response = app.get(
            "/compatibility?collection_concept_id=C1234-TEST"
            "&granule_ur=MOD09A1.A2020001.h12v04.hdf"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["granule_ur"] == "MOD09A1.A2020001.h12v04.hdf"
        assert mock_evaluate.call_args.kwargs["granule_ur"] == (
            "MOD09A1.A2020001.h12v04.hdf"
        )

    def test_endpoint_openapi_documents_optional_parameters(self, app):
        """Test the /compatibility endpoint documents optional sample controls."""
        response = app.get("/api")

        assert response.status_code == 200
        parameters = response.json()["paths"]["/compatibility"]["get"]["parameters"]
        group_param = next(param for param in parameters if param["name"] == "group")
        assert group_param["description"] == (
            "Select a specific zarr group from a zarr hierarchy. "
            "Could be associated with a zoom level or dataset."
        )
        granule_ur_param = next(
            param for param in parameters if param["name"] == "granule_ur"
        )
        assert granule_ur_param["description"] == "Unique granule record id"
