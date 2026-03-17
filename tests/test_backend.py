"""Test backend functions"""

import typing as t
import unittest.mock as mock
from collections.abc import Callable, Mapping

import pytest
import rasterio
from geojson_pydantic import Polygon
from httpx import Client
from mypy_boto3_s3.service_resource import Object
from rio_tiler.models import ImageData

from rio_tiler.errors import NoAssetFoundError

from titiler.cmr.backend import CMRBackend
from titiler.cmr.models import (
    Granule,
    GranuleSearch,
    GranuleSpatialExtent,
    RelatedUrl,
)
from titiler.cmr.query import CMR_GRANULE_SEARCH_API
from titiler.cmr.reader import MultiBaseGranuleReader


def test_bounds_granule_ur_not_found() -> None:
    """Test that NoAssetFoundError is raised when granule_ur search returns no results."""
    with mock.patch("titiler.cmr.backend.get_granules", return_value=iter([])):
        backend = CMRBackend(
            input=GranuleSearch(granule_ur="nonexistent-granule-ur"),
            client=Client(base_url=CMR_GRANULE_SEARCH_API),
            reader=MultiBaseGranuleReader,
        )
        with pytest.raises(NoAssetFoundError):
            _ = backend.bounds


@pytest.mark.vcr
def test_get_assets() -> None:
    """Test fetching asset metadata from CMR"""
    backend = CMRBackend(
        input=GranuleSearch(
            collection_concept_id="C2021957657-LPCLOUD",
            temporal="2024-02-11T00:00:00Z/2024-02-13T23:59:59Z",
        ),
        client=Client(base_url=CMR_GRANULE_SEARCH_API),
        reader=MultiBaseGranuleReader,
        s3_access=True,
    )
    granules = backend.assets_for_bbox(-91.663, 47.862, -91.537, 47.928)

    assert granules
    granule = granules[0]
    assets = granule.get_assets(regex="B01")
    assert "B01" in assets


def stub_get_granules() -> list[Granule]:
    """Return a list of stubbed Granule objects for testing CMR interactions."""
    return [
        Granule(
            id="test-granule-id",
            granule_ur="test-granule-ur",
            collection_concept_id="TEST_COLLECTION",
            related_urls=[
                RelatedUrl(
                    **{
                        "URL": "s3://test-bucket/test-file.tif",
                        "Type": "GET DATA VIA DIRECT ACCESS",
                    }
                ),
                RelatedUrl(
                    **{"URL": "https://foo.bar/test-file.tif", "Type": "GET DATA"}
                ),
                RelatedUrl(
                    **{
                        "URL": "https://foo.bar/s3credentials",
                        "Type": "VIEW RELATED INFORMATION",
                        "Description": "api endpoint to retrieve temporary credentials",
                    }
                ),
            ],
            spatial_extent=GranuleSpatialExtent(
                **{
                    "HorizontalSpatialDomain": {
                        "Geometry": {
                            "BoundingRectangles": [
                                {
                                    "WestBoundingCoordinate": -10,
                                    "EastBoundingCoordinate": 10,
                                    "NorthBoundingCoordinate": 1,
                                    "SouthBoundingCoordinate": 0,
                                }
                            ]
                        }
                    }
                }
            ),
        )
    ]


@pytest.mark.parametrize(
    "method_call",
    [
        lambda self: self.tile(0, 0, 0),
        lambda self: self.part(
            bbox=(0, 0, 1, 1),
        ),
        lambda self: self.feature(
            shape={"type": "LineString", "coordinates": [[-10, -10], [10, 10]]},
        ),
    ],
    ids=["tile", "part", "feature"],
)
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.filterwarnings("ignore::UserWarning")
def test_s3_credentials_used_for_session_creation(
    method_call: Callable,
    rasterio_env_kwargs: Mapping[str, t.Any],
    # Ensures s3://test-bucket/test-file.tif is written to Moto's mock bucket
    test_s3_object_tif: Object,
) -> None:
    """Test that s3_credentials from get_s3_credentials are used to create AWS session."""

    called_get_s3_credentials = False

    def mock_get_s3_credentials(endpoint: str):
        nonlocal called_get_s3_credentials
        called_get_s3_credentials = True

        def provider():
            return {
                "access_key_id": "key",
                "secret_access_key": "secret",
                "token": "token",
            }

        return provider

    with mock.patch(
        "titiler.cmr.backend.get_granules", return_value=stub_get_granules()
    ):
        backend = CMRBackend(
            input=GranuleSearch(),
            client=Client(base_url=CMR_GRANULE_SEARCH_API),
            reader=MultiBaseGranuleReader,
            s3_access=True,
            get_s3_credentials=mock_get_s3_credentials,
        )
        with rasterio.Env(**rasterio_env_kwargs):
            image_data: ImageData
            image_data, _ = method_call(backend)

    assert called_get_s3_credentials
    assert image_data.data.ndim == 3  # bands, height, width
    assert image_data.data.shape[0] == 3  # Number of bands in tif


def _make_backend() -> CMRBackend:
    """Return a CMRBackend with no-op CMR client for unit testing."""
    return CMRBackend(
        input=GranuleSearch(),
        client=Client(base_url=CMR_GRANULE_SEARCH_API),
        reader=MultiBaseGranuleReader,
    )


def test_get_assets_forwards_skipcovered_true() -> None:
    """get_assets passes skipcovered=True through to get_granules."""
    with mock.patch("titiler.cmr.backend.get_granules", return_value=iter([])) as mg:
        _make_backend().get_assets(Polygon.from_bounds(0, 0, 1, 1), skipcovered=True)

    _, kwargs = mg.call_args
    assert kwargs.get("skipcovered") is True


def test_get_assets_forwards_skipcovered_false() -> None:
    """get_assets passes skipcovered=False through to get_granules."""
    with mock.patch("titiler.cmr.backend.get_granules", return_value=iter([])) as mg:
        _make_backend().get_assets(Polygon.from_bounds(0, 0, 1, 1), skipcovered=False)

    _, kwargs = mg.call_args
    assert kwargs.get("skipcovered") is False


def test_get_assets_skipcovered_none_not_forwarded() -> None:
    """When skipcovered=None, get_granules is not given a skipcovered kwarg."""
    with mock.patch("titiler.cmr.backend.get_granules", return_value=iter([])) as mg:
        _make_backend().get_assets(Polygon.from_bounds(0, 0, 1, 1), skipcovered=None)

    _, kwargs = mg.call_args
    assert "skipcovered" not in kwargs
