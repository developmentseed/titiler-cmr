"""Test backend functions"""

import pytest
import typing as t
from collections.abc import Callable, Mapping

from earthaccess.results import DataGranule
from mypy_boto3_s3.service_resource import Object
from rio_tiler.models import ImageData

from titiler.cmr.backend import Access, AWSCredentials, CMRBackend


@pytest.mark.vcr
@pytest.mark.parametrize(
    "access,expectation", [("direct", "s3"), ("external", "https")]
)
def test_get_assets(access: Access, expectation: str) -> None:
    """Test fetching asset metadata from CMR"""
    bbox = (-91.663, 47.862, -91.537, 47.928)
    band = "B01"
    with CMRBackend() as backend:
        assets = backend.get_assets(
            *bbox,
            access=access,
            bands_regex=band,
            concept_id="C2021957657-LPCLOUD",
            temporal=("2024-02-11", "2024-02-13"),
        )

    asset = assets.pop(0)
    assert asset
    asset_url = asset.get("url")
    assert asset_url
    assert isinstance(asset_url, dict)
    assert asset_url[band].startswith(expectation)


def stub_find_granules(count: int = -1, **kwargs: t.Any) -> list[DataGranule]:
    """Return a list of stubbed DataGranule objects for testing CMR interactions.

    This helper simulates granule search results without requiring real CMR queries.

    Args:
        count: Unused parameter representing the desired number of granules.
        **kwargs: Additional keyword arguments matching the real find_granules signature.

    Returns:
        A list containing a single DataGranule with minimal related URL metadata.
    """
    return [
        DataGranule(
            {
                "meta": {"provider-id": "TEST_PROVIDER"},
                "umm": {
                    "CollectionReference": {},
                    "SpatialExtent": {},
                    "TemporalExtent": {},
                    "RelatedUrls": [
                        {
                            "Type": "GET DATA VIA DIRECT ACCESS",
                            "URL": "s3://test-bucket/test-file.tif",
                        }
                    ],
                },
            }
        )
    ]


@pytest.mark.parametrize(
    "method_call",
    [
        lambda self: self.tile(
            # self,
            tile_x=0,
            tile_y=0,
            tile_z=0,
            cmr_query={},
            access="direct",
        ),
        lambda self: self.part(
            # self,
            bbox=(0, 0, 1, 1),
            cmr_query={},
            access="direct",
        ),
        lambda self: self.feature(
            # self,
            shape={"type": "LineString", "coordinates": [[-10, -10], [10, 10]]},
            cmr_query={},
            access="direct",
        ),
    ],
    ids=["tile", "part", "feature"],
)
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_s3_credentials_used_for_session_creation(
    method_call: Callable,
    rasterio_env_kwargs: Mapping[str, t.Any],
    # Ensures s3://test-bucket/test-file.tif is written to Moto's mock bucket
    test_s3_object_tif: Object,
) -> None:
    """Test that s3_credentials from _get_s3_credentials are used to create AWS session."""

    called_get_s3_credentials = False

    def mock_get_s3_credentials(provider: str) -> AWSCredentials:
        nonlocal called_get_s3_credentials

        called_get_s3_credentials = True

        return {
            "accessKeyId": "test_access_key",
            "secretAccessKey": "test_secret_key",
            "sessionToken": "test_session_token",
            "expiration": "test_expiration",
        }

    with CMRBackend(
        get_s3_credentials=mock_get_s3_credentials,
        find_granules=stub_find_granules,
        rasterio_env_kwargs=rasterio_env_kwargs,
    ) as backend:
        image_data: ImageData
        image_data, assets = method_call(backend)
        expected_assets = [
            {"url": "s3://test-bucket/test-file.tif", "provider": "TEST_PROVIDER"}
        ]

        assert called_get_s3_credentials
        assert assets == expected_assets
        assert image_data.assets == expected_assets
        assert image_data.data.ndim == 3  # bands, height, width
        assert image_data.data.shape[0] == 3  # Number of bands in tif
