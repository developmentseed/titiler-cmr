"""Test backend functions"""

from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from rio_tiler.models import ImageData

from titiler.cmr.backend import Access, CMRBackend


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


def test_s3_credentials_used_for_session_creation() -> None:
    """Test that s3_credentials from _get_s3_credentials are used to create AWS session."""
    from rio_tiler.io import Reader

    # Mock s3 credentials that would be returned by _get_s3_credentials
    mock_s3_credentials = {
        "accessKeyId": "test_access_key",
        "secretAccessKey": "test_secret_key",
        "sessionToken": "test_session_token",
    }

    # Mock asset that would be returned by assets_for_tile
    mock_asset = {
        "url": "s3://test-bucket/test-file.tif",
        "provider": "TEST_PROVIDER",
    }

    # Create a proper ImageData object to return from tile
    mock_image_data = ImageData(
        np.zeros((3, 256, 256), dtype=np.uint8)  # RGB image
    )

    # Create a mock class that will pass isinstance checks
    class MockReader:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            mock_instance = MagicMock()
            mock_instance.tile.return_value = mock_image_data
            return mock_instance

        def __eq__(other):
            # Make MockReader == Reader return True
            if other is Reader:
                return True
            return super().__eq__(other)

        def __exit__(self, *args):
            pass

    with CMRBackend(reader=MockReader) as backend:
        # Mock assets_for_tile to return our test asset
        with (
            patch.object(backend, "assets_for_tile", return_value=[mock_asset]),
            patch.object(
                backend, "_get_s3_credentials", return_value=mock_s3_credentials
            ) as mock_get_creds,
            patch.object(backend, "_create_aws_session") as mock_create_session,
            patch("rasterio.Env"),
        ):
            # Mock the session to return a valid context manager
            mock_session = MagicMock()
            mock_create_session.return_value = mock_session

            # Call tile, which should trigger the credential flow
            backend.tile(
                0,
                0,
                0,
                cmr_query={
                    "concept_id": "C2021957657-LPCLOUD",
                    "temporal": ("2024-02-11", "2024-02-13"),
                },
            )

        # Verify that _get_s3_credentials was called with the asset
        mock_get_creds.assert_called_once_with(mock_asset)

        # Verify that _create_aws_session was called with the credentials
        # returned by _get_s3_credentials
        mock_create_session.assert_called_once_with(mock_s3_credentials)
