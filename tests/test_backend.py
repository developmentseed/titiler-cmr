"""Test backend functions"""

import pytest
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


def test_tile_opener_options_passed_to_reader(mocker):
    """Test that opener_options are passed to the reader in the tile method"""
    from titiler.cmr.reader import xarray_open_dataset
    from titiler.xarray.io import Reader

    # Setup s3 credentials
    aws_s3_credentials = {
        "s3_credentials": {
            "accessKeyId": "test_key",
            "secretAccessKey": "test_secret",
            "sessionToken": "test_token",
        }
    }

    # Setup reader options
    reader_options = {
        "variable": "foo",
        "opener": xarray_open_dataset,
        "opener_options": aws_s3_credentials,
    }

    mock_init = mocker.patch.object(Reader, "__init__", return_value=None)

    # Initialize backend
    backend = CMRBackend(reader=Reader, reader_options=reader_options)

    # Mock assets_for_tile to return test assets
    def mock_assets_for_tile(*args, **kwargs):
        return [{"url": "s3://test-bucket/test.zarr", "provider": "TEST_PROVIDER"}]

    backend.assets_for_tile = mock_assets_for_tile

    # This will fail somewhere after Reader.__init__, but we don't care
    try:
        backend.tile(
            tile_x=0,
            tile_y=0,
            tile_z=0,
            cmr_query={},
            bands_regex="",
            **aws_s3_credentials,
        )
    except (AttributeError, Exception):
        pass  # Expected - we only care about __init__ call

    # Verify what we care about
    mock_init.assert_called()
    call_args, call_kwargs = mock_init.call_args
    assert call_kwargs["opener_options"] == {
        "s3_credentials": {
            "key": "test_key",
            "secret": "test_secret",
            "token": "test_token",
        }
    }
