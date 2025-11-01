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


@pytest.fixture
def reader_options():
    """Fixture for reader options"""
    from titiler.cmr.reader import xarray_open_dataset

    return {"variable": "foo", "opener": xarray_open_dataset}


@pytest.fixture
def expected_opener_options():
    """Expected format for opener_options after transformation"""
    return {
        "s3_credentials": {
            "key": "test_key",
            "secret": "test_secret",
            "token": "test_token",
        }
    }


def setup_backend_with_mock(
    mocker, reader_options, mock_method_name, mock_return_value
):
    """
    Helper to setup backend with mocked Reader.__init__ and asset retrieval method

    Args:
        mocker: pytest-mock mocker fixture
        reader_options: Configuration for the reader
        mock_method_name: Name of the method to mock (e.g., 'assets_for_tile')
        mock_return_value: List of assets to return from the mocked method

    Returns:
        tuple: (backend, mock_init)
    """
    from titiler.xarray.io import Reader

    mock_init = mocker.patch.object(Reader, "__init__", return_value=None)
    backend = CMRBackend(reader=Reader, reader_options=reader_options)
    setattr(backend, mock_method_name, lambda *args, **kwargs: mock_return_value)

    aws_s3_credentials = {
        "accessKeyId": "test_key",
        "secretAccessKey": "test_secret",
        "sessionToken": "test_token",
    }

    # Mock _get_s3_credentials to return test credentials
    mocker.patch.object(backend, "_get_s3_credentials", return_value=aws_s3_credentials)

    return backend, mock_init


def assert_opener_options_passed(mock_init, expected_opener_options):
    """Assert that opener_options were passed correctly to Reader.__init__"""
    mock_init.assert_called()
    call_args, call_kwargs = mock_init.call_args
    assert call_kwargs["opener_options"] == expected_opener_options


@pytest.mark.parametrize(
    "method_name,method_call",
    [
        (
            "tile",
            lambda backend: backend.tile(
                tile_x=0, tile_y=0, tile_z=0, cmr_query={}, bands_regex=""
            ),
        ),
        (
            "part",
            lambda backend: backend.part(
                bbox=(0, 0, 1, 1), cmr_query={}, bands_regex=""
            ),
        ),
        (
            "feature",
            lambda backend: backend.feature(
                shape={"type": "Point", "coordinates": [0, 0]},
                cmr_query={},
                bands_regex="",
            ),
        ),
    ],
)
def test_opener_options_passed_to_reader(
    mocker,
    reader_options,
    expected_opener_options,
    method_name,
    method_call,
):
    """Test that opener_options are passed to the reader in tile/part/feature methods"""
    # Map method names to their corresponding asset retrieval methods
    asset_method_map = {
        "tile": "assets_for_tile",
        "part": "assets_for_bbox",
        "feature": "get_assets",
    }

    mock_assets = [{"url": "s3://test-bucket/test.zarr", "provider": "TEST_PROVIDER"}]
    backend, mock_init = setup_backend_with_mock(
        mocker, reader_options, asset_method_map[method_name], mock_assets
    )

    # Call the method - we expect it to fail after Reader.__init__, which is fine
    try:
        method_call(backend)
    except (AttributeError, Exception):
        pass  # Expected - we only care about __init__ call

    # Verify opener_options were passed correctly
    assert_opener_options_passed(mock_init, expected_opener_options)
