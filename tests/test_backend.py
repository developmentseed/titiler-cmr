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
