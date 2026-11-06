"""Tests for utils.py"""

import pytest
from httpx2 import Client

from titiler.cmr.query import CMR_GRANULE_SEARCH_API
from titiler.cmr.utils import get_aoi_native_shape


@pytest.mark.vcr
def test_aoi_native_shape_no_resolution() -> None:
    """The shape is None for a collection whose metadata does not include a resolution."""
    with Client(base_url=CMR_GRANULE_SEARCH_API) as client:
        shape = get_aoi_native_shape(
            concept_id="C2723754864-GES_DISC",
            client=client,
            minx=-180,
            miny=-90,
            maxx=180,
            maxy=90,
            coord_crs="epsg:4326",
        )

    assert shape is None


@pytest.mark.vcr
def test_aoi_native_shape() -> None:
    """The globe at GAMSSA's native 0.25 degree is 720 x 1440 pixels."""
    with Client(base_url=CMR_GRANULE_SEARCH_API) as client:
        shape = get_aoi_native_shape(
            concept_id="C2036881735-POCLOUD",
            client=client,
            minx=-180,
            miny=-90,
            maxx=180,
            maxy=90,
            coord_crs="epsg:4326",
        )

    assert shape == (720, 1440)


@pytest.mark.vcr
def test_aoi_native_shape_beyond_the_globe() -> None:
    """An AOI that extends past the globe is sized as the globe."""
    with Client(base_url=CMR_GRANULE_SEARCH_API) as client:
        shape = get_aoi_native_shape(
            concept_id="C2036881735-POCLOUD",
            client=client,
            minx=-1e308,
            miny=-90,
            maxx=1e308,
            maxy=90,
            coord_crs="epsg:4326",
        )

    assert shape == (720, 1440)
