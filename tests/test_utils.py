"""Tests for utils.py"""

import pytest
from httpx import Client

from titiler.cmr.query import CMR_GRANULE_SEARCH_API
from titiler.cmr.utils import calculate_time_series_request_size


@pytest.mark.vcr
def test_calculate_request_size_no_resolution() -> None:
    """If a concept_id does not include the horizontal resolution in the UMM metadata
    it should return 0"""
    request_size = calculate_time_series_request_size(
        concept_id="C2723754864-GES_DISC",
        client=Client(base_url=CMR_GRANULE_SEARCH_API),
        n_time_steps=2,
        minx=-180,
        miny=-90,
        maxx=180,
        maxy=90,
        coord_crs="epsg:4326",
    )

    assert request_size == 0


@pytest.mark.vcr
def test_calculate_request_size() -> None:
    """A concept_id that DOES include the horizontal resolution will return a positive value"""
    request_size = calculate_time_series_request_size(
        concept_id="C2036881735-POCLOUD",
        client=Client(base_url=CMR_GRANULE_SEARCH_API),
        n_time_steps=2,
        minx=-180,
        miny=-90,
        maxx=180,
        maxy=90,
        coord_crs="epsg:4326",
    )

    assert 0 < request_size < 1e10
