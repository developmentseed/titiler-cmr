"""titiler.cmr tests configuration."""

import os
from collections.abc import Callable
from typing import Any

import pytest
from fastapi.testclient import TestClient
from geojson_pydantic import Feature, FeatureCollection, Polygon
from vcr.request import Request

from titiler.cmr.backend import CMRBackend
from titiler.cmr.settings import AuthSettings

# Create a custom AuthSettings instance
custom_auth_settings = AuthSettings(
    strategy="iam",
    access="external",
)


def before_record_cb(request: Request):
    """Do not cache requests to the test client"""
    # testserver is the default host for TestClient
    # find_or_create_token is an EDL API call, but we do NOT want to record tokens
    if request.host == "testserver" or request.path.endswith("find_or_create_token"):
        return None
    return request


@pytest.fixture(scope="session")
def vcr_config():
    """Do not cache requests to the test client"""
    return {
        "filter_headers": [("authorization", "DUMMY")],
        "before_record_request": before_record_cb,
    }


@pytest.fixture(scope="session", autouse=True)
def override_auth_settings(session_mocker):
    """Override AuthSettings for all tests."""
    session_mocker.patch(
        "titiler.cmr.settings.AuthSettings", return_value=custom_auth_settings
    )
    session_mocker.patch("titiler.cmr.backend.s3_auth_config", custom_auth_settings)


@pytest.fixture(scope="session")
def app():
    """Create a TestClient instance for the app."""
    from titiler.cmr.main import app

    with TestClient(app) as client:
        yield client


@pytest.fixture
def mock_cmr_get_assets(monkeypatch):
    """Replace remote urls with local file paths"""
    original_get_assets = CMRBackend.get_assets

    def mocked_get_assets(*args, **kwargs):
        assets = original_get_assets(*args, **kwargs)

        prefixes = (
            "https://data.lpdaac.earthdatacloud.nasa.gov/",
            "https://archive.podaac.earthdata.nasa.gov/",
        )
        data_dir = os.path.join(os.path.dirname(__file__), "data")
        for asset in assets:
            if isinstance(asset["url"], dict):
                for band, url in asset["url"].items():
                    for prefix in prefixes:
                        if url.startswith(prefix):
                            asset["url"][band] = url.replace(
                                prefix, f"file://{data_dir}/"
                            )
            elif isinstance(asset["url"], str):
                for prefix in prefixes:
                    if asset["url"].startswith(prefix):
                        asset["url"] = asset["url"].replace(
                            prefix, f"file://{data_dir}/"
                        )

        return assets

    monkeypatch.setattr(CMRBackend, "get_assets", mocked_get_assets)


@pytest.fixture(scope="function")
def arctic_bounds() -> tuple[float, float, float, float]:
    """bbox coordinates for an area in the arctic"""
    return -20.799, 75.011, 14.483, 83.559


@pytest.fixture(scope="function")
def arctic_geojson(arctic_bounds: tuple[float, float, float, float]) -> dict[str, Any]:
    """geojson representation of an area in the arctic"""
    return Feature(
        type="Feature",
        properties={},
        geometry=Polygon.from_bounds(*arctic_bounds),
    ).model_dump(exclude_none=True)


@pytest.fixture(scope="function")
def global_bounds() -> tuple[float, float, float, float]:
    """bbox coordinates for the globe"""
    return -180, -90, 180, 90


@pytest.fixture(scope="function")
def global_geojson(global_bounds: tuple[float, float, float, float]) -> dict[str, Any]:
    """geojson representation of the whole globe"""
    return Feature(
        type="Feature",
        properties={},
        geometry=Polygon.from_bounds(*global_bounds),
    ).model_dump(exclude_none=True)


@pytest.fixture(scope="function")
def mn_bounds() -> tuple[float, float, float, float]:
    """bbox coordinates for an area in northern minnesota"""
    return -91.705, 48.179, -91.459, 48.3


@pytest.fixture(scope="function")
def mn_geojson(mn_bounds: tuple[float, float, float, float]) -> dict[str, Any]:
    """geojson representation of an area in northern minnesota"""
    return Feature(
        type="Feature",
        properties={},
        geometry=Polygon.from_bounds(*mn_bounds),
    ).model_dump(exclude_none=True)


@pytest.fixture(scope="function")
def great_lakes_geojson() -> dict[str, Any]:
    """geojson FeatureCollection representation of Lake Michigan and Lake Huron"""
    # Lake Michigan bounds (approximate)
    lake_michigan_bounds = (-87.5, 41.5, -85.0, 46.0)
    # Lake Huron bounds (approximate)
    lake_huron_bounds = (-84.0, 43.0, -79.0, 46.0)

    lake_michigan = Feature(
        type="Feature",
        properties={"name": "Lake Michigan"},
        geometry=Polygon.from_bounds(*lake_michigan_bounds),
    )

    lake_huron = Feature(
        type="Feature",
        properties={"name": "Lake Huron"},
        geometry=Polygon.from_bounds(*lake_huron_bounds),
    )

    return FeatureCollection(
        type="FeatureCollection",
        features=[lake_michigan, lake_huron],
    ).model_dump(exclude_none=True)


@pytest.fixture(scope="session")
def xarray_query_params() -> Callable[..., dict[str, str]]:
    """reusable set of query parameters for xarray backend requests"""

    def _xarray_query_params(
        concept_id: str = "C2036881735-POCLOUD",
        variable: str = "sea_ice_fraction",
        datetime: str = "2024-10-11T00:00:01Z/2024-10-11T23:59:59Z",
        sel: str | None = None,
        sel_method: str | None = None,
    ):
        return {
            "backend": "xarray",
            "concept_id": concept_id,
            "variable": variable,
            "datetime": datetime,
            **({"sel": sel} if sel else {}),
            **({"sel_method": sel_method} if sel_method else {}),
        }

    return _xarray_query_params


@pytest.fixture(scope="session")
def rasterio_query_params() -> dict[str, str]:
    """reusable set of query parameters for rasterio backend requests"""
    return {
        "concept_id": "C2021957657-LPCLOUD",
        "datetime": "2024-10-09T00:00:01Z/2024-10-09T23:59:59Z",
        "backend": "rasterio",
        "bands_regex": "Fmask",
        "bands": "Fmask",
    }
