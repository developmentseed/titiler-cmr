"""titiler.cmr tests configuration."""

import os
from typing import Any, Dict, Tuple

import pytest
from fastapi.testclient import TestClient
from geojson_pydantic import Feature, Polygon
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
    if request.host == "testserver":  # This is the default host for TestClient
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
def arctic_bounds() -> Tuple[float, float, float, float]:
    """bbox coordinates for an area in the arctic"""
    return -20.799, 75.011, 14.483, 83.559


@pytest.fixture(scope="function")
def arctic_geojson(arctic_bounds: Tuple[float, float, float, float]) -> Dict[str, Any]:
    """geojson representation of an area in the arctic"""
    return Feature(
        type="Feature",
        properties={},
        geometry=Polygon.from_bounds(*arctic_bounds),
    ).model_dump(exclude_none=True)


@pytest.fixture(scope="function")
def mn_bounds() -> Tuple[float, float, float, float]:
    """bbox coordinates for an area in northern minnesota"""
    return -91.705, 48.179, -91.459, 48.3


@pytest.fixture(scope="function")
def mn_geojson(mn_bounds: Tuple[float, float, float, float]) -> Dict[str, Any]:
    """geojson representation of an area in northern minnesota"""
    return Feature(
        type="Feature",
        properties={},
        geometry=Polygon.from_bounds(*mn_bounds),
    ).model_dump(exclude_none=True)


@pytest.fixture(scope="session")
def xarray_query_params() -> Dict[str, str]:
    """reusable set of query parameters for xarray backend requests"""
    return {
        "backend": "xarray",
        "concept_id": "C2036881735-POCLOUD",
        "variable": "sea_ice_fraction",
        "datetime": "2024-10-11T00:00:01Z/2024-10-11T23:59:59Z",
    }


@pytest.fixture(scope="session")
def rasterio_query_params() -> Dict[str, str]:
    """reusable set of query parameters for rasterio backend requests"""
    return {
        "concept_id": "C2021957657-LPCLOUD",
        "datetime": "2024-10-09T00:00:01Z/2024-10-09T23:59:59Z",
        "backend": "rasterio",
        "bands_regex": "Fmask",
        "bands": "Fmask",
    }
