"""titiler.cmr tests configuration."""

import json
import os
from typing import Any, Dict, Tuple
from urllib.parse import urlparse

import httpx
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
            "https://data.gesdisc.earthdata.nasa.gov/",
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


class MockGranule:
    """Mock class for earthaccess granule results"""

    def __init__(self, data_links_urls, provider_id="GES_DISC"):
        """init MockGranule"""
        self.data_links_urls = data_links_urls
        self.meta = {"provider-id": provider_id}

    def data_links(self, access=None):
        """get data links"""
        return self.data_links_urls

    def __getitem__(self, key):
        """define get method"""
        if key == "meta":
            return self.meta
        raise KeyError(key)


@pytest.fixture
def mock_earthaccess_search_data(monkeypatch):
    """Mock earthaccess.search_data to return consistent test data"""
    import os

    import earthaccess

    # Path to test data file
    test_data_path = os.path.join(
        os.path.dirname(__file__),
        "data/data/TCR2_MON_VERTCONCS/TRPSCRO3M3D.1/TROPESS_reanalysis_mon_o3_2021.nc",
    )
    file_url = f"file://{test_data_path}"

    def mock_search_data(*args, **kwargs):
        # Return mock granules that point to our test data file
        return [MockGranule([file_url])]

    monkeypatch.setattr(earthaccess, "search_data", mock_search_data)


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
def global_bounds() -> Tuple[float, float, float, float]:
    """bbox coordinates for the globe"""
    return -180, -90, 180, 90


@pytest.fixture(scope="function")
def global_geojson(global_bounds: Tuple[float, float, float, float]) -> Dict[str, Any]:
    """geojson representation of the whole globe"""
    return Feature(
        type="Feature",
        properties={},
        geometry=Polygon.from_bounds(*global_bounds),
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


@pytest.fixture(scope="function")
def great_lakes_geojson() -> Dict[str, Any]:
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
def xarray_query_params() -> Dict[str, str]:
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
def rasterio_query_params() -> Dict[str, str]:
    """reusable set of query parameters for rasterio backend requests"""
    return {
        "concept_id": "C2021957657-LPCLOUD",
        "datetime": "2024-10-09T00:00:01Z/2024-10-09T23:59:59Z",
        "backend": "rasterio",
        "bands_regex": "Fmask",
        "bands": "Fmask",
    }


@pytest.fixture(scope="session")
def tropess_query_params() -> Dict[str, Any]:
    """reusable set of query parameters for the tropess dataset"""

    return {
        "backend": "xarray",
        "concept_id": "C2837626477-GES_DISC",
        "variable": "o3",
        "datetime": "2021-01-01T00:00:01Z/2021-02-28T23:59:59Z",
        "step": "P1M",
        "temporal_mode": "point",
        "use_sel_for_datetime": True,
        "sel_time_method": True,
    }


def _create_mock_response(tc_response):
    """Create a mock httpx.Response-like object from TestClient response"""

    class MockResponse:
        def __init__(self, tc_response):
            self.status_code = tc_response.status_code
            self.headers = tc_response.headers
            self.content = tc_response.content
            self._json = None

        def json(self):
            if self._json is None:
                self._json = json.loads(self.content)
            return self._json

    return MockResponse(tc_response)


def _make_sync_request(app, method: str, path: str, **kwargs):
    """Execute synchronous request using TestClient"""
    if method == "GET":
        return app.get(path)
    elif method == "POST":
        json_data = kwargs.get("json")
        return app.post(path, json=json_data)
    else:
        raise ValueError(f"{method} not supported for testserver requests")


async def _handle_testserver_request(app, url: str, method: str, **kwargs):
    """Handle requests to testserver using TestClient"""
    import asyncio
    from concurrent.futures import ThreadPoolExecutor

    parsed_url = urlparse(url)
    path = parsed_url.path
    if parsed_url.query:
        path += f"?{parsed_url.query}"

    def sync_request():
        return _make_sync_request(app, method, path, **kwargs)

    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        response = await loop.run_in_executor(executor, sync_request)

    return _create_mock_response(response)


async def _handle_http_request(url: str, method: str, **kwargs):
    """Handle regular HTTP requests using httpx"""
    async with httpx.AsyncClient() as client:
        if method == "POST":
            _method = client.post
        elif method == "GET":
            _method = client.get
        else:
            raise ValueError(f"{method} must be one of GET or POST")

        return await _method(url, **kwargs)


@pytest.fixture
def patch_timestep_request(app, monkeypatch):
    """Patch timestep_request to use TestClient for testserver requests"""
    from titiler.cmr import timeseries

    async def patched_timestep_request(url: str, method: str, **kwargs):
        """Route testserver requests through TestClient, others through normal HTTP"""
        parsed_url = urlparse(url)

        if parsed_url.hostname == "testserver":
            return await _handle_testserver_request(app, url, method, **kwargs)
        else:
            return await _handle_http_request(url, method, **kwargs)

    monkeypatch.setattr(timeseries, "timestep_request", patched_timestep_request)
