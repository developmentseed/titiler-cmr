"""titiler.cmr tests configuration."""

import os
from collections.abc import Callable, Iterator, Mapping
from typing import Any

import boto3
import moto
import pytest
from fastapi.testclient import TestClient
from geojson_pydantic import Feature, FeatureCollection, Polygon
from httpx import Client
from moto.server import ThreadedMotoServer
from mypy_boto3_s3.service_resource import Bucket, Object, S3ServiceResource
from vcr.request import Request

from titiler.cmr.backend import CMRBackend
from titiler.cmr.query import CMR_GRANULE_SEARCH_API


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


@pytest.fixture(scope="session")
def app():
    """Create a TestClient instance for the app."""
    from titiler.cmr.main import app

    # Do NOT use a context manager so that we do NOT invoke lifespan during testing.
    # Set app state manually since lifespan is skipped.
    app.state.client = Client(base_url=CMR_GRANULE_SEARCH_API)
    app.state.s3_access = False
    app.state.earthdata_token_provider = None
    app.state.get_s3_credentials = None

    return TestClient(app)


@pytest.fixture
def mock_cmr_get_assets(monkeypatch):
    """Replace remote urls with local file paths"""
    original_get_assets = CMRBackend.get_assets

    def mocked_get_assets(*args, **kwargs):
        granules = original_get_assets(*args, **kwargs)

        prefixes = (
            "https://data.lpdaac.earthdatacloud.nasa.gov/",
            "https://archive.podaac.earthdata.nasa.gov/",
        )
        data_dir = os.path.join(os.path.dirname(__file__), "data")

        result = []
        for granule in granules:
            new_related_urls = []
            for ru in granule.related_urls:
                new_url = ru.url
                for prefix in prefixes:
                    if ru.url.startswith(prefix):
                        new_url = ru.url.replace(prefix, f"file://{data_dir}/")
                        break
                new_related_urls.append(ru.model_copy(update={"url": new_url}))
            result.append(granule.model_copy(update={"related_urls": new_related_urls}))

        return result

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
    # Lake Huron bounds (approximate) — same 2.5°×4.5° size as Lake Michigan so that
    # both features produce the same-shaped arrays (titiler.mosaic reuses the same
    # pixel_selection instance across FeatureCollection features).
    lake_huron_bounds = (-84.0, 43.0, -81.5, 47.5)

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
        collection_concept_id: str = "C2036881735-POCLOUD",
        variables: str = "sea_ice_fraction",
        temporal: str = "2024-10-11T00:00:01Z/2024-10-11T23:59:59Z",
        sel: str | None = None,
        sel_method: str | None = None,
    ):
        return {
            "collection_concept_id": collection_concept_id,
            "variables": variables,
            "temporal": temporal,
            **({"sel": sel} if sel else {}),
            **({"sel_method": sel_method} if sel_method else {}),
        }

    return _xarray_query_params


@pytest.fixture(scope="session")
def rasterio_query_params() -> dict[str, str]:
    """reusable set of query parameters for rasterio backend requests"""
    return {
        "collection_concept_id": "C2021957657-LPCLOUD",
        "temporal": "2024-10-09T00:00:01Z/2024-10-09T23:59:59Z",
        "assets_regex": "Fmask",
        "assets": "Fmask",
    }


@pytest.fixture(scope="session")
def fake_tif_bytes() -> bytes:
    """Create a random RGB image as a GeoTIFF."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_bounds

    # Generate synthetic data
    height, width = 256, 256
    red = np.linspace(0, 255, height * width).reshape((height, width)).astype(np.uint8)
    green = np.fliplr(red).copy()
    blue = np.flipud(red).copy()
    array = np.stack([red, green, blue])

    transform = from_bounds(
        west=-180, south=-85.06, east=180, north=85.06, width=width, height=height
    )
    profile = {
        "driver": "GTiff",
        "width": width,
        "height": height,
        "count": 3,
        "crs": "EPSG:4326",
        "transform": transform,
        "dtype": array.dtype,
    }

    with MemoryFile() as memfile:
        with memfile.open(**profile) as dataset:
            dataset.write(array)

        return memfile.read()


@pytest.fixture(scope="session")
def moto_server_netloc():
    """Fixture to run a mocked AWS S3 server for testing."""
    # Note: pass `port=0` to get a random free port.
    server = ThreadedMotoServer(ip_address="localhost", port=0)
    server.start()
    host, port = server.get_host_and_port()

    try:
        yield f"{host}:{port}"
    finally:
        server.stop()


@pytest.fixture
def aws_credentials(monkeypatch: pytest.MonkeyPatch):
    """Mock AWS Credentials for moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def s3_resource(
    aws_credentials, moto_server_netloc: str
) -> Iterator[S3ServiceResource]:
    """Yield a mocked S3 client."""
    with moto.mock_aws():
        yield boto3.resource("s3", endpoint_url=f"http://{moto_server_netloc}")


@pytest.fixture
def test_bucket(s3_resource: S3ServiceResource) -> Bucket:
    """Create a mock bucket named 'test-bucket'."""
    bucket = s3_resource.Bucket("test-bucket")
    bucket.create()

    return bucket


@pytest.fixture
def test_s3_object_tif(fake_tif_bytes: bytes, test_bucket: Bucket) -> Object:
    """Write a dummy ImageData object as a TIF file to an S3 bucket."""
    return test_bucket.put_object(Key="test-file.tif", Body=fake_tif_bytes)


@pytest.fixture
def rasterio_env_kwargs(moto_server_netloc: str) -> Mapping[str, Any]:
    """Return kwargs suitable for rasterio.Env during testing with S3."""

    # These settings allow GDAL to play nicely with Moto.
    return {
        "AWS_HTTPS": False,
        "AWS_VIRTUAL_HOSTING": False,
        "AWS_S3_ENDPOINT": moto_server_netloc,
        "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    }
