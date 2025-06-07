"""test titiler-cmr app."""

import io
import warnings
from copy import deepcopy
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path
from typing import Tuple

import pytest
from fastapi.testclient import TestClient
from geojson_pydantic import Feature, Polygon
from httpx import Response
from PIL import Image
from rasterio.errors import NotGeoreferencedWarning

from titiler.cmr.timeseries import TimeseriesMediaType
from titiler.core.models.mapbox import TileJSON


def test_landing(app):
    """Test / endpoint."""
    response = app.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    body = response.json()
    assert body["title"] == "titiler-cmr"
    assert body["links"]

    response = app.get("/?f=html")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "titiler-cmr" in response.text

    # Check accept headers
    response = app.get("/", headers={"accept": "text/html"})
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "titiler-cmr" in response.text

    # accept quality
    response = app.get(
        "/", headers={"accept": "application/json;q=0.9, text/html;q=1.0"}
    )
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "titiler-cmr" in response.text

    # accept quality but only json is available
    response = app.get("/", headers={"accept": "text/csv;q=1.0, application/json"})
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    body = response.json()
    assert body["title"] == "titiler-cmr"

    # accept quality but only json is available
    response = app.get("/", headers={"accept": "text/csv;q=1.0, */*"})
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    body = response.json()
    assert body["title"] == "titiler-cmr"

    # Invalid accept, return default
    response = app.get("/", headers={"accept": "text/htm"})
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    body = response.json()
    assert body["title"] == "titiler-cmr"
    assert body["links"]

    # make sure `?f=` has priority over headers
    response = app.get("/?f=json", headers={"accept": "text/html"})
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    body = response.json()
    assert body["title"] == "titiler-cmr"


def test_docs(app):
    """Test /api endpoint."""
    response = app.get("/api")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    body = response.json()
    assert body["openapi"]

    response = app.get("/api.html")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_conformance(app):
    """Test /conformance endpoint."""
    response = app.get("/conformance")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    body = response.json()
    assert body["conformsTo"]

    response = app.get("/conformance?f=html")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Conformance" in response.text


@pytest.mark.vcr
def test_rasterio_tilejson(app, rasterio_query_params):
    """Test /tilejson.json endpoint for rasterio backend"""

    response = app.get(
        "/WebMercatorQuad/tilejson.json",
        params={
            **rasterio_query_params,
            "datetime": "2024-10-11T00:00:00Z/2024-10-12T23:59:59Z",
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"

    tilejson = response.json()
    assert tilejson["bounds"] == [-180.0, -90.0, 180.0, 90.0]


@pytest.mark.vcr
def test_rasterio_statistics(app, mock_cmr_get_assets, mn_geojson):
    """Test /statistics endpoint for a polygon that straddles the boundary between two HLS granules"""

    concept_id = "C2021957657-LPCLOUD"
    band = "Fmask"
    datetime_range = "2024-10-09T00:00:01Z/2024-10-09T23:59:59Z"

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("ignore", NotGeoreferencedWarning)

        response = app.post(
            "/statistics",
            params={
                "concept_id": concept_id,
                "datetime": datetime_range,
                "backend": "rasterio",
                "bands_regex": band,
                "bands": band,
                "dst_crs": "epsg:32615",
            },
            json=mn_geojson,
        )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/geo+json"
    resp = response.json()
    stats = resp["properties"]["statistics"]
    assert len(stats) == 1

    # numbers corroborated by QGIS zonal stats for these files and polygon
    assert stats[band]["majority"] == 64.0
    assert stats[band]["minority"] == 96.0
    assert stats[band]["sum"] == 19888616.0
    assert round(stats[band]["count"]) == 273132


@pytest.mark.vcr
def test_rasterio_feature(
    app, mock_cmr_get_assets, rasterio_query_params, mn_geojson
) -> None:
    """Test /feature endpoint for rasterio backend"""
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("ignore", NotGeoreferencedWarning)

        response = app.post(
            "/feature",
            params={
                **rasterio_query_params,
                "format": "tif",
                "width": 100,
                "height": 100,
            },
            json=mn_geojson,
        )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/tiff; application=geotiff"


@pytest.mark.vcr
def test_rasterio_part(
    app,
    mock_cmr_get_assets,
    rasterio_query_params,
    mn_bounds: Tuple[float, float, float, float],
) -> None:
    """Test /part endpoint for rasterio backend"""

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("ignore", NotGeoreferencedWarning)
        response = app.get(
            f"/bbox/{','.join(str(coord) for coord in mn_bounds)}/100x100.tif",
            params={
                **rasterio_query_params,
                "format": "tif",
            },
        )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/tiff; application=geotiff"


@pytest.mark.vcr
def test_xarray_tilejson(app, xarray_query_params):
    """Test /tilejson.json endpoint for xarray backend"""

    response = app.get(
        "/WebMercatorQuad/tilejson.json",
        params={
            **xarray_query_params,
            "datetime": "2024-10-11T00:00:00Z/2024-10-12T23:59:59Z",
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"

    tilejson = response.json()
    assert tilejson["bounds"] == [-180.0, -90.0, 180.0, 90.0]


@pytest.mark.vcr
def test_xarray_statistics(
    app, mock_cmr_get_assets, xarray_query_params, arctic_geojson
):
    """Test /statistics endpoint"""
    response = app.post(
        "/statistics",
        params=xarray_query_params,
        json=arctic_geojson,
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/geo+json"
    resp = response.json()
    stats = resp["properties"]["statistics"]
    assert len(stats) == 1

    # numbers corroborated by QGIS zonal stats for this file and polygon
    stats = list(stats.values())[0]
    assert round(stats["median"], 1) == 0.8
    assert round(stats["sum"]) == 2420
    assert round(stats["mean"], 2) == 0.53


@pytest.mark.vcr
def test_xarray_feature(
    app, mock_cmr_get_assets, xarray_query_params, arctic_geojson
) -> None:
    """Test /feature endpoint for xarray backend"""
    response = app.post(
        "/feature",
        params={
            **xarray_query_params,
            "format": "tif",
        },
        json=arctic_geojson,
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/tiff; application=geotiff"


@pytest.mark.vcr
def test_xarray_part(
    app,
    mock_cmr_get_assets,
    xarray_query_params,
    arctic_bounds: Tuple[float, float, float, float],
) -> None:
    """Test /bbox endpoint for xarray backend"""
    response = app.get(
        f"/bbox/{','.join(str(coord) for coord in arctic_bounds)}.tif",
        params={
            **xarray_query_params,
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/tiff; application=geotiff"

    size = (10, 10)
    with pytest.warns():
        response = app.get(
            f"/bbox/{','.join(str(coord) for coord in arctic_bounds)}/{'x'.join(str(x) for x in size)}.png",
            params={
                **xarray_query_params,
            },
        )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"

    image_data = io.BytesIO(response.content)
    image = Image.open(image_data)

    # Check dimensions
    assert image.size == size, f"Expected image size {size}, but got {image.size}"


@pytest.mark.vcr
def test_timeseries_statistics(
    app: TestClient,
    mocker,
    mock_cmr_get_assets,
    xarray_query_params,
    arctic_geojson,
) -> None:
    """Test /timeseries/statistics endpoint

    Since the /timeseries/statistics endpoint sends more requests to internal endpoints
    we need to catch those requests and mock a response since we can't forward them to
    the test client.
    """
    arctic_stats = deepcopy(arctic_geojson)
    arctic_stats["properties"]["statistics"] = {
        "sea_ice_fraction": {
            "min": 0.0,
            "max": 1.0,
            "mean": 0.3,
            "count": 4493.0,
            "sum": 1463.7,
            "std": 0.3,
            "median": 0.0,
            "majority": 0.0,
            "minority": 0.28,
            "unique": 87.0,
            "histogram": [
                [2322, 38, 56, 300, 536, 426, 427, 388],
                [0.0, 0.125, 0.25, 0.375, 0.5, 0.625, 0.75, 0.875, 1.0],
            ],
            "valid_percent": 93.72,
            "masked_pixels": 301.0,
            "valid_pixels": 4493.0,
            "percentile_2": 0.0,
            "percentile_98": 0.99,
        }
    }

    async def mock_timestep_request(url: str, **kwargs) -> Response:
        return Response(
            status_code=200,
            json=arctic_stats,
        )

    mocker.patch("titiler.cmr.timeseries.timestep_request", new=mock_timestep_request)

    response = app.post(
        "/timeseries/statistics",
        params={
            **xarray_query_params,
            "datetime": "2024-10-11T00:00:00Z/2024-10-12T23:59:59Z",
            "step": "P1D",
        },
        json=arctic_geojson,
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/geo+json"

    assert set(response.json()["properties"]["statistics"].keys()) == {
        "2024-10-11T00:00:00+00:00/2024-10-11T23:59:59+00:00",
        "2024-10-12T00:00:00+00:00/2024-10-12T23:59:59+00:00",
    }


def test_timeseries_tilejson(
    app,
    mocker,
    mock_cmr_get_assets,
    xarray_query_params,
    arctic_geojson,
) -> None:
    """Test /timeseries/tilejson endpoint

    Since the /timeseries/tilejson endpoint sends more requests to internal endpoints
    we need to catch those requests and mock a response since we can't forward them to
    the test client.
    """
    arctic_tilejson = TileJSON(
        tiles=["https://testserver/{z}/{x}/{y}"],
        minzoom=0,
        maxzoom=1,
    )

    async def mock_timestep_request(url: str, **kwargs) -> Response:
        return Response(
            status_code=200,
            json=arctic_tilejson.model_dump(exclude_none=True),
        )

    mocker.patch("titiler.cmr.timeseries.timestep_request", new=mock_timestep_request)

    response = app.get(
        "/timeseries/WebMercatorQuad/tilejson.json",
        params={
            **xarray_query_params,
            "datetime": "2024-10-11T00:00:00Z/2024-10-12T23:59:59Z",
            "step": "P1D",
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"

    assert set(response.json().keys()) == {
        "2024-10-11T00:00:00+00:00/2024-10-11T23:59:59+00:00",
        "2024-10-12T00:00:00+00:00/2024-10-12T23:59:59+00:00",
    }


def test_timeseries_gif(
    app,
    mocker,
    mock_cmr_get_assets,
    xarray_query_params,
    arctic_bounds,
) -> None:
    """Test /timeseries/bbox endpoint

    Since the /timeseries/bbox endpoint sends more requests to internal endpoints
    we need to catch those requests and mock a response since we can't forward them to
    the test client.
    """
    png = Path(__file__).resolve().parent.parent / "titiler-cmr.png"
    arctic_png_content = png.read_bytes()

    async def mock_timestep_request(url: str, **kwargs) -> Response:
        return Response(
            status_code=200,
            content=arctic_png_content,
        )

    mocker.patch("titiler.cmr.timeseries.timestep_request", new=mock_timestep_request)

    response = app.get(
        f"/timeseries/bbox/{','.join(str(coord) for coord in arctic_bounds)}.gif",
        params={
            **xarray_query_params,
            "datetime": "2024-10-11T00:00:00Z/2024-10-12T23:59:59Z",
            "step": "P1D",
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == TimeseriesMediaType.gif


def test_unbounded_start(app, xarray_query_params) -> None:
    """Make sure a datetime interval with an unbounded start returns a 400"""
    response = app.get(
        "/timeseries",
        params={
            **xarray_query_params,
            "datetime": "../2024-10-12T23:59:59Z",
            "step": "P1D",
        },
    )

    assert response.status_code == 400


def test_max_datetime(app, xarray_query_params) -> None:
    """Make sure a request that yields too many sub-requests returns a 400"""

    response = app.get(
        "/timeseries",
        params={
            **xarray_query_params,
            "datetime": "2008-10-12T00:00:01Z/2024-10-12T23:59:59Z",
            "step": "P1D",
        },
    )

    assert response.status_code == 400


@pytest.mark.vcr
def test_timeseries_statistics_image_size_limit(
    app,
    global_bounds,
    global_geojson,
):
    """Make sure statistics requests for too large of an AOI return a 400"""
    minx, miny, maxx, maxy = global_bounds
    image_size = (maxx - minx) / 0.01 * (maxy - miny) / 0.01
    size_limit = 1.5e10
    n_days = ceil(size_limit / image_size)
    response = app.post(
        "/timeseries/statistics",
        params={
            "backend": "xarray",
            "concept_id": "C1996881146-POCLOUD",
            "variable": "analysed_sst",
            "datetime": f"2024-01-01T00:00:00Z/2024-01-{n_days}T23:59:59Z",
            "step": "P1D",
        },
        json=global_geojson,
    )

    assert response.status_code == 400
    assert "The AOI for this request is too large" in response.text


def test_timeseries_statistics_request_size_limit(
    app,
):
    """Make sure statistics requests for too large of an AOI x time points return a 400"""
    minx, miny, maxx, maxy = -40, -40, 0, 0
    image_size = (maxx - minx) / 0.01 * (maxy - miny) / 0.01
    size_limit = 1.5e10
    n_days = ceil(size_limit / image_size)

    start_datetime = datetime(year=2011, month=1, day=1, hour=0, minute=0, second=1)
    end_datetime = start_datetime + timedelta(days=n_days)

    large_geojson = Feature(
        type="Feature",
        properties={},
        geometry=Polygon.from_bounds(minx, miny, maxx, maxy),
    ).model_dump(exclude_none=True)

    response = app.post(
        "/timeseries/statistics",
        params={
            "backend": "xarray",
            "concept_id": "C1996881146-POCLOUD",
            "variable": "analysed_sst",
            "datetime": "/".join(
                dt.isoformat() for dt in [start_datetime, end_datetime]
            ),
            "step": "P1D",
        },
        json=large_geojson,
    )

    assert response.status_code == 400
    assert "This request is too large" in response.text


@pytest.mark.vcr
def test_timeseries_bbox_limit(
    app,
    global_bounds,
):
    """Make sure time series image requests that are too large return a 400"""
    minx, miny, maxx, maxy = global_bounds
    image_size = (maxx - minx) / 0.01 * (maxy - miny) / 0.01
    size_limit = 1e8
    n_days = ceil(size_limit / image_size)
    response = app.get(
        f"/timeseries/bbox/{','.join(str(coord) for coord in global_bounds)}.gif",
        params={
            "backend": "xarray",
            "concept_id": "C1996881146-POCLOUD",
            "variable": "analysed_sst",
            "datetime": f"2024-01-01T00:00:00Z/2024-01-{n_days}T23:59:59Z",
            "step": "P1D",
        },
    )

    assert response.status_code == 400
