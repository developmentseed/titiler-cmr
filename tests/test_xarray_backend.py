"""Test titiler-cmr xarray backend."""

import io
from copy import deepcopy
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path
from typing import Tuple
from urllib.parse import urlencode

import pytest
from fastapi.testclient import TestClient
from geojson_pydantic import Feature, Polygon
from httpx import Response
from PIL import Image

from titiler.cmr.timeseries import TimeseriesMediaType
from titiler.core.models.mapbox import TileJSON


@pytest.mark.vcr
def test_xarray_tilejson(app, xarray_query_params):
    """Test /tilejson.json endpoint for xarray backend"""

    response = app.get(
        "/WebMercatorQuad/tilejson.json",
        params={
            **xarray_query_params(),
            "datetime": "2024-10-11T00:00:00Z/2024-10-12T23:59:59Z",
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"

    tilejson = response.json()
    assert tilejson["bounds"] == [-180.0, -90.0, 180.0, 90.0]


@pytest.mark.vcr
def test_xarray_tilejson_with_sel(app, xarray_query_params):
    """Test /tilejson.json endpoint for xarray backend"""
    datetime = "2010-01-01T00:00:00"
    sel = [f"time={datetime}", "lev=1000"]
    sel_method = "nearest"

    response = app.get(
        "/WebMercatorQuad/tilejson.json",
        params={
            **xarray_query_params(
                concept_id="C2837626477-GES_DISC",
                variable="o3",
                datetime=datetime,
                sel=sel,
                sel_method=sel_method,
            ),
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"

    tilejson = response.json()
    assert urlencode({"sel": sel[0]}) in tilejson["tiles"][0]
    assert urlencode({"sel": sel[1]}) in tilejson["tiles"][0]
    assert urlencode({"sel_method": sel_method}) in tilejson["tiles"][0]


@pytest.mark.vcr
@pytest.mark.parametrize("geojson_fixture", ["arctic_geojson", "great_lakes_geojson"])
def test_xarray_statistics(
    app, mock_cmr_get_assets, xarray_query_params, request, geojson_fixture
):
    """Test /statistics endpoint with both Feature and FeatureCollection"""
    geojson = request.getfixturevalue(geojson_fixture)

    response = app.post(
        "/statistics",
        params=xarray_query_params(),
        json=geojson,
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/geo+json"
    resp = response.json()

    # Handle both Feature and FeatureCollection responses
    if resp["type"] == "FeatureCollection":
        # For FeatureCollection, check that we get statistics for each feature
        assert len(resp["features"]) == 2  # Lake Michigan and Lake Huron
        for feature in resp["features"]:
            assert "properties" in feature
            assert "statistics" in feature["properties"]
            stats = feature["properties"]["statistics"]

            assert len(stats) == 1
            stats = list(stats.values())[0]
            assert round(stats["median"], 1) == 0
            assert round(stats["sum"]) == 0
            assert round(stats["mean"], 2) == 0
    else:
        # For single Feature, check the properties directly
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
            **xarray_query_params(),
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
            **xarray_query_params(),
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/tiff; application=geotiff"

    size = (10, 10)
    with pytest.warns():
        response = app.get(
            f"/bbox/{','.join(str(coord) for coord in arctic_bounds)}/{'x'.join(str(x) for x in size)}.png",
            params={
                **xarray_query_params(),
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
            **xarray_query_params(),
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
            **xarray_query_params(),
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
            **xarray_query_params(),
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
            **xarray_query_params(),
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
            **xarray_query_params(),
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
