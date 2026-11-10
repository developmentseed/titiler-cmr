"""Test titiler-cmr xarray backend."""

import io
from datetime import datetime, timedelta
from math import ceil
from typing import Tuple
from urllib.parse import urlencode

import pytest
from fastapi.testclient import TestClient
from geojson_pydantic import Feature, Polygon
from PIL import Image

from titiler.cmr.timeseries import TimeseriesMediaType


@pytest.mark.vcr
def test_xarray_tilejson(app, xarray_query_params):
    """Test /tilejson.json endpoint for xarray backend"""

    response = app.get(
        "xarray/WebMercatorQuad/tilejson.json",
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
    """Test /xarray/tilejson.json endpoint for xarray backend"""
    datetime = "2010-01-01T00:00:00"
    sel = [f"time=nearest::{datetime}", "lev=1000"]

    response = app.get(
        "/xarray/WebMercatorQuad/tilejson.json",
        params={
            **xarray_query_params(
                collection_concept_id="C2837626477-GES_DISC",
                variables="o3",
                temporal=datetime,
                sel=sel,
            ),
        },
    )
    print(response.text)
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"

    tilejson = response.json()
    assert urlencode({"sel": sel[0]}) in tilejson["tiles"][0]
    assert urlencode({"sel": sel[1]}) in tilejson["tiles"][0]


@pytest.mark.vcr
@pytest.mark.parametrize("geojson_fixture", ["arctic_geojson", "great_lakes_geojson"])
def test_xarray_statistics(
    app, mock_cmr_get_assets, xarray_query_params, request, geojson_fixture
):
    """Test /statistics endpoint with both Feature and FeatureCollection"""
    geojson = request.getfixturevalue(geojson_fixture)

    response = app.post(
        "/xarray/statistics",
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
        assert round(stats["sum"]) == 2377
        assert round(stats["mean"], 2) == 0.52


@pytest.mark.vcr("test_xarray_statistics[arctic_geojson].yaml")
def test_xarray_statistics_expression_mask(
    app, mock_cmr_get_assets, xarray_query_params, arctic_geojson
):
    """A where() expression keeps masked pixels out of /statistics."""
    response = app.post(
        "/xarray/statistics",
        params={
            **xarray_query_params(),
            "variables": ["analysed_sst", "sea_ice_fraction"],
            "expression": "where(b2 < 0.15, b1, nan)",
        },
        json=arctic_geojson,
    )

    assert response.status_code == 200
    stats = response.json()["properties"]["statistics"]["b1"]
    assert stats["count"] == 1723  # 4542 for analysed_sst without the mask


@pytest.mark.vcr("test_xarray_statistics[arctic_geojson].yaml")
def test_xarray_statistics_unknown_variable(
    app, mock_cmr_get_assets, xarray_query_params, arctic_geojson
):
    """An unknown variable reaches the client as a 400 with xarray's message."""
    response = app.post(
        "/xarray/statistics",
        params={**xarray_query_params(), "variables": ["analysed_sst", "wieght"]},
        json=arctic_geojson,
    )

    assert response.status_code == 400
    assert "No variable named 'wieght'" in response.json()["detail"]


@pytest.mark.vcr
def test_xarray_feature(
    app, mock_cmr_get_assets, xarray_query_params, arctic_geojson
) -> None:
    """Test /feature endpoint for xarray backend"""
    response = app.post(
        "/xarray/feature",
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
        f"/xarray/bbox/{','.join(str(coord) for coord in arctic_bounds)}.tif",
        params={
            **xarray_query_params(),
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/tiff; application=geotiff"

    size = (10, 10)
    with pytest.warns():
        response = app.get(
            f"/xarray/bbox/{','.join(str(coord) for coord in arctic_bounds)}/{'x'.join(str(x) for x in size)}.png",
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
    mock_cmr_get_assets,
    xarray_query_params,
    arctic_geojson,
) -> None:
    """Test /xarray/timeseries/statistics endpoint"""
    response = app.post(
        "/xarray/timeseries/statistics",
        params={
            **xarray_query_params(),
            # Each interval stays inside one day, because an interval that
            # touches midnight also matches the neighboring day's granule.
            "temporal": (
                "2024-10-11T06:00:00Z/2024-10-11T18:00:00Z,"
                "2024-10-12T06:00:00Z/2024-10-12T18:00:00Z"
            ),
            "step": "P1D",
        },
        json=arctic_geojson,
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/geo+json"

    statistics = response.json()["properties"]["statistics"]
    assert set(statistics.keys()) == {
        "2024-10-11T06:00:00+00:00/2024-10-11T18:00:00+00:00",
        "2024-10-12T06:00:00+00:00/2024-10-12T18:00:00+00:00",
    }
    # The steps read different days' granules, so their results differ.
    first, second = statistics.values()
    assert first != second


def test_timeseries_tilejson(
    app,
    xarray_query_params,
) -> None:
    """Test /xarray/timeseries/tilejson endpoint"""
    response = app.get(
        "/xarray/timeseries/WebMercatorQuad/tilejson.json",
        params={
            **xarray_query_params(),
            "temporal": "2024-10-11T00:00:00Z/2024-10-12T23:59:59Z",
            "step": "P1D",
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"

    assert set(response.json().keys()) == {
        "2024-10-11T00:00:00+00:00/2024-10-11T23:59:59+00:00",
        "2024-10-12T00:00:00+00:00/2024-10-12T23:59:59+00:00",
    }


@pytest.mark.vcr
def test_timeseries_gif(
    app,
    mock_cmr_get_assets,
    xarray_query_params,
    arctic_bounds,
) -> None:
    """Test /xarray/timeseries/bbox endpoint"""
    response = app.get(
        f"/xarray/timeseries/bbox/{','.join(str(coord) for coord in arctic_bounds)}.gif",
        params={
            **xarray_query_params(),
            # Each interval stays inside one day, because an interval that
            # touches midnight also matches the neighboring day's granule.
            "temporal": (
                "2024-10-11T06:00:00Z/2024-10-11T18:00:00Z,"
                "2024-10-12T06:00:00Z/2024-10-12T18:00:00Z"
            ),
            "step": "P1D",
            "rescale": "0,1",
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == TimeseriesMediaType.gif
    # Identical frames merge into one, so two frames show that the steps
    # returned different images.
    assert Image.open(io.BytesIO(response.content)).n_frames == 2


@pytest.mark.vcr
def test_timeseries_statistics_no_granules(
    app: TestClient,
    xarray_query_params,
    arctic_geojson,
) -> None:
    """A request without `step` that matches no granules returns empty statistics."""
    response = app.post(
        "/xarray/timeseries/statistics",
        # The collection has no granules in this range.
        params=xarray_query_params(
            temporal="2001-01-01T00:00:00Z/2001-01-02T00:00:00Z"
        ),
        json=arctic_geojson,
    )

    assert response.status_code == 200
    assert response.json()["properties"]["statistics"] == {}


@pytest.mark.vcr
def test_timeseries_gif_no_granules(
    app: TestClient,
    xarray_query_params,
    arctic_bounds,
) -> None:
    """A request without `step` that matches no granules returns 204."""
    response = app.get(
        f"/xarray/timeseries/bbox/{','.join(str(coord) for coord in arctic_bounds)}.gif",
        # The collection has no granules in this range.
        params=xarray_query_params(
            temporal="2001-01-01T00:00:00Z/2001-01-02T00:00:00Z"
        ),
    )

    assert response.status_code == 204


def test_unbounded_start(app, xarray_query_params) -> None:
    """Make sure a datetime interval with an unbounded start returns a 400"""
    response = app.get(
        "/timeseries",
        params={
            **xarray_query_params(),
            "temporal": "../2024-10-12T23:59:59Z",
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
            "temporal": "2008-10-12T00:00:01Z/2024-10-12T23:59:59Z",
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
        "/xarray/timeseries/statistics",
        params={
            "collection_concept_id": "C1996881146-POCLOUD",
            "variables": "analysed_sst",
            "temporal": f"2024-01-01T00:00:00Z/2024-01-{n_days}T23:59:59Z",
            "step": "P1D",
        },
        json=global_geojson,
    )

    assert response.status_code == 400
    assert "The AOI for this request is too large" in response.text


@pytest.mark.vcr
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
        "/xarray/timeseries/statistics",
        params={
            "collection_concept_id": "C1996881146-POCLOUD",
            "variables": "analysed_sst",
            "temporal": "/".join(
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
        f"/xarray/timeseries/bbox/{','.join(str(coord) for coord in global_bounds)}.gif",
        params={
            "collection_concept_id": "C1996881146-POCLOUD",
            "variables": "analysed_sst",
            "temporal": f"2024-01-01T00:00:00Z/2024-01-{n_days}T23:59:59Z",
            "step": "P1D",
        },
    )

    assert response.status_code == 400
