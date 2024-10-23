"""test titiler-cmr app."""

from typing import Tuple

import pytest
from httpx import Response
from rasterio.errors import NotGeoreferencedWarning

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
def test_rasterio_statistics(app, mock_cmr_get_assets, mn_geojson):
    """Test /statistics endpoint for a polygon that straddles the boundary between two HLS granules"""

    concept_id = "C2021957657-LPCLOUD"
    band = "Fmask"
    datetime_range = "2024-10-09T00:00:01Z/2024-10-09T23:59:59Z"

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
    with pytest.warns(
        (PendingDeprecationWarning, NotGeoreferencedWarning),
        match=r"is_tiled|no geotransform",
    ):
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

    with pytest.warns(
        (PendingDeprecationWarning, NotGeoreferencedWarning),
        match=r"is_tiled|no geotransform",
    ):
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
    variable = xarray_query_params["variable"]
    assert stats[variable]["median"] == 0.79
    assert stats[variable]["sum"] == 2376.73
    assert round(stats[variable]["mean"], 3) == 0.523


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
    """Test /part endpoint for xarray backend"""
    response = app.get(
        f"/bbox/{','.join(str(coord) for coord in arctic_bounds)}.tif",
        params={
            **xarray_query_params,
            "format": "tif",
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/tiff; application=geotiff"


def test_timeseries_statistics(
    app,
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
    arctic_stats = arctic_geojson.copy()
    arctic_stats["properties"]["statistics"] = {
        "sea_ice_fraction": {
            "min": 0,
            "max": 1,
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
            "start_datetime": "2024-10-11T00:00:00Z",
            "end_datetime": "2024-10-12T23:59:59Z",
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
    """Test /timeseries/statistics endpoint

    Since the /timeseries/statistics endpoint sends more requests to internal endpoints
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
            "start_datetime": "2024-10-11T00:00:00Z",
            "end_datetime": "2024-10-12T23:59:59Z",
            "step": "P1D",
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"

    assert set(response.json()["timeseries_tilejsons"].keys()) == {
        "2024-10-11T00:00:00+00:00/2024-10-11T23:59:59+00:00",
        "2024-10-12T00:00:00+00:00/2024-10-12T23:59:59+00:00",
    }
