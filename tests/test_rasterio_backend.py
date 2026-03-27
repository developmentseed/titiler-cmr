"""Test titiler-cmr rasterio backend."""

import warnings
from typing import Tuple

import pytest
from rasterio.errors import NotGeoreferencedWarning


@pytest.mark.vcr
def test_rasterio_tilejson(app, rasterio_query_params):
    """Test /tilejson.json endpoint for rasterio backend"""

    response = app.get(
        "/rasterio/WebMercatorQuad/tilejson.json",
        params={
            **rasterio_query_params,
            "temporal": "2024-10-11T00:00:00Z/2024-10-12T23:59:59Z",
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"

    tilejson = response.json()
    assert tilejson["bounds"] == [-180.0, -90.0, 180.0, 90.0]


@pytest.mark.vcr
def test_rasterio_statistics(app, mock_cmr_get_assets, mn_geojson):
    """Test /statistics endpoint for a polygon that straddles the boundary between two HLS granules"""

    collection_concept_id = "C2021957657-LPCLOUD"
    asset = "Fmask"
    temporal = "2024-10-09T00:00:01Z/2024-10-09T23:59:59Z"

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("ignore", NotGeoreferencedWarning)

        response = app.post(
            "/rasterio/statistics",
            params={
                "collection_concept_id": collection_concept_id,
                "temporal": temporal,
                "assets_regex": asset,
                "assets": [asset],
                "asset_as_band": "true",
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
    band = "b1"
    assert stats[band]["description"] == asset
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
            "/rasterio/feature",
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
            f"/rasterio/bbox/{','.join(str(coord) for coord in mn_bounds)}/100x100.tif",
            params={
                **rasterio_query_params,
                "format": "tif",
            },
        )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/tiff; application=geotiff"
