"""Test titiler-cmr rasterio backend."""

import io
from typing import Tuple

import pytest
from geojson_pydantic import Feature, Polygon
from PIL import Image


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

    response = app.get(
        f"/rasterio/bbox/{','.join(str(coord) for coord in mn_bounds)}/100x100.tif",
        params={
            **rasterio_query_params,
            "format": "tif",
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "image/tiff; application=geotiff"


@pytest.mark.vcr
@pytest.mark.parametrize(
    "size_params",
    [{"width": 256, "height": 256}, {"max_size": 256}],
    ids=["width-height", "max_size"],
)
def test_timeseries_statistics_sized_by_output_size(
    app, rasterio_query_params, size_params
) -> None:
    """A request that sets its output size is sized by it, not by its AOI."""
    # At HLS's native 30m, this 3 x 3 degree AOI is over the per-step limit.
    geojson = Feature(
        type="Feature",
        properties={},
        geometry=Polygon.from_bounds(-93, 46, -90, 49),
    ).model_dump(exclude_none=True)

    response = app.post(
        "/rasterio/timeseries/statistics",
        params={
            **rasterio_query_params,
            # HLS does not have granules this early, so the step does not read data.
            "temporal": "2000-01-01T00:00:00Z/2000-01-01T23:59:59Z",
            "step": "P1D",
            **size_params,
        },
        json=geojson,
    )

    assert response.status_code == 200
    assert response.json()["properties"]["statistics"] == {}


@pytest.mark.vcr
def test_timeseries_statistics_aoi_limit_suggests_output_size(
    app, rasterio_query_params
) -> None:
    """A rasterio request over the AOI limit is told that it can set an output size."""
    # At HLS's native 30m, this 3 x 3 degree AOI is over the per-step limit.
    geojson = Feature(
        type="Feature",
        properties={},
        geometry=Polygon.from_bounds(-93, 46, -90, 49),
    ).model_dump(exclude_none=True)

    response = app.post(
        "/rasterio/timeseries/statistics",
        params={
            **rasterio_query_params,
            "temporal": "2000-01-01T00:00:00Z/2000-01-01T23:59:59Z",
            "step": "P1D",
        },
        json=geojson,
    )

    assert response.status_code == 400
    assert "set width and height or max_size" in response.json()["detail"]


@pytest.mark.vcr
def test_timeseries_statistics_width_height_limit(
    app, rasterio_query_params, mn_geojson
) -> None:
    """A width and height over the per-step limit are rejected, however small the AOI."""
    response = app.post(
        "/rasterio/timeseries/statistics",
        params={
            **rasterio_query_params,
            "temporal": "2000-01-01T00:00:00Z/2000-01-01T23:59:59Z",
            "step": "P1D",
            # 8000 x 8000 pixels is over the per-step limit of 5.625e7.
            "width": 8000,
            "height": 8000,
        },
        json=mn_geojson,
    )

    assert response.status_code == 400
    assert "width and height" in response.json()["detail"]


@pytest.mark.vcr
def test_timeseries_statistics_max_size_limit(app, rasterio_query_params) -> None:
    """A max_size that caps the AOI above the per-step limit is rejected."""
    # At HLS's native 30m, this 3 x 3 degree AOI is about 1e8 pixels per step,
    # and max_size caps it at 8000 x 8000, still over the limit of 5.625e7.
    geojson = Feature(
        type="Feature",
        properties={},
        geometry=Polygon.from_bounds(-93, 46, -90, 49),
    ).model_dump(exclude_none=True)

    response = app.post(
        "/rasterio/timeseries/statistics",
        params={
            **rasterio_query_params,
            "temporal": "2000-01-01T00:00:00Z/2000-01-01T23:59:59Z",
            "step": "P1D",
            "max_size": 8000,
        },
        json=geojson,
    )

    assert response.status_code == 400
    assert "Try again with a smaller AOI or max_size." in response.json()["detail"]


@pytest.mark.vcr
def test_timeseries_statistics_max_size_over_native_size(
    app, rasterio_query_params, mn_geojson
) -> None:
    """A max_size larger than the AOI at native resolution does not enlarge it."""
    response = app.post(
        "/rasterio/timeseries/statistics",
        params={
            **rasterio_query_params,
            # HLS does not have granules this early, so the step does not read data.
            "temporal": "2000-01-01T00:00:00Z/2000-01-01T23:59:59Z",
            "step": "P1D",
            # rio-tiler never enlarges a read to max_size, so this AOI is read
            # at its native size of about 820 x 403 pixels, under the limit,
            # although 8000 x 8000 is over it.
            "max_size": 8000,
        },
        json=mn_geojson,
    )

    assert response.status_code == 200
    assert response.json()["properties"]["statistics"] == {}


@pytest.mark.vcr
def test_timeseries_gif_sized_by_output_size(app, rasterio_query_params) -> None:
    """A GIF request with a custom size is sized by it, not by its AOI."""
    # At HLS's native 30m, this 3 x 3 degree AOI is over the per-step limit.
    response = app.get(
        "/rasterio/timeseries/bbox/-93,46,-90,49/256x256.gif",
        params={
            **rasterio_query_params,
            "temporal": "2000-01-01T00:00:00Z/2000-01-01T23:59:59Z",
            "step": "P1D",
        },
    )

    # HLS does not have granules this early, so no step returns a frame.
    assert response.status_code == 204


@pytest.mark.vcr
def test_timeseries_gif_output_size_times_steps_limit(
    app,
    rasterio_query_params,
    mn_bounds: Tuple[float, float, float, float],
) -> None:
    """Each step's output size counts toward the GIF endpoint's total limit."""
    # 7 steps of 4000 x 4000 pixels are over the total limit of 1e8, although
    # each step is under the per-step limit.
    response = app.get(
        f"/rasterio/timeseries/bbox/{','.join(str(coord) for coord in mn_bounds)}"
        "/4000x4000.gif",
        params={
            **rasterio_query_params,
            "temporal": "2000-01-01T00:00:00Z/2000-01-07T23:59:59Z",
            "step": "P1D",
        },
    )

    assert response.status_code == 400
    assert "fewer time steps than 7" in response.json()["detail"]


@pytest.mark.vcr
def test_timeseries_statistics_negative_width(
    app, rasterio_query_params, mn_geojson
) -> None:
    """A negative width does not let a large height past the per-step limit."""
    response = app.post(
        "/rasterio/timeseries/statistics",
        params={
            **rasterio_query_params,
            "temporal": "2000-01-01T00:00:00Z/2000-01-01T23:59:59Z",
            "step": "P1D",
            # rio-tiler reads a negative width as 1, so this step is 1 x 1e8
            # pixels, over the per-step limit of 5.625e7.
            "width": -1,
            "height": 100_000_000,
        },
        json=mn_geojson,
    )

    assert response.status_code == 400
    assert "width and height" in response.json()["detail"]


@pytest.mark.vcr
def test_timeseries_statistics_width_only(app, rasterio_query_params) -> None:
    """A request that sets only width is sized by the output that width implies."""
    # At HLS's native 30m, this 3 x 3 degree AOI is over the per-step limit,
    # but rio-tiler reads it at 256 x 256 pixels to keep its aspect ratio.
    geojson = Feature(
        type="Feature",
        properties={},
        geometry=Polygon.from_bounds(-93, 46, -90, 49),
    ).model_dump(exclude_none=True)

    response = app.post(
        "/rasterio/timeseries/statistics",
        params={
            **rasterio_query_params,
            # HLS does not have granules this early, so the step does not read data.
            "temporal": "2000-01-01T00:00:00Z/2000-01-01T23:59:59Z",
            "step": "P1D",
            "width": 256,
        },
        json=geojson,
    )

    assert response.status_code == 200
    assert response.json()["properties"]["statistics"] == {}


@pytest.mark.vcr
def test_timeseries_statistics_max_size_elongated_aoi(
    app, rasterio_query_params
) -> None:
    """max_size caps only the longer side of an AOI, so a narrow AOI stays small."""
    # At HLS's native 30m, this 30 x 0.5 degree AOI is about 1e5 x 1667 pixels,
    # which max_size shrinks to about 8000 x 134, under the per-step limit.
    geojson = Feature(
        type="Feature",
        properties={},
        geometry=Polygon.from_bounds(-100, 46, -70, 46.5),
    ).model_dump(exclude_none=True)

    response = app.post(
        "/rasterio/timeseries/statistics",
        params={
            **rasterio_query_params,
            # HLS does not have granules this early, so the step does not read data.
            "temporal": "2000-01-01T00:00:00Z/2000-01-01T23:59:59Z",
            "step": "P1D",
            "max_size": 8000,
        },
        json=geojson,
    )

    assert response.status_code == 200
    assert response.json()["properties"]["statistics"] == {}


@pytest.mark.vcr
def test_timeseries_statistics_unknown_collection(
    app, rasterio_query_params, mn_geojson
) -> None:
    """An unknown collection is reported before any step is sent."""
    response = app.post(
        "/rasterio/timeseries/statistics",
        params={
            **rasterio_query_params,
            "collection_concept_id": "C9999999999-LPCLOUD",
            "temporal": "2000-01-01T00:00:00Z/2000-01-01T23:59:59Z",
            "step": "P1D",
            "width": 256,
            "height": 256,
        },
        json=mn_geojson,
    )

    assert response.status_code == 404


@pytest.mark.vcr
def test_timeseries_gif_zero_width(
    app,
    mock_cmr_get_assets,
    rasterio_query_params,
    mn_bounds: Tuple[float, float, float, float],
) -> None:
    """A GIF with a zero width gets frames of the requested height."""
    # rio-tiler derives a zero width from the height and the AOI's aspect
    # ratio, and the size check does too, so the steps need the same size.
    response = app.get(
        f"/rasterio/timeseries/bbox/{','.join(str(coord) for coord in mn_bounds)}"
        "/0x50.gif",
        params={**rasterio_query_params, "step": "P1D"},
    )

    assert response.status_code == 200
    assert Image.open(io.BytesIO(response.content)).height == 50


@pytest.mark.vcr
def test_timeseries_statistics_width_height_limit_unknown_resolution(
    app, rasterio_query_params, mn_geojson
) -> None:
    """A width and height over the limit are rejected without a native resolution."""
    # MOD11A1's CMR record does not include a resolution that the size check
    # reads, but a width and height fix the output size anyway.
    response = app.post(
        "/rasterio/timeseries/statistics",
        params={
            **rasterio_query_params,
            "collection_concept_id": "C1748058432-LPCLOUD",
            "temporal": "2000-01-01T00:00:00Z/2000-01-01T23:59:59Z",
            "step": "P1D",
            "width": 100_000,
            "height": 100_000,
        },
        json=mn_geojson,
    )

    assert response.status_code == 400
    assert "width and height" in response.json()["detail"]
