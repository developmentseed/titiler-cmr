"""Test CMRTilerFactory assets endpoints."""

import unittest.mock as mock

import pytest

from titiler.cmr.models import Granule, GranuleSpatialExtent


def _make_stub_granule(granule_id: str, granule_ur: str) -> Granule:
    """Build a Granule with a simple bounding rectangle geometry."""
    return Granule(
        id=granule_id,
        granule_ur=granule_ur,
        collection_concept_id="C123-PROV",
        related_urls=[],
        spatial_extent=GranuleSpatialExtent(
            **{
                "HorizontalSpatialDomain": {
                    "Geometry": {
                        "BoundingRectangles": [
                            {
                                "WestBoundingCoordinate": -100,
                                "EastBoundingCoordinate": -90,
                                "NorthBoundingCoordinate": 50,
                                "SouthBoundingCoordinate": 40,
                            }
                        ]
                    }
                }
            }
        ),
    )


STUB_GRANULES = [
    _make_stub_granule("G1-PROV", "granule-1"),
    _make_stub_granule("G2-PROV", "granule-2"),
]


@pytest.fixture
def mock_get_granules():
    """Patch get_granules in the backend to return stub Granule objects."""
    with mock.patch(
        "titiler.cmr.backend.get_granules",
        side_effect=lambda *args, **kwargs: iter(STUB_GRANULES),
    ):
        yield


def _assert_granule_feature_collection(body: dict) -> None:
    """Assert that a response body is a valid GranuleFeatureCollection."""
    assert body["type"] == "FeatureCollection"
    assert len(body["features"]) == len(STUB_GRANULES)

    for i, feature in enumerate(body["features"]):
        granule = STUB_GRANULES[i]
        assert feature["type"] == "Feature"
        assert feature["geometry"] is not None
        assert feature["geometry"]["type"] == "Polygon"
        props = feature["properties"]
        assert props["id"] == granule.id
        assert props["granule_ur"] == granule.granule_ur
        assert props["collection_concept_id"] == granule.collection_concept_id


def test_bbox_assets_returns_granule_list(app, mock_get_granules):
    """Test that /bbox/.../assets returns a JSON list of granules by default."""
    response = app.get(
        "/bbox/-100,40,-90,50/assets",
        params={"collection_concept_id": "C123-PROV"},
    )
    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, list)
    assert len(body) == len(STUB_GRANULES)
    for i, item in enumerate(body):
        assert item["id"] == STUB_GRANULES[i].id


def test_bbox_assets_returns_feature_collection(app, mock_get_granules):
    """Test that /bbox/.../assets?f=geojson returns a GeoJSON FeatureCollection."""
    response = app.get(
        "/bbox/-100,40,-90,50/assets",
        params={"collection_concept_id": "C123-PROV", "f": "geojson"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    _assert_granule_feature_collection(response.json())


def test_point_assets_returns_granule_list(app, mock_get_granules):
    """Test that /point/.../assets returns a JSON list of granules by default."""
    response = app.get(
        "/point/-95,45/assets",
        params={"collection_concept_id": "C123-PROV"},
    )
    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, list)
    assert len(body) == len(STUB_GRANULES)


def test_point_assets_returns_feature_collection(app, mock_get_granules):
    """Test that /point/.../assets?f=geojson returns a GeoJSON FeatureCollection."""
    response = app.get(
        "/point/-95,45/assets",
        params={"collection_concept_id": "C123-PROV", "f": "geojson"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    _assert_granule_feature_collection(response.json())


def test_tile_assets_returns_granule_list(app, mock_get_granules):
    """Test that /tiles/.../assets returns a JSON list of granules by default."""
    response = app.get(
        "/tiles/WebMercatorQuad/0/0/0/assets",
        params={"collection_concept_id": "C123-PROV"},
    )
    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, list)
    assert len(body) == len(STUB_GRANULES)


def test_tile_assets_returns_feature_collection(app, mock_get_granules):
    """Test that /tiles/.../assets?f=geojson returns a GeoJSON FeatureCollection."""
    response = app.get(
        "/tiles/WebMercatorQuad/0/0/0/assets",
        params={"collection_concept_id": "C123-PROV", "f": "geojson"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    _assert_granule_feature_collection(response.json())


def test_bbox_assets_empty_result(app, mock_get_granules):
    """Test that an empty granule result returns an empty list."""
    with mock.patch(
        "titiler.cmr.backend.get_granules",
        side_effect=lambda *args, **kwargs: iter([]),
    ):
        response = app.get(
            "/bbox/-100,40,-90,50/assets",
            params={"collection_concept_id": "C123-PROV"},
        )
    assert response.status_code == 200
    assert response.json() == []


def test_prefixed_assets_routes_do_not_exist(app):
    """Verify that /xarray/.../assets and /rasterio/.../assets no longer exist."""
    for prefix in ("/xarray", "/rasterio"):
        r = app.get(
            f"{prefix}/bbox/-100,40,-90,50/assets",
            params={"collection_concept_id": "C123-PROV"},
        )
        assert r.status_code == 404, (
            f"Expected 404 for {prefix}/bbox/.../assets, got {r.status_code}"
        )
