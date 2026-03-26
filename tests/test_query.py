"""Tests for titiler.cmr.query functions."""

import unittest.mock as mock

import pytest
import shapely
import shapely.geometry
from httpx import ReadTimeout

from titiler.cmr.errors import CMRQueryTimeout
from titiler.cmr.models import Granule, GranuleSearch, GranuleSpatialExtent
from titiler.cmr.query import _is_fully_covered, get_collection, get_granules


def _make_granule(minx: float, miny: float, maxx: float, maxy: float) -> Granule:
    """Build a minimal Granule with a bounding-rectangle geometry."""
    return Granule(
        id="test-id",
        granule_ur="test-granule-ur",
        collection_concept_id="TEST_COLLECTION",
        related_urls=[],
        spatial_extent=GranuleSpatialExtent(
            **{
                "HorizontalSpatialDomain": {
                    "Geometry": {
                        "BoundingRectangles": [
                            {
                                "WestBoundingCoordinate": minx,
                                "EastBoundingCoordinate": maxx,
                                "NorthBoundingCoordinate": maxy,
                                "SouthBoundingCoordinate": miny,
                            }
                        ]
                    }
                }
            }
        ),
    )


class TestIsFullyCovered:
    """Tests for _is_fully_covered."""

    def test_no_search_shape_returns_false(self):
        """When search_shape is None, always returns False."""
        granule = _make_granule(0, 0, 1, 1)
        covered = shapely.GeometryCollection()
        done, _ = _is_fully_covered(granule, None, covered)
        assert not done

    def test_no_granule_geometry_returns_false(self):
        """When granule has no geometry, returns False."""
        granule = Granule(
            id="test-id",
            granule_ur="test-granule-ur",
            collection_concept_id="TEST_COLLECTION",
            related_urls=[],
        )
        search_shape = shapely.geometry.box(0, 0, 1, 1)
        covered = shapely.GeometryCollection()
        done, _ = _is_fully_covered(granule, search_shape, covered)
        assert not done

    def test_exact_coverage_returns_true(self):
        """Granule exactly matching the search shape returns done=True."""
        granule = _make_granule(0, 0, 1, 1)
        search_shape = shapely.geometry.box(0, 0, 1, 1)
        covered = shapely.GeometryCollection()
        done, _ = _is_fully_covered(granule, search_shape, covered)
        assert done

    def test_overshooting_granule_returns_true(self):
        """Granule polygon that overshoots all tile edges returns done=True."""
        granule = _make_granule(-0.1, -0.1, 1.1, 1.1)
        search_shape = shapely.geometry.box(0, 0, 1, 1)
        covered = shapely.GeometryCollection()
        done, _ = _is_fully_covered(granule, search_shape, covered)
        assert done

    def test_partial_coverage_returns_false(self):
        """Granule covering only part of the search shape returns done=False."""
        granule = _make_granule(0, 0, 0.5, 1)
        search_shape = shapely.geometry.box(0, 0, 1, 1)
        covered = shapely.GeometryCollection()
        done, _ = _is_fully_covered(granule, search_shape, covered)
        assert not done

    def test_slight_overshoot_without_tolerance_returns_true(self):
        """Polygon barely over the tile edge returns True without tolerance."""
        # CMR polygon overshoots right edge by 0.001 — declares coverage, but
        # actual raster data may not reach the tile edge (overshoot ≠ data).
        granule = _make_granule(0, 0, 1.001, 1)
        search_shape = shapely.geometry.box(0, 0, 1, 1)
        covered = shapely.GeometryCollection()
        done, _ = _is_fully_covered(
            granule, search_shape, covered, coverage_tolerance=0.0
        )
        assert done

    def test_slight_overshoot_with_tolerance_returns_false(self):
        """With tolerance, a barely-overshooting polygon is not enough — fetch more.

        The tolerance requires the coverage to extend well beyond the tile edge
        before declaring full coverage, compensating for CMR polygon overshoot
        relative to actual raster data.
        """
        granule = _make_granule(0, 0, 1.001, 1)
        search_shape = shapely.geometry.box(0, 0, 1, 1)
        covered = shapely.GeometryCollection()
        done, _ = _is_fully_covered(
            granule, search_shape, covered, coverage_tolerance=0.01
        )
        assert not done

    def test_large_overshoot_with_tolerance_returns_true(self):
        """When coverage extends well beyond the tile, tolerance is satisfied."""
        granule = _make_granule(-0.1, -0.1, 1.1, 1.1)
        search_shape = shapely.geometry.box(0, 0, 1, 1)
        covered = shapely.GeometryCollection()
        done, _ = _is_fully_covered(
            granule, search_shape, covered, coverage_tolerance=0.01
        )
        assert done

    def test_covered_geometry_accumulates(self):
        """covered geometry is updated to include the new granule's area."""
        granule = _make_granule(0, 0, 0.5, 1)
        search_shape = shapely.geometry.box(0, 0, 1, 1)
        covered = shapely.GeometryCollection()
        _, covered = _is_fully_covered(granule, search_shape, covered)
        assert not covered.is_empty
        assert covered.area > 0


def _make_umm_item(
    granule_id: str,
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
) -> dict:
    """Build a minimal UMM JSON item dict with a bounding-rectangle geometry."""
    return {
        "meta": {
            "concept-id": granule_id,
            "native-id": f"granule-ur-{granule_id}",
            "collection-concept-id": "TEST_COLLECTION",
        },
        "umm": {
            "SpatialExtent": {
                "HorizontalSpatialDomain": {
                    "Geometry": {
                        "BoundingRectangles": [
                            {
                                "WestBoundingCoordinate": minx,
                                "EastBoundingCoordinate": maxx,
                                "NorthBoundingCoordinate": maxy,
                                "SouthBoundingCoordinate": miny,
                            }
                        ]
                    }
                }
            }
        },
    }


def _make_umm_item_no_geometry(granule_id: str) -> dict:
    """Build a minimal UMM JSON item dict with no spatial extent."""
    return {
        "meta": {
            "concept-id": granule_id,
            "native-id": f"granule-ur-{granule_id}",
            "collection-concept-id": "TEST_COLLECTION",
        },
        "umm": {},
    }


def _mock_client(items: list[dict]) -> mock.MagicMock:
    """Return a mock httpx Client whose .get() returns a single-page CMR response."""
    response = mock.MagicMock()
    response.json.return_value = {"hits": len(items), "items": items}
    response.headers.get.return_value = None  # no cmr-search-after → single page
    response.url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

    client = mock.MagicMock()
    client.get.return_value = response
    return client


class TestGetGranulesSkipcovered:
    """Tests for get_granules skipcovered behaviour."""

    def test_duplicate_geometry_skipped(self):
        """Second granule with identical bbox is skipped when skipcovered=True."""
        client = _mock_client(
            [_make_umm_item("G1", 0, 0, 1, 1), _make_umm_item("G2", 0, 0, 1, 1)]
        )
        results = list(get_granules(GranuleSearch(), client, skipcovered=True))

        assert len(results) == 1
        assert results[0].id == "G1"

    def test_skipcovered_false_yields_all(self):
        """Duplicate geometries are not filtered when skipcovered=False (default)."""
        client = _mock_client(
            [_make_umm_item("G1", 0, 0, 1, 1), _make_umm_item("G2", 0, 0, 1, 1)]
        )
        results = list(get_granules(GranuleSearch(), client))

        assert len(results) == 2

    def test_distinct_geometries_both_yielded(self):
        """Granules with different geometries are both yielded when skipcovered=True."""
        client = _mock_client(
            [_make_umm_item("G1", 0, 0, 1, 1), _make_umm_item("G2", 1, 0, 2, 1)]
        )
        results = list(get_granules(GranuleSearch(), client, skipcovered=True))

        assert len(results) == 2

    def test_no_geometry_always_yielded(self):
        """Granules with no geometry are never skipped even when skipcovered=True."""
        client = _mock_client(
            [_make_umm_item_no_geometry("G1"), _make_umm_item_no_geometry("G2")]
        )
        results = list(get_granules(GranuleSearch(), client, skipcovered=True))

        assert len(results) == 2

    def test_third_duplicate_also_skipped(self):
        """All subsequent granules matching the first geometry are skipped."""
        client = _mock_client(
            [
                _make_umm_item("G1", 0, 0, 1, 1),
                _make_umm_item("G2", 0, 0, 1, 1),
                _make_umm_item("G3", 0, 0, 1, 1),
            ]
        )
        results = list(get_granules(GranuleSearch(), client, skipcovered=True))

        assert len(results) == 1
        assert results[0].id == "G1"


class TestCMRQueryTimeout:
    """Tests that ReadTimeout from httpx is re-raised as CMRQueryTimeout."""

    def test_get_granules_raises_on_timeout(self):
        """get_granules raises CMRQueryTimeout when the CMR request times out."""
        client = mock.MagicMock()
        client.get.side_effect = ReadTimeout("timed out")

        with pytest.raises(CMRQueryTimeout):
            list(get_granules(GranuleSearch(), client))

    def test_get_collection_raises_on_timeout(self):
        """get_collection raises CMRQueryTimeout when the CMR request times out."""
        client = mock.MagicMock()
        client.get.side_effect = ReadTimeout("timed out")

        with pytest.raises(CMRQueryTimeout):
            get_collection("C1234-PROVIDER", client)
