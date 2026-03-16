"""Tests for titiler.cmr.query functions."""

import shapely
import shapely.geometry

from titiler.cmr.models import Granule, GranuleSpatialExtent
from titiler.cmr.query import _is_fully_covered


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
