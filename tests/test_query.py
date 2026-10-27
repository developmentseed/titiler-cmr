"""Tests for titiler.cmr.query functions."""

import pytest
import shapely
import shapely.geometry
from fastapi import HTTPException
from httpx2 import (
    Client,
    ConnectError,
    ConnectTimeout,
    LocalProtocolError,
    MockTransport,
    PoolTimeout,
    ReadError,
    ReadTimeout,
    RemoteProtocolError,
    Response,
    WriteTimeout,
)

from titiler.cmr.errors import CMROutage, CMRQueryTimeout
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


def _mock_client(items: list[dict]) -> Client:
    """Return a Client whose granules request returns one CMR page.

    The response has no cmr-search-after header, so get_granules stops after it.
    """
    return Client(
        transport=MockTransport(
            lambda request: Response(200, json={"hits": len(items), "items": items})
        )
    )


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
    """Tests that a CMR timeout is re-raised as CMRQueryTimeout."""

    @pytest.mark.parametrize("error", [ConnectTimeout, ReadTimeout, WriteTimeout])
    def test_get_granules_raises_on_timeout(self, error):
        """get_granules raises CMRQueryTimeout when the CMR request times out."""

        def handler(request):
            raise error("timed out", request=request)

        client = Client(transport=MockTransport(handler), timeout=20.0)

        with pytest.raises(
            CMRQueryTimeout, match=r"CMR granule search timed out after 20\.0 seconds"
        ):
            list(get_granules(GranuleSearch(), client))


class TestCMRErrorResponses:
    """Tests that CMR error responses become clear errors for the client."""

    @pytest.mark.parametrize("error", [PoolTimeout, LocalProtocolError])
    def test_own_failure_is_not_reported_as_cmr_failure(self, error):
        """A failure on titiler-cmr's side escapes as a 500, not as CMR's.

        A full connection pool or a malformed request header is titiler-cmr's
        own fault, so a 502 or 504 would send whoever investigates to look at
        CMR.
        """

        def handler(request):
            raise error("own failure", request=request)

        client = Client(transport=MockTransport(handler))

        with pytest.raises(error):
            list(get_granules(GranuleSearch(), client))

    def test_get_granules_raises_on_redirect(self):
        """get_granules raises CMROutage when CMR answers with a redirect."""
        client = Client(transport=MockTransport(lambda request: Response(301)))

        with pytest.raises(
            CMROutage,
            match=r"CMR granule search failed with 301 \(Moved Permanently\)",
        ):
            list(get_granules(GranuleSearch(), client))

    def test_get_granules_raises_on_body_that_is_not_json(self):
        """get_granules raises CMROutage when a 200's body is not JSON."""
        client = Client(
            transport=MockTransport(
                lambda request: Response(200, text="<html>maintenance</html>")
            )
        )

        with pytest.raises(
            CMROutage, match="CMR granule search returned an unreadable response"
        ):
            list(get_granules(GranuleSearch(), client))

    def test_get_collection_raises_on_body_of_wrong_shape(self):
        """get_collection raises CMROutage when a 200's JSON is not a search result."""
        client = Client(
            transport=MockTransport(lambda request: Response(200, json={"hits": 1}))
        )

        with pytest.raises(
            CMROutage, match="CMR collection search returned an unreadable response"
        ):
            get_collection("C1234-PROVIDER", client)

    @pytest.mark.parametrize(
        "body",
        ['{"errors": "bad param"}', '{"errors": []}', "<html>Bad Request</html>"],
        ids=["errors-string", "errors-empty", "not-json"],
    )
    def test_4xx_without_cmr_messages_keeps_body_as_detail(self, body):
        """A CMR 4xx whose body lacks a non-empty errors list keeps it as detail."""
        client = Client(
            transport=MockTransport(lambda request: Response(400, text=body))
        )

        with pytest.raises(HTTPException) as exc_info:
            get_collection("C1234-PROVIDER", client)

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == body

    @pytest.mark.parametrize("error", [ConnectError, RemoteProtocolError])
    def test_get_granules_raises_on_connection_failure(self, error):
        """get_granules raises CMROutage when CMR cannot be reached."""

        def handler(request):
            raise error("connection failed", request=request)

        client = Client(transport=MockTransport(handler))

        with pytest.raises(
            CMROutage, match="CMR granule search failed: connection failed"
        ):
            list(get_granules(GranuleSearch(), client))

    def test_connection_failure_without_a_message_reports_its_type(self):
        """A connection failure that httpx leaves without a message reports its type."""

        def handler(request):
            raise ReadError("", request=request)

        client = Client(transport=MockTransport(handler))

        with pytest.raises(CMROutage, match="CMR granule search failed: ReadError$"):
            list(get_granules(GranuleSearch(), client))

    def test_get_granules_raises_on_corrupt_response_body(self):
        """get_granules raises CMROutage when CMR's gzip body cannot be decoded."""
        client = Client(
            transport=MockTransport(
                lambda request: Response(
                    200, headers={"Content-Encoding": "gzip"}, content=b"not gzip"
                )
            )
        )

        with pytest.raises(CMROutage, match="CMR granule search failed: "):
            list(get_granules(GranuleSearch(), client))

    def test_get_granules_raises_on_server_error(self):
        """get_granules raises CMROutage when CMR returns a 5xx."""
        client = Client(transport=MockTransport(lambda request: Response(503)))

        with pytest.raises(
            CMROutage,
            match=r"CMR granule search failed with 503 \(Service Unavailable\)",
        ):
            list(get_granules(GranuleSearch(), client))

    def test_get_collection_raises_on_server_error(self):
        """get_collection raises CMROutage when CMR returns a 5xx."""
        client = Client(transport=MockTransport(lambda request: Response(503)))

        with pytest.raises(
            CMROutage,
            match=r"CMR collection search failed with 503 \(Service Unavailable\)",
        ):
            get_collection("C1234-PROVIDER", client)

    def test_get_granules_reports_cmr_messages_as_text(self):
        """A CMR 4xx keeps its status, with CMR's messages as plain text."""
        client = Client(
            transport=MockTransport(
                lambda request: Response(
                    400,
                    json={
                        "errors": [
                            "Parameter [baz] was not recognized.",
                            "Parameter [foo] was not recognized.",
                        ]
                    },
                )
            )
        )

        with pytest.raises(HTTPException) as exc_info:
            list(get_granules(GranuleSearch(), client))

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == (
            "Parameter [baz] was not recognized.; Parameter [foo] was not recognized."
        )
