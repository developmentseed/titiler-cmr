"""Tests for legacy backwards-compatibility redirect routes and parameter aliases."""

from unittest.mock import patch
from urllib.parse import parse_qs, urlparse


from titiler.cmr.compatibility import CompatibilityResponse


def _qs(url: str) -> dict:
    """Parse a URL's query string into a dict of lists."""
    return parse_qs(urlparse(url).query)


# ---------------------------------------------------------------------------
# Parameter alias tests (no redirects — old names accepted on new-style routes)
# ---------------------------------------------------------------------------


class TestGranuleSearchParamAliases:
    """concept_id and datetime are accepted as aliases on new-style endpoints."""

    def test_concept_id_alias(self, app):
        """concept_id is accepted in place of collection_concept_id."""
        with patch("titiler.cmr.compatibility.evaluate_concept_compatibility") as mock:
            mock.return_value = CompatibilityResponse(
                concept_id="C1234-TEST",
                backend="xarray",
                datetime=[],
                links=[],
            )
            r = app.get("/compatibility?concept_id=C1234-TEST")
        assert r.status_code == 200
        mock.assert_called_once()
        assert mock.call_args[0][0] == "C1234-TEST"

    def test_collection_concept_id_still_works(self, app):
        """collection_concept_id (new name) continues to work."""
        with patch("titiler.cmr.compatibility.evaluate_concept_compatibility") as mock:
            mock.return_value = CompatibilityResponse(
                concept_id="C1234-TEST",
                backend="xarray",
                datetime=[],
                links=[],
            )
            r = app.get("/compatibility?collection_concept_id=C1234-TEST")
        assert r.status_code == 200
        mock.assert_called_once()
        assert mock.call_args[0][0] == "C1234-TEST"


# ---------------------------------------------------------------------------
# Legacy redirect route tests
# ---------------------------------------------------------------------------


class TestLegacyGetRedirects:
    """GET legacy routes redirect with 301 and rename params."""

    def test_tilejson_redirects_to_rasterio_by_default(self, app):
        """No backend param → defaults to rasterio."""
        r = app.get(
            "/WebMercatorQuad/tilejson.json",
            params={
                "collection_concept_id": "C123",
                "temporal": "2024-01-01T00:00:00Z",
            },
            follow_redirects=False,
        )
        assert r.status_code == 301
        loc = r.headers["location"]
        assert loc.startswith("http://testserver/rasterio/")
        assert "WebMercatorQuad/tilejson.json" in loc

    def test_tilejson_redirects_to_xarray_when_specified(self, app):
        """backend=xarray → redirects to /xarray/..."""
        r = app.get(
            "/WebMercatorQuad/tilejson.json",
            params={"backend": "xarray", "collection_concept_id": "C123"},
            follow_redirects=False,
        )
        assert r.status_code == 301
        loc = r.headers["location"]
        assert "/xarray/WebMercatorQuad/tilejson.json" in loc
        assert "backend" not in _qs(loc)

    def test_concept_id_renamed_in_redirect(self, app):
        """concept_id is renamed to collection_concept_id in the redirect URL."""
        r = app.get(
            "/WebMercatorQuad/tilejson.json",
            params={"concept_id": "C123", "temporal": "2024-01-01T00:00:00Z"},
            follow_redirects=False,
        )
        assert r.status_code == 301
        qs = _qs(r.headers["location"])
        assert qs["collection_concept_id"] == ["C123"]
        assert "concept_id" not in qs

    def test_datetime_renamed_to_temporal_in_redirect(self, app):
        """datetime is renamed to temporal in the redirect URL."""
        r = app.get(
            "/WebMercatorQuad/tilejson.json",
            params={
                "collection_concept_id": "C123",
                "datetime": "2024-01-01T00:00:00Z",
            },
            follow_redirects=False,
        )
        assert r.status_code == 301
        qs = _qs(r.headers["location"])
        assert qs["temporal"] == ["2024-01-01T00:00:00Z"]
        assert "datetime" not in qs

    def test_bands_regex_renamed_in_redirect(self, app):
        """bands_regex is renamed to assets_regex in the redirect URL."""
        r = app.get(
            "/WebMercatorQuad/tilejson.json",
            params={"collection_concept_id": "C123", "bands_regex": "B0[0-9]"},
            follow_redirects=False,
        )
        assert r.status_code == 301
        qs = _qs(r.headers["location"])
        assert "assets_regex" in qs
        assert "bands_regex" not in qs

    def test_all_renames_applied_together(self, app):
        """concept_id, datetime, and bands_regex are all renamed in one redirect."""
        r = app.get(
            "/WebMercatorQuad/map.html",
            params={
                "backend": "rasterio",
                "concept_id": "C456",
                "datetime": "2024-06-01T00:00:00Z",
                "bands_regex": "B04",
            },
            follow_redirects=False,
        )
        assert r.status_code == 301
        loc = r.headers["location"]
        assert "/rasterio/WebMercatorQuad/map.html" in loc
        qs = _qs(loc)
        assert qs["collection_concept_id"] == ["C456"]
        assert qs["temporal"] == ["2024-06-01T00:00:00Z"]
        assert qs["assets_regex"] == ["B04"]
        assert "backend" not in qs
        assert "concept_id" not in qs
        assert "datetime" not in qs
        assert "bands_regex" not in qs

    def test_tile_route_redirects(self, app):
        """Plain tile route redirects correctly."""
        r = app.get(
            "/tiles/WebMercatorQuad/5/10/12",
            params={"collection_concept_id": "C123"},
            follow_redirects=False,
        )
        assert r.status_code == 301
        assert "/rasterio/tiles/WebMercatorQuad/5/10/12" in r.headers["location"]

    def test_tile_route_with_format_redirects(self, app):
        """Tile route with format redirects correctly."""
        r = app.get(
            "/tiles/WebMercatorQuad/5/10/12.png",
            params={"collection_concept_id": "C123"},
            follow_redirects=False,
        )
        assert r.status_code == 301
        assert "/rasterio/tiles/WebMercatorQuad/5/10/12.png" in r.headers["location"]

    def test_tile_route_with_scale_and_format_redirects(self, app):
        """Tile route with scale and format redirects correctly."""
        r = app.get(
            "/tiles/WebMercatorQuad/5/10/12@2x.png",
            params={"collection_concept_id": "C123"},
            follow_redirects=False,
        )
        assert r.status_code == 301
        assert "/rasterio/tiles/WebMercatorQuad/5/10/12@2x.png" in r.headers["location"]

    def test_preview_route_redirects(self, app):
        """Preview route redirects correctly."""
        r = app.get(
            "/preview",
            params={"collection_concept_id": "C123", "backend": "xarray"},
            follow_redirects=False,
        )
        assert r.status_code == 301
        assert "/xarray/preview" in r.headers["location"]

    def test_bbox_route_redirects(self, app):
        """/bbox route redirects correctly."""
        r = app.get(
            "/bbox/-100,40,-90,50.png",
            params={"collection_concept_id": "C123"},
            follow_redirects=False,
        )
        assert r.status_code == 301
        assert "/rasterio/bbox/" in r.headers["location"]
        assert ".png" in r.headers["location"]

    def test_timeseries_tilejson_redirects(self, app):
        """/timeseries tilejson redirects correctly."""
        r = app.get(
            "/timeseries/WebMercatorQuad/tilejson.json",
            params={"collection_concept_id": "C123", "backend": "xarray"},
            follow_redirects=False,
        )
        assert r.status_code == 301
        assert (
            "/xarray/timeseries/WebMercatorQuad/tilejson.json" in r.headers["location"]
        )

    def test_timeseries_bbox_redirects(self, app):
        """/timeseries/bbox route redirects correctly."""
        r = app.get(
            "/timeseries/bbox/-100,40,-90,50.gif",
            params={"concept_id": "C123", "datetime": "2024-01-01T00:00:00Z"},
            follow_redirects=False,
        )
        assert r.status_code == 301
        loc = r.headers["location"]
        assert "/rasterio/timeseries/bbox/" in loc
        assert ".gif" in loc
        qs = _qs(loc)
        assert "collection_concept_id" in qs
        assert "concept_id" not in qs
        assert "temporal" in qs
        assert "datetime" not in qs


class TestLegacyPostRedirects:
    """POST legacy routes redirect with 308 (preserves method + body)."""

    def test_statistics_post_uses_308(self, app):
        """POST /statistics redirects with 308."""
        r = app.post(
            "/statistics",
            params={"collection_concept_id": "C123"},
            json={"type": "Feature", "geometry": None, "properties": {}},
            follow_redirects=False,
        )
        assert r.status_code == 308
        assert "/rasterio/statistics" in r.headers["location"]

    def test_feature_post_uses_308(self, app):
        """POST /feature redirects with 308 and renames concept_id."""
        r = app.post(
            "/feature",
            params={"concept_id": "C123"},
            json={"type": "Feature", "geometry": None, "properties": {}},
            follow_redirects=False,
        )
        assert r.status_code == 308
        loc = r.headers["location"]
        assert "/rasterio/feature" in loc
        qs = _qs(loc)
        assert qs["collection_concept_id"] == ["C123"]
        assert "concept_id" not in qs

    def test_part_post_uses_308(self, app):
        """POST /part redirects with 308."""
        r = app.post(
            "/part",
            params={"collection_concept_id": "C123", "backend": "xarray"},
            json={},
            follow_redirects=False,
        )
        assert r.status_code == 308
        assert "/xarray/part" in r.headers["location"]

    def test_timeseries_statistics_post_uses_308(self, app):
        """POST /timeseries/statistics redirects with 308."""
        r = app.post(
            "/timeseries/statistics",
            params={
                "collection_concept_id": "C123",
                "temporal": "2024-01-01T00:00:00Z",
            },
            json={"type": "Feature", "geometry": None, "properties": {}},
            follow_redirects=False,
        )
        assert r.status_code == 308
        assert "/rasterio/timeseries/statistics" in r.headers["location"]

    def test_feature_format_post_uses_308(self, app):
        """POST /feature.{format} redirects with 308."""
        r = app.post(
            "/feature.png",
            params={"collection_concept_id": "C123"},
            json={},
            follow_redirects=False,
        )
        assert r.status_code == 308
        assert "/rasterio/feature.png" in r.headers["location"]

    def test_feature_size_post_uses_308(self, app):
        """POST /feature/{width}x{height}.{format} redirects with 308."""
        r = app.post(
            "/feature/256x256.png",
            params={"collection_concept_id": "C123"},
            json={},
            follow_redirects=False,
        )
        assert r.status_code == 308
        assert "/rasterio/feature/256x256.png" in r.headers["location"]


class TestLegacyMultiValueParams:
    """Multi-value query parameters (e.g. sel=x&sel=y) are preserved in redirects."""

    def test_multi_value_sel_preserved(self, app):
        """Multiple sel params are all forwarded, not collapsed to one."""
        r = app.get(
            "/WebMercatorQuad/tilejson.json",
            params=[
                ("collection_concept_id", "C123"),
                ("sel", "time={datetime}"),
                ("sel", "band=1"),
            ],
            follow_redirects=False,
        )
        assert r.status_code == 301
        qs = _qs(r.headers["location"])
        assert qs["sel"] == ["time={datetime}", "band=1"]

    def test_multi_value_param_with_rename(self, app):
        """Renamed param with a single value still works alongside multi-value params."""
        r = app.get(
            "/WebMercatorQuad/tilejson.json",
            params=[
                ("concept_id", "C123"),
                ("sel", "time={datetime}"),
                ("sel", "band=1"),
            ],
            follow_redirects=False,
        )
        assert r.status_code == 301
        qs = _qs(r.headers["location"])
        assert qs["collection_concept_id"] == ["C123"]
        assert "concept_id" not in qs
        assert qs["sel"] == ["time={datetime}", "band=1"]


class TestLegacyNewNameNotOverwritten:
    """If both old and new param names are present, new name takes precedence."""

    def test_collection_concept_id_takes_precedence_over_concept_id(self, app):
        """When both collection_concept_id and concept_id are sent, new name wins."""
        r = app.get(
            "/WebMercatorQuad/tilejson.json",
            params={"collection_concept_id": "C_NEW", "concept_id": "C_OLD"},
            follow_redirects=False,
        )
        assert r.status_code == 301
        qs = _qs(r.headers["location"])
        assert qs["collection_concept_id"] == ["C_NEW"]
        assert "concept_id" not in qs

    def test_temporal_takes_precedence_over_datetime(self, app):
        """When both temporal and datetime are sent, temporal wins."""
        r = app.get(
            "/WebMercatorQuad/tilejson.json",
            params={
                "collection_concept_id": "C123",
                "temporal": "2024-01-01T00:00:00Z",
                "datetime": "2023-01-01T00:00:00Z",
            },
            follow_redirects=False,
        )
        assert r.status_code == 301
        qs = _qs(r.headers["location"])
        assert qs["temporal"] == ["2024-01-01T00:00:00Z"]
        assert "datetime" not in qs
