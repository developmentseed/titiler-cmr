"""test titiler-cmr app."""

import pytest


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


@pytest.mark.parametrize("bbox", ["a,b,c,d", "1,2,3", "1,2,3,4,5", "1,2,3,nan"])
def test_bounding_box_malformed_returns_422(app, bbox):
    """Malformed bounding_box values are rejected with 422 instead of crashing."""
    response = app.get(
        "/xarray/info",
        params={
            "collection_concept_id": "C1-X",
            "bounding_box": bbox,
            "variables": "v",
            "temporal": "2024-01-01",
        },
    )
    assert response.status_code == 422


@pytest.mark.parametrize("bbox", ["-100,40,-90,50", ""])
def test_bounding_box_valid_returns_200(app, bbox):
    """A well-formed or empty bounding_box is accepted and returns 200."""
    response = app.get(
        "/xarray/info",
        params={
            "collection_concept_id": "C1-X",
            "bounding_box": bbox,
            "variables": "v",
            "temporal": "2024-01-01",
        },
    )
    assert response.status_code == 200
