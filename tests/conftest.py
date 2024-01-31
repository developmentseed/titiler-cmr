"""titiler.cmr tests configuration."""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def app(monkeypatch):
    """App fixture."""

    from titiler.cmr.main import app

    with TestClient(app) as client:
        yield client
