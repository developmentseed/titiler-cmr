"""titiler.cmr tests configuration."""

import pytest
from fastapi.testclient import TestClient

from titiler.cmr.settings import AuthSettings


@pytest.fixture
def mock_auth_settings():
    """Fixture to provide modified AuthSettings."""
    return AuthSettings(
        strategy="iam",
        access="external",
    )


@pytest.fixture
def app(monkeypatch, mock_auth_settings):
    """App fixture with mocked AuthSettings."""

    # Patch the AuthSettings before importing the app
    monkeypatch.setattr("titiler.cmr.settings.AuthSettings", lambda: mock_auth_settings)

    # Now import the app (after patching)
    from titiler.cmr.main import app

    with TestClient(app) as client:
        yield client
