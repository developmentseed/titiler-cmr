"""NASA Earthdata S3 credential provider."""

import threading
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from httpx import Client, HTTPError

from titiler.cmr.logger import logger
from titiler.cmr.utils import retry

if TYPE_CHECKING:
    from obstore.store import S3Config, S3Credential

CREDENTIAL_REFRESH_BUFFER = timedelta(minutes=5)


class EarthdataS3CredentialProvider:
    """obstore-compatible credential provider for NASA Earthdata S3 access.

    Fetches temporary S3 credentials from a NASA DAAC endpoint using an
    Earthdata bearer token, and caches them internally until near expiry.
    Thread-safe; a single instance can be shared across readers.
    """

    config: "S3Config" = {"region": "us-west-2"}

    def __init__(self, credentials_url: str, auth_token: str) -> None:
        """Construct a new EarthdataS3CredentialProvider."""
        self._url = credentials_url
        self._auth_token = auth_token
        self._lock = threading.Lock()
        self._cached: "S3Credential | None" = None

    def __call__(self) -> "S3Credential":
        """Return cached credentials, refreshing if near expiry."""
        with self._lock:
            if not self._is_valid():
                self._cached = self._fetch()
            return self._cached

    def _is_valid(self) -> bool:
        if self._cached is None:
            return False
        expires_at = self._cached.get("expires_at")
        if expires_at is None:
            return True
        return expires_at > datetime.now(UTC) + CREDENTIAL_REFRESH_BUFFER

    @retry(5, HTTPError, 1)
    def _fetch(self) -> "S3Credential":
        logger.info("Fetching temporary S3 credentials from %s", self._url)
        with Client() as client:
            response = client.get(
                self._url,
                headers={"Authorization": f"Bearer {self._auth_token}"},
                timeout=10,
            )
        response.raise_for_status()
        creds = response.json()
        logger.info(
            "Fetched temporary S3 credentials from %s, expiring at %s.",
            self._url,
            creds.get("expiration", "an unknown time"),
        )
        return {
            "access_key_id": creds["accessKeyId"],
            "secret_access_key": creds["secretAccessKey"],
            "token": creds["sessionToken"],
            "expires_at": datetime.fromisoformat(creds["expiration"]),
        }
