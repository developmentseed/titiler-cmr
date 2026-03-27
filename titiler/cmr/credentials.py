"""NASA Earthdata credential management."""

import threading
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from httpx import Client, HTTPError
from obstore.auth.earthdata import NasaEarthdataCredentialProvider

from titiler.cmr.logger import logger
from titiler.cmr.utils import retry

if TYPE_CHECKING:
    pass

TOKEN_REFRESH_BUFFER = timedelta(minutes=5)


class EarthdataTokenProvider:
    """Manages an Earthdata Login bearer token, refreshing before expiry.

    Fetches a token from the NASA URS find-or-create-token endpoint on first
    call and caches it until near expiry. Thread-safe; a single instance can
    be shared across requests.
    """

    def __init__(self, username: str, password: str) -> None:
        """Construct a new EarthdataTokenProvider."""
        self._username = username
        self._password = password
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at: datetime | None = None

    def __call__(self) -> str:
        """Return the current token, refreshing if near expiry."""
        with self._lock:
            if not self._is_valid():
                self._fetch()
        assert self._token is not None
        return self._token

    def _is_valid(self) -> bool:
        if self._token is None:
            return False
        if self._expires_at is None:
            return True
        return self._expires_at > datetime.now(UTC) + TOKEN_REFRESH_BUFFER

    @retry(5, HTTPError, 1)
    def _fetch(self) -> None:
        logger.info("Fetching Earthdata Login bearer token")
        with Client() as client:
            response = client.post(
                "https://urs.earthdata.nasa.gov/api/users/find_or_create_token",
                auth=(self._username, self._password),
                headers={"Accept": "application/json"},
                timeout=10,
            )
        response.raise_for_status()
        data = response.json()
        self._token = data["access_token"]
        expiration_str = data.get("expiration_date")
        if expiration_str:
            try:
                # URS returns expiration_date as "MM/DD/YYYY"
                self._expires_at = datetime.strptime(
                    expiration_str, "%m/%d/%Y"
                ).replace(tzinfo=UTC)
                logger.info(
                    "Earthdata bearer token acquired, expiring %s", expiration_str
                )
            except ValueError:
                logger.warning(
                    "Could not parse token expiration_date %r; treating as non-expiring",
                    expiration_str,
                )
                self._expires_at = None
        else:
            self._expires_at = None
            logger.info("Earthdata bearer token acquired (no expiry reported)")


class GetS3Credentials:
    """Factory for NasaEarthdataCredentialProvider instances, cached by endpoint URL.

    Creates and caches one provider per S3 credentials endpoint so that the
    same provider instance (with its internal credential cache managed by
    obstore) is reused across requests for the same endpoint.
    """

    def __init__(self, username: str, password: str) -> None:
        """Construct a new GetS3Credentials factory."""
        self._auth = (username, password)
        self._cache: dict[str, NasaEarthdataCredentialProvider] = {}
        self._lock = threading.Lock()

    def __call__(self, endpoint: str) -> NasaEarthdataCredentialProvider:
        """Return a credential provider for the given S3 credentials endpoint."""
        with self._lock:
            if endpoint not in self._cache:
                self._cache[endpoint] = NasaEarthdataCredentialProvider(
                    endpoint, auth=self._auth
                )
            return self._cache[endpoint]
