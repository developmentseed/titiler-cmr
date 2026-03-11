"""NASA Earthdata credential management."""

import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import cachetools
from httpx import Client, HTTPError

from titiler.cmr.logger import logger
from titiler.cmr.utils import retry

if TYPE_CHECKING:
    from obstore.store import S3Config, S3Credential

CREDENTIAL_REFRESH_BUFFER = timedelta(minutes=5)
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
        self._on_refresh: list[Callable[[], None]] = []

    def __call__(self) -> str:
        """Return the current token, refreshing if near expiry."""
        with self._lock:
            if not self._is_valid():
                self._fetch()
                for cb in self._on_refresh:
                    cb()
        assert self._token is not None
        return self._token

    def register_refresh_callback(self, cb: Callable[[], None]) -> None:
        """Register a callback invoked after each token refresh."""
        self._on_refresh.append(cb)

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
    """Factory for EarthdataS3CredentialProvider instances, cached by endpoint URL.

    Wraps provider creation with a TTL-based cache so the same provider
    instance (with its own internal credential cache) is reused across
    requests for the same endpoint. The cache can be cleared (e.g. on token
    refresh) so new providers pick up the latest token.
    """

    def __init__(self, token_provider: Callable[[], str]) -> None:
        """Construct a new GetS3Credentials factory."""
        self._token_provider = token_provider
        self._cache: cachetools.TTLCache = cachetools.TTLCache(maxsize=100, ttl=50 * 60)
        self._lock = threading.RLock()

    @cachetools.cachedmethod(lambda self: self._cache, lock=lambda self: self._lock)
    def __call__(self, endpoint: str) -> "EarthdataS3CredentialProvider":
        """Return a credential provider for the given S3 credentials endpoint."""
        return EarthdataS3CredentialProvider(endpoint, self._token_provider())

    def clear(self) -> None:
        """Clear the credential provider cache."""
        with self._lock:
            self._cache.clear()
        logger.info("S3 credential provider cache cleared after token refresh")


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

    def __getstate__(self) -> dict:
        """Return picklable state, excluding the threading lock."""
        state = self.__dict__.copy()
        del state["_lock"]
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore state and recreate the threading lock after unpickling."""
        self.__dict__.update(state)
        self._lock = threading.Lock()

    def __call__(self) -> "S3Credential":
        """Return cached credentials, refreshing if near expiry."""
        with self._lock:
            if not self._is_valid():
                self._cached = self._fetch()
            assert self._cached is not None
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
