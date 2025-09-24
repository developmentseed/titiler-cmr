"""ZarrReader.

Originaly from titiler-xarray
"""
from __future__ import annotations

import os
import pickle
from datetime import datetime
from typing import Any, Dict, Optional, Type
from urllib.parse import urlparse

import attr

import xarray as xr
import obstore
import earthaccess
from zarr.storage import ObjectStore

from cachetools import TTLCache
from morecantile import TileMatrixSet
from rio_tiler.constants import WEB_MERCATOR_TMS, WGS84_CRS
from rio_tiler.errors import InvalidBandName
from rio_tiler.io import BaseReader, MultiBandReader, Reader
from obstore.auth.earthdata import NasaEarthdataCredentialProvider

from titiler.cmr.settings import CacheSettings

# Use simple in-memory cache for now (we can switch to redis later)
cache_config = CacheSettings()
cache_client: Any = TTLCache(maxsize=cache_config.maxsize, ttl=cache_config.ttl)

def get_obstore_s3_credentials():
        """Get obstore credentials from earthaccess."""
        try:
            auth = earthaccess.login(strategy="environment")
        except Exception:
            auth = earthaccess.login(strategy="interactive")
        creds = auth.get_s3_credentials(daac="PODAAC")
        return {
            "access_key_id": creds["accessKeyId"],
            "secret_access_key": creds["secretAccessKey"],
            "token": creds["sessionToken"],
            "expires_at": datetime.fromisoformat(creds["expiration"]),
        }

class ObstoreReader:
    _reader: ReadableFile

    def __init__(self, store: ObjectStore, path: str) -> None:
        """
        Create an obstore file reader that implements the read, readall, seek, and tell methods, which
        can be used in libraries that expect file-like objects.

        Parameters
        ----------
        store
            [ObjectStore][obstore.store.ObjectStore] for reading the file.
        path
            The path to the file within the store. This should not include the prefix.
        """
        self._reader = obstore.open_reader(store, path)

    def read(self, size: int, /) -> bytes:
        return self._reader.read(size).to_bytes()

    def readall(self) -> bytes:
        return self._reader.read().to_bytes()

    def seek(self, offset: int, whence: int = 0, /):
        # TODO: Check on default for whence
        return self._reader.seek(offset, whence)

    def tell(self) -> int:
        return self._reader.tell()


def parse_url_to_store_and_key(src_path: str, credential_provider=None):
        """Parse URL to get obstore and file key/path."""
        parsed = urlparse(src_path)
        scheme = (parsed.scheme or "").lower()
        
        if scheme == "s3":
            # s3://bucket/path/file.nc
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            if credential_provider is None:
                credential_provider = get_obstore_s3_credentials()
                #credential_provider = NasaEarthdataCredentialProvider()
            store = obstore.store.from_url(f"s3://{bucket}", credential_provider=credential_provider)
            
        elif scheme in ("http", "https"):
            # https://host/path/file.nc
            base = f"{scheme}://{parsed.netloc}"
            key = parsed.path.lstrip("/")
            store = obstore.store.from_url(base, credential_provider=credential_provider)
            
        elif scheme in ("", "file"):
            # Local file: file:///path/file.nc or /path/file.nc
            local_path = parsed.path if scheme == "file" else src_path
            directory = os.path.dirname(local_path)
            key = os.path.basename(local_path)
            store = obstore.store.from_url(f"file://{directory}")
            
        else:
            raise ValueError(f"Unsupported URL scheme: {scheme}")
        
        return store, key

def xarray_open_dataset(
    src_path: str,
    group: Optional[str] = None,
    decode_times: bool = True,
    credential_provider: Optional[object] = None,
    *,
    consolidated: Optional[bool] = True,
    use_cache: bool = True,
    **kwargs: Any,
):
    # TODO: can we import the internals of titiler.xarray.io.xarray_open_dataset?
    ## TODO 2: Virtualizarr??!
    """
    Open Xarray dataset via obstore (no earthaccess/fsspec/s3fs).
    """
    # Generate cache key and attempt to fetch the dataset from cache
    cache_key = f"{src_path}_{group}" if group is not None else src_path
    data_bytes = cache_client.get(cache_key, None)
    if data_bytes:
        return pickle.loads(data_bytes)

    parsed = urlparse(src_path)
    protocol = parsed.scheme or "file"
    host = parsed.hostname or ""

    is_netcdf = src_path.lower().endswith((".nc", ".nc4"))

    # pick a default provider for S3/Earthdata if none provided
    #if credential_provider is None and (protocol == "s3" or any(k in host for k in ["nasa.gov", "earthdata", "urs.earthdata"])):
    #    credential_provider = NasaEarthdataCredentialProvider()

    if not is_netcdf:
        # Zarr path: use obstore → zarr
        store = obstore.store.from_url(src_path, credential_provider=credential_provider)
        zstore = ObjectStore(store, read_only=True)
        ds = xr.open_dataset(
            zstore,
            group=group,
            engine="zarr",
            decode_times=decode_times,
            decode_coords="all",
            consolidated=consolidated,
            **kwargs,
        )
    else:
        store, key = parse_url_to_store_and_key(src_path)
        reader = ObstoreReader(store, key)

        ds = xr.open_dataset(
            reader,
            engine="h5netcdf",
            decode_times=decode_times,
            decode_coords="all",
            **kwargs,
            )

    # Serialize the dataset to bytes using pickle
    #cache_client[cache_key] = pickle.dumps(ds)
    
    return ds


@attr.s
class MultiFilesBandsReader(MultiBandReader):
    """Multiple Files as Bands."""

    input: Dict[str, str] = attr.ib()
    tms: TileMatrixSet = attr.ib(default=WEB_MERCATOR_TMS)

    reader_options: Dict = attr.ib(factory=dict)
    reader: Type[BaseReader] = attr.ib(default=Reader)

    minzoom: int = attr.ib()
    maxzoom: int = attr.ib()

    @minzoom.default
    def _minzoom(self):
        return self.tms.minzoom

    @maxzoom.default
    def _maxzoom(self):
        return self.tms.maxzoom

    def __attrs_post_init__(self):
        """Fetch Reference band to get the bounds."""
        self.bands = list(self.input)
        self.bounds = (-180.0, -90, 180.0, 90)
        self.crs = WGS84_CRS
        # with self.reader(
        #     self.input[0],
        #     tms=self.tms,
        #     **self.reader_options,
        # ) as cog:
        #     self.bounds = cog.bounds
        #     self.crs = cog.crs
        #     self.minzoom = cog.minzoom
        #     self.maxzoom = cog.maxzoom

    def _get_band_url(self, band: str) -> str:
        """Validate band's name and return band's url."""
        if band not in self.bands:
            raise InvalidBandName(f"{band} is not valid")

        return self.input[band]
