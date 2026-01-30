"""ZarrReader.

Originaly from titiler-xarray
"""

import os
import pickle
from collections.abc import Callable
from functools import wraps
from typing import Any, Dict, Optional, cast
from urllib.parse import urlparse

import attr
import earthaccess
import fsspec
import rasterio
import rasterio.env
import s3fs
import xarray
from cachetools import TTLCache
from morecantile.models import TileMatrixSet
from rio_tiler.constants import WEB_MERCATOR_TMS, WGS84_CRS
from rio_tiler.errors import InvalidBandName
from rio_tiler.io.base import BaseReader, MultiBandReader
from rio_tiler.io.rasterio import Reader

from titiler.cmr.logger import logger
from titiler.cmr.settings import CacheSettings

# Use simple in-memory cache for now (we can switch to redis later)
cache_config = CacheSettings()
cache_client: Any = TTLCache(maxsize=cache_config.maxsize, ttl=cache_config.ttl)


def get_filesystem(
    src_path: str,
    protocol: str,
    xr_engine: str,
    auth: earthaccess.Auth,
    anon: bool = True,
    s3_credentials: Optional[Dict] = None,
):
    """Get the filesystem for the given source path."""

    if protocol == "s3":
        s3_filesystem = s3fs.S3FileSystem(
            requester_pays=os.environ.get("AWS_REQUEST_PAYER") == "requester",
            **(s3_credentials or {}),
        )

        logger.info(
            "Using fsspec to open %s %s temporary S3 credentials.",
            src_path,
            "with" if s3_credentials else "without",
        )

        return (
            s3_filesystem.open(src_path)
            if xr_engine == "h5netcdf"
            else s3fs.S3Map(root=src_path, s3=s3_filesystem)
        )

    if protocol == "reference":
        reference_args = {"fo": src_path, "remote_options": {"anon": anon}}
        return fsspec.filesystem("reference", **reference_args).get_mapper("")

    if protocol in {"https", "http", "file"}:
        if protocol in {"https", "http"}:
            filesystem = get_fsspec_filesystem(auth)
        else:
            filesystem = fsspec.filesystem(protocol)  # type: ignore

        return (
            filesystem.open(src_path)
            if xr_engine == "h5netcdf"
            else filesystem.get_mapper(src_path)
        )

    raise ValueError(f"Unsupported protocol: {protocol}")


def get_fsspec_filesystem(auth: earthaccess.Auth) -> fsspec.AbstractFileSystem:
    """Get fsspec HTTPS filesystem with EDL Authorization header."""

    # Since earthaccess does not override the terrible fsspec defaults in its
    # get_fsspec_https_session function, we don't use it.  Further, even if we
    # were to use it, it would not use the Store object associated with the Auth
    # instance we created, but rather it would implicitly create separate Auth
    # and Store instances.
    return fsspec.filesystem(
        "https",
        cache_type="background",
        block_size=8 * 1024 * 1024,
        client_kwargs={
            "headers": {
                # We are assuming that the access token was populated during
                # call to earthaccess.login.
                "Authorization": f"Bearer {auth.token['access_token']}",  # type: ignore
            },
        },
    )


def xarray_open_dataset(
    src_path: str,
    auth: earthaccess.Auth,
    group: Optional[Any] = None,
    decode_times: Optional[bool] = True,
    s3_credentials: Optional[Dict] = None,
) -> xarray.Dataset:
    # TODO: can we import the internals of titiler.xarray.io.xarray_open_dataset?
    """Modified version of titiler.xarray.io.xarray_open_dataset with
    custom handler for earthaccess authentication over https
    """
    # Generate cache key and attempt to fetch the dataset from cache
    cache_key = f"{src_path}_{group}" if group is not None else src_path
    data_bytes = cache_client.get(cache_key, None)
    if data_bytes:
        return pickle.loads(data_bytes)

    parsed = urlparse(src_path)
    protocol = parsed.scheme or "file"

    if any(src_path.lower().endswith(ext) for ext in [".nc", ".nc4"]):
        xr_engine = "h5netcdf"
    else:
        xr_engine = "zarr"

    file_handler = get_filesystem(
        src_path,
        protocol,
        xr_engine,
        anon=(s3_credentials is None),
        auth=auth,
        s3_credentials=s3_credentials,
    )

    # Arguments for xarray.open_dataset
    # Default args
    xr_open_args: Dict[str, Any] = {
        "decode_coords": "all",
        "decode_times": decode_times,
    }

    # Argument if we're opening a datatree
    if group is not None:
        xr_open_args["group"] = group

    # NetCDF arguments
    if xr_engine == "h5netcdf":
        xr_open_args.update(
            {
                "engine": "h5netcdf",
                "lock": False,
            }
        )

        ds = xarray.open_dataset(file_handler, **xr_open_args)

    # Fallback to Zarr
    else:
        ds = xarray.open_zarr(file_handler, **xr_open_args)

    # Serialize the dataset to bytes using pickle
    cache_client[cache_key] = pickle.dumps(ds)

    return ds


@attr.s
class MultiFilesBandsReader(MultiBandReader):
    """Multiple Files as Bands."""

    input: dict[str, str] = attr.ib()
    tms: TileMatrixSet = attr.ib(default=WEB_MERCATOR_TMS)

    reader_options: dict[str, Any] = attr.ib(factory=dict)
    reader: type[BaseReader] = attr.ib(default=Reader)

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

        self.reader = make_env_inheriting_reader(self.reader)

    def _get_band_url(self, band: str) -> str:
        """Validate band's name and return band's url."""
        if band not in self.bands:
            raise InvalidBandName(f"{band} is not valid")

        return self.input[band]


def make_env_inheriting_reader[R: BaseReader](reader_type: type[R]) -> type[R]:
    """Decorate a BaseReader type to inherit the current rasterio environment.

    Decorates the specified type's initialization method (`__init__`) such that
    it executes within a rasterio environment that is configured identically to
    the rasterio environment that is active at the time this function is
    invoked.

    This allows initialization to occur in another thread, but within the
    context of a rasterio environment configured the same as the current
    environment (which may be in a different thread).

    Parameters
    ----------
    reader_type
        The base reader class to decorate.

    Returns
    -------
    type[R]
        A new reader class that inherits the current (at the time of invoking
        this function) rasterio environment during initialization of instances
        of the class.
    """
    EnvInheritingReader = type(
        "EnvInheritingReader",
        (reader_type,),
        {"__init__": inherit_rasterio_env(reader_type.__init__)},
    )

    return cast(type[R], EnvInheritingReader)


def inherit_rasterio_env[**P, R](f: Callable[P, R]) -> Callable[P, R]:
    """Wrap a function to run in a rasterio environment like the current one.

    This function differs from the function
    `rasterio.env.ensure_env_with_credentials` in that this function copies the
    rasterio environment active at the time this function is invoked, rather
    than at the time the returned function is invoked. This enables replicating
    the environment from one thread (the one active at the time this function is
    invoked) to another thread (the one active at the time the returned function
    is invoked).

    Parameters
    ----------
    f
        The function to wrap.

    Returns
    -------
    Callable[P, R]
        A function that executes within a rasterio environment configured
        identically to the currently active environment, or the original
        function if no rasterio environment is currently active.
    """
    if (env := rasterio.env.getenv() if rasterio.env.hasenv() else None) is None:
        return f

    @wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        with rasterio.Env():
            rasterio.env.setenv(**env)
            return f(*args, **kwargs)

    return wrapper
