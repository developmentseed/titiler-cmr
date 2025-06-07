"""ZarrReader.

Originaly from titiler-xarray
"""

import pickle
from typing import Any, Dict, Optional, Type
from urllib.parse import urlparse

import attr
import earthaccess
import fsspec
import numpy
import s3fs
import xarray
from cachetools import TTLCache
from morecantile import TileMatrixSet
from rio_tiler.constants import WEB_MERCATOR_TMS, WGS84_CRS
from rio_tiler.errors import InvalidBandName
from rio_tiler.io import BaseReader, MultiBandReader, Reader

from titiler.cmr.settings import CacheSettings

# Use simple in-memory cache for now (we can switch to redis later)
cache_config = CacheSettings()
cache_client: Any = TTLCache(maxsize=cache_config.maxsize, ttl=cache_config.ttl)


def get_filesystem(
    src_path: str,
    protocol: str,
    xr_engine: str,
    anon: bool = True,
    s3_credentials: Optional[Dict] = None,
):
    """
    Get the filesystem for the given source path.
    """
    if protocol == "s3":
        s3_credentials = s3_credentials or {}
        s3_filesystem = s3fs.S3FileSystem(**s3_credentials)
        return (
            s3_filesystem.open(src_path)
            if xr_engine == "h5netcdf"
            else s3fs.S3Map(root=src_path, s3=s3_filesystem)
        )

    elif protocol == "reference":
        reference_args = {"fo": src_path, "remote_options": {"anon": anon}}
        return fsspec.filesystem("reference", **reference_args).get_mapper("")

    elif protocol in ["https", "http", "file"]:
        if protocol in ["https", "http"]:
            filesystem = earthaccess.get_fsspec_https_session()
        else:
            filesystem = fsspec.filesystem(protocol)  # type: ignore
        return (
            filesystem.open(src_path)
            if xr_engine == "h5netcdf"
            else filesystem.get_mapper(src_path)
        )

    else:
        raise ValueError(f"Unsupported protocol: {protocol}")


def xarray_open_dataset(
    src_path: str,
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
        src_path, protocol, xr_engine, s3_credentials=s3_credentials
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


def arrange_coordinates(da: xarray.DataArray) -> xarray.DataArray:
    """
    Arrange coordinates to DataArray.
    An rioxarray.exceptions.InvalidDimensionOrder error is raised if the coordinates are not in the correct order time, y, and x.
    See: https://github.com/corteva/rioxarray/discussions/674
    We conform to using x and y as the spatial dimension names. You can do this a bit more elegantly with metpy but that is a heavy dependency.
    """
    if "x" not in da.dims and "y" not in da.dims:
        latitude_var_name = "lat"
        longitude_var_name = "lon"
        if "latitude" in da.dims:
            latitude_var_name = "latitude"
        if "longitude" in da.dims:
            longitude_var_name = "longitude"
        da = da.rename({latitude_var_name: "y", longitude_var_name: "x"})
    if "time" in da.dims:
        da = da.transpose("time", "y", "x")
    else:
        da = da.transpose("y", "x")
    return da


def get_variable(
    ds: xarray.Dataset,
    variable: str,
    time_slice: Optional[str] = None,
    drop_dim: Optional[str] = None,
) -> xarray.DataArray:
    """Get Xarray variable as DataArray."""
    da = ds[variable]
    da = arrange_coordinates(da)
    # TODO: add test
    if drop_dim:
        dim_to_drop, dim_val = drop_dim.split("=")
        da = da.sel({dim_to_drop: dim_val}).drop(dim_to_drop)
    da = arrange_coordinates(da)

    if (da.x > 180).any():
        # Adjust the longitude coordinates to the -180 to 180 range
        da = da.assign_coords(x=(da.x + 180) % 360 - 180)

        # Sort the dataset by the updated longitude coordinates
        da = da.sortby(da.x)

    # Make sure we have a valid CRS
    crs = da.rio.crs or "epsg:4326"
    da.rio.write_crs(crs, inplace=True)

    if "time" in da.dims:
        if time_slice:
            time_as_str = time_slice.split("T")[0]
            if da["time"].dtype == "O":
                da["time"] = da["time"].astype("datetime64[ns]")
            da = da.sel(
                time=numpy.array(time_as_str, dtype=numpy.datetime64), method="nearest"
            )
        else:
            da = da.isel(time=0)

    return da


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
