"""CMR Granule Reader"""

import pickle
import threading
import time
import urllib.parse
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterator,
    Literal,
    Sequence,
    Set,
    Type,
)

import attr
import numpy as np
import obstore.store
import rasterio
import xarray as xr
from cachetools import TTLCache
from morecantile import TileMatrixSet
from obspec_utils.readers import BlockStoreReader
from rasterio.session import AWSSession
from rio_tiler.constants import WEB_MERCATOR_TMS, WGS84_CRS
from rio_tiler.errors import InvalidAssetName, MissingAssets
from rio_tiler.expression import get_expression_blocks
from rio_tiler.io.base import MultiBaseReader
from rio_tiler.io.rasterio import Reader
from rio_tiler.io.xarray import Options, XarrayReader
from rio_tiler.types import AssetInfo, AssetWithOptions
from titiler.xarray.io import _parse_dsl
from xarray import DataArray, Dataset
from xarray import open_dataset as xarray_open_dataset

from titiler.cmr.credentials import EarthdataS3CredentialProvider
from titiler.cmr.errors import InvalidMediaType, S3CredentialsEndpointMissing
from titiler.cmr.logger import logger
from titiler.cmr.models import Granule
from titiler.cmr.settings import CacheSettings

if TYPE_CHECKING:
    from obstore.store import ClientConfig

NETCDF = "application/netcdf"
HDF5 = "application/x-hdf5"

MEDIA_TYPES = {
    ".tif": "image/tiff; application=geotiff; profile=cloud-optimized",
    ".nc": NETCDF,
    ".h5": HDF5,
}

DEFAULT_VALID_TYPES = set(MEDIA_TYPES.keys())

X_DIM_NAMES = [
    "lon",
    "longitude",
    "LON",
    "LONGITUDE",
    "Lon",
    "Longitude",
    "xCoordinates",  # for NISAR :|
]
Y_DIM_NAMES = [
    "lat",
    "latitude",
    "LAT",
    "LATITUDE",
    "Lat",
    "Latitude",
    "yCoordinates",  # for NISAR :|
]

cache_config = CacheSettings()
cache_client: Any = TTLCache(maxsize=cache_config.maxsize, ttl=cache_config.ttl)
_dataset_cache_lock = threading.Lock()


def _arrange_dims(
    da: DataArray, x_dim_names: list[str], y_dim_names: list[str]
) -> DataArray:
    """Arrange coordinates and time dimensions.

    An rioxarray.exceptions.InvalidDimensionOrder error is raised if the coordinates are not in the correct order time, y, and x.
    See: https://github.com/corteva/rioxarray/discussions/674

    We conform to using x and y as the spatial dimension names..

    """
    if "x" not in da.dims and "y" not in da.dims:
        try:
            y_dim = next(name for name in y_dim_names if name in da.dims)
            x_dim = next(name for name in x_dim_names if name in da.dims)
        except StopIteration as e:
            raise ValueError(
                f"Couldn't find X and Y spatial coordinates in {da.dims}"
            ) from e

        da = da.rename({y_dim: "y", x_dim: "x"})

    if extra_dims := [d for d in da.dims if d not in ["x", "y"]]:
        da = da.transpose(*extra_dims, "y", "x")
    else:
        da = da.transpose("y", "x")

    # If min/max values are stored in `valid_range` we add them in `valid_min/valid_max`
    vmin, vmax = da.attrs.get("valid_min"), da.attrs.get("valid_max")
    if "valid_range" in da.attrs and not (vmin is not None and vmax is not None):
        valid_range = da.attrs.get("valid_range")
        da.attrs.update({"valid_min": valid_range[0], "valid_max": valid_range[1]})  # type: ignore

    return da


def get_variables(
    ds: Dataset,
    variables: list[str],
    sel: list[str] | None = None,
    expression: str | None = None,
    x_dim_names: list[str] = X_DIM_NAMES,
    y_dim_names: list[str] = Y_DIM_NAMES,
) -> DataArray:
    """Get Xarray variable as DataArray.

    Args:
        ds (xarray.Dataset): Xarray Dataset.
        variable (str): Variable to extract from the Dataset.
        sel (list of str, optional): List of Xarray Indexes.

    Returns:
        xarray.DataArray: 2D or 3D DataArray.

    """
    da = xr.concat([ds[variable] for variable in variables], dim="band")
    squeeze_dims = [d for d in da.dims if d != "band" and da.sizes[d] == 1]
    if squeeze_dims:
        da = da.squeeze(squeeze_dims)

    for selector in _parse_dsl(sel):
        dimension = selector["dimension"]
        values = selector["values"]
        method = selector["method"]

        # TODO: add more casting
        # cast string to dtype of the dimension
        if da[dimension].dtype != "O":
            values = [da[dimension].dtype.type(v) for v in values]

        da = da.sel(
            {dimension: values[0] if len(values) < 2 else values},
            method=method,
        )

    da = _arrange_dims(da, x_dim_names=x_dim_names, y_dim_names=y_dim_names)

    if expression:
        logger.info(f"applying expression: {expression}")
        pre_expression_crs = da.rio.crs
        expression_blocks = get_expression_blocks(expression)
        band_vars = {
            f"b{i + 1}": da.isel(band=i, drop=True) for i in range(da.sizes["band"])
        }
        namespace = {**band_vars, "np": np, "xr": xr}
        results = [
            eval(block, {"__builtins__": {}}, namespace) for block in expression_blocks
        ]
        da = results[0] if len(results) == 1 else xr.concat(results, dim="band")
        if pre_expression_crs is not None:
            da = da.rio.write_crs(pre_expression_crs)

    # Make sure we have a valid CRS
    crs = da.rio.crs or "epsg:4326"
    da = da.rio.write_crs(crs)

    if crs == "epsg:4326" and (da.x > 180).any():
        # Adjust the longitude coordinates to the -180 to 180 range
        da = da.assign_coords(x=(da.x + 180) % 360 - 180)

        # Sort the dataset by the updated longitude coordinates
        da = da.sortby(da.x)

    assert len(da.dims) in [2, 3], "titiler.xarray can only work with 2D or 3D dataset"

    return da


def open_dataset(
    src_path: str,
    group: str | None = None,
    decode_times: bool = True,
    decode_coords: Literal["all", "coordinates"] = "all",
    auth_token: str | None = None,
    credential_provider: EarthdataS3CredentialProvider | None = None,
    **kwargs,
) -> Dataset:
    """Open a remote NetCDF/HDF5 dataset, using a cache to avoid redundant fetches."""

    # Generate cache key and attempt to fetch the dataset from cache
    cache_key = f"{src_path}_{group}" if group is not None else src_path

    # Fast path: no lock needed for a cache hit
    data_bytes = cache_client.get(cache_key, None)
    if data_bytes:
        logger.info(f"loading {cache_key} from cache")
        return pickle.loads(data_bytes)

    # Slow path: serialize concurrent openers to prevent cache stampede
    with _dataset_cache_lock:
        # Re-check: another thread may have populated the cache while we waited
        data_bytes = cache_client.get(cache_key, None)
        if data_bytes:
            logger.info(f"loading {cache_key} from cache")
            return pickle.loads(data_bytes)

        parsed = urllib.parse.urlparse(src_path)
        store_root = f"{parsed.scheme}://{parsed.netloc}"

        logger.info("getting object store")
        if credential_provider is not None:
            store = obstore.store.from_url(
                store_root, credential_provider=credential_provider
            )
        elif auth_token:
            client_options: ClientConfig = {
                "default_headers": {"Authorization": f"Bearer {auth_token}"}
            }
            store = obstore.store.from_url(store_root, client_options=client_options)
        else:
            store = obstore.store.from_url(store_root)

        reader = BlockStoreReader(store, parsed.path, block_size=8 * 1024**2)

        logger.info(f"opening {src_path}")
        t0 = time.perf_counter()
        ds = xarray_open_dataset(
            reader,  # type: ignore[arg-type]
            group=group,
            decode_times=decode_times,
            decode_coords=decode_coords,
            decode_timedelta=decode_times,
            phony_dims="sort",
            engine="h5netcdf",
            lock=False,
            chunks={},
            **kwargs,
        )
        read_time = time.perf_counter() - t0
        logger.info(f"reading {src_path} took {read_time:.2f}s")

        cache_client[cache_key] = pickle.dumps(ds)

        return ds


def _get_assets(
    granule: Granule,
    regex: str | None = None,
    include_asset_types: Set[str] | None = None,
    exclude_asset_types: Set[str] | None = None,
) -> Iterator:
    """Get valid asset list."""
    for asset, asset_info in granule.get_assets(regex=regex).items():
        _ext = asset_info.ext

        if _ext and (exclude_asset_types and _ext in exclude_asset_types):
            continue

        if _ext and (include_asset_types and _ext not in include_asset_types):
            continue

        yield asset


def _to_granule(granule: Granule | dict) -> Granule:
    if isinstance(granule, dict):
        return Granule(**granule)

    return granule


@attr.s
class MultiBaseGranuleReader(MultiBaseReader):
    """CMR Granule Reader."""

    granule: Granule = attr.ib(converter=_to_granule)
    input: str | None = attr.ib(default=None)

    tms: TileMatrixSet = attr.ib(default=WEB_MERCATOR_TMS)
    minzoom: int = attr.ib(default=None)
    maxzoom: int = attr.ib(default=None)

    assets_regex: str | None = attr.ib(default=None)
    s3_access: bool = attr.ib(default=False)
    auth_token: str | None = attr.ib(default=None)
    get_s3_credentials: Callable | None = attr.ib(default=None)

    include_assets: Set[str] | None = attr.ib(default=None)
    exclude_assets: Set[str] | None = attr.ib(default=None)

    include_asset_types: Set[str] = attr.ib(default=DEFAULT_VALID_TYPES)
    exclude_asset_types: Set[str] | None = attr.ib(default=None)

    assets: Sequence[str] = attr.ib(init=False)
    default_assets: Sequence[str] | None = attr.ib(default=["0"])

    reader: Type[Reader] | Type[XarrayReader] = attr.ib(default=Reader)
    reader_options: dict[str, Any] = attr.ib(factory=dict)

    fetch_options: dict[str, Any] = attr.ib(factory=dict)

    ctx: rasterio.Env = attr.ib(default=rasterio.Env)

    _credential_provider: EarthdataS3CredentialProvider | None = attr.ib(
        init=False, default=None
    )

    def __attrs_post_init__(self):
        """Load asset list and set attributes"""

        self.bounds = tuple(self.granule.bbox)
        self.crs = WGS84_CRS

        self.minzoom = self.minzoom if self.minzoom is not None else self._minzoom
        self.maxzoom = self.maxzoom if self.maxzoom is not None else self._maxzoom

        self.assets = self.get_asset_list()
        if not self.assets:
            raise MissingAssets(
                "No valid asset found. Asset's media types not supported"
            )

        if self.s3_access and self.get_s3_credentials is not None:
            try:
                endpoint = self.granule.s3_credentials_endpoint
                self._credential_provider = self.get_s3_credentials(endpoint)
            except S3CredentialsEndpointMissing:
                logger.warning(
                    "No S3 credentials endpoint found for granule %s; "
                    "falling back to unauthenticated S3 access",
                    self.granule.id,
                )

    def get_asset_list(self) -> list[str]:
        """Get valid asset list"""
        return list(
            _get_assets(
                self.granule,
                regex=self.assets_regex,
                include_asset_types=self.include_asset_types,
                exclude_asset_types=self.exclude_asset_types,
            )
        )

    def _get_asset_info(self, asset: str | AssetWithOptions) -> AssetInfo:
        """Validate asset names and return asset's info."""
        assert isinstance(asset, str)
        if asset not in self.assets:
            raise InvalidAssetName(
                f"'{asset}' is not valid, should be one of {self.assets}"
            )

        asset_info = self.granule.get_assets(regex=self.assets_regex)[asset]
        media_type = MEDIA_TYPES.get(asset_info.ext)

        if not media_type:
            raise InvalidMediaType(f"{asset} has an invalid media type")

        reader_options = self.reader_options.copy()

        env = {}
        if self._credential_provider is not None:
            creds = self._credential_provider()
            env = {
                "session": AWSSession(
                    aws_access_key_id=creds["access_key_id"],
                    aws_secret_access_key=creds["secret_access_key"],
                    aws_session_token=creds["token"],
                )
            }

        info = AssetInfo(
            name=asset,
            url=asset_info.direct_href if self.s3_access else asset_info.external_href,
            media_type=media_type,
            reader_options=reader_options,
            method_options={},
            env=env,
        )

        return info


@attr.s
class XarrayGranuleReader(XarrayReader):
    """Custom Xarray Reader that gets the asset href from a Granule"""

    src_path: Granule = attr.ib()
    variables: list[str] = attr.ib()

    options: Options = attr.ib(factory=Options)

    # xarray.Dataset options
    opener: Callable[..., Dataset] = attr.ib(default=open_dataset)
    opener_options: dict = attr.ib(factory=dict)

    s3_access: bool = attr.ib(default=False)
    auth_token: str | None = attr.ib(default=None)
    get_s3_credentials: Callable | None = attr.ib(default=None)

    group: str | None = attr.ib(default=None)
    decode_times: bool = attr.ib(default=True)

    # xarray.DataArray options
    sel: list[str] | None = attr.ib(default=None)
    method: Literal["nearest", "pad", "ffill", "backfill", "bfill"] | None = attr.ib(
        default=None
    )

    tms: TileMatrixSet = attr.ib(default=WEB_MERCATOR_TMS)
    expression: str | None = attr.ib(default=None)

    ds: Dataset = attr.ib(init=False)
    input: DataArray = attr.ib(init=False)

    _dims: list = attr.ib(init=False, factory=list)

    def __attrs_post_init__(self):
        """Set bounds and CRS."""
        opener_options = {
            "group": self.group,
            "decode_times": self.decode_times,
            **self.opener_options,
        }
        if self.s3_access and self.get_s3_credentials is not None:
            try:
                endpoint = self.src_path.s3_credentials_endpoint
                opener_options["credential_provider"] = self.get_s3_credentials(
                    endpoint
                )
            except S3CredentialsEndpointMissing:
                logger.warning(
                    "No S3 credentials endpoint found for granule %s; "
                    "falling back to unauthenticated S3 access",
                    self.src_path.id,
                )
                opener_options["auth_token"] = self.auth_token
        else:
            opener_options["auth_token"] = self.auth_token

        assets = self.src_path.get_assets()
        asset = assets["0"]
        href = asset.direct_href if self.s3_access else asset.external_href

        # for this reader the assets are keyed with numeric index
        # the real data asset is assumed to be the first one
        self.ds = self.opener(href, **opener_options)
        self.input = get_variables(
            self.ds,
            self.variables,
            sel=self.sel,
            expression=self.expression,
        )
        super().__attrs_post_init__()
