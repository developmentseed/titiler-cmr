"""CMR Granule Reader"""

import functools
import pickle
import urllib.parse
from typing import (
    TYPE_CHECKING,
    Any,
    Iterator,
    Literal,
    Sequence,
    Set,
    Type,
)

import attr
import obstore.store
import rasterio
from cachetools import TTLCache
from morecantile import TileMatrixSet
from obspec_utils.readers import BlockStoreReader
from rio_tiler.constants import WEB_MERCATOR_TMS, WGS84_CRS
from rio_tiler.errors import InvalidAssetName, MissingAssets
from rio_tiler.io.base import BaseReader, MultiBaseReader
from rio_tiler.io.rasterio import Reader
from rio_tiler.types import AssetInfo
from titiler.xarray.io import Reader as XarrayReader
from xarray import Dataset
from xarray import open_dataset as xarray_open_dataset

from titiler.cmr.errors import InvalidMediaType
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

cache_config = CacheSettings()
cache_client: Any = TTLCache(maxsize=cache_config.maxsize, ttl=cache_config.ttl)


def open_dataset(
    src_path: str,
    group: str | None = None,
    decode_times: bool = True,
    decode_coords: Literal["all", "coordinates"] = "all",
    auth_token: str | None = None,
    **kwargs,
) -> Dataset:
    """Open a remote NetCDF/HDF5 dataset, using a cache to avoid redundant fetches."""
    logger.info(f"opening {src_path}")

    # Generate cache key and attempt to fetch the dataset from cache
    cache_key = f"{src_path}_{group}" if group is not None else src_path
    data_bytes = cache_client.get(cache_key, None)
    if data_bytes:
        logger.info(f"loading {cache_key} from cache")
        return pickle.loads(data_bytes)

    parsed = urllib.parse.urlparse(src_path)

    store_root = f"{parsed.scheme}://{parsed.netloc}"
    client_options: ClientConfig = {}
    if auth_token:
        client_options["default_headers"] = {"Authorization": f"Bearer {auth_token}"}

    store = obstore.store.from_url(store_root, client_options=client_options)

    reader = BlockStoreReader(store, parsed.path, block_size=8 * 1024**2)

    ds = xarray_open_dataset(
        reader,
        group=group,
        decode_times=decode_times,
        decode_coords=decode_coords,
        engine="h5netcdf",
        **kwargs,
    )

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
class GranuleReader(MultiBaseReader):
    """CMR Granule Reader."""

    granule: Granule = attr.ib(converter=_to_granule)
    input: str | None = attr.ib(default=None)

    tms: TileMatrixSet = attr.ib(default=WEB_MERCATOR_TMS)
    minzoom: int = attr.ib(default=None)
    maxzoom: int = attr.ib(default=None)

    assets_regex: str | None = attr.ib(default=None)
    s3_access: bool = attr.ib(default=False)
    auth_token: str | None = attr.ib(default=None)

    include_assets: Set[str] | None = attr.ib(default=None)
    exclude_assets: Set[str] | None = attr.ib(default=None)

    include_asset_types: Set[str] = attr.ib(default=DEFAULT_VALID_TYPES)
    exclude_asset_types: Set[str] | None = attr.ib(default=None)

    assets: Sequence[str] = attr.ib(init=False)
    default_assets: Sequence[str] | None = attr.ib(default=["0"])

    reader: Type[BaseReader] = attr.ib(default=Reader)
    reader_options: dict[str, Any] = attr.ib(factory=dict)

    fetch_options: dict[str, Any] = attr.ib(factory=dict)

    ctx: rasterio.Env = attr.ib(default=rasterio.Env)

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

    def _get_reader(self, asset_info: AssetInfo) -> type[Reader] | type[XarrayReader]:
        """Get Asset Reader."""
        if asset_info["media_type"] in [NETCDF, HDF5]:
            return XarrayReader
        else:
            return Reader

    def _get_asset_info(self, asset: str) -> AssetInfo:
        """Validate asset names and return asset's info."""
        if asset not in self.assets:
            raise InvalidAssetName(
                f"'{asset}' is not valid, should be one of {self.assets}"
            )

        asset_info = self.granule.get_assets(regex=self.assets_regex)[asset]
        media_type = MEDIA_TYPES.get(asset_info.ext)

        if not media_type:
            raise InvalidMediaType(f"{asset} has an invalid media type")

        reader_options = self.reader_options.copy()
        if media_type in [NETCDF, HDF5]:
            opener = functools.partial(open_dataset, auth_token=self.auth_token)
            reader_options.update({"opener": opener})

        info = AssetInfo(
            name=asset,
            url=asset_info.direct_href if self.s3_access else asset_info.external_href,
            media_type=media_type,
            reader_options=reader_options,
            method_options={},
        )

        return info
