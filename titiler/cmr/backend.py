"""TiTiler.cmr custom Mosaic Backend."""

from typing import Any, Dict, List, Optional, Tuple, Type, TypedDict

import attr
import earthaccess
import rasterio
from cachetools import TTLCache, cached
from cachetools.keys import hashkey
from cogeo_mosaic.backends import BaseBackend
from cogeo_mosaic.errors import NoAssetFoundError
from cogeo_mosaic.mosaic import MosaicJSON
from earthaccess.auth import Auth
from morecantile import Tile, TileMatrixSet
from rasterio.crs import CRS
from rasterio.warp import transform_bounds
from rio_tiler.constants import WEB_MERCATOR_TMS, WGS84_CRS
from rio_tiler.io import BaseReader, Reader
from rio_tiler.models import ImageData
from rio_tiler.mosaic import mosaic_reader
from rio_tiler.types import BBox

from titiler.cmr.settings import AuthSettings, CacheSettings, RetrySettings
from titiler.cmr.utils import retry

cache_config = CacheSettings()
retry_config = RetrySettings()
s3_auth_config = AuthSettings()


@cached(  # type: ignore
    TTLCache(maxsize=100, ttl=60),
    key=lambda auth, daac: hashkey(auth.tokens[0]["access_token"], daac),
)
def aws_s3_credential(auth: Auth, provider: str) -> Dict:
    """Get AWS S3 credential through earthaccess."""
    return auth.get_s3_credentials(provider=provider)


class Asset(TypedDict, total=False):
    """Simple Asset model."""

    url: str
    type: str
    provider: str


@attr.s
class CMRBackend(BaseBackend):
    """CMR Mosaic Backend."""

    tms: TileMatrixSet = attr.ib(default=WEB_MERCATOR_TMS)
    minzoom: int = attr.ib()
    maxzoom: int = attr.ib()

    reader: Type[BaseReader] = attr.ib(default=Reader)
    reader_options: Dict = attr.ib(factory=dict)

    # default values for bounds
    bounds: BBox = attr.ib(default=(-180, -90, 180, 90))

    crs: CRS = attr.ib(default=WGS84_CRS)
    geographic_crs: CRS = attr.ib(default=WGS84_CRS)

    # The reader is read-only (outside init)
    mosaic_def: MosaicJSON = attr.ib(init=False)

    auth: Optional[Auth] = attr.ib(default=None)

    input: str = attr.ib("CMR", init=False)

    _backend_name = "CMR"

    def __attrs_post_init__(self) -> None:
        """Post Init."""
        # Construct a FAKE mosaicJSON
        # mosaic_def has to be defined.
        # we set `tiles` to an empty list.
        self.mosaic_def = MosaicJSON(
            mosaicjson="0.0.3",
            name=self.input,
            bounds=self.bounds,
            minzoom=self.minzoom,
            maxzoom=self.maxzoom,
            tiles={},
        )

    @minzoom.default
    def _minzoom(self):
        return self.tms.minzoom

    @maxzoom.default
    def _maxzoom(self):
        return self.tms.maxzoom

    def write(self, overwrite: bool = True) -> None:
        """This method is not used but is required by the abstract class."""
        pass

    def update(self) -> None:
        """We overwrite the default method."""
        pass

    def _read(self) -> MosaicJSON:
        """This method is not used but is required by the abstract class."""
        pass

    def assets_for_tile(self, x: int, y: int, z: int, **kwargs: Any) -> List[Asset]:
        """Retrieve assets for tile."""
        bbox = self.tms.bounds(Tile(x, y, z))
        return self.get_assets(*bbox, **kwargs)

    def assets_for_point(
        self,
        lng: float,
        lat: float,
        coord_crs: CRS = WGS84_CRS,
        **kwargs: Any,
    ) -> List[Asset]:
        """Retrieve assets for point."""
        raise NotImplementedError

    def assets_for_bbox(
        self,
        xmin: float,
        ymin: float,
        xmax: float,
        ymax: float,
        coord_crs: CRS = WGS84_CRS,
        **kwargs: Any,
    ) -> List[Asset]:
        """Retrieve assets for bbox."""
        if coord_crs != WGS84_CRS:
            xmin, ymin, xmax, ymax = transform_bounds(
                coord_crs,
                WGS84_CRS,
                xmin,
                ymin,
                xmax,
                ymax,
            )

        return self.get_assets(xmin, ymin, xmax, ymax, **kwargs)

    @cached(  # type: ignore
        TTLCache(maxsize=cache_config.maxsize, ttl=cache_config.ttl),
        key=lambda self, xmin, ymin, xmax, ymax, **kwargs: hashkey(
            xmin, ymin, xmax, ymax, **kwargs
        ),
    )
    @retry(
        tries=retry_config.retry,
        delay=retry_config.delay,
        exceptions=(),
    )
    def get_assets(
        self,
        xmin: float,
        ymin: float,
        xmax: float,
        ymax: float,
        limit: int = 100,
        **kwargs: Any,
    ) -> List[Asset]:
        """Find assets."""
        try:
            results = earthaccess.search_data(
                bounding_box=(xmin, ymin, xmax, ymax),
                count=limit,
                **kwargs,
            )
        except RuntimeError:
            return []

        assets: List[Asset] = []
        for r in results:
            assets.append(
                {
                    "url": r.data_links(access="direct")[
                        0
                    ],  # NOTE: should we not do this?
                    "provider": r["meta"]["provider-id"],
                }
            )

        return assets

    @property
    def _quadkeys(self) -> List[str]:
        return []

    def tile(
        self,
        tile_x: int,
        tile_y: int,
        tile_z: int,
        cmr_query: Dict,
        **kwargs: Any,
    ) -> Tuple[ImageData, List[str]]:
        """Get Tile from multiple observation."""
        mosaic_assets = self.assets_for_tile(
            tile_x,
            tile_y,
            tile_z,
            **cmr_query,
        )

        if not mosaic_assets:
            raise NoAssetFoundError(
                f"No assets found for tile {tile_z}-{tile_x}-{tile_y}"
            )

        def _reader(asset: Asset, x: int, y: int, z: int, **kwargs: Any) -> ImageData:
            if s3_auth_config.strategy == "environment" and self.auth:
                s3_credentials = aws_s3_credential(self.auth, asset["provider"])

            else:
                s3_credentials = None

            if isinstance(self.reader, Reader):
                aws_session = None
                if s3_credentials:
                    aws_session = rasterio.session.AWSSession(
                        aws_access_key_id=s3_credentials["accessKeyId"],
                        aws_secret_access_key=s3_credentials["secretAccessKey"],
                        aws_session_token=s3_credentials["sessionToken"],
                    )

                with rasterio.Env(aws_session):
                    with self.reader(
                        asset["url"],
                        tms=self.tms,
                        **self.reader_options,
                    ) as src_dst:
                        return src_dst.tile(x, y, z, **kwargs)

            if s3_credentials:
                options = {
                    **self.reader_options,
                    "s3_credentials": {
                        "key": s3_credentials["accessKeyId"],
                        "secret": s3_credentials["secretAccessKey"],
                        "token": s3_credentials["sessionToken"],
                    },
                }
            else:
                options = self.reader_options

            with self.reader(
                asset["url"],
                tms=self.tms,
                **options,
            ) as src_dst:
                return src_dst.tile(x, y, z, **kwargs)

        return mosaic_reader(mosaic_assets, _reader, tile_x, tile_y, tile_z, **kwargs)

    def point(
        self,
        lon: float,
        lat: float,
        cmr_query: Dict,
        coord_crs: CRS = WGS84_CRS,
        **kwargs: Any,
    ) -> List:
        """Get Point value from multiple observation."""
        raise NotImplementedError

    def part(
        self,
        bbox: BBox,
        cmr_query: Dict,
        dst_crs: Optional[CRS] = None,
        bounds_crs: CRS = WGS84_CRS,
        **kwargs: Any,
    ) -> Tuple[ImageData, List[str]]:
        """Create an Image from multiple items for a bbox."""
        raise NotImplementedError

    def feature(
        self,
        shape: Dict,
        cmr_query: Dict,
        dst_crs: Optional[CRS] = None,
        shape_crs: CRS = WGS84_CRS,
        max_size: int = 1024,
        **kwargs: Any,
    ) -> Tuple[ImageData, List[str]]:
        """Create an Image from multiple items for a GeoJSON feature."""
        raise NotImplementedError
