"""TiTiler.cmr custom Mosaic Backend."""

import os
import re
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Protocol,
    Tuple,
    Type,
    TypedDict,
    Union,
)

import attr
import earthaccess
import rasterio
import rasterio.session
from cachetools import TTLCache, cached
from cachetools.keys import hashkey
from cogeo_mosaic.backends.base import BaseBackend
from cogeo_mosaic.errors import NoAssetFoundError
from cogeo_mosaic.mosaic import MosaicJSON
from earthaccess.results import DataGranule
from morecantile.commons import Tile
from morecantile.models import TileMatrixSet
from rasterio import CRS
from rasterio.features import bounds
from rasterio.warp import transform_bounds, transform_geom
from rio_tiler.constants import WEB_MERCATOR_TMS, WGS84_CRS
from rio_tiler.io.base import BaseReader
from rio_tiler.io.rasterio import Reader
from rio_tiler.models import ImageData
from rio_tiler.mosaic.reader import mosaic_reader
from rio_tiler.types import BBox

from titiler.cmr.logger import logger
from titiler.cmr.settings import AuthSettings, CacheSettings, RetrySettings
from titiler.cmr.utils import retry

Access = Literal["direct", "external"]

cache_config = CacheSettings()
retry_config = RetrySettings()
s3_auth_config = AuthSettings()


class AWSCredentials(TypedDict, total=True):
    """AWS S3 temporary credentials."""

    accessKeyId: str
    secretAccessKey: str
    sessionToken: str
    # Parseable with datetime.fromisoformat (e.g., 2025-12-12 21:07:02+00:00)
    expiration: str


class Asset(TypedDict, total=True):
    """Simple Asset model."""

    url: Union[str, Mapping[str, str]]
    provider: str
    s3_credentials_url: str | None


class FindGranules(Protocol):
    """Protocol for functions that search for granules in CMR."""

    def __call__(self, count: int, **kwargs: Any) -> list[DataGranule]:
        """Match signature of earthaccess.search_data."""
        ...


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

    input: str = attr.ib("CMR", init=False)

    _backend_name: str = attr.ib(default="CMR")

    find_granules: FindGranules = attr.ib(default=earthaccess.search_data)
    auth: earthaccess.Auth | None = attr.ib(default=None)
    get_s3_credentials: Callable[[str], AWSCredentials] | None = attr.ib(default=None)
    rasterio_env_kwargs: Mapping[str, Any] = attr.ib(factory=dict)

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

    def _get_s3_credentials(self, asset: Asset) -> Optional[AWSCredentials]:
        """Get s3 credentials from kwargs or via auth."""
        return (
            self.get_s3_credentials(endpoint)
            if self.get_s3_credentials
            and (endpoint := asset.get("s3_credentials_url", None))
            else None
        )

    def _build_reader_options(self, s3_credentials: Optional[AWSCredentials]) -> Dict:
        """Build reader options with opener_options if s3_credentials provided."""
        return {
            **self.reader_options,
            "opener_options": {
                "auth": self.auth,
                **{
                    "s3_credentials": (
                        {
                            "key": s3_credentials["accessKeyId"],
                            "secret": s3_credentials["secretAccessKey"],
                            "token": s3_credentials["sessionToken"],
                        }
                        if s3_credentials
                        else {}
                    )
                },
            },
        }

    def _create_aws_session(
        self, s3_credentials: Optional[AWSCredentials]
    ) -> Optional[rasterio.session.AWSSession]:
        """Create rasterio AWSSession from s3 credentials."""
        if s3_credentials:
            return rasterio.session.AWSSession(
                aws_access_key_id=s3_credentials["accessKeyId"],
                aws_secret_access_key=s3_credentials["secretAccessKey"],
                aws_session_token=s3_credentials["sessionToken"],
            )
        return None

    def assets_for_tile(
        self, x: int, y: int, z: int, access: Access | None = None, **kwargs: Any
    ) -> List[Asset]:
        """Retrieve assets for tile."""
        access = access or s3_auth_config.access
        bbox = self.tms.bounds(Tile(x, y, z))
        return self.get_assets(*bbox, access=access, **kwargs)

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
        access: Access | None = None,
        **kwargs: Any,
    ) -> List[Asset]:
        """Retrieve assets for bbox."""
        access = access or s3_auth_config.access

        if coord_crs != WGS84_CRS:
            xmin, ymin, xmax, ymax = transform_bounds(
                coord_crs,
                WGS84_CRS,
                xmin,
                ymin,
                xmax,
                ymax,
            )

        return self.get_assets(xmin, ymin, xmax, ymax, access=access, **kwargs)

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
        bands_regex: Optional[str] = None,
        access: Access | None = None,
        **kwargs: Any,
    ) -> List[Asset]:
        """Find assets."""
        access = access or s3_auth_config.access
        xmin, ymin, xmax, ymax = (round(n, 8) for n in [xmin, ymin, xmax, ymax])
        assets: List[Asset] = []

        # earthaccess.search_data interprets a single datetime object as an unbounded interval
        # so pass the one datetime as a tuple to perform the actual temporal intersection query
        # with a single point in time
        if temporal := kwargs.get("temporal"):
            if isinstance(temporal, datetime):
                kwargs["temporal"] = (temporal, temporal)
        try:
            results = self.find_granules(
                bounding_box=(xmin, ymin, xmax, ymax),
                count=limit,
                **kwargs,
            )
        except Exception:
            logger.exception("Granule search failed")
            return assets

        for r in results:
            if bands_regex:
                links = r.data_links(access=access)

                band_urls = []
                for url in links:
                    if match := re.search(bands_regex, os.path.basename(url)):
                        band_urls.append((match.group(), url))

                urls = dict(band_urls)
                if urls:
                    assets.append(
                        {
                            "url": urls,
                            "provider": r["meta"]["provider-id"],
                            "s3_credentials_url": r.get_s3_credentials_endpoint(),
                        }
                    )

            else:
                assets.append(
                    {
                        "url": r.data_links(access=access)[0],
                        "provider": r["meta"]["provider-id"],
                        "s3_credentials_url": r.get_s3_credentials_endpoint(),
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
        bands_regex: Optional[str] = None,
        access: Access | None = None,
        **kwargs: Any,
    ) -> Tuple[ImageData, List[str]]:
        """Get Tile from multiple observation."""
        logger.info("searching for assets")
        access = access or s3_auth_config.access
        mosaic_assets = self.assets_for_tile(
            tile_x,
            tile_y,
            tile_z,
            **cmr_query,
            access=access,
            bands_regex=bands_regex,
        )
        logger.info(f"found {len(mosaic_assets)} assets")

        if not mosaic_assets:
            raise NoAssetFoundError(
                f"No assets found for tile {tile_z}-{tile_x}-{tile_y}"
            )

        def _reader(asset: Asset, x: int, y: int, z: int, **kwargs: Any) -> ImageData:
            s3_credentials = self._get_s3_credentials(asset)

            if any(
                field.name == "opener_options" for field in attr.fields(self.reader)
            ):
                options = self._build_reader_options(s3_credentials)

                with self.reader(
                    asset["url"],
                    tms=self.tms,  # type: ignore
                    **options,
                ) as src_dst:
                    return src_dst.tile(x, y, z, **kwargs)
            else:
                with (
                    rasterio.Env(
                        self._create_aws_session(s3_credentials),
                        **self.rasterio_env_kwargs,
                    ),
                    self.reader(
                        asset["url"],
                        tms=self.tms,  # type: ignore
                        **self.reader_options,
                    ) as src_dst,
                ):
                    return src_dst.tile(x, y, z, **kwargs)

        logger.info("reading assets")
        return mosaic_reader(mosaic_assets, _reader, tile_x, tile_y, tile_z, **kwargs)

    def point(
        self,
        lon: float,
        lat: float,
        cmr_query: Dict,
        coord_crs: CRS = WGS84_CRS,
        bands_regex: Optional[str] = None,
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
        bands_regex: Optional[str] = None,
        access: Access | None = None,
        **kwargs: Any,
    ) -> Tuple[ImageData, List[str]]:
        """Create an Image from multiple items for a bbox."""
        access = access or s3_auth_config.access
        xmin, ymin, xmax, ymax = bbox

        mosaic_assets = self.assets_for_bbox(
            xmin,
            ymin,
            xmax,
            ymax,
            coord_crs=bounds_crs,
            access=access,
            bands_regex=bands_regex,
            **cmr_query,
        )

        if not mosaic_assets:
            raise NoAssetFoundError("No assets found for bbox input")

        def _reader(asset: Asset, bbox: BBox, **kwargs: Any) -> ImageData:
            s3_credentials = self._get_s3_credentials(asset)

            if any(
                field.name == "opener_options" for field in attr.fields(self.reader)
            ):
                options = self._build_reader_options(s3_credentials)

                with self.reader(
                    asset["url"],  # type: ignore
                    **options,
                ) as src_dst:
                    return src_dst.part(bbox, **kwargs)
            else:
                with (
                    rasterio.Env(
                        self._create_aws_session(s3_credentials),
                        **self.rasterio_env_kwargs,
                    ),
                    self.reader(
                        asset["url"],  # type: ignore
                        **self.reader_options,
                    ) as src_dst,
                ):
                    return src_dst.part(bbox, **kwargs)

        return mosaic_reader(
            mosaic_assets,
            _reader,
            bbox,
            bounds_crs=bounds_crs,
            dst_crs=dst_crs or bounds_crs,
            **kwargs,
        )

    def feature(
        self,
        shape: Dict,
        cmr_query: Dict,
        dst_crs: Optional[CRS] = None,
        shape_crs: CRS = WGS84_CRS,
        bands_regex: Optional[str] = None,
        access: Access | None = None,
        **kwargs: Any,
    ) -> Tuple[ImageData, List[str]]:
        """Create an Image from multiple items for a GeoJSON feature."""
        access = access or s3_auth_config.access

        if "geometry" in shape:
            shape = shape["geometry"]

        shape_wgs84 = shape

        if shape_crs != WGS84_CRS:
            shape_wgs84 = transform_geom(shape_crs, WGS84_CRS, shape["geometry"])

        shape_bounds = bounds(shape_wgs84)

        mosaic_assets = self.get_assets(
            *shape_bounds,
            access=access,
            bands_regex=bands_regex,
            **cmr_query,
        )

        if not mosaic_assets:
            raise NoAssetFoundError("No assets found for Geometry")

        def _reader(asset: Asset, shape: Dict, **kwargs: Any) -> ImageData:
            s3_credentials = self._get_s3_credentials(asset)

            if any(
                field.name == "opener_options" for field in attr.fields(self.reader)
            ):
                options = self._build_reader_options(s3_credentials)

                with self.reader(
                    asset["url"],  # type: ignore
                    **options,
                ) as src_dst:
                    return src_dst.feature(shape, **kwargs)
            else:
                with (
                    rasterio.Env(
                        self._create_aws_session(s3_credentials),
                        **self.rasterio_env_kwargs,
                    ),
                    self.reader(
                        asset["url"],  # type: ignore
                        **self.reader_options,
                    ) as src_dst,
                ):
                    return src_dst.feature(shape, **kwargs)

        return mosaic_reader(
            mosaic_assets,
            _reader,
            shape,
            shape_crs=shape_crs,
            dst_crs=dst_crs or shape_crs,
            **kwargs,
        )
