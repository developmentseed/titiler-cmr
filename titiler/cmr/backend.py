"""CMR mosaic backend."""

from collections.abc import Callable
from typing import Any, Type, cast

import attr
from geojson_pydantic.geometries import Geometry, Point, Polygon
from httpx import Client
from morecantile import Tile, TileMatrixSet
from rasterio.crs import CRS
from rasterio.warp import transform, transform_bounds
from rio_tiler.constants import WEB_MERCATOR_TMS, WGS84_CRS
from rio_tiler.errors import NoAssetFoundError
from rio_tiler.mosaic.backend import BaseBackend
from rio_tiler.types import BBox

from titiler.cmr.logger import logger
from titiler.cmr.models import Granule, GranuleSearch
from titiler.cmr.query import get_granules
from titiler.cmr.reader import MultiBaseGranuleReader, XarrayGranuleReader


@attr.s
class CMRBackend(BaseBackend):
    """Mosaic backend for CMR granule search."""

    # CMR search parameters
    input: GranuleSearch = attr.ib()  # type: ignore[assignment]
    client: Client = attr.ib()
    reader: Type[MultiBaseGranuleReader] | Type[XarrayGranuleReader] = attr.ib()

    tms: TileMatrixSet = attr.ib(default=WEB_MERCATOR_TMS)

    reader_options: dict = attr.ib(factory=dict)

    auth_token: str | None = attr.ib(default=None)
    s3_access: bool = attr.ib(default=False)
    get_s3_credentials: Callable | None = attr.ib(default=None)

    def __attrs_post_init__(self):
        """Initialize reader options from auth_token and s3_access."""
        if self.auth_token:
            self.reader_options["auth_token"] = self.auth_token
        self.reader_options["s3_access"] = self.s3_access
        if self.get_s3_credentials is not None:
            self.reader_options["get_s3_credentials"] = self.get_s3_credentials

    crs: CRS = attr.ib(default=WGS84_CRS)

    # TODO: do this correctly
    minzoom: int = attr.ib(0)
    maxzoom: int = attr.ib(18)

    @property
    def bounds(self) -> BBox:
        """Return the bounding box of the mosaic."""
        if self.input.granule_ur:
            granule = next(
                get_granules(search_params=self.input, client=self.client),
                None,
            )
            if granule is None:
                raise NoAssetFoundError(
                    f"No assets found for search with these parameters {self.input}"
                )
            return granule.bbox

        return cast(
            BBox,
            tuple(float(x) for x in self.input.bounding_box.split(","))
            if self.input.bounding_box
            else (-180, -90, 180, 90),
        )

    def get_assets(
        self, geometry: Geometry, exitwhenfull: bool = True
    ) -> list[Granule]:
        """Return granules intersecting the given geometry."""
        logger.info("starting granule search")
        assets = list(
            get_granules(
                geometry=geometry,
                search_params=self.input,
                client=self.client,
                exitwhenfull=exitwhenfull,
            )
        )
        logger.info(f"found {len(assets)} granules")

        return assets

    def assets_for_tile(self, x: int, y: int, z: int, **kwargs: Any) -> list[Granule]:
        """Return granules intersecting a given tile."""
        bbox = self.tms.bounds(Tile(x, y, z))
        return self.get_assets(Polygon.from_bounds(*bbox), **kwargs)

    def assets_for_point(
        self,
        lng: float,
        lat: float,
        coord_crs: CRS | None = None,
        **kwargs: Any,
    ) -> list[Granule]:
        """Return granules intersecting a given point."""
        if coord_crs != WGS84_CRS:
            xs, ys = transform(coord_crs, WGS84_CRS, [lng], [lat])
            lng, lat = xs[0], ys[0]

        return self.get_assets(
            Point(
                type="Point",
                coordinates=(lng, lat),  # type: ignore
            ),
            **kwargs,
        )

    def assets_for_bbox(
        self,
        xmin: float,
        ymin: float,
        xmax: float,
        ymax: float,
        coord_crs: CRS | None = None,
        **kwargs,
    ) -> list[Granule]:
        """Retrieve assets for bbox."""
        if not coord_crs:
            coord_crs = WGS84_CRS

        if coord_crs != WGS84_CRS:
            xmin, ymin, xmax, ymax = transform_bounds(
                coord_crs,
                WGS84_CRS,
                xmin,
                ymin,
                xmax,
                ymax,
            )

        return self.get_assets(Polygon.from_bounds(xmin, ymin, xmax, ymax), **kwargs)
