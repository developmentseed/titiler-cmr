"""titiler.cmr.factory: router factories."""

import logging
from typing import Annotated, Callable, Literal

import morecantile
from attrs import define, field
from fastapi import APIRouter, Depends, Path, Query
from rio_tiler.constants import WGS84_CRS
from titiler.core.dependencies import (
    DatasetParams as RasterioDatasetParams,
)
from titiler.core.dependencies import (
    DefaultDependency,
)
from titiler.mosaic.factory import CoordCRSParams
from titiler.mosaic.factory import MosaicTilerFactory as BaseFactory
from titiler.xarray.dependencies import (
    DatasetParams as XarrayDatasetParams,
)
from titiler.xarray.dependencies import (
    XarrayParams,
)

from titiler.cmr.backend import CMRBackend
from titiler.cmr.dependencies import (
    BackendParams,
    CMRAssetsParams,
    GranuleSearch,
    GranuleSearchBackendParams,
    GranuleSearchParams,
)
from titiler.cmr.models import (
    Granule,
    GranuleFeatureCollection,
    granules_to_feature_collection,
)
from titiler.cmr.reader import MultiBaseGranuleReader, XarrayGranuleReader

logger = logging.getLogger(__name__)


@define(kw_only=True)
class CMRTilerFactory(BaseFactory):
    """Custom MosaicTiler for CMR Mosaic Backend."""

    path_dependency: Callable[..., GranuleSearch] = field(default=GranuleSearchParams)
    dataset_reader: type[MultiBaseGranuleReader] | type[XarrayGranuleReader] = field(
        default=MultiBaseGranuleReader
    )

    reader_dependency: (
        type[DefaultDependency] | type[CMRAssetsParams] | type[XarrayParams] | Callable
    ) = field(default=DefaultDependency)  # type: ignore[assignment]

    # Rasterio Dataset Options (nodata, unscale, resampling, reproject)
    dataset_dependency: type[RasterioDatasetParams] | type[XarrayDatasetParams]

    # Indexes/Expression Dependencies
    layer_dependency: type[DefaultDependency] = field(default=DefaultDependency)

    backend: type[CMRBackend] = CMRBackend
    backend_dependency: type[DefaultDependency] = BackendParams

    assets_accessor_dependency: type[DefaultDependency] = GranuleSearchBackendParams

    def register_routes(self) -> None:
        """Register routes, excluding /assets (defined separately in assets_router)."""
        self.info()
        self.tilesets()
        self.tile()
        if self.add_viewer:
            self.map_viewer()
        self.tilejson()
        self.point()

        if self.add_part:
            self.part()

        if self.add_statistics:
            self.statistics()

        if self.add_ogc_maps:
            self.ogc_maps()


###############################################################################
# Standalone assets router — backend-independent CMR granule metadata queries

assets_router = APIRouter()


@assets_router.get(
    "/bbox/{minx},{miny},{maxx},{maxy}/assets",
    response_model=list[Granule] | GranuleFeatureCollection,
    response_model_exclude_none=True,
    responses={200: {"description": "Return granules in bounding box"}},
)
def assets_for_bbox(
    minx: Annotated[float, Path(description="Bounding box min X")],
    miny: Annotated[float, Path(description="Bounding box min Y")],
    maxx: Annotated[float, Path(description="Bounding box max X")],
    maxy: Annotated[float, Path(description="Bounding box max Y")],
    src_path: GranuleSearch = Depends(GranuleSearchParams),
    backend_params=Depends(BackendParams),
    assets_accessor_params=Depends(GranuleSearchBackendParams),
    coord_crs=Depends(CoordCRSParams),
    f: Annotated[
        Literal["json", "geojson"],
        Query(description="Response format"),
    ] = "json",
) -> list[Granule] | GranuleFeatureCollection:
    """Return granules overlapping a bounding box."""
    logger.info("assets_for_bbox: querying CMR for granules in bbox")
    with CMRBackend(
        src_path,
        reader=MultiBaseGranuleReader,
        **backend_params.as_dict(),
    ) as src_dst:
        granules = src_dst.assets_for_bbox(
            minx,
            miny,
            maxx,
            maxy,
            coord_crs=coord_crs or WGS84_CRS,
            **assets_accessor_params.as_dict(),
        )

    return granules_to_feature_collection(granules) if f == "geojson" else granules


@assets_router.get(
    "/point/{lon},{lat}/assets",
    response_model=list[Granule] | GranuleFeatureCollection,
    response_model_exclude_none=True,
    responses={200: {"description": "Return granules at a point"}},
)
def assets_for_point(
    lon: Annotated[float, Path(description="Longitude")],
    lat: Annotated[float, Path(description="Latitude")],
    src_path: GranuleSearch = Depends(GranuleSearchParams),
    backend_params=Depends(BackendParams),
    assets_accessor_params=Depends(GranuleSearchBackendParams),
    coord_crs=Depends(CoordCRSParams),
    f: Annotated[
        Literal["json", "geojson"],
        Query(description="Response format"),
    ] = "json",
) -> list[Granule] | GranuleFeatureCollection:
    """Return granules overlapping a point."""
    logger.info("assets_for_point: querying CMR for granules at point")
    with CMRBackend(
        src_path,
        reader=MultiBaseGranuleReader,
        **backend_params.as_dict(),
    ) as src_dst:
        granules = src_dst.assets_for_point(
            lon,
            lat,
            coord_crs=coord_crs or WGS84_CRS,
            **assets_accessor_params.as_dict(),
        )

    return granules_to_feature_collection(granules) if f == "geojson" else granules


@assets_router.get(
    "/tiles/{tileMatrixSetId}/{z}/{x}/{y}/assets",
    response_model=list[Granule] | GranuleFeatureCollection,
    response_model_exclude_none=True,
    responses={200: {"description": "Return granules for a tile"}},
)
def assets_for_tile(
    tileMatrixSetId: Annotated[  # type: ignore[valid-type]
        Literal[tuple(morecantile.tms.list())],
        Path(description="Identifier selecting one of the TileMatrixSetId supported."),
    ],
    z: Annotated[
        int,
        Path(
            description="Identifier (Z) selecting one of the scales defined in the TileMatrixSet and representing the scaleDenominator the tile.",
        ),
    ],
    x: Annotated[
        int,
        Path(
            description="Column (X) index of the tile on the selected TileMatrix. It cannot exceed the MatrixHeight-1 for the selected TileMatrix.",
        ),
    ],
    y: Annotated[
        int,
        Path(
            description="Row (Y) index of the tile on the selected TileMatrix. It cannot exceed the MatrixWidth-1 for the selected TileMatrix.",
        ),
    ],
    src_path: GranuleSearch = Depends(GranuleSearchParams),
    backend_params=Depends(BackendParams),
    assets_accessor_params=Depends(GranuleSearchBackendParams),
    f: Annotated[
        Literal["json", "geojson"],
        Query(description="Response format"),
    ] = "json",
) -> list[Granule] | GranuleFeatureCollection:
    """Return granules overlapping a tile."""
    logger.info("assets_for_tile: querying CMR for granules in tile")
    tms = morecantile.tms.get(tileMatrixSetId)
    with CMRBackend(
        src_path,
        tms=tms,
        reader=MultiBaseGranuleReader,
        **backend_params.as_dict(),
    ) as src_dst:
        granules = src_dst.assets_for_tile(
            x,
            y,
            z,
            **assets_accessor_params.as_dict(),
        )

    return granules_to_feature_collection(granules) if f == "geojson" else granules
