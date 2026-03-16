"""titiler.cmr.factory: router factories."""

import logging
from typing import Annotated, Callable, Literal

import rasterio
from attrs import define, field
from fastapi import Depends, Path
from rio_tiler.constants import WGS84_CRS
from titiler.core.dependencies import (
    DatasetParams as RasterioDatasetParams,
)
from titiler.core.dependencies import (
    DefaultDependency,
)
from titiler.mosaic.factory import MosaicTilerFactory as BaseFactory
from titiler.mosaic.factory import CoordCRSParams
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

    def assets(self) -> None:
        """Register /assets endpoints returning GeoJSON FeatureCollections."""

        @self.router.get(
            "/bbox/{minx},{miny},{maxx},{maxy}/assets",
            response_model=GranuleFeatureCollection,
            response_model_exclude_none=True,
            responses={
                200: {
                    "description": "Return granules in bounding box as a GeoJSON FeatureCollection"
                }
            },
            operation_id=f"{self.operation_prefix}getAssetsForBoundingBox",
        )
        def assets_for_bbox(
            minx: Annotated[float, Path(description="Bounding box min X")],
            miny: Annotated[float, Path(description="Bounding box min Y")],
            maxx: Annotated[float, Path(description="Bounding box max X")],
            maxy: Annotated[float, Path(description="Bounding box max Y")],
            src_path=Depends(self.path_dependency),
            backend_params=Depends(self.backend_dependency),
            reader_params=Depends(self.reader_dependency),
            assets_accessor_params=Depends(self.assets_accessor_dependency),
            coord_crs=Depends(CoordCRSParams),
            env=Depends(self.environment_dependency),
        ) -> GranuleFeatureCollection:
            """Return granules overlapping a bounding box as a GeoJSON FeatureCollection."""
            with rasterio.Env(**env):
                logger.info(
                    f"opening data with backend: {self.backend} and reader {self.dataset_reader}"
                )
                with self.backend(
                    src_path,
                    reader=self.dataset_reader,
                    reader_options=reader_params.as_dict(),
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
            return granules_to_feature_collection(granules)

        @self.router.get(
            "/point/{lon},{lat}/assets",
            response_model=GranuleFeatureCollection,
            response_model_exclude_none=True,
            responses={
                200: {
                    "description": "Return granules at a point as a GeoJSON FeatureCollection"
                }
            },
            operation_id=f"{self.operation_prefix}getAssetsForPoint",
        )
        def assets_for_lon_lat(
            lon: Annotated[float, Path(description="Longitude")],
            lat: Annotated[float, Path(description="Latitude")],
            src_path=Depends(self.path_dependency),
            coord_crs=Depends(CoordCRSParams),
            backend_params=Depends(self.backend_dependency),
            reader_params=Depends(self.reader_dependency),
            assets_accessor_params=Depends(self.assets_accessor_dependency),
            env=Depends(self.environment_dependency),
        ) -> GranuleFeatureCollection:
            """Return granules overlapping a point as a GeoJSON FeatureCollection."""
            with rasterio.Env(**env):
                logger.info(
                    f"opening data with backend: {self.backend} and reader {self.dataset_reader}"
                )
                with self.backend(
                    src_path,
                    reader=self.dataset_reader,
                    reader_options=reader_params.as_dict(),
                    **backend_params.as_dict(),
                ) as src_dst:
                    granules = src_dst.assets_for_point(
                        lon,
                        lat,
                        coord_crs=coord_crs or WGS84_CRS,
                        **assets_accessor_params.as_dict(),
                    )
            return granules_to_feature_collection(granules)

        @self.router.get(
            "/tiles/{tileMatrixSetId}/{z}/{x}/{y}/assets",
            response_model=GranuleFeatureCollection,
            response_model_exclude_none=True,
            responses={
                200: {
                    "description": "Return granules for a tile as a GeoJSON FeatureCollection"
                }
            },
            operation_id=f"{self.operation_prefix}getAssetsForTile",
        )
        def assets_for_tile(
            tileMatrixSetId: Annotated[  # type: ignore[valid-type]
                Literal[tuple(self.supported_tms.list())],
                Path(
                    description="Identifier selecting one of the TileMatrixSetId supported."
                ),
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
            src_path=Depends(self.path_dependency),
            backend_params=Depends(self.backend_dependency),
            reader_params=Depends(self.reader_dependency),
            assets_accessor_params=Depends(self.assets_accessor_dependency),
            env=Depends(self.environment_dependency),
        ) -> GranuleFeatureCollection:
            """Return granules overlapping a tile as a GeoJSON FeatureCollection."""
            tms = self.supported_tms.get(tileMatrixSetId)
            with rasterio.Env(**env):
                logger.info(
                    f"opening data with backend: {self.backend} and reader {self.dataset_reader}"
                )
                with self.backend(
                    src_path,
                    tms=tms,
                    reader=self.dataset_reader,
                    reader_options=reader_params.as_dict(),
                    **backend_params.as_dict(),
                ) as src_dst:
                    granules = src_dst.assets_for_tile(
                        x,
                        y,
                        z,
                        **assets_accessor_params.as_dict(),
                    )
            return granules_to_feature_collection(granules)
