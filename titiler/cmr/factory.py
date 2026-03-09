"""titiler.cmr.factory: router factories."""

from typing import Callable

from attrs import define, field
from titiler.core.dependencies import (
    AssetsExprParams,
    DefaultDependency,
)
from titiler.core.dependencies import (
    DatasetParams as RasterioDatasetParams,
)
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
from titiler.cmr.reader import MultiBaseGranuleReader, XarrayGranuleReader


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
    layer_dependency: type[DefaultDependency] | type[AssetsExprParams] = field(
        default=DefaultDependency
    )

    backend: type[CMRBackend] = CMRBackend
    backend_dependency: type[DefaultDependency] = BackendParams

    assets_accessor_dependency: type[DefaultDependency] = GranuleSearchBackendParams
