"""Tests for the timeseries extension"""

from fastapi import FastAPI
from titiler.xarray.dependencies import DatasetParams as XarrayDatasetParams

from titiler.cmr.factory import CMRTilerFactory
from titiler.cmr.reader import XarrayGranuleReader
from titiler.cmr.timeseries import TimeseriesExtension


def test_timeseries_extension() -> None:
    """Test timeseries extension endpoints"""
    tiler = CMRTilerFactory(dataset_dependency=XarrayDatasetParams)
    tiler_plus_timeseries = CMRTilerFactory(
        dataset_reader=XarrayGranuleReader,
        dataset_dependency=XarrayDatasetParams,
        extensions=[TimeseriesExtension()],
    )
    # Check that we added routes
    assert len(tiler_plus_timeseries.router.routes) > len(tiler.router.routes)

    timeseries_app = FastAPI()
    timeseries_app.include_router(tiler_plus_timeseries.router)

    assert any(
        "/timeseries/statistics" in str(route) for route in timeseries_app.router.routes
    )
