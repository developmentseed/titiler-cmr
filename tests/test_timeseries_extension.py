"""Tests for the timeseries extension"""

import pytest
from fastapi import FastAPI

from titiler.cmr.factory import Endpoints
from titiler.cmr.timeseries import TimeseriesExtension


# @pytest.mark.vcr
@pytest.mark.asyncio
async def test_timeseries_extension() -> None:
    """Test timeseries extension endpoints"""
    tiler = Endpoints()
    tiler_plus_timeseries = Endpoints(extensions=[TimeseriesExtension()])
    # Check that we added two routes
    assert len(tiler_plus_timeseries.router.routes) == len(tiler.router.routes) + 2

    timeseries_app = FastAPI()
    timeseries_app.include_router(tiler_plus_timeseries.router)

    assert any(
        "/timeseries/statistics" in str(route) for route in timeseries_app.router.routes
    )
