"""Tests for reader.py"""

import re

import numpy as np
import pytest
import xarray as xr
from titiler.core.errors import BadRequestError

from titiler.cmr.reader import get_variables


def test_unknown_variable_raises() -> None:
    """An unknown variable is a client error that keeps xarray's suggestion."""
    ds = xr.Dataset(
        {"weight": (("lat", "lon"), np.ones((2, 3)))},
        coords={"lat": [0.0, 1.0], "lon": [0.0, 1.0, 2.0]},
    )

    with pytest.raises(BadRequestError, match="Did you mean"):
        get_variables(ds, ["wieght"])


@pytest.mark.parametrize("qa_dims", [("lat_qa", "lon_qa"), ("lat", "lon_qa")])
def test_mask_on_another_grid_raises(qa_dims: tuple[str, str]) -> None:
    """Variables on different grids are a 400."""
    ds = xr.Dataset(
        {
            "sst": (("lat", "lon"), np.ones((2, 3))),
            "qa": (qa_dims, np.zeros((2, 4))),
        },
        coords={"lat": [0.0, 1.0], "lon": [0.0, 1.0, 2.0]},
    )

    with pytest.raises(BadRequestError, match=re.escape(f"qa {qa_dims}")):
        get_variables(ds, ["sst", "qa"], expression="where(b2 < 0.15, b1, nan)")


def test_time_without_sel_raises() -> None:
    """A time dimension with no sel is a 400."""
    ds = xr.Dataset(
        {
            "sst": (("time", "lat", "lon"), np.ones((2, 2, 3))),
            "ice": (("lat", "lon"), np.zeros((2, 3))),
        },
        coords={
            "time": np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[ns]"),
            "lat": [0.0, 1.0],
            "lon": [0.0, 1.0, 2.0],
        },
    )

    with pytest.raises(BadRequestError, match="must select one value"):
        get_variables(ds, ["sst", "ice"], expression="where(b2 < 0.15, b1, nan)")


def test_variables_on_different_coordinates_raise() -> None:
    """Variables whose coordinates differ under the same dimension names fail."""
    dt = xr.DataTree.from_dict(
        {
            "a": xr.Dataset(
                {"sst": (("lat", "lon"), np.ones((2, 3)))},
                coords={"lat": [0.0, 1.0], "lon": [0.0, 1.0, 2.0]},
            ),
            "b": xr.Dataset(
                {"qa": (("lat", "lon"), np.ones((2, 3)))},
                coords={"lat": [0.5, 1.5], "lon": [0.0, 1.0, 2.0]},
            ),
        }
    )

    with pytest.raises(BadRequestError, match="must share a grid"):
        get_variables(dt, ["a/sst", "b/qa"])  # type: ignore[arg-type]


def test_time_invariant_mask_with_sel() -> None:
    """A (lat, lon) mask stacks with (time, lat, lon) data once sel picks a time."""
    ds = xr.Dataset(
        {
            "sst": (("time", "lat", "lon"), np.ones((2, 2, 3))),
            "ice": (("lat", "lon"), np.zeros((2, 3))),
        },
        coords={
            "time": np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[ns]"),
            "lat": [0.0, 1.0],
            "lon": [0.0, 1.0, 2.0],
        },
    )

    da = get_variables(
        ds,
        ["sst", "ice"],
        sel=["time=2024-01-02"],
        expression="where(b2 < 0.15, b1, nan)",
    )

    assert da.dims == ("y", "x")
