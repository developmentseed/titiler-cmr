"""test titiler-pgstac dependencies."""

from datetime import datetime, timezone

import pytest
from starlette.requests import Request

from titiler.cmr import dependencies
from titiler.cmr.enums import MediaType
from titiler.cmr.errors import InvalidDatetime
from titiler.xarray.dependencies import CompatXarrayParams


def test_media_type():
    """test accept_media_type dependency."""
    assert (
        dependencies.accept_media_type(
            "application/json;q=0.9, text/html;q=1.0",
            [MediaType.json, MediaType.html],
        )
        == MediaType.html
    )

    assert (
        dependencies.accept_media_type(
            "application/json;q=0.9, text/html;q=0.8",
            [MediaType.json, MediaType.html],
        )
        == MediaType.json
    )

    # if no quality then default to 1.0
    assert (
        dependencies.accept_media_type(
            "application/json;q=0.9, text/html",
            [MediaType.json, MediaType.html],
        )
        == MediaType.html
    )

    # Invalid Quality
    assert (
        dependencies.accept_media_type(
            "application/json;q=w, , text/html;q=0.1",
            [MediaType.json, MediaType.html],
        )
        == MediaType.html
    )

    assert (
        dependencies.accept_media_type(
            "*",
            [MediaType.json, MediaType.html],
        )
        == MediaType.json
    )


def test_output_type():
    """test OutputType dependency."""
    req = Request(
        {
            "type": "http",
            "client": None,
            "query_string": "",
            "headers": ((b"accept", b"application/json"),),
        },
        None,
    )
    assert (
        dependencies.OutputType(
            req,
        )
        == MediaType.json
    )

    req = Request(
        {
            "type": "http",
            "client": None,
            "query_string": "",
            "headers": ((b"accept", b"text/html"),),
        },
        None,
    )
    assert (
        dependencies.OutputType(
            req,
        )
        == MediaType.html
    )

    req = Request(
        {"type": "http", "client": None, "query_string": "", "headers": ()}, None
    )
    assert not dependencies.OutputType(req)

    # FastAPI will parse the request first and inject `f=json` in the dependency
    req = Request(
        {
            "type": "http",
            "client": None,
            "query_string": "f=json",
            "headers": ((b"accept", b"text/html"),),
        },
        None,
    )
    assert dependencies.OutputType(req, f="json") == MediaType.json


test_datetime = datetime(year=2018, month=2, day=12, hour=9, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "temporal,res",
    [
        (
            "2018-02-12T09:00:00Z",
            test_datetime,
        ),
        (
            "2018-02-12T09:00:00Z/",
            (test_datetime, None),
        ),
        (
            "2018-02-12T09:00:00Z/..",
            (test_datetime, None),
        ),
        ("/2018-02-12T09:00:00Z", (None, test_datetime)),
        ("../2018-02-12T09:00:00Z", (None, test_datetime)),
        (
            "2018-02-12T09:00:00Z/2019-02-12T09:00:00Z",
            (
                test_datetime,
                datetime(year=2019, month=2, day=12, hour=9, tzinfo=timezone.utc),
            ),
        ),
    ],
)
def test_cmr_query(temporal, res):
    """test cmr query dependency."""
    assert (
        dependencies.cmr_query(concept_id="something", datetime=temporal)["temporal"]
        == res
    )


def test_cmr_query_more():
    """test cmr query dependency."""
    assert dependencies.cmr_query(
        concept_id="something",
    ) == {"concept_id": "something"}

    with pytest.raises(InvalidDatetime):
        dependencies.cmr_query(
            concept_id="something",
            datetime="yo/yo/yo",
        )

    with pytest.raises(InvalidDatetime):
        dependencies.cmr_query(
            concept_id="something",
            datetime="2019-02-12",
        )

    with pytest.raises(InvalidDatetime):
        dependencies.cmr_query(
            concept_id="something",
            datetime="2019-02-12T09:00:00Z/2019-02-12",
        )


def test_interpolated_xarray_params_single_datetime():
    """Test InterpolatedXarrayParams with single datetime interpolation."""
    xarray_params = CompatXarrayParams(
        variable="temperature", sel=["time={datetime}", "lev=1000"], method="nearest"
    )

    single_datetime = datetime(2025, 9, 23, 0, 0, 0, tzinfo=timezone.utc)
    cmr_query_params = {"concept_id": "test_concept", "temporal": single_datetime}

    result = dependencies.interpolated_xarray_ds_params(xarray_params, cmr_query_params)

    assert result.sel == [f"time={single_datetime.isoformat()}", "lev=1000"]
    assert result.variable == "temperature"
    assert result.method == "nearest"


def test_interpolated_xarray_params_datetime_range():
    """Test InterpolatedXarrayParams with datetime range (uses start datetime)."""
    xarray_params = CompatXarrayParams(
        variable="temperature", sel=["time={datetime}"], method="nearest"
    )

    start_datetime = datetime(2025, 9, 23, 0, 0, 0, tzinfo=timezone.utc)
    end_datetime = datetime(2025, 9, 24, 0, 0, 0, tzinfo=timezone.utc)
    cmr_query_params = {
        "concept_id": "test_concept",
        "temporal": (start_datetime, end_datetime),
    }

    result = dependencies.interpolated_xarray_ds_params(xarray_params, cmr_query_params)

    assert result.sel == [f"time={start_datetime.isoformat()}"]


def test_interpolated_xarray_params_no_datetime_template():
    """Test InterpolatedXarrayParams when sel doesn't contain datetime template."""
    xarray_params = CompatXarrayParams(
        variable="temperature",
        sel=["time=2025-01-01T00:00:00Z", "lev=1000"],
        method="nearest",
    )

    single_datetime = datetime(2025, 9, 23, 0, 0, 0, tzinfo=timezone.utc)
    cmr_query_params = {"concept_id": "test_concept", "temporal": single_datetime}

    result = dependencies.interpolated_xarray_ds_params(xarray_params, cmr_query_params)

    assert result.sel == ["time=2025-01-01T00:00:00Z", "lev=1000"]


def test_interpolated_xarray_params_no_sel():
    """Test InterpolatedXarrayParams when sel is None or empty."""
    xarray_params = CompatXarrayParams(
        variable="temperature", sel=None, method="nearest"
    )

    single_datetime = datetime(2025, 9, 23, 0, 0, 0, tzinfo=timezone.utc)
    cmr_query_params = {"concept_id": "test_concept", "temporal": single_datetime}

    result = dependencies.interpolated_xarray_ds_params(xarray_params, cmr_query_params)

    assert result.sel is None
    assert result.variable == "temperature"


def test_interpolated_xarray_params_multiple_templates():
    """Test InterpolatedXarrayParams with multiple datetime templates."""
    xarray_params = CompatXarrayParams(
        variable="temperature",
        sel=["time={datetime}", "start_time={datetime}", "lev=1000"],
        method="nearest",
    )

    single_datetime = datetime(2025, 9, 23, 12, 30, 45, tzinfo=timezone.utc)
    cmr_query_params = {"concept_id": "test_concept", "temporal": single_datetime}

    result = dependencies.interpolated_xarray_ds_params(xarray_params, cmr_query_params)

    expected = [
        f"time={single_datetime.isoformat()}",
        f"start_time={single_datetime.isoformat()}",
        "lev=1000",
    ]
    assert result.sel == expected
