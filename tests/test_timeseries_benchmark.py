"""Benchmark tests for titiler-cmr endpoints.

Sends parameterized requests to the deployed API (AWS Lambda) so we can
evaluate the limits for /timeseries requests.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

import httpx
import pytest
from isodate import parse_duration

from titiler.cmr.dependencies import RasterioParams, ReaderParams, ZarrParams
from titiler.cmr.timeseries import TemporalMode

API_URL = "https://dev-titiler-cmr.delta-backend.com"
# API_URL = "http://localhost:8081"  # for local docker network


@dataclass
class ConceptConfig:
    """Configuration for benchmark queries"""

    concept_id: str
    resolution_meters: float | int
    backend: Literal["rasterio", "xarray"]
    start_datetime: datetime
    step: str
    temporal_mode: TemporalMode
    rasterio_params: Optional[RasterioParams] = None
    zarr_params: Optional[ZarrParams] = None
    reader_params: Optional[ReaderParams] = None


CONCEPT_CONFIGS = {
    "GAMSSA": ConceptConfig(
        concept_id="C2036881735-POCLOUD",
        resolution_meters=28000,
        backend="xarray",
        start_datetime=datetime(2010, 1, 1),
        step="P1D",
        temporal_mode=TemporalMode.point,
        zarr_params=ZarrParams(variable="analysed_sst"),
    ),
}


@pytest.mark.benchmark(
    group="gif-timepoints",
    min_rounds=3,
    warmup=False,
)
@pytest.mark.parametrize(
    ["concept_config_id", "bbox_size"],
    [
        ("GAMSSA", f"{bbox_size[0]}x{bbox_size[1]}")
        for bbox_size in [
            # (64, 64),
            (128, 128),
            (360, 180),
        ]
    ],
)
@pytest.mark.parametrize(
    "num_timepoints",
    [
        10,
        100,
        500,
        1000,
    ],
)
@pytest.mark.parametrize(
    "img_size",
    [
        f"{2**i}x{2**i}"
        for i in [
            # 9,
            # 10,
            11,
        ]
    ],
)
def test_gif_timepoints(
    benchmark,
    concept_config_id: str,
    bbox_size: str,
    num_timepoints: int,
    img_size: str,
):
    """Benchmark GIF generation with different numbers of timepoints."""
    concept_config = CONCEPT_CONFIGS.get(concept_config_id)
    if not concept_config:
        raise ValueError(f"there is no ConceptConfig with key {concept_config_id}")
    _bbox_size = [float(x) for x in bbox_size.split("x")]
    x_len, y_len = _bbox_size
    bbox = (-1 * x_len / 2, -1 * y_len / 2, x_len / 2, y_len / 2)
    bbox_str = ",".join(map(str, bbox))

    error_count = 0
    success_count = 0

    url = f"{API_URL}/timeseries/bbox/{bbox_str}/{img_size}.gif"

    end_datetime = (
        concept_config.start_datetime
        + parse_duration(concept_config.step) * num_timepoints
    )

    params = {
        "concept_id": concept_config.concept_id,
        "datetime": f"{concept_config.start_datetime.isoformat()}/{end_datetime.isoformat()}",
        "step": concept_config.step,
        "temporal_mode": concept_config.temporal_mode.value,
        "backend": concept_config.backend,
    }

    for query_params in [
        concept_config.rasterio_params,
        concept_config.zarr_params,
        concept_config.reader_params,
    ]:
        if query_params:
            params.update(query_params.as_dict())

    def run_gif_request():
        nonlocal error_count, success_count
        try:
            with httpx.Client() as client:
                response = client.get(url, params=params, timeout=None)
                if response.status_code == 500:
                    error_count += 1
                    return 0  # Return 0 for failed requests
                response.raise_for_status()
                success_count += 1
                return len(response.content)
        except Exception as e:
            error_count += 1
            print(f"Request failed with error: {str(e)}")
            return 0  # Return 0 for failed requests

    result = benchmark(run_gif_request)

    benchmark.extra_info.update(
        {
            "response_size": result,
            "error_count": error_count,
            "success_count": success_count,
            "error_rate": f"{(error_count / (error_count + success_count)) * 100:.2f}%",
        }
    )

    benchmark.name = (
        f"{concept_config_id}-{num_timepoints}-bbox:{bbox_size[0]}_{bbox_size[1]}-img:{img_size[0]}x{img_size[1]}",
    )


@pytest.mark.benchmark(
    group="statistics",
    min_rounds=3,
    warmup=False,
)
@pytest.mark.parametrize(
    ["concept_config_id", "bbox_size"],
    [
        ("GAMSSA", f"{bbox_size[0]}x{bbox_size[1]}")
        for bbox_size in [
            # (16, 16),
            (64, 64),
            # (128, 128),
            # (360, 180),
        ]
    ],
)
@pytest.mark.parametrize(
    "num_timepoints",
    [
        10,
        100,
        1000,
        2000,
    ],
)
def test_statistics(
    benchmark,
    concept_config_id: str,
    bbox_size: str,
    num_timepoints: int,
):
    """Benchmark statistics endpoint with different numbers of timepoints."""

    concept_config = CONCEPT_CONFIGS.get(concept_config_id)
    if not concept_config:
        raise ValueError(f"there is no ConceptConfig with key {concept_config_id}")

    _bbox_size = [float(x) for x in bbox_size.split("x")]
    x_len, y_len = _bbox_size
    bbox = (-1 * x_len / 2, -1 * y_len / 2, x_len / 2, y_len / 2)

    url = f"{API_URL}/timeseries/statistics"
    end_datetime = (
        concept_config.start_datetime
        + parse_duration(concept_config.step) * num_timepoints
    )

    params = {
        "concept_id": concept_config.concept_id,
        "datetime": f"{concept_config.start_datetime.isoformat()}/{end_datetime.isoformat()}",
        "step": concept_config.step,
        "temporal_mode": concept_config.temporal_mode.value,
        "backend": concept_config.backend,
    }

    for query_params in [
        concept_config.rasterio_params,
        concept_config.zarr_params,
        concept_config.reader_params,
    ]:
        if query_params:
            params.update(query_params.as_dict())

    geojson = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [bbox[0], bbox[1]],
                    [bbox[2], bbox[1]],
                    [bbox[2], bbox[3]],
                    [bbox[0], bbox[3]],
                    [bbox[0], bbox[1]],
                ]
            ],
        },
        "properties": {},
    }

    error_count = 0
    success_count = 0

    def run_statistics_request():
        nonlocal error_count, success_count
        try:
            with httpx.Client() as client:
                response = client.post(url, params=params, json=geojson, timeout=None)
                if response.status_code == 500:
                    error_count += 1
                    return 0  # Return 0 for failed requests
                response.raise_for_status()
                success_count += 1
                return len(response.content)
        except Exception as e:
            error_count += 1
            print(f"Request failed with error: {str(e)}")
            return 0  # Return 0 for failed requests

    result = benchmark(run_statistics_request)

    benchmark.extra_info.update(
        {
            "response_size": result,
            "error_count": error_count,
            "success_count": success_count,
            "error_rate": f"{(error_count / (error_count + success_count)) * 100:.2f}%",
        }
    )

    benchmark.name = (
        f"{concept_config_id}-{num_timepoints}-bbox:{bbox_size[0]}_{bbox_size[1]}",
    )
