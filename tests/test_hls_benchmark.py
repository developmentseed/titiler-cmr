"""Benchmarks for HLS tile requests"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, NamedTuple, Tuple

import httpx
import morecantile
import pytest

ENDPOINT = "https://dev-titiler-cmr.delta-backend.com"
TMS = morecantile.tms.get("WebMercatorQuad")
TEST_LNG, TEST_LAT = -92.1, 46.8
TILES_HEIGHT = 5
TILES_WIDTH = 5


class CollectionConfig(NamedTuple):
    """Configuration for a collection"""

    collection_id: str
    concept_id: str
    base_date: datetime

    # band configurations
    rgb_bands: List[str]
    ndvi_bands: Tuple[str, str]  # (red_band, nir_band)
    single_band: str


LANDSAT = CollectionConfig(
    collection_id="HLSL30",
    concept_id="C2021957657-LPCLOUD",
    base_date=datetime(2023, 2, 24, 0, 0, 1),
    rgb_bands=["B04", "B03", "B02"],
    ndvi_bands=("B04", "B05"),
    single_band="B04",
)

SENTINEL = CollectionConfig(
    collection_id="HLSS30",
    concept_id="C2021957295-LPCLOUD",
    base_date=datetime(2023, 2, 13, 0, 0, 1),
    rgb_bands=["B04", "B03", "B02"],
    ndvi_bands=("B04", "B8A"),
    single_band="B04",
)

COLLECTIONS = {
    "HLSL30": LANDSAT,
    "HLSS30": SENTINEL,
}

# test parameters
ZOOM_LEVELS = [6, 7, 8, 9, 10]
INTERVAL_DAYS = [1, 7]
N_BANDS = [1, 2, 3]


def get_surrounding_tiles(
    x: int, y: int, zoom: int, width: int = TILES_WIDTH, height: int = TILES_HEIGHT
) -> List[Tuple[int, int]]:
    """Get a list of surrounding tiles for a viewport"""
    tiles = []
    offset_x = width // 2
    offset_y = height // 2

    for y_pos in range(y - offset_y, y + offset_y + 1):
        for x_pos in range(x - offset_x, x + offset_x + 1):
            # Ensure x, y are valid for the zoom level
            max_tile = 2**zoom - 1
            x_valid = max(0, min(x_pos, max_tile))
            y_valid = max(0, min(y_pos, max_tile))
            tiles.append((x_valid, y_valid))

    return tiles


def get_band_params(
    collection_config: CollectionConfig, n_bands: int
) -> Dict[str, Any]:
    """Get band-specific parameters based on collection and band count"""
    params: Dict[str, Any] = {
        "backend": "rasterio",
        "bands_regex": "B[0-9][0-9]",
    }

    if n_bands == 3:
        # RGB visualization
        params["color_formula"] = "Gamma RGB 3.5 Saturation 1.7 Sigmoidal RGB 15 0.35"
        params["bands"] = collection_config.rgb_bands
    elif n_bands == 2:
        # NDVI visualization
        red_band, nir_band = collection_config.ndvi_bands
        params["bands"] = [red_band, nir_band]
        params["expression"] = f"({nir_band}-{red_band})/({nir_band}+{red_band})"
        params["colormap_name"] = "greens"
        params["rescale"] = "-1,1"
    elif n_bands == 1:
        # Single band visualization
        params["bands"] = [collection_config.single_band]
        params["colormap_name"] = "viridis"
        params["rescale"] = "0,5000"

    return params


async def fetch_tile(
    client: httpx.AsyncClient,
    endpoint: str,
    z: int,
    x: int,
    y: int,
    collection_config: CollectionConfig,
    interval_days: int,
    n_bands: int,
) -> httpx.Response:
    """Fetch a single HLS tile"""
    url = f"{endpoint}/tiles/WebMercatorQuad/{z}/{x}/{y}.png"

    start_date = collection_config.base_date
    end_date = start_date + timedelta(days=interval_days)
    datetime_range = f"{start_date.isoformat()}/{end_date.isoformat()}"

    params: Dict[str, Any] = {
        "concept_id": collection_config.concept_id,
        "datetime": datetime_range,
    }

    params.update(get_band_params(collection_config, n_bands))

    start_time = datetime.now()
    try:
        response = await client.get(url, params=params, timeout=30.0)
        response.raise_for_status()
        elapsed = (datetime.now() - start_time).total_seconds()

        response.elapsed = timedelta(seconds=elapsed)
        return response
    except Exception:
        # Create a mock response for exceptions
        mock_response = httpx.Response(500, request=httpx.Request("GET", url))
        mock_response.elapsed = datetime.now() - start_time
        return mock_response


async def fetch_viewport_tiles(
    endpoint: str,
    collection_config: CollectionConfig,
    zoom: int,
    lng: float,
    lat: float,
    interval_days: int,
    n_bands: int,
) -> List[Dict]:
    """Fetch all tiles for a viewport and return detailed metrics"""
    tile = TMS.tile(lng=lng, lat=lat, zoom=zoom)
    tiles = get_surrounding_tiles(tile.x, tile.y, zoom)

    results = []

    async with httpx.AsyncClient() as client:
        tasks = [
            fetch_tile(
                client,
                endpoint,
                zoom,
                x,
                y,
                collection_config,
                interval_days,
                n_bands,
            )
            for x, y in tiles
        ]
        responses = await asyncio.gather(*tasks)

        for (x, y), response in zip(tiles, responses):
            # Capture detailed metrics for each tile
            results.append(
                {
                    "x": x,
                    "y": y,
                    "status_code": response.status_code,
                    "response_time": response.elapsed.total_seconds(),
                    "response_size": len(response.content)
                    if hasattr(response, "content")
                    else 0,
                    "has_data": response.status_code == 200,  # 204 means no data
                    "is_error": response.status_code >= 400,
                }
            )

    return results


@pytest.fixture(scope="session", autouse=True)
def warm_up_api():
    """Perform a single warmup request to the API before all tests."""
    asyncio.run(
        fetch_viewport_tiles(
            endpoint=ENDPOINT,
            collection_config=LANDSAT,
            zoom=8,
            lng=TEST_LNG,
            lat=TEST_LAT,
            interval_days=1,
            n_bands=3,
        )
    )


@pytest.mark.benchmark(
    group="hls-tiles",
    min_rounds=2,
    warmup=False,
)
@pytest.mark.parametrize("collection_id", list(COLLECTIONS.keys()))
@pytest.mark.parametrize("zoom", ZOOM_LEVELS)
@pytest.mark.parametrize("interval_days", INTERVAL_DAYS)
@pytest.mark.parametrize("n_bands", N_BANDS)
def test_hls_tiles(
    benchmark,
    collection_id: str,
    zoom: int,
    interval_days: int,
    n_bands: int,
):
    """Test HLS tile performance with various parameters"""
    collection_config = COLLECTIONS[collection_id]

    def tile_benchmark():
        # Run the async function in a synchronous context
        results = asyncio.run(
            fetch_viewport_tiles(
                endpoint=ENDPOINT,
                collection_config=collection_config,
                zoom=zoom,
                lng=TEST_LNG,
                lat=TEST_LAT,
                interval_days=interval_days,
                n_bands=n_bands,
            )
        )
        return results

    # Run the benchmark
    results = benchmark(tile_benchmark)

    # Calculate summary statistics
    total_tiles = len(results)
    success_count = sum(1 for r in results if r["has_data"])
    no_data_count = sum(1 for r in results if r["status_code"] == 204)
    error_count = sum(1 for r in results if r["is_error"])

    avg_response_time = (
        sum(r["response_time"] for r in results) / total_tiles if total_tiles else 0
    )
    avg_response_size = (
        sum(r["response_size"] for r in results if r["has_data"]) / success_count
        if success_count
        else 0
    )

    # Add detailed metrics to the benchmark results
    benchmark.extra_info.update(
        {
            "collection": collection_id,
            "zoom": zoom,
            "interval_days": interval_days,
            "band_count": n_bands,
            "total_tiles": total_tiles,
            "success_count": success_count,
            "no_data_count": no_data_count,
            "error_count": error_count,
            "success_rate": success_count / total_tiles if total_tiles else 0,
            "avg_response_time": avg_response_time,
            "avg_response_size": avg_response_size,
        }
    )
