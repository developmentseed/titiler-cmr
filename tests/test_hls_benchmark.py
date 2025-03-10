"""Benchmarks for HLS tile requests"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import httpx
import morecantile
import pytest

HLS_COLLECTIONS = {
    "HLSL30": "C2021957657-LPCLOUD",  # Landsat
    "HLSS30": "C2021957295-LPCLOUD",  # Sentinel-2
}

ENDPOINT = "https://dev-titiler-cmr.delta-backend.com"
BASE_DATE = datetime(2023, 6, 1)  # Fixed base date

TILES_HEIGHT = 5
TILES_WIDTH = 5

TMS = morecantile.tms.get("WebMercatorQuad")

TEST_LOCATIONS = [
    (-92.1161, 46.8199, "Duluth"),
    # (13.4050, 52.5200, "Berlin"),
    # (31.0335, -17.8252, "Harare"),
    # (116.4074, 39.9042, "Beijing"),
    # (151.2093, -33.8688, "Sydney"),
]

BAND_COMBINATIONS = [
    ["B05"],
    # ["B04", "B03"],
    # ["B04", "B03", "B02"],
]

ZOOM_LEVELS = [
    # 6,
    7,
    # 8,
    # 9,
    # 10,
]
INTERVAL_DAYS = [
    1,
    # 3,
    # 5,
    # 7,
    # 10,
    # 12,
    # 16,
]


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


async def fetch_tile(
    client: httpx.AsyncClient,
    endpoint: str,
    z: int,
    x: int,
    y: int,
    concept_id: str,
    interval_days: int,
    bands: List[str],
) -> httpx.Response:
    """Fetch a single HLS tile"""
    url = f"{endpoint}/tiles/WebMercatorQuad/{z}/{x}/{y}.png"

    end_date = BASE_DATE + timedelta(days=interval_days)
    datetime_range = f"{BASE_DATE.isoformat()}/{end_date.isoformat()}"

    params = {
        "concept_id": concept_id,
        "datetime": datetime_range,
        "backend": "rasterio",
        "bands_regex": "B[0-9][0-9]",
        "bands": bands,
    }

    if len(bands) == 3:
        params["color_formula"] = "Gamma RGB 3.5 Saturation 1.7 Sigmoidal RGB 15 0.35"
    elif len(bands) == 1:
        params["colormap_name"] = "viridis"
        params["rescale"] = "0,5000"

    start_time = datetime.now()
    try:
        response = await client.get(url, params=params, timeout=30.0)
        elapsed = (datetime.now() - start_time).total_seconds()

        # Add elapsed time to response for consistency with real responses
        response.elapsed = timedelta(seconds=elapsed)
        return response
    except Exception:
        # Create a mock response for exceptions
        mock_response = httpx.Response(500, request=httpx.Request("GET", url))
        mock_response.elapsed = datetime.now() - start_time
        return mock_response


async def fetch_viewport_tiles(
    endpoint: str,
    concept_id: str,
    zoom: int,
    lng: float,
    lat: float,
    interval_days: int,
    bands: List[str],
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
                concept_id,
                interval_days,
                bands,
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


@pytest.mark.benchmark(
    group="hls-tiles",
    min_rounds=2,
    warmup=True,
    warmup_iterations=1,
)
@pytest.mark.parametrize("collection", list(HLS_COLLECTIONS.keys()))
@pytest.mark.parametrize("zoom", ZOOM_LEVELS)
@pytest.mark.parametrize("interval_days", INTERVAL_DAYS)
@pytest.mark.parametrize("location", TEST_LOCATIONS)
@pytest.mark.parametrize("bands", BAND_COMBINATIONS)
def test_hls_tiles(
    benchmark,
    collection: str,
    zoom: int,
    interval_days: int,
    location: Tuple[float, float, str],
    bands: List[str],
):
    """Test HLS tile performance with various parameters"""
    lng, lat, location_name = location
    concept_id = HLS_COLLECTIONS[collection]

    def tile_benchmark():
        # Run the async function in a synchronous context
        results = asyncio.run(
            fetch_viewport_tiles(
                endpoint=ENDPOINT,
                concept_id=concept_id,
                zoom=zoom,
                lng=lng,
                lat=lat,
                interval_days=interval_days,
                bands=bands,
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
            "collection": collection,
            "zoom": zoom,
            "interval_days": interval_days,
            "location": location_name,
            "lat": lat,
            "lng": lng,
            "bands": "-".join(bands),
            "band_count": len(bands),
            "total_tiles": total_tiles,
            "success_count": success_count,
            "no_data_count": no_data_count,
            "error_count": error_count,
            "success_rate": success_count / total_tiles if total_tiles else 0,
            "avg_response_time": avg_response_time,
            "avg_response_size": avg_response_size,
        }
    )


if __name__ == "__main__":
    # This allows running with python -m pytest tile_benchmark_test.py -v
    pytest.main([__file__, "-v"])
