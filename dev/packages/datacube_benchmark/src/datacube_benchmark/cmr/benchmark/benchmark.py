"""
Unified benchmarking for TiTiler-CMR.

This module provides an async, extensible toolkit to measure TiTiler-CMR
performance across common scenarios:

- **Viewport**: request a window of tiles around a lon/lat at multiple zooms
- **Tileset**: enumerate all tiles intersecting a bbox (with optional caps)
- **Statistics**: call `/timeseries/statistics` for a geometry and time range

"""

from __future__ import annotations

import asyncio
import time
from asyncio import BoundedSemaphore
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import random

import httpx
import morecantile
import pandas as pd
import psutil
from geojson_pydantic import Feature

from datacube_benchmark import (
    BaseBenchmarker,
    DatasetParams,
    create_bbox_feature,
    fetch_tile,
    get_surrounding_tiles,
    get_tileset_tiles,
)

# ---------------------------------------
# top level benchmarking compatibility check
# ---------------------------------------


async def check_titiler_cmr_compatibility(
    endpoint: str,
    dataset: DatasetParams,
    *,
    timeout_s: float = 30.0,
    max_connections: int = 10,
    max_connections_per_host: int = 10,
    raise_on_incompatible: bool = False,
    bounds_fraction: float = 0.05,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Call TiTiler-CMR `/compatibility` and return timing + details.

    Parameters
    ----------
    endpoint : str
        Base URL of the TiTiler-CMR deployment.
    dataset : DatasetParams
        Dataset configuration (concept_id, backend, datetime_range, etc.).
    timeout_s : float, optional
        Request timeout (default: 30s).
    raise_on_incompatible : bool, optional
        If True, raise RuntimeError when compatible == False.
    bounds_fraction : float, optional
        Fraction of total dataset area to use for random bounds compatibility check
        (default: 0.05 = 5% of area). Only used when geometry is not provided.

    Returns
    -------
    Dict[str, Any]
        {
          success, compatible, elapsed_s, status_code, url,
          details (server payload), error (if any)
        }
    """
    benchmarker = TiTilerCMRBenchmarker(
        endpoint=endpoint,
        timeout_s=timeout_s,
        max_connections=max_connections,
        max_connections_per_host=max_connections_per_host,
    )
    result = await benchmarker.check_compatibility(
        dataset, bounds_fraction=bounds_fraction, **kwargs
    )
    if raise_on_incompatible and result.get("success") and not result.get("compatible"):
        reasons = result.get("details", {}).get("reasons") or result.get(
            "details", {}
        ).get("messages")
        raise RuntimeError(f"Dataset not compatible: {reasons or 'no reason provided'}")
    return result


# ---------------------------------------
# top level public API
# ---------------------------------------


async def benchmark_viewport(
    endpoint: str,
    dataset: DatasetParams,
    lng: float,
    lat: float,
    *,
    viewport_width: int = 5,
    viewport_height: int = 5,
    tms_id: str = "WebMercatorQuad",
    tile_format: str = "png",
    tile_scale: int = 1,
    min_zoom: int = 7,
    max_zoom: int = 10,
    timeout_s: float = 30.0,
    max_connections: int = 32,
    max_connections_per_host: int = 32,
    max_concurrent: int = 32,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Benchmark tile rendering for a *viewport* centered at (lng, lat).
    This is a high-level convenience wrapper around
    ``TiTilerCMRBenchmarker.benchmark_tiles``. It builds a tiling strategy that
    selects a (viewport_width × viewport_height) neighborhood of tiles around
    the center tile at each zoom in ``[min_zoom, max_zoom]``, then measures
    latency, status, and size for each tile request across all timesteps.

    Parameters
    ----------
    endpoint : str
        Base URL of the TiTiler-CMR deployment.
    dataset : DatasetParams
        Dataset and query parameters (concept_id, backend, datetime_range, kwargs).
    lng : float
        Center longitude of the viewport.
    lat : float
        Center latitude of the viewport.
    viewport_width : int, optional
        Number of tiles in the X direction (default: 5).
    viewport_height : int, optional
        Number of tiles in the Y direction (default: 5).
    tms_id : str, optional
        Tile matrix set ID (default: "WebMercatorQuad").
    tile_format : str, optional
        Tile format (default: "png").
    tile_scale : int, optional
        Tile scale factor (default: 1).
    min_zoom : int, optional
        Minimum zoom level (default: 7).
    max_zoom : int, optional
        Maximum zoom level (default: 10).
    timeout_s : float, optional
        Request timeout in seconds (default: 30.0).
    max_connections : int, optional
        Maximum total concurrent connections (default: 20).
    max_connections_per_host : int, optional
        Maximum concurrent connections per host (default: 20).
    **kwargs : Any
        Additional query parameters for the API.

    Returns
    -------
    pd.DataFrame
            Results for each tile request, including status, latency, and size.
    """
    benchmarker = TiTilerCMRBenchmarker(
        endpoint=endpoint,
        tms_id=tms_id,
        tile_format=tile_format,
        tile_scale=tile_scale,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        timeout_s=timeout_s,
        max_connections=max_connections,
        max_connections_per_host=max_connections_per_host,
        max_concurrent=max_concurrent,
    )

    def viewport_strategy(
        zoom: int, tms: morecantile.TileMatrixSet, _tilejson_info: Dict[str, Any]
    ) -> List[Tuple[int, int]]:
        center = tms.tile(lng=lng, lat=lat, zoom=zoom)
        return get_surrounding_tiles(
            center_x=center.x,
            center_y=center.y,
            zoom=zoom,
            width=viewport_width,
            height=viewport_height,
        )

    return await benchmarker.benchmark_tiles(
        dataset, viewport_strategy, warmup_per_zoom=1, **kwargs
    )


async def benchmark_tileset(
    endpoint: str,
    dataset: DatasetParams,
    *,
    bounds: List[float],
    max_tiles_per_zoom: Optional[int] = 100,
    tms_id: str = "WebMercatorQuad",
    tile_format: str = "png",
    tile_scale: int = 1,
    min_zoom: int = 7,
    max_zoom: int = 10,
    timeout_s: float = 30.0,
    max_connections: int = 32,
    max_connections_per_host: int = 32,
    max_concurrent: int = 32,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Benchmark tile rendering for a *full tileset* over given bounds.
    This wrapper enumerates all tiles intersecting the supplied `bounds` (or the
    bounds from TileJSON if omitted) for each zoom level in ``[min_zoom, max_zoom]``.
    Optionally caps the number of tiles per zoom to avoid overly large runs.

    Parameters
    ----------
    endpoint : str
        Base URL of the TiTiler-CMR deployment.
    dataset : DatasetParams
        Dataset and query parameters (concept_id, backend, datetime_range, kwargs).
    bounds : list of float, optional
        Bounding box [min_lon, min_lat, max_lon, max_lat] to cover.
    max_tiles_per_zoom : int, optional
        If set, limits the number of tiles per zoom level to this count.
    tms_id : str, optional
        Tile matrix set ID (default: "WebMercatorQuad").
    tile_format : str, optional
            Tile image format (e.g., "png", "jpg", "webp"). (default: "png").
    tile_scale : int, optional
            Tile scale factor (default: 1).
    min_zoom : int, optional
        Minimum zoom level (default: 7).
    max_zoom : int, optional
        Maximum zoom level (default: 10).
    timeout_s : float, optional
        Request timeout in seconds (default: 30.0).
    max_connections : int, optional
        Maximum total concurrent connections (default: 20).
    max_connections_per_host : int, optional
        Maximum concurrent connections per host (default: 20).
    **kwargs : Any
        Additional query parameters for the API.

    Returns
    -------
    pd.DataFrame
        Results for each tile request, including status, latency, and size.
    """
    benchmarker = TiTilerCMRBenchmarker(
        endpoint=endpoint,
        tms_id=tms_id,
        tile_format=tile_format,
        tile_scale=tile_scale,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        timeout_s=timeout_s,
        max_connections=max_connections,
        max_connections_per_host=max_connections_per_host,
        max_concurrent=max_concurrent,
    )

    def tileset_strategy(
        zoom: int, tms: morecantile.TileMatrixSet, tilejson_info: Dict[str, Any]
    ) -> List[Tuple[int, int]]:
        b = bounds or tilejson_info.get("bounds")
        if not b:
            raise ValueError("No bounds provided and none available in TileJSON.")
        tiles = get_tileset_tiles(bounds=b, zoom=zoom, tms=tms)
        if max_tiles_per_zoom is not None and len(tiles) > max_tiles_per_zoom:
            tiles = tiles[:max_tiles_per_zoom]
        return tiles

    return await benchmarker.benchmark_tiles(
        dataset, tileset_strategy, warmup_per_zoom=1, **kwargs
    )


async def benchmark_statistics(
    endpoint: str,
    dataset: DatasetParams,
    geometry: Optional[Union[Feature, Dict[str, Any]]] = None,
    *,
    timeout_s: float = 300.0,
    max_connections: int = 10,
    max_connections_per_host: int = 10,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Benchmark the `/timeseries/statistics` endpoint for a geometry.
    This high-level helper delegates to ``TiTilerCMRBenchmarker.benchmark_statistics``.
    If `geometry` is omitted, the TileJSON bounds for the dataset/time range are
    used to construct a bounding box feature. The result includes timing,
    HTTP status, and the statistics payload keyed by timestep.
    Parameters
    ----------
    endpoint : str
        Base URL of the TiTiler-CMR deployment.
    dataset : DatasetParams
        Dataset configuration.
    geometry : Union[Feature, Dict[str, Any]], optional
        GeoJSON Feature or geometry to analyze. If None, uses bounds from tilejson.
    timeout_s : float, optional
        Request timeout in seconds (default: 300.0).
    max_connections : int, optional
        Maximum total concurrent connections (default: 10).
    max_connections_per_host : int, optional
        Maximum concurrent connections per host (default: 10).
    **kwargs : Any
        Additional query parameters for the API.

    Returns
    -------
    Dict[str, Any]
        Statistics result with timing, memory, and metadata.

    """
    benchmarker = TiTilerCMRBenchmarker(
        endpoint=endpoint,
        timeout_s=timeout_s,
        max_connections=max_connections,
        max_connections_per_host=max_connections_per_host,
    )
    return await benchmarker.benchmark_statistics(dataset, geometry, **kwargs)


class TiTilerCMRBenchmarker(BaseBenchmarker):
    """
    Main benchmarking utility for TiTiler-CMR.
    Supports benchmarking of tile rendering and statistics endpoints
    across different strategies (viewport, tileset, custom).
    """

    def __init__(
        self,
        endpoint: str,
        *,
        tms_id: str = "WebMercatorQuad",
        tile_format: str = "png",
        tile_scale: int = 1,
        min_zoom: int = 7,
        max_zoom: int = 10,
        max_concurrent: int = 32,
        **base_kwargs: Any,
    ):
        """
        Initialize the TiTilerCMRBenchmarker.
        """
        super().__init__(endpoint, **base_kwargs)
        self.tms_id = tms_id
        self.tile_format = tile_format
        self.tile_scale = tile_scale
        self.min_zoom = min_zoom
        self.max_zoom = max_zoom
        self.max_concurrent = max_concurrent

    async def benchmark_tiles(  # noqa: C901
        self,
        dataset: DatasetParams,
        tiling_strategy: Callable,
        warmup_per_zoom: int = 1,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Benchmark tile rendering performance for TiTiler-CMR.
        It can be adopted for a viewport or whole tileset generation at a zoom level.

        Parameters
        ----------
        dataset : DatasetParams
            Dataset and query parameters (concept_id, backend, datetime_range, kwargs).
        tiling_strategy : Callable
            Function that returns tiles for a given zoom level.
            Signature: (zoom, tms, tilejson_info) -> List[Tuple[int, int]]
        warmup_per_zoom : int, optional
            Number of warmup tiles to fetch per zoom level before timing.
            Default is 1.
        **kwargs : Any
            Additional query parameters for the API.

        Returns
        -------
        pd.DataFrame
            Results for each tile request, including status, latency, and size.
        """
        self._log_header("Tile Benchmark", dataset)

        tile_params = list(
            dataset.to_query_params(
                tile_format=self.tile_format, tile_scale=self.tile_scale, **kwargs
            )
        )
        print(f"Query params: {len(tile_params)} parameters")
        for k, v in tile_params:
            print(f"  {k}: {v}")

        async with self._create_http_client() as client:
            tilejson_info = await self._get_tilejson_info(client, tile_params)
            tiles_endpoints = tilejson_info["tiles_endpoints"]
            tms = morecantile.tms.get(self.tms_id)

            # --- 1. Discover all tiles across all zoom levels ---
            jobs = []
            per_zoom_tiles = {}
            for zoom in range(self.min_zoom, self.max_zoom + 1):
                tiles = tiling_strategy(zoom, tms, tilejson_info)
                per_zoom_tiles[zoom] = tiles
                for x, y in tiles:
                    jobs.append((zoom, x, y))

            # --- 2. Set up global concurrency controls ---
            sem = BoundedSemaphore(self.max_concurrent)
            proc = psutil.Process()
            jitter_ms = 5

            async def _fetch_one_tile(z, x, y):
                # Small jitter to de-synchronize requests
                await asyncio.sleep((hash((z, x, y)) % jitter_ms) * 0.001)

                async with sem:
                    try:
                        return await fetch_tile(
                            client=client,
                            tiles_endpoints=tiles_endpoints,
                            z=z,
                            x=x,
                            y=y,
                            timeout_s=self.timeout_s,
                            proc=proc,
                        )
                    except Exception as ex:
                        return [
                            {
                                "zoom": z,
                                "x": x,
                                "y": y,
                                "is_error": True,
                                "ok": False,
                                "status_code": None,
                                "error_text": f"{type(ex).__name__}: {ex}",
                            }
                        ]

            # --- 3. Run warmup requests (bypassing semaphore) ---
            warmed_tiles = set()
            if warmup_per_zoom > 0:
                for zoom, tiles in per_zoom_tiles.items():
                    for x, y in tiles[:warmup_per_zoom]:
                        if (zoom, x, y) not in warmed_tiles:
                            try:
                                await fetch_tile(
                                    client=client,
                                    tiles_endpoints=tiles_endpoints,
                                    z=zoom,
                                    x=x,
                                    y=y,
                                    timeout_s=self.timeout_s,
                                    proc=proc,
                                )
                                warmed_tiles.add((zoom, x, y))
                            except Exception:
                                pass  # Ignore warmup failures

            # --- 4. Create and run main benchmark tasks ---
            tasks_to_run = [
                asyncio.create_task(_fetch_one_tile(z, x, y)) for z, x, y in jobs
            ]

            run_started_at = time.perf_counter()
            all_rows = []
            for future in asyncio.as_completed(tasks_to_run):
                result = await future
                if isinstance(result, list):
                    all_rows.extend(result)

            run_elapsed = time.perf_counter() - run_started_at
            print(f"Total execution time: {run_elapsed:.3f}s")

            # Add total elapsed time to each record
            for r in all_rows:
                r["total_run_elapsed_s"] = run_elapsed

        return self._process_results(all_rows)

    async def check_compatibility(
        self,
        dataset: DatasetParams,
        geometry: Optional[Union[Feature, Dict[str, Any]]] = None,
        bounds_fraction: float = 0.05,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Check dataset compatibility with TiTiler-CMR `/compatibility` endpoint.

        Parameters
        ----------
        dataset : DatasetParams
            Dataset configuration.
        geometry : Union[Feature, Dict[str, Any]], optional
            GeoJSON Feature or geometry for statistics test.
        bounds_fraction : float, optional
            Fraction of dataset area to use for random bounds when geometry is None
            (default: 0.05 = 5% of area).
        **kwargs : Any
            Additional query parameters.

        Returns
        -------
        Dict[str, Any]
            Compatibility result with timing and metadata.
        """
        self._log_header("Compatibility Check", dataset)

        issue_detected = False
        tilejson_info: Dict[str, Any] = {}
        n_timesteps: int = 0
        stats_result: Dict[str, Any] = {"success": False, "statistics": {}}

        try:
            async with self._create_http_client() as client:
                # Build params WITHOUT tile-format/scale extras for this preflight
                tile_params = list(dataset.to_query_params(**kwargs))

                # 1) TileJSON — discover tiles (timesteps/granules) and bounds
                tilejson_info = await self._get_tilejson_info(client, tile_params)
                tiles_endpoints = tilejson_info.get("tiles_endpoints", [])
                n_timesteps = len(tiles_endpoints)
                print(f"Found {n_timesteps} timesteps/granules from TileJSON")

                # 2) Geometry fallback from bounds
                if geometry is None:
                    bounds = tilejson_info.get("bounds")
                    if not bounds:
                        raise ValueError(
                            "No geometry provided and no bounds available from TileJSON"
                        )
                    geometry = create_bbox_feature(*bounds)
                    random_bounds = generate_random_bounds_within(
                        bounds, fraction=bounds_fraction
                    )  # 5% of area
                    geometry = create_bbox_feature(*random_bounds)
                    print(
                        f"Using random bounds for compatibility check: {random_bounds}"
                    )

                # 3) Run a small statistics preview to ensure server-side flow works
                stats_result = await self._fetch_statistics(
                    client=client,
                    dataset=dataset,
                    geometry=geometry,
                    **kwargs,
                )

        except httpx.HTTPStatusError as ex:
            response = ex.response
            status_code = response.status_code
            error_text = response.text
            print(f"HTTP {status_code} error during compatibility check")
            issue_detected = True
            stats_result = {
                "success": False,
                "elapsed_s": 0,
                "status_code": status_code,
                "n_timesteps": 0,
                "url": str(response.request.url),
                "statistics": {},
                "error": f"HTTP {status_code}: {error_text}",
            }

        except Exception as ex:
            print(f"Compatibility check failed: {ex}")
            issue_detected = True
            stats_result = {"success": False, "error": str(ex)}

        if stats_result.get("success"):
            print(f"Statistics returned {len(stats_result['statistics'])} timesteps")
            compatibility_status = "compatible"

        else:
            print(f"Statistics request failed: {stats_result.get('error')}")
            issue_detected = True

            compatibility_status = (
                "compatible"
                if (n_timesteps > 0 and not issue_detected)
                else "issues_detected"
            )

        return {
            "concept_id": dataset.concept_id,
            "backend": dataset.backend,
            "n_timesteps": n_timesteps,
            "tilejson_bounds": tilejson_info.get("bounds"),
            "statistics": (
                self._statistics_to_dataframe(stats_result.get("statistics", {}))
                if stats_result.get("success")
                else pd.DataFrame()
            ),
            "compatibility": compatibility_status,
            "success": compatibility_status == "compatible",
            "compatible": compatibility_status == "compatible",
            "error": stats_result.get("error") if issue_detected else None,
        }

    async def benchmark_statistics(
        self,
        dataset: DatasetParams,
        geometry: Optional[Union[Feature, Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Benchmark statistics endpoint performance with timing and memory metrics.

        Parameters
        ----------
        dataset : DatasetParams
            Dataset configuration.
        geometry : Union[Feature, Dict[str, Any]], optional
            GeoJSON Feature or geometry to analyze. If None, uses bounds from tilejson.
        **kwargs : Any
            Additional query parameters.

        Returns
        -------
        Dict[str, Any]
            Statistics result with timing, memory, and metadata.
        """
        self._log_header("Statistics Benchmark", dataset)
        async with self._create_http_client() as client:
            if geometry is None:
                raise ValueError("No geometry provided!")
            return await self._fetch_statistics(
                client=client, dataset=dataset, geometry=geometry, **kwargs
            )

    async def _fetch_statistics(
        self,
        client: httpx.AsyncClient,
        dataset: DatasetParams,
        geometry: Union[Feature, Dict[str, Any]],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Posts the provided GeoJSON Feature or raw geometry to the TiTiler-CMR
        `/timeseries/statistics` endpoint and returns per-timestep summary
        statistics for pixels intersecting the geometry.

        Parameters
        ----------
        client : httpx.AsyncClient
            HTTP client for requests.
        dataset : DatasetParams
            Dataset configuration.
        geometry : Union[Feature, Dict[str, Any]]
            GeoJSON Feature or geometry.
        **kwargs : Any
            Additional query parameters.

        Returns
        -------
        Dict[str, Any]
            Statistics result and metadata and timing.
        """
        url = f"{self.endpoint.rstrip('/')}/timeseries/statistics"
        tile_params = dict(dataset.to_query_params(**kwargs))

        if hasattr(geometry, "model_dump"):
            geojson_body = geometry.model_dump(exclude_none=True)
        elif isinstance(geometry, dict):
            geojson_body = geometry
        else:
            raise ValueError("geometry must be a GeoJSON Feature or dict")

        try:
            data, elapsed, status = await self._request_json(
                client,
                method="POST",
                url=url,
                params=tile_params,
                json_payload=geojson_body,
                timeout_s=self.timeout_s,
            )

            stats = data.get("properties", {}).get("statistics", {})
            return {
                "success": True,
                "elapsed_s": elapsed,
                "status_code": status,
                "n_timesteps": len(stats) if isinstance(stats, dict) else 0,
                "url": url,
                "statistics": stats,
                "error": None,
            }
        except Exception as ex:
            return {
                "success": False,
                "elapsed_s": 0,
                "status_code": None,
                "n_timesteps": 0,
                "url": url,
                "statistics": {},
                "error": f"{type(ex).__name__}: {ex}",
            }

    async def _get_tilejson_info(
        self, client: httpx.AsyncClient, params: List[Tuple[str, str]]
    ) -> Dict[str, Any]:
        """
        Query TiTiler-CMR TileJSON and return parsed tiles endpoints, and bounds.

        Parameters
        ----------
        client : httpx.AsyncClient
            HTTP client for requests.
        params : list of tuple
            Query parameters for the request.

        Returns
        -------
        dict
            Dictionary with entries, tilejson, tile endpoints, and bounds.
        """
        url = f"{self.endpoint.rstrip('/')}/{self.tms_id}/tilejson.json"
        ts_json, _, _ = await self._request_json(
            client,
            method="GET",
            url=url,
            params=dict(params),
            timeout_s=self.timeout_s,
        )
        tiles_endpoints = ts_json.get("tiles", [])

        if not tiles_endpoints:
            raise RuntimeError("No tile endpoints found in TileJSON response")

        bounds = ts_json.get("bounds")
        return {
            "tilejson": ts_json,
            "tiles_endpoints": tiles_endpoints,
            "bounds": bounds,
        }

    async def _request_json(
        self,
        client: httpx.AsyncClient,
        *,
        method: str,
        url: str,
        timeout_s: float,
        params: Optional[Dict[str, Any]] = None,
        json_payload: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], float, int]:
        """
        Unified JSON request helper for GET/POST with consistent error handling.
        Returns
        -------
        (payload, elapsed_s, status_code)
        """
        t0 = time.perf_counter()
        try:
            if method.upper() == "GET":
                response = await client.get(url, params=params or {}, timeout=timeout_s)
            elif method.upper() == "POST":
                response = await client.post(
                    url, params=params or {}, json=json_payload, timeout=timeout_s
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method!r}")
            response.raise_for_status()
            elapsed = time.perf_counter() - t0
            data = response.json()
            return data if isinstance(data, dict) else {}, elapsed, response.status_code
        except httpx.HTTPStatusError as ex:
            response = ex.response
            elapsed = time.perf_counter() - t0
            print("~~~~~~~~~~~~~~~~ ERROR JSON REQUEST ~~~~~~~~~~~~~~~~")
            print(f"URL: {response.request.url}")
            print(f"Error: {response.status_code} {response.reason_phrase}")
            print(f"Body: {response.text}")
            raise

    @staticmethod
    def _statistics_to_dataframe(stats: Dict[str, Any]) -> pd.DataFrame:
        """
        Flatten TiTiler-CMR statistics dict into a DataFrame, assuming
        inner and outer timestamps match. Histogram arrays are dropped.
        Output columns:
          - timestamp (ISO8601 string)
          - scalar metrics (min, max, mean, count, sum, std, median, majority,
            minority, unique, valid_percent, masked_pixels, valid_pixels,
            percentile_2, percentile_98)
        """
        rows: List[Dict[str, Any]] = []
        if not isinstance(stats, dict):
            return pd.DataFrame()
        for _, inner in stats.items():
            if not isinstance(inner, dict) or not inner:
                continue
            inner_ts, metrics = next(iter(inner.items()))
            if not isinstance(metrics, dict):
                continue
            row: Dict[str, Any] = {"timestamp": inner_ts}
            for k, v in metrics.items():
                if k == "histogram":
                    continue
                row[k] = v
            rows.append(row)
        df = pd.DataFrame(rows)
        for col in df.columns:
            if col != "timestamp":
                df[col] = pd.to_numeric(df[col])
        if not df.empty and "timestamp" in df.columns:
            df = df.sort_values("timestamp")
        return df.reset_index(drop=True)


def generate_random_bounds_within(
    parent_bounds: List[float], fraction: float = 0.1
) -> List[float]:
    """
    Generate random bounds within parent bounds.

    Parameters
    ----------
    parent_bounds : List[float]
        Parent bounding box [min_lon, min_lat, max_lon, max_lat]
    fraction : float, optional
        Approximate fraction of parent area to cover (default: 0.1 = 10%)

    Returns
    -------
    List[float]
        Random bounding box [min_lon, min_lat, max_lon, max_lat] within parent
    """
    min_lon, min_lat, max_lon, max_lat = parent_bounds

    # Calculate dimensions
    lon_range = max_lon - min_lon
    lat_range = max_lat - min_lat

    # Calculate size of random box (square root to get linear dimension from area fraction)
    scale = fraction**0.5
    random_lon_size = lon_range * scale
    random_lat_size = lat_range * scale

    # Generate random center point with enough margin for the box
    margin_lon = random_lon_size / 2
    margin_lat = random_lat_size / 2

    center_lon = random.uniform(min_lon + margin_lon, max_lon - margin_lon)
    center_lat = random.uniform(min_lat + margin_lat, max_lat - margin_lat)

    # Create random bounds around the center
    random_bounds = [
        center_lon - margin_lon,  # min_lon
        center_lat - margin_lat,  # min_lat
        center_lon + margin_lon,  # max_lon
        center_lat + margin_lat,  # max_lat
    ]

    return random_bounds


def tiling_benchmark_summary(df):
    """
    Compute and (optionally) print summary statistics for tile benchmark results.
    Groups by zoom level and reports:
      - n_tiles
      - ok_pct, no_data_pct, error_pct
      - median_latency_s, p95_latency_s

    Parameters
    ----------
    df : pandas.DataFrame
        Raw per-tile results with at least ``zoom`` ``ok``, and ``response_time_sec``.

    Returns
    -------
    pandas.DataFrame
        Summary statistics by zoom level (count, median, p95, etc.).
    """
    for col in ["response_time_sec"]:
        if col not in df.columns:
            raise KeyError(
                f"Required column '{col}' not found. Available columns: {list(df.columns)}"
            )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    summary = (
        df.groupby(["zoom"], dropna=False)
        .apply(
            lambda g: pd.Series(
                {
                    "n_tiles": int(len(g)),
                    "ok_pct": 100.0 * (g["ok"].sum() / len(g)) if len(g) else 0.0,
                    "no_data_pct": 100.0 * (g["no_data"].sum() / len(g))
                    if len(g)
                    else 0.0,
                    "error_pct": 100.0 * (g["is_error"].sum() / len(g))
                    if len(g)
                    else 0.0,
                    "median_latency_s": g["response_time_sec"].median(),
                    "p95_latency_s": g["response_time_sec"].quantile(0.95),
                }
            ),
            include_groups=False,
        )
        .reset_index()
        .sort_values(["zoom"])
    )
    return summary


__all__ = [
    "check_titiler_cmr_compatibility",
    "benchmark_viewport",
    "benchmark_tileset",
    "benchmark_statistics",
    "tiling_benchmark_summary",
    "TiTilerCMRBenchmarker",
]


if __name__ == "__main__":

    async def main():
        """Example usage of the unified TiTiler-CMR benchmarking system."""
        endpoint = "https://staging.openveda.cloud/api/titiler-cmr"

        ds_xarray = DatasetParams(
            concept_id="C2723754864-GES_DISC",
            backend="xarray",
            datetime_range="2022-03-01T00:00:01Z/2022-03-01T23:59:59Z",
            variable="precipitation",
            step="P1D",
            temporal_mode="point",
        )

        ds_hls = DatasetParams(
            concept_id="C2036881735-POCLOUD",
            backend="rasterio",
            datetime_range="2024-10-01T00:00:01Z/2024-10-10T00:00:01Z",
            bands=["B04", "B03", "B02"],
            bands_regex="B[0-9][0-9]",
            step="P1D",
            temporal_mode="point",
        )

        print("\n=== Example 1: Viewport Tile Benchmarking [Xarray]===")
        df_viewport = await benchmark_viewport(
            endpoint=endpoint,
            dataset=ds_xarray,
            lng=-95.0,
            lat=29.0,
            viewport_width=4,
            viewport_height=4,
            min_zoom=5,
            max_zoom=18,
            timeout_s=60.0,
            max_concurrent=32,
        )

        print(f"Viewport results: {len(df_viewport)} tile requests")
        print(df_viewport.head())
        print(tiling_benchmark_summary(df_viewport))

        print("\n=== Example 2: Viewport Tile Benchmarking [RasterIO]===")
        df_viewport2 = await benchmark_viewport(
            endpoint=endpoint,
            dataset=ds_hls,
            lng=29,
            lat=25.0,
            viewport_width=4,
            viewport_height=4,
            min_zoom=7,
            max_zoom=18,
            timeout_s=60.0,
            max_concurrent=32,
        )
        print(f"Viewport results: {len(df_viewport2)} tile requests")
        print(df_viewport2.head())
        print(tiling_benchmark_summary(df_viewport2))

        print("\n=== Example 3: Tileset Tile Benchmarking ===")
        gulf_bounds = [-98.676, 18.857, -95.623, 31.097]
        df_tileset = await benchmark_tileset(
            endpoint=endpoint,
            dataset=ds_hls,
            bounds=gulf_bounds,
            max_tiles_per_zoom=25,
            min_zoom=7,
            max_zoom=18,
            timeout_s=60.0,
            max_concurrent=32,
        )

        print(f"Tileset results: {len(df_tileset)} tile requests")
        print(tiling_benchmark_summary(df_tileset))

        print("\n=== Example 4: Statistics Benchmarking ===")
        gulf_geometry = create_bbox_feature(-98.676, 18.857, -81.623, 31.097)
        stats_result = await benchmark_statistics(
            endpoint=endpoint,
            dataset=ds_xarray,
            geometry=gulf_geometry,
            timeout_s=300.0,
        )
        print("Statistics result:")
        print(f"  Success: {stats_result['success']}")
        print(f"  Elapsed: {stats_result['elapsed_s']:.2f}s")
        print(f"  Timesteps: {stats_result['n_timesteps']}")
        print(
            f"  Statistics keys: {list(stats_result.get('statistics', {}).keys())[:3]}..."
        )

        print("\n=== Example 5: Compatibility Test ===")

        result = await check_titiler_cmr_compatibility(
            endpoint=endpoint,
            dataset=ds_xarray,
            bounds_fraction=0.01,
        )

        print("Compatibility result:")
        print(f"{result}")
        print(result["compatibility"])

    asyncio.run(main())
