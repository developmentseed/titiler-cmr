"""Timeseries extension for titiler.cmr

The /timeseries endpoints provide an API for retrieving data for a timeseries that
would otherwise need to be sent as a set of independent requests.

The /timeseries endpoints follow this basic pattern to assemble results for a timeseries:
- The 'temporal' parameter (required) is combined with the optional 'step'
  and 'temporal_mode' parameters to produce a list of specific temporal parameters
  that can be passed to the lower-level endpoints.
- The /timeseries endoint will construct a list of GET or POST requests to the
  lower-level endpoint and execute them asynchronously over HTTP
- The results are results are combined into a format appropriate for the endpoint's
  response type (e.g. PNGs combined into a GIF for the /timeseries/bbox endpoint).
"""

import asyncio
import io
import logging
import os
from dataclasses import dataclass, fields
from datetime import datetime, timedelta, timezone
from enum import Enum
from time import time
from types import DynamicClassAttribute
from typing import Annotated, Any, Dict, List, Literal, Optional, Tuple, Union
from urllib.parse import urlencode

import httpx
import psutil
from attrs import define
from fastapi import APIRouter, Body, Depends, Path, Query, Request, Response
from fastapi.exceptions import HTTPException
from fastapi.responses import StreamingResponse
from geojson_pydantic import Feature, FeatureCollection
from geojson_pydantic.geometries import Geometry
from isodate import parse_duration
from PIL import Image
from pydantic import BaseModel
from titiler.core.algorithm import algorithms as available_algorithms
from titiler.core.dependencies import CoordCRSParams, DefaultDependency, DstCRSParams
from titiler.core.factory import BaseFactory, FactoryExtension
from titiler.core.models.mapbox import TileJSON
from titiler.core.models.responses import Statistics
from titiler.core.resources.enums import ImageType
from titiler.core.resources.responses import GeoJSONResponse

from titiler.cmr.dependencies import GranuleSearch, GranuleSearchParams
from titiler.cmr.errors import InvalidDatetime
from titiler.cmr.factory import CMRTilerFactory
from titiler.cmr.logger import logger
from titiler.cmr.query import get_granules
from titiler.cmr.reader import XarrayGranuleReader
from titiler.cmr.settings import ApiSettings
from titiler.cmr.utils import (
    calculate_time_series_request_size,
    get_geojson_bounds,
    parse_datetime,
)

settings = ApiSettings()

# this section should eventually get moved to titiler.extensions.timeseries
timeseries_img_endpoint_params: Dict[str, Any] = {
    "responses": {
        200: {
            "content": {
                "image/gif": {},
            },
            "description": "Return an image.",
        }
    },
    "response_class": Response,
}


# TODO: remove after upgrading to titiler>=0.19
class TimeseriesMediaType(str, Enum):
    """Responses Media types formerly known as MIME types."""

    gif = "image/gif"


class TimeseriesImageType(str, Enum):
    """Available output image type."""

    gif = "gif"

    @DynamicClassAttribute
    def mediatype(self):
        """Return image media type."""
        return TimeseriesMediaType[self._name_].value


TimeseriesStatistics = Dict[str, Statistics]


class TimeseriesStatisticsInGeoJSON(BaseModel):
    """Statistics model in geojson response."""

    statistics: TimeseriesStatistics

    model_config = {"extra": "allow"}


TimeseriesStatisticsGeoJSON = Union[
    FeatureCollection[Feature[Geometry, TimeseriesStatisticsInGeoJSON]],
    Feature[Geometry, TimeseriesStatisticsInGeoJSON],
]

TimeseriesTileJSON = Dict[str, TileJSON]


class TemporalMode(str, Enum):
    """Temporal modes for queries.

    point: queries will be sent for single points in time
    interval: queries will cover the time between two points
    """

    point = "point"
    interval = "interval"


@dataclass
class TimeseriesParams(DefaultDependency):
    """Timeseries parameters"""

    temporal: Annotated[
        Optional[str],
        Query(
            description="Either a date-time, an interval, or a comma-separated list of date-times or intervals."
            "Date and time expressions adhere to rfc3339 ('2020-06-01T09:00:00Z') format."
            "Half-bounded intervals are allowed as long as you provide a start date.",
            openapi_examples={
                "A date-time": {"value": "2018-02-12T09:00:00Z"},
                "A bounded interval": {
                    "value": "2018-02-12T09:00:00Z/2018-03-18T09:00:00Z"
                },
                "Half-bounded intervals (start)": {"value": "2018-02-12T09:00:00Z/.."},
                "A list of date-times": {
                    "value": "2018-02-12T09:00:00Z,2019-01-12T09:00:00Z"
                },
                "A list of intervals": {
                    "value": "2018-02-12T09:00:00Z/2018-03-18T09:00:00Z,2018-04-12T09:00:00Z/2018-05-09T00:00:00Z"
                },
            },
        ),
    ] = None
    datetime_param: Annotated[
        Optional[str],
        Query(alias="datetime", include_in_schema=False),
    ] = None
    step: Annotated[
        Optional[str],
        Query(
            description="Time step between timeseries intervals, expressed as [ISO 8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations)"
        ),
    ] = None
    temporal_mode: Annotated[
        Literal[TemporalMode.point, TemporalMode.interval],
        Query(
            description="Point: CMR queries will be made for specific moments in time. "
            "Interval: CMR queries will be made for the intervals between the points along the timeseries and results will be mosaiced into a single array before summarization."
        ),
    ] = TemporalMode.interval

    def __post_init__(self):
        """Apply legacy parameter aliases."""
        if self.datetime_param and not self.temporal:
            self.temporal = self.datetime_param
        if not self.temporal:
            raise HTTPException(400, "'temporal' is required")


# Include the `datetime` HTTP alias so old-style ?datetime=... is also stripped
# from sub-request URLs when building timeseries requests.
timeseries_field_names = {field.name for field in fields(TimeseriesParams)} | {
    "datetime"
}


def generate_datetime_ranges(
    start_datetime: datetime,
    end_datetime: datetime,
    step: str,
    temporal_mode: Literal[
        TemporalMode.interval, TemporalMode.point
    ] = TemporalMode.interval,
) -> List[Union[Tuple[datetime], Tuple[datetime, datetime]]]:
    """Split a datetime range into step-sized sub-ranges.

    In ``point`` mode each element is a 1-tuple ``(datetime,)`` for the start
    of each step.  In ``interval`` mode each element is a 2-tuple
    ``(start, end)`` where the end is nudged back by 1 second (or 1
    millisecond for sub-second steps) to avoid overlap with the next interval.

    If the computed list would be empty (e.g. ``start_datetime == end_datetime``),
    returns ``[(start_datetime, end_datetime)]``.

    Args:
        start_datetime: Beginning of the overall date range.
        end_datetime: End of the overall date range.
        step: ISO 8601 duration string (e.g. ``"P1D"``, ``"PT1H"``).
        temporal_mode: Whether to produce point instants or closed intervals.

    Returns:
        List of 1-tuples (point mode) or 2-tuples (interval mode).
    """
    step_delta = parse_duration(step)

    ranges: List[Union[Tuple[datetime], Tuple[datetime, datetime]]] = []
    current = start_datetime

    step_timedelta = (current + step_delta) - current
    is_small_timestep = step_timedelta <= timedelta(seconds=1)

    while current < end_datetime:
        if temporal_mode == TemporalMode.point:
            # For points in time case, return a tuple with just one exact datetime
            next_step = current + step_delta
            ranges.append((current,))
        else:
            next_step = min(current + step_delta, end_datetime)
            if next_step == end_datetime:
                ranges.append((current, next_step))
                break

            if is_small_timestep:
                # Subtract 1 millisecond for small timesteps
                ranges.append((current, next_step - timedelta(microseconds=1)))
            else:
                # Subtract 1 second for larger timesteps
                ranges.append((current, next_step - timedelta(seconds=1)))

        current = next_step

        if current == end_datetime:
            ranges.append((end_datetime,))

    if not ranges:
        return [(start_datetime, end_datetime)]

    return ranges


def build_request_urls(
    base_url: str,
    request: Request,
    param_list: List[BaseModel],
):
    """Build lower-level request URLs from a base_url, a request, and a list of
    additional query parameters. Preserves multiple values for the same parameter.
    """
    urls = []

    # Convert query_params to list of tuples, excluding timeseries fields
    non_timeseries_params = [
        (key, value)
        for key, value in request.query_params.multi_items()
        if key not in timeseries_field_names
    ]

    for _params in param_list:
        model_params = [
            (str(key), str(value))
            for key, value in _params.model_dump(exclude_none=True).items()
        ]

        url = (
            f"{base_url}?{urlencode(non_timeseries_params + model_params, doseq=True)}"
        )
        urls.append(url)

    return urls


async def timestep_request(
    url: str, method: Literal["POST", "GET"], **kwargs
) -> httpx.Response:
    """Asynchronously send a GET or POST request to a URL.

    Args:
        url: Full URL to request.
        method: HTTP method — either ``"GET"`` or ``"POST"``.
        **kwargs: Additional arguments forwarded to the underlying httpx method
            (e.g. ``json``, ``timeout``).

    Returns:
        The httpx Response object.

    Raises:
        ValueError: If ``method`` is not ``"GET"`` or ``"POST"``.
    """
    async with httpx.AsyncClient() as client:
        _method: Any
        if method == "POST":
            _method = client.post
        elif method == "GET":
            _method = client.get
        else:
            raise ValueError(f"{method} must be one of GET or POST")

        response = await _method(url, **kwargs)

        return response


# The rest is titiler-cmr specific

TimeseriesCMRQueryParameters = List[GranuleSearch]


def timeseries_cmr_query(  # noqa: C901
    request: Request,
    granule_search: GranuleSearch = Depends(GranuleSearchParams),
    timeseries_params: TimeseriesParams = Depends(TimeseriesParams),
    minx: Optional[float] = None,
    miny: Optional[float] = None,
    maxx: Optional[float] = None,
    maxy: Optional[float] = None,
) -> TimeseriesCMRQueryParameters:
    """Convert a timeseries query into a set of CMR query parameters.

    If no step is provided with timeseries_params, a query will be sent to CMR
    to identify all unique timesteps in granules between the provided start/stop_datetime.
    """
    if not granule_search.collection_concept_id:
        raise HTTPException(status_code=400, detail="collection_concept_id is required")

    if timeseries_params.temporal is None:
        raise HTTPException(status_code=400, detail="temporal is required")
    temporal_inputs = timeseries_params.temporal.split(",")

    temporal_params = []

    for temporal_input in temporal_inputs:
        try:
            datetime_, start, end = parse_datetime(temporal_input)
        except InvalidDatetime as e:
            raise HTTPException(
                status_code=400,
                detail=f"{timeseries_params.temporal} is an invalid temporal input",
            ) from e

        if datetime_:
            temporal_params.append(datetime_.isoformat())

        elif start and timeseries_params.step:
            datetime_ranges = generate_datetime_ranges(
                start_datetime=start,
                end_datetime=end or datetime.now(tz=timezone.utc),
                step=timeseries_params.step,
                temporal_mode=timeseries_params.temporal_mode,
            )

            temporal_params.extend(
                [
                    "/".join([t.isoformat() for t in datetime_range])
                    for datetime_range in datetime_ranges
                ]
            )

        # if a start (and possibly end) are provided but no step, query CMR to identify
        # unique timestamps from available granules
        elif start and not timeseries_params.step:
            search_end = end or datetime.now(tz=timezone.utc)
            bbox_str = (
                f"{minx},{miny},{maxx},{maxy}"
                if (
                    minx is not None
                    and miny is not None
                    and maxx is not None
                    and maxy is not None
                )
                else granule_search.bounding_box
            )
            cmr_search = GranuleSearch(
                collection_concept_id=granule_search.collection_concept_id,
                temporal=f"{start.isoformat()}/{search_end.isoformat()}",
                bounding_box=bbox_str,
            )
            for granule in get_granules(cmr_search, client=request.app.state.client):
                rdt = (
                    granule.temporal_extent.range_date_time
                    if granule.temporal_extent
                    else None
                )
                if rdt and rdt.beginning_date_time:
                    g_start = datetime.fromisoformat(
                        rdt.beginning_date_time.replace("Z", "+00:00")
                    )
                    g_end = (
                        datetime.fromisoformat(
                            rdt.ending_date_time.replace("Z", "+00:00")
                        )
                        if rdt.ending_date_time
                        else g_start
                    )
                    midpoint = g_start + (g_end - g_start) / 2
                    temporal_params.append(midpoint.isoformat())

        else:
            raise HTTPException(
                status_code=400,
                detail="you must provide a temporal interval with a defined start time or a "
                "list of comma-separated temporal strings",
            )

    if len(temporal_params) > settings.time_series_max_requests:
        raise HTTPException(
            status_code=400,
            detail=f"this request ({len(temporal_params)}) exceeds the maximum number of distinct "
            f"time series points/intervals of {settings.time_series_max_requests}",
        )

    return [
        GranuleSearch(
            collection_concept_id=granule_search.collection_concept_id,
            granule_ur=granule_search.granule_ur,
            cloud_cover=granule_search.cloud_cover,
            bounding_box=granule_search.bounding_box,
            sort_key=granule_search.sort_key,
            temporal=temporal_,
        )
        for temporal_ in temporal_params
    ]


def timeseries_cmr_query_no_bbox(
    request: Request,
    granule_search: GranuleSearch = Depends(GranuleSearchParams),
    timeseries_params: TimeseriesParams = Depends(TimeseriesParams),
) -> TimeseriesCMRQueryParameters:
    """Timeseries query but without bbox as a parameter.

    Needed this because FastAPI would expect bbox in the POST request body for
    the /timeseries/statistics endpoint when using Depends(timeseries_query)
    """
    return timeseries_cmr_query(
        request=request,
        granule_search=granule_search,
        timeseries_params=timeseries_params,
        minx=None,
        miny=None,
        maxx=None,
        maxy=None,
    )


timeseries_router = APIRouter()


@timeseries_router.get(
    "/timeseries",
    response_model=TimeseriesCMRQueryParameters,
    response_model_exclude_none=True,
    responses={
        200: {
            "description": "Return the list of concept_id and datetime query parameters "
            "for a timeseries query"
        }
    },
    tags=["Timeseries"],
)
def get_timeseries_parameters(
    query=Depends(timeseries_cmr_query),
):
    """Get timeseries request parameters"""
    return query


@define
class TimeseriesExtension(FactoryExtension):
    """Timeseries extension"""

    def register(self, factory: BaseFactory) -> None:
        """Register timeseries endpoints to the MosaicTilerFactory"""
        assert isinstance(factory, CMRTilerFactory)
        self.register_statistics(factory=factory)
        self.register_tilejson(factory=factory)
        self.register_images(factory=factory)

    def register_statistics(self, factory: CMRTilerFactory):
        """Register timeseries statistics endpoint"""
        is_xarray = factory.dataset_reader == XarrayGranuleReader

        @factory.router.post(
            "/timeseries/statistics",
            summary="Summary statistics for each point/interval along a timeseries",
            response_model=TimeseriesStatisticsGeoJSON,
            response_model_exclude_none=True,
            response_class=GeoJSONResponse,
            responses={
                200: {
                    "content": {"application/geo+json": {}},
                    "description": "Return timeseries statistics for geojson features.",
                }
            },
            tags=["Timeseries", "Statistics"],
        )
        async def timeseries_geojson_statistics(
            request: Request,
            geojson: Annotated[
                Union[FeatureCollection, Feature],
                Body(description="GeoJSON Feature or FeatureCollection.", embed=False),
            ],
            query=Depends(timeseries_cmr_query_no_bbox),
            coord_crs=Depends(CoordCRSParams),
            dst_crs=Depends(DstCRSParams),
            reader_params=Depends(factory.reader_dependency),
            dataset_params=Depends(factory.dataset_dependency),
            layer_params=Depends(factory.layer_dependency),
            post_process=Depends(factory.process_dependency),
            stats_params=Depends(factory.stats_dependency),
            histogram_params=Depends(factory.histogram_dependency),
            image_params=Depends(factory.img_part_dependency),
        ):
            """For all points/intervals along a timeseries, calculate summary statistics
            for the pixels that intersect a GeoJSON feature.
            """
            start_time = time()
            process = psutil.Process(os.getpid())
            logging.info("Checking size of time series request")

            # check for unconstrained image reading operations
            if is_xarray or (
                not image_params.max_size
                or not (image_params.height and image_params.width)
            ):
                # get bbox for geojson:
                minx, miny, maxx, maxy = get_geojson_bounds(geojson)

                request_size = calculate_time_series_request_size(
                    concept_id=query[0].collection_concept_id if query else "",
                    client=request.app.state.client,
                    n_time_steps=len(query),
                    minx=minx,
                    miny=miny,
                    maxx=maxx,
                    maxy=maxy,
                    coord_crs=coord_crs,
                )

                image_size = request_size / len(query)
                if image_size > settings.time_series_max_image_size:
                    raise HTTPException(
                        status_code=400,
                        detail="The AOI for this request is too large for the /statistics endpoint for this dataset. "
                        "Try again with either a smaller AOI",
                    )

                if request_size > settings.time_series_statistics_max_total_size:
                    raise HTTPException(
                        status_code=400,
                        detail=f"This request is too large for the /timeseries/statistics endpoint for this dataset. "
                        f"Try again with either a smaller AOI or fewer time steps than {len(query)}",
                    )

            logging.info(
                f"Initial memory usage: {process.memory_info().rss / 1024 / 1024} MB"
            )
            urls = build_request_urls(
                base_url=str(factory.url_for(request, "geojson_statistics")),
                request=request,
                param_list=query,
            )

            timestep_requests = await asyncio.gather(
                *[
                    timestep_request(
                        url,
                        method="POST",
                        json=geojson.model_dump(exclude_none=True),
                        timeout=None,
                    )
                    for url in urls
                ]
            )

            logging.info(
                f"Time to fetch individual statistics: {time() - start_time:.2f}s"
            )
            logging.info(
                f"Memory after fetching: {process.memory_info().rss / 1024 / 1024} MB"
            )
            logging.info(f"Number of statistics responses: {len(timestep_requests)}")
            logging.info(
                f"Starting stats reduction with {len(timestep_requests)} items"
            )
            combine_start = time()
            datetime_strs = [d.temporal for d in query]
            if not isinstance(geojson, Feature):
                raise HTTPException(
                    status_code=400, detail="Expected a GeoJSON Feature"
                )
            if geojson.properties is None:
                geojson.properties = {}
            geojson.properties["statistics"] = {}
            for r, datetime_str in zip(timestep_requests, datetime_strs):
                if r.status_code == 200:
                    geojson.properties["statistics"][datetime_str] = r.json()[
                        "properties"
                    ]["statistics"]

            logging.info(f"Time to create output: {time() - combine_start:.2f}s")
            logging.info(f"Total time: {time() - start_time:.2f}s")
            logging.info(
                f"Final memory usage: {process.memory_info().rss / 1024 / 1024} MB"
            )
            return geojson

    def register_tilejson(self, factory: CMRTilerFactory):
        """Register tilejson timeseries endpoint"""

        @factory.router.get(
            "/timeseries/{tileMatrixSetId}/tilejson.json",
            summary="TileJSON for all points/intervals along a timeseries",
            response_model=TimeseriesTileJSON,
            responses={
                200: {"description": "Return a set of tilejsons for a timeseries"}
            },
            response_model_exclude_none=True,
            tags=["Timeseries", "TileJSON"],
        )
        async def timeseries_tilejson(
            request: Request,
            tileMatrixSetId: Annotated[  # type: ignore
                Literal[tuple(factory.supported_tms.list())],
                Path(description="Identifier for a supported TileMatrixSet"),
            ],
            query=Depends(timeseries_cmr_query),
            tile_format: Annotated[
                Optional[ImageType],
                Query(
                    description="Default will be automatically defined if the output image needs a mask (png) or not (jpeg).",
                ),
            ] = None,
            tile_scale: Annotated[
                int,
                Query(
                    gt=0, lt=4, description="Tile size scale. 1=256x256, 2=512x512..."
                ),
            ] = 1,
            minzoom: Annotated[
                Optional[int],
                Query(description="Overwrite default minzoom."),
            ] = None,
            maxzoom: Annotated[
                Optional[int],
                Query(description="Overwrite default maxzoom."),
            ] = None,
            reader_params=Depends(factory.reader_dependency),
            dataset_params=Depends(factory.dataset_dependency),
            layer_params=Depends(factory.layer_dependency),
            post_process=Depends(available_algorithms.dependency),
            colormap=Depends(factory.colormap_dependency),
            render_params=Depends(factory.render_dependency),
        ) -> TimeseriesTileJSON:
            """Get a set of tilejsons for all points/intervals along a timeseries."""
            urls = build_request_urls(
                base_url=str(
                    factory.url_for(
                        request, "tilejson", tileMatrixSetId=tileMatrixSetId
                    )
                ),
                request=request,
                param_list=query,
            )

            timestep_requests = await asyncio.gather(
                *[timestep_request(url, method="GET") for url in urls]
            )

            results = [request.json() for request in timestep_requests]

            datetime_strs = [d.temporal for d in query]

            return dict(zip(datetime_strs, results))

    def register_images(self, factory: CMRTilerFactory):
        """Register image preview methods"""
        is_xarray = factory.dataset_reader == XarrayGranuleReader

        prefix = factory.router_prefix.strip("/")

        @factory.router.get(
            "/timeseries/bbox/{minx},{miny},{maxx},{maxy}.{format}",
            tags=["Timeseries", "Images"],
            operation_id=f"{prefix}_timeseries_gif_default_size"
            if prefix
            else "timeseries_gif_default_size",
            summary="Create an animation from a timeseries of PNGs (default size)",
            **timeseries_img_endpoint_params,
        )
        @factory.router.get(
            "/timeseries/bbox/{minx},{miny},{maxx},{maxy}/{width}x{height}.{format}",
            tags=["Timeseries", "Images"],
            operation_id=f"{prefix}_timeseries_gif_custom_size"
            if prefix
            else "timeseries_gif_custom_size",
            summary="Create an animation from a timeseries of PNGs (custom size)",
            **timeseries_img_endpoint_params,
        )
        async def bbox_timeseries_image(
            request: Request,
            minx: Annotated[float, Path(description="Bounding box min X")],
            miny: Annotated[float, Path(description="Bounding box min Y")],
            maxx: Annotated[float, Path(description="Bounding box max X")],
            maxy: Annotated[float, Path(description="Bounding box max Y")],
            format: Annotated[
                Optional[TimeseriesImageType],
                "Default will be automatically defined if the output image needs a mask (png) or not (jpeg).",
            ] = TimeseriesImageType.gif,
            query=Depends(timeseries_cmr_query),
            fps: Annotated[
                int,
                Query(gt=1, description="Frames per second for the gif"),
            ] = 10,
            coord_crs=Depends(CoordCRSParams),
            dst_crs=Depends(DstCRSParams),
            reader_params=Depends(factory.reader_dependency),
            dataset_params=Depends(factory.dataset_dependency),
            layer_params=Depends(factory.layer_dependency),
            post_process=Depends(factory.process_dependency),
            image_params=Depends(factory.img_part_dependency),
            colormap=Depends(factory.colormap_dependency),
            render_params=Depends(factory.render_dependency),
        ):
            """Create an animation along a timeseries for a bbox.

            Currently only the `GIF` format is supported but `MP4` is on the roadmap.
            """
            start_time = time()
            process = psutil.Process(os.getpid())

            # check for unconstrained image reading operations
            if is_xarray or (
                not image_params.max_size
                or not (image_params.height and image_params.width)
            ):
                request_size = calculate_time_series_request_size(
                    concept_id=query[0].collection_concept_id if query else "",
                    client=request.app.state.client,
                    n_time_steps=len(query),
                    minx=minx,
                    miny=miny,
                    maxx=maxx,
                    maxy=maxy,
                    coord_crs=coord_crs,
                )

                image_size = request_size / len(query)
                if image_size > settings.time_series_max_image_size:
                    raise HTTPException(
                        status_code=400,
                        detail="The AOI for this request is too large for the /bbox endpoint for this dataset. "
                        "Try again with either a smaller AOI",
                    )

                if request_size > settings.time_series_image_max_total_size:
                    raise HTTPException(
                        status_code=400,
                        detail=f"This request is too large for the /timeseries/bbox endpoint for this dataset. "
                        f"Try again with either a smaller AOI or fewer time steps than {len(query)}",
                    )

            logging.info(
                f"Initial memory usage: {process.memory_info().rss / 1024 / 1024} MB"
            )

            path_params = {
                "minx": minx,
                "miny": miny,
                "maxx": maxx,
                "maxy": maxy,
            }

            if not is_xarray and image_params.height and image_params.width:
                path_params["height"] = image_params.height
                path_params["width"] = image_params.width

            urls = build_request_urls(
                base_url=str(
                    factory.url_for(
                        request,
                        "bbox_image",
                        format="png",
                        **path_params,
                    )
                ),
                request=request,
                param_list=query,
            )

            logger.info(f"generated {len(urls)} request urls")

            timestep_requests = await asyncio.gather(
                *[timestep_request(url, method="GET", timeout=None) for url in urls]
            )

            logging.info(f"Time to fetch PNGs: {time() - start_time:.2f}s")
            logging.info(
                f"Memory after fetching: {process.memory_info().rss / 1024 / 1024} MB"
            )
            logging.info(f"Number of PNG responses: {len(timestep_requests)}")

            convert_start_time = time()
            pngs = []
            for r in timestep_requests:
                if r.status_code == 200:
                    pngs.append(Image.open(io.BytesIO(r.content)))
                elif r.status_code == 204:
                    continue
                else:
                    r.raise_for_status()

            logging.info(f"Time to convert to PIL: {time() - convert_start_time:.2f}s")
            logging.info(
                f"Memory after PIL conversion: {process.memory_info().rss / 1024 / 1024} MB"
            )
            logging.info(
                f"First image dimensions: {pngs[0].size if pngs else 'No images'}"
            )

            logging.info(f"Starting GIF creation with {len(pngs)} frames")
            gif_start = time()

            gif_bytes = io.BytesIO()

            pngs[0].save(
                gif_bytes,
                format="GIF",
                save_all=True,
                append_images=pngs[1:],
                loop=0,
                duration=1000 // fps,
            )

            gif_bytes.seek(0)

            logging.info(f"Time to create GIF: {time() - gif_start:.2f}s")
            logging.info(f"Total time: {time() - start_time:.2f}s")
            logging.info(
                f"Final memory usage: {process.memory_info().rss / 1024 / 1024} MB"
            )

            return StreamingResponse(gif_bytes, media_type=TimeseriesMediaType.gif)
