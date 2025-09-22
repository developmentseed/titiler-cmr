"""Timeseries extension for titiler.cmr

The /timeseries endpoints provide an API for retrieving data for a timeseries that
would otherwise need to be sent as a set of independent requests.

The /timeseries endpoints follow this basic pattern to assemble results for a timeseries:
- The 'datetime' parameter (required) is combined with the optional 'step'
  and 'temporal_mode' parameters to produce a list of specific datetime parameters
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

import earthaccess
import httpx
import psutil
from attrs import define
from fastapi import Body, Depends, Path, Query, Request, Response
from fastapi.exceptions import HTTPException
from fastapi.responses import StreamingResponse
from geojson_pydantic import Feature, FeatureCollection
from geojson_pydantic.geometries import Geometry
from isodate import parse_duration
from PIL import Image
from pydantic import BaseModel

from titiler.cmr.dependencies import ConceptID
from titiler.cmr.errors import InvalidDatetime
from titiler.cmr.factory import Endpoints
from titiler.cmr.logger import logger
from titiler.cmr.settings import ApiSettings
from titiler.cmr.utils import (
    calculate_time_series_request_size,
    get_geojson_bounds,
    parse_datetime,
)
from titiler.core.algorithm import algorithms as available_algorithms
from titiler.core.dependencies import CoordCRSParams, DefaultDependency, DstCRSParams
from titiler.core.factory import FactoryExtension
from titiler.core.models.mapbox import TileJSON
from titiler.core.models.responses import Statistics
from titiler.core.resources.enums import ImageType
from titiler.core.resources.responses import GeoJSONResponse

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

    datetime: Annotated[
        str,
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
    ]
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
    use_sel_for_datetime: Annotated[
        bool,
        Query(
            description="When True and using xarray backend, datetime values will be converted to sel parameters for within-granule temporal selection. "
            "This allows time series analysis on datasets with multiple timesteps per granule (e.g., annual files with monthly data)."
        ),
    ] = False
    sel_time_dim: Annotated[
        str,
        Query(
            description="Name of the time dimension in the xarray dataset for sel-based temporal selection. Only used when use_sel_for_datetime=True."
        ),
    ] = "time"


timeseries_field_names = [field.name for field in fields(TimeseriesParams)]


def generate_datetime_ranges(
    start_datetime: datetime,
    end_datetime: datetime,
    step: str,
    temporal_mode: Literal[
        TemporalMode.interval, TemporalMode.point
    ] = TemporalMode.interval,
) -> List[Union[Tuple[datetime], Tuple[datetime, datetime]]]:
    """Generate datetime ranges"""
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


def convert_datetime_to_sel(datetime_str: str, time_dim: str = "time") -> str:
    """Convert a datetime string to xarray sel parameter format.

    Args:
        datetime_str: Datetime string in ISO format or interval format
        time_dim: Name of the time dimension in the xarray dataset

    Returns:
        String in format "time=2018-01-01T00:00:00" for use in sel parameter
    """
    if "/" in datetime_str:
        # For intervals, use the start datetime for selection
        start_str = datetime_str.split("/")[0]
        return f"{time_dim}={start_str}"
    else:
        # Single datetime
        return f"{time_dim}={datetime_str}"


def build_request_urls(
    base_url: str,
    request: Request,
    param_list: List[BaseModel],
    use_sel_for_datetime: bool = False,
    sel_time_dim: str = "time",
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
            (str(key), str(value)) for key, value in _params.model_dump().items()
        ]

        # Start with existing parameters
        all_params = non_timeseries_params + model_params

        # If using sel for datetime, add the datetime sel parameter
        if use_sel_for_datetime and hasattr(_params, "datetime"):
            sel_param = convert_datetime_to_sel(_params.datetime, sel_time_dim)
            all_params.append(("sel", sel_param))

            # Only add sel_method if it's not already present in the request
            has_sel_method = any(key == "sel_method" for key, _ in all_params)
            if not has_sel_method:
                all_params.append(("sel_method", "nearest"))

        url = f"{base_url}?{urlencode(all_params, doseq=True)}"
        urls.append(url)

    logger.info(f"example request url: {urls[0]}")

    return urls


async def timestep_request(
    url: str, method: Literal["POST", "GET"], **kwargs
) -> httpx.Response:
    """Asyncronously send a GET or POST request to a URL with additional parameters"""
    async with httpx.AsyncClient() as client:
        if method == "POST":
            _method = client.post
        elif method == "GET":
            _method = client.get
        else:
            raise ValueError(f"{method} must be one of GET or POST")

        response = await _method(url, **kwargs)

        return response


# The rest is titiler-cmr specific
class CMRQueryParameters(BaseModel):
    """parameters for CMR queries"""

    concept_id: str
    datetime: str


TimeseriesCMRQueryParameters = List[CMRQueryParameters]


def timeseries_cmr_query(
    concept_id: ConceptID,
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
    datetime_inputs = timeseries_params.datetime.split(",")

    datetime_params = []

    for datetime_input in datetime_inputs:
        try:
            datetime_, start, end = parse_datetime(datetime_input)
        except InvalidDatetime as e:
            raise HTTPException(
                status_code=400,
                detail=f"{timeseries_params.datetime} is an invalid datetime input",
            ) from e

        if datetime_:
            datetime_params.append(datetime_.isoformat())

        elif start and timeseries_params.step:
            datetime_ranges = generate_datetime_ranges(
                start_datetime=start,
                end_datetime=end or datetime.now(tz=timezone.utc),
                step=timeseries_params.step,
                temporal_mode=timeseries_params.temporal_mode,
            )

            datetime_params.extend(
                [
                    "/".join([t.isoformat() for t in datetime_range])
                    for datetime_range in datetime_ranges
                ]
            )

        # if a start (and possibly end) are provided but no step, query CMR to identify unique
        # datetimes from a granule search
        elif start and not timeseries_params.step:
            # query CMR for this concept id and the full date range, return exact datetime intervals
            # for all granules returned by the search
            search_params: Dict[str, Any] = {"temporal": (start, end)}

            # add bounding box filter if provided
            if minx and miny and maxx and maxy:
                bbox = (minx, miny, maxx, maxy)
                search_params["bounding_box"] = bbox

            try:
                granules = earthaccess.search_data(
                    concept_id=concept_id,
                    **search_params,
                )
            # if there are no results we get an IndexError which we should just treat as an empty list
            except IndexError:
                return []

            for granule in granules:
                temporal_extent = granule["umm"]["TemporalExtent"]["RangeDateTime"]
                start = datetime.fromisoformat(
                    temporal_extent["BeginningDateTime"].replace("Z", "+00:00")
                )
                end = datetime.fromisoformat(
                    temporal_extent["EndingDateTime"].replace("Z", "+00:00")
                )
                midpoint = start + (end - start) / 2
                datetime_params.append(midpoint.isoformat())

        else:
            raise HTTPException(
                status_code=400,
                detail="you must provide a datetime interval with a defined start time or a "
                "list of comma-separated datetime strings",
            )

    if len(datetime_params) > settings.time_series_max_requests:
        raise HTTPException(
            status_code=400,
            detail=f"this request ({len(datetime_params)}) exceeds the maximum number of distinct "
            f"time series points/intervals of {settings.time_series_max_requests}",
        )

    return [
        CMRQueryParameters(
            concept_id=concept_id,
            datetime=datetime_,
        )
        for datetime_ in datetime_params
    ]


def timeseries_cmr_query_no_bbox(
    concept_id: ConceptID,
    timeseries_params=Depends(TimeseriesParams),
) -> List[CMRQueryParameters]:
    """Timeseries query but without bbox as a parameter.

    Needed this because FastAPI would expect bbox in the POST request body for
    the /timeseries/statistics endpoint when using Depends(timeseries_query)
    """
    return timeseries_cmr_query(
        concept_id, timeseries_params, minx=None, miny=None, maxx=None, maxy=None
    )


@define
class TimeseriesExtension(FactoryExtension):
    """Timeseries extension"""

    def register(self, factory: Endpoints):
        """Register timeseries endpoints to the MosaicTilerFactory"""
        self.register_statistics(factory=factory)
        self.register_tilejson(factory=factory)
        self.register_images(factory=factory)

        @factory.router.get(
            "/timeseries",
            response_model=TimeseriesCMRQueryParameters,
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
            logger.info("generating timeseries parameters")
            return query

    def register_statistics(self, factory: Endpoints):
        """Register timeseries statistics endpoint"""

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
            timeseries_params=Depends(TimeseriesParams),
            coord_crs=Depends(CoordCRSParams),
            dst_crs=Depends(DstCRSParams),
            rasterio_params=Depends(factory.rasterio_dependency),
            xarray_io_params=Depends(factory.xarray_io_params),
            xarray_ds_params=Depends(factory.xarray_ds_params),
            reader_params=Depends(factory.reader_dependency),
            post_process=Depends(factory.process_dependency),
            stats_params=Depends(factory.stats_dependency),
            histogram_params=Depends(factory.histogram_dependency),
            image_params=Depends(factory.img_part_dependency),
        ):
            """For all points/intervals along a timeseries, calculate summary statistics
            for the pixels that intersect a GeoJSON feature.
            """
            # Validate parameter combinations
            if (
                timeseries_params.use_sel_for_datetime
                and reader_params.backend != "xarray"
            ):
                raise HTTPException(
                    status_code=400,
                    detail="use_sel_for_datetime=True requires backend=xarray",
                )

            start_time = time()
            process = psutil.Process(os.getpid())
            logging.info("Checking size of time series request")

            # check for unconstrained image reading operations
            if reader_params.backend == "xarray" or (
                not image_params.max_size
                or not (image_params.height and image_params.width)
            ):
                # get bbox for geojson:
                minx, miny, maxx, maxy = get_geojson_bounds(geojson)

                request_size = calculate_time_series_request_size(
                    concept_id=request.query_params["concept_id"],
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
                use_sel_for_datetime=timeseries_params.use_sel_for_datetime,
                sel_time_dim=timeseries_params.sel_time_dim,
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
            datetime_strs = [d.datetime for d in query]
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

    def register_tilejson(self, factory: Endpoints):
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
            timeseries_params=Depends(TimeseriesParams),
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
            xarray_io_params=Depends(factory.xarray_io_params),
            xarray_ds_params=Depends(factory.xarray_ds_params),
            rasterio_params=Depends(factory.rasterio_dependency),
            reader_params=Depends(factory.reader_dependency),
            post_process=Depends(available_algorithms.dependency),
            colormap=Depends(factory.colormap_dependency),
            render_params=Depends(factory.render_dependency),
        ) -> TimeseriesTileJSON:
            """Get a set of tilejsons for all points/intervals along a timeseries."""
            # Validate parameter combinations
            if (
                timeseries_params.use_sel_for_datetime
                and reader_params.backend != "xarray"
            ):
                raise HTTPException(
                    status_code=400,
                    detail="use_sel_for_datetime=True requires backend=xarray",
                )

            urls = build_request_urls(
                base_url=str(
                    factory.url_for(
                        request, "tilejson_endpoint", tileMatrixSetId=tileMatrixSetId
                    )
                ),
                request=request,
                param_list=query,
                use_sel_for_datetime=timeseries_params.use_sel_for_datetime,
                sel_time_dim=timeseries_params.sel_time_dim,
            )

            timestep_requests = await asyncio.gather(
                *[timestep_request(url, method="GET") for url in urls]
            )

            results = [request.json() for request in timestep_requests]

            datetime_strs = [d.datetime for d in query]

            return dict(zip(datetime_strs, results))

    def register_images(self, factory: Endpoints):
        """Register image preview methods"""

        @factory.router.get(
            "/timeseries/bbox/{minx},{miny},{maxx},{maxy}.{format}",
            tags=["Timeseries", "Images"],
            operation_id="timeseries_gif_default_size",
            summary="Create an animation from a timeseries of PNGs (default size)",
            **timeseries_img_endpoint_params,
        )
        @factory.router.get(
            "/timeseries/bbox/{minx},{miny},{maxx},{maxy}/{width}x{height}.{format}",
            tags=["Timeseries", "Images"],
            operation_id="timeseries_gif_custom_size",
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
            timeseries_params=Depends(TimeseriesParams),
            fps: Annotated[
                int,
                Query(gt=1, description="Frames per second for the gif"),
            ] = 10,
            coord_crs=Depends(CoordCRSParams),
            dst_crs=Depends(DstCRSParams),
            rasterio_params=Depends(factory.rasterio_dependency),
            xarray_io_params=Depends(factory.xarray_io_params),
            xarray_ds_params=Depends(factory.xarray_ds_params),
            reader_params=Depends(factory.reader_dependency),
            post_process=Depends(factory.process_dependency),
            image_params=Depends(factory.img_part_dependency),
            colormap=Depends(factory.colormap_dependency),
            render_params=Depends(factory.render_dependency),
        ):
            """Create an animation along a timeseries for a bbox.

            Currently only the `GIF` format is supported but `MP4` is on the roadmap.
            """
            # Validate parameter combinations
            if (
                timeseries_params.use_sel_for_datetime
                and reader_params.backend != "xarray"
            ):
                raise HTTPException(
                    status_code=400,
                    detail="use_sel_for_datetime=True requires backend=xarray",
                )

            start_time = time()
            process = psutil.Process(os.getpid())

            # check for unconstrained image reading operations
            if reader_params.backend == "xarray" or (
                not image_params.max_size
                or not (image_params.height and image_params.width)
            ):
                request_size = calculate_time_series_request_size(
                    concept_id=request.query_params["concept_id"],
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

            if (
                reader_params.backend == "rasterio"
                and image_params.height
                and image_params.width
            ):
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
                use_sel_for_datetime=timeseries_params.use_sel_for_datetime,
                sel_time_dim=timeseries_params.sel_time_dim,
            )

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
