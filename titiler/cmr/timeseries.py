"""Timeseries extension for titiler.cmr"""

import asyncio
import io
import re
from dataclasses import dataclass, fields
from datetime import datetime, timedelta
from enum import Enum
from types import DynamicClassAttribute
from typing import Annotated, Any, Dict, List, Literal, Optional, Tuple, Union
from urllib.parse import urlencode

import earthaccess
import httpx
from attrs import define
from dateutil.relativedelta import relativedelta
from fastapi import Body, Depends, Path, Query, Request, Response
from fastapi.exceptions import HTTPException
from fastapi.responses import StreamingResponse
from geojson_pydantic import Feature, FeatureCollection
from geojson_pydantic.geometries import Geometry
from PIL import Image
from pydantic import BaseModel

from titiler.cmr.dependencies import ConceptID
from titiler.cmr.factory import Endpoints
from titiler.core.algorithm import algorithms as available_algorithms
from titiler.core.dependencies import CoordCRSParams, DefaultDependency, DstCRSParams
from titiler.core.factory import FactoryExtension
from titiler.core.models.mapbox import TileJSON
from titiler.core.models.responses import Statistics
from titiler.core.resources.enums import ImageType
from titiler.core.resources.responses import GeoJSONResponse

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


@dataclass
class TimeseriesParams(DefaultDependency):
    """Timeseries parameters"""

    start_datetime: Annotated[
        Optional[str],
        Query(
            description="Start datetime for timeseries request",
        ),
    ] = None
    end_datetime: Annotated[
        Optional[str],
        Query(
            description="End datetime for timeseries request",
        ),
    ] = None
    step: Annotated[
        Optional[str],
        Query(
            description="Time step between timeseries intervals, expressed as [ISO 8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations)"
        ),
    ] = None
    step_idx: Annotated[
        Optional[int],
        Query(description="Optional (zero-indexed) index of the desired time step"),
    ] = None
    exact: Annotated[
        Optional[bool],
        Query(
            description="If true, queries will be made for a point-in-time at each step. If false, queries will be made for the entire interval between steps"
        ),
    ] = None
    datetimes: Annotated[
        Optional[str],
        Query(
            description="Optional list of comma-separated specific time points or time intervals to summarize over"
        ),
    ] = None


timeseries_field_names = [field.name for field in fields(TimeseriesParams)]


def parse_duration(duration: str) -> relativedelta:
    """Parse ISO 8601 duration string to relativedelta."""
    match = re.match(
        r"P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?",
        duration,
    )
    if not match or not any(m for m in match.groups()):
        raise ValueError(f"{duration} is an invalid duration format")

    years, months, weeks, days, hours, minutes, seconds = [
        float(g) if g else 0 for g in match.groups()
    ]
    return relativedelta(
        years=int(years),
        months=int(months),
        weeks=int(weeks),
        days=int(days),
        hours=int(hours),
        minutes=int(minutes),
        seconds=int(seconds),
        microseconds=int((seconds % 1) * 1e6),
    )


def generate_datetime_ranges(
    start_datetime: datetime, end_datetime: datetime, step: str, exact: bool = False
) -> List[Union[Tuple[datetime], Tuple[datetime, datetime]]]:
    """Generate datetime ranges"""
    start = start_datetime
    end = end_datetime
    step_delta = parse_duration(step)

    ranges: List[Union[Tuple[datetime], Tuple[datetime, datetime]]] = []
    current = start

    step_timedelta = (current + step_delta) - current
    is_small_timestep = step_timedelta <= timedelta(seconds=1)

    while current < end:
        if exact:
            # For exact case, return a tuple with just one exact datetime
            next_step = current + step_delta
            ranges.append((current,))
        else:
            next_step = min(current + step_delta, end)
            if next_step == end:
                ranges.append((current, next_step))
                break

            if is_small_timestep:
                # Subtract 1 millisecond for small timesteps
                ranges.append((current, next_step - timedelta(microseconds=1)))
            else:
                # Subtract 1 second for larger timesteps
                ranges.append((current, next_step - timedelta(seconds=1)))

        current = next_step

        if current == end:
            ranges.append((end,))

    if not ranges:
        return [(start, end)]

    return ranges


def build_request_urls(
    base_url: str,
    request: Request,
    param_list: List[BaseModel],
):
    """Build lower-level request URLs from a base_url, a request, and a list of
    additional query parameters
    """
    urls = []
    non_timeseries_params = {
        key: value
        for key, value in request.query_params.items()
        if key not in timeseries_field_names
    }
    for _params in param_list:
        request_params = {
            **non_timeseries_params,
            **_params.model_dump(),
        }
        url = f"{base_url}?{urlencode(request_params)}"
        urls.append(url)

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


class CMRQueryParameters(BaseModel):
    """parameters for CMR queries"""

    concept_id: str
    datetime: str


TimeseriesCMRQueryParameters = List[CMRQueryParameters]


def timeseries_query(
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
    # if a comma-separated list of datetimes is provided, just use those
    if timeseries_params.datetimes:
        datetime_params = timeseries_params.datetimes.split(",")

    # if a start, end, and step are provided use the generate_datetime_ranges function
    elif (
        timeseries_params.start_datetime
        and timeseries_params.end_datetime
        and timeseries_params.step
    ):
        datetime_ranges = generate_datetime_ranges(
            start_datetime=datetime.fromisoformat(
                timeseries_params.start_datetime.replace("Z", "+00:00")
            ),
            end_datetime=datetime.fromisoformat(
                timeseries_params.end_datetime.replace("Z", "+00:00")
            ),
            step=timeseries_params.step,
            exact=timeseries_params.exact
            if timeseries_params.exact is not None
            else False,
        )

        datetime_params = [
            "/".join([t.isoformat() for t in datetime_range])
            for datetime_range in datetime_ranges
        ]

    # if a start and end are provided but no step, query CMR to identify unique datetimes from
    # a granule search
    elif (
        timeseries_params.start_datetime
        and timeseries_params.end_datetime
        and not timeseries_params.step
    ):
        # query CMR for this concept id and the full date range, return exact datetime intervals
        # for all granules returned by the search
        search_params: Dict[str, Any] = {
            "temporal": (
                timeseries_params.start_datetime,
                timeseries_params.end_datetime,
            )
        }

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

        print(
            f"found {len(granules)} granules between "
            f"{timeseries_params.start_datetime} and {timeseries_params.end_datetime}"
        )
        datetime_params = []
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
            detail="you must provide at least start_datetime and end_datetime, or specific datetimes",
        )

    return [
        CMRQueryParameters(
            concept_id=concept_id,
            datetime=datetime_,
        )
        for datetime_ in datetime_params
    ]


def timeseries_query_no_bbox(
    concept_id: ConceptID,
    timeseries_params=Depends(TimeseriesParams),
) -> List[CMRQueryParameters]:
    """Timeseries query but without bbox as a parameter.

    Needed this because FastAPI would expect bbox in the POST request body for
    the /timeseries/statistics endpoint when using Depends(timeseries_query)
    """
    return timeseries_query(
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
            query=Depends(timeseries_query),
        ):
            return query

    def register_statistics(self, factory: Endpoints):
        """Register timeseries statistics endpoint"""

        @factory.router.post(
            "/timeseries/statistics",
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
            query=Depends(timeseries_query_no_bbox),
            coord_crs=Depends(CoordCRSParams),
            dst_crs=Depends(DstCRSParams),
            rasterio_params=Depends(factory.rasterio_dependency),
            zarr_params=Depends(factory.zarr_dependency),
            reader_params=Depends(factory.reader_dependency),
            post_process=Depends(factory.process_dependency),
            stats_params=Depends(factory.stats_dependency),
            histogram_params=Depends(factory.histogram_dependency),
            image_params=Depends(factory.img_part_dependency),
        ):
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

            datetime_strs = [d.datetime for d in query]
            geojson.properties["statistics"] = {}
            for r, datetime_str in zip(timestep_requests, datetime_strs):
                if r.status_code == 200:
                    geojson.properties["statistics"][datetime_str] = r.json()[
                        "properties"
                    ]["statistics"]

            return geojson

    def register_tilejson(self, factory: Endpoints):
        """Register tilejson timeseries endpoint"""

        @factory.router.get(
            "/timeseries/{tileMatrixSetId}/tilejson.json",
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
            query=Depends(timeseries_query),
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
            zarr_params=Depends(factory.zarr_dependency),
            rasterio_params=Depends(factory.rasterio_dependency),
            reader_params=Depends(factory.reader_dependency),
            post_process=Depends(available_algorithms.dependency),
            rescale=Depends(factory.rescale_dependency),
            color_formula=Depends(factory.color_formula_dependency),
            colormap=Depends(factory.colormap_dependency),
            render_params=Depends(factory.render_dependency),
        ) -> TimeseriesTileJSON:
            urls = build_request_urls(
                base_url=str(
                    factory.url_for(
                        request, "tilejson_endpoint", tileMatrixSetId=tileMatrixSetId
                    )
                ),
                request=request,
                param_list=query,
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
            tags=["Timeseries", "images"],
            **timeseries_img_endpoint_params,
        )
        @factory.router.get(
            "/timeseries/bbox/{minx},{miny},{maxx},{maxy}/{width}x{height}.{format}",
            tags=["Timeseries", "images"],
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
            query=Depends(timeseries_query),
            fps: Annotated[
                int,
                Query(gt=1, description="Frames per second for the gif"),
            ] = 10,
            coord_crs=Depends(CoordCRSParams),
            dst_crs=Depends(DstCRSParams),
            rasterio_params=Depends(factory.rasterio_dependency),
            zarr_params=Depends(factory.zarr_dependency),
            reader_params=Depends(factory.reader_dependency),
            post_process=Depends(factory.process_dependency),
            image_params=Depends(factory.img_part_dependency),
            rescale=Depends(factory.rescale_dependency),
            color_formula=Depends(factory.color_formula_dependency),
            colormap=Depends(factory.colormap_dependency),
            render_params=Depends(factory.render_dependency),
        ):
            """Create image from a bbox."""
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
            )

            timestep_requests = await asyncio.gather(
                *[timestep_request(url, method="GET", timeout=None) for url in urls]
            )

            pngs = []
            for r in timestep_requests:
                if r.status_code == 200:
                    pngs.append(Image.open(io.BytesIO(r.content)))
                elif r.status_code == 204:
                    continue
                else:
                    r.raise_for_status()

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

            return StreamingResponse(gif_bytes, media_type=TimeseriesMediaType.gif)
