"""Timeseries extension for titiler.cmr"""

import asyncio
import io
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from types import DynamicClassAttribute
from typing import Annotated, Any, Dict, List, Literal, Optional, Tuple
from urllib.parse import urlencode

import httpx
from attrs import define
from dateutil.relativedelta import relativedelta
from fastapi import Body, Depends, Path, Query, Request, Response
from fastapi.exceptions import HTTPException
from fastapi.responses import StreamingResponse
from geojson_pydantic import Feature
from PIL import Image
from pydantic import BaseModel

from titiler.cmr.dependencies import ConceptID
from titiler.cmr.factory import Endpoints
from titiler.core.algorithm import algorithms as available_algorithms
from titiler.core.dependencies import CoordCRSParams, DefaultDependency, DstCRSParams
from titiler.core.factory import FactoryExtension
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
    """Available Output image type."""

    gif = "gif"

    @DynamicClassAttribute
    def mediatype(self):
        """Return image media type."""
        return TimeseriesMediaType[self._name_].value


@dataclass
class TimeseriesParams(DefaultDependency):
    """Timeseries parameters"""

    start_datetime: Annotated[
        Optional[str],
        Query(
            description="Start datetime for timeseries request",
        ),
    ]
    end_datetime: Annotated[
        Optional[str],
        Query(
            description="End datetime for timeseries request",
        ),
    ]
    step: Annotated[
        Optional[str],
        Query(
            description="Time step across which items/granules will be aggregated, expressed as [ISO 8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations)"
        ),
    ]
    step_idx: Annotated[
        Optional[int],
        Query(description="Optional (zero-indexed) index of the desired time step"),
    ] = None
    # intervals: Annotated[
    #     Optional[List[List[str]]],
    #     Query(
    #         description="Optional list of specific time points or time intervals to summarize over"
    #     ),
    # ] = None


def parse_duration(duration: str) -> relativedelta:
    """Parse ISO 8601 duration string to relativedelta."""
    match = re.match(
        r"P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?",
        duration,
    )
    if not match:
        raise ValueError("Invalid duration format")

    years, months, days, hours, minutes, seconds = [
        float(g) if g else 0 for g in match.groups()
    ]
    return relativedelta(
        years=int(years),
        months=int(months),
        days=int(days),
        hours=int(hours),
        minutes=int(minutes),
        seconds=int(seconds),
        microseconds=int((seconds % 1) * 1e6),
    )


def generate_datetime_ranges(
    start_datetime: datetime, end_datetime: datetime, step: str
) -> List[Tuple[datetime, datetime]]:
    """Generate datetime ranges"""
    start = start_datetime
    end = end_datetime
    step_delta = parse_duration(step)

    ranges = []
    current = start

    step_timedelta = (current + step_delta) - current
    is_small_timestep = step_timedelta <= timedelta(seconds=1)

    while current < end:
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

    if not ranges:
        return [(start, end)]

    return ranges


def timeseries_query(
    concept_id: ConceptID,
    timeseries_params=Depends(TimeseriesParams),
) -> List[Dict[str, str]]:
    """Convert a timeseries query into a set of CMR queries"""
    # Validate and process timeseries parameters
    if (
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
        )
    else:
        raise HTTPException(
            status_code=400,
            detail="you must provide start_datetime, end_datetime, and step",
        )

    datetime_params = [
        "/".join([start.isoformat(), end.isoformat()]) for start, end in datetime_ranges
    ]

    return [
        {
            "datetime": datetime_,
            "concept_id": concept_id,
        }
        for datetime_ in datetime_params
    ]


async def timestep_request(
    url: str, method: Literal["POST", "GET"], **kwargs
) -> httpx.Response:
    """Asyncronously send a POST request to a URL with additional parameters"""
    async with httpx.AsyncClient() as client:
        if method == "POST":
            _method = client.post
        elif method == "GET":
            _method = client.get
        else:
            raise ValueError(f"{method} must be one of GET or POST")

        response = await _method(url, **kwargs)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)

        return response


class TimeseriesTileJSON(BaseModel):
    """TileJSONs for a timeseries"""

    timeseries_tilejsons: Dict[str, Dict[str, Any]]


@define
class TimeseriesExtension(FactoryExtension):
    """Timeseries extension"""

    def register(self, factory: Endpoints):
        """Register timeseries endpoints to the MosaicTilerFactory"""

        @factory.router.post(
            "/timeseries/statistics",
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
                Feature,
                Body(description="GeoJSON Feature or FeatureCollection."),
            ],
            query=Depends(timeseries_query),
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
            # Construct the base URL for the original endpoint
            base_url = str(factory.url_for(request, "geojson_statistics"))

            # Create a list of URLs for each time interval
            urls = []
            for timeseries_query_params in query:
                url = f"{base_url}?{urlencode({**request.query_params, **timeseries_query_params})}"
                urls.append(url)

            # Fetch all URLs concurrently
            timestep_requests = await asyncio.gather(
                *[
                    timestep_request(
                        url, method="POST", json=geojson.model_dump(exclude_none=True)
                    )
                    for url in urls
                ]
            )

            results = [
                request.json()["properties"]["statistics"]
                for request in timestep_requests
            ]

            # Combine results into a single response
            datetime_strs = [d["datetime"] for d in query]

            geojson.properties["statistics"] = dict(zip(datetime_strs, results))

            return geojson

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
            query=Depends(timeseries_query),
        ) -> TimeseriesTileJSON:
            # Construct the base URL for the original endpoint
            base_url = str(
                factory.url_for(
                    request, "tilejson_endpoint", tileMatrixSetId=tileMatrixSetId
                )
            )

            # Create a list of URLs for each time interval
            urls = []
            for timeseries_query_params in query:
                url = f"{base_url}?{urlencode({**request.query_params, **timeseries_query_params})}"
                urls.append(url)

            # Fetch all URLs concurrently
            timestep_requests = await asyncio.gather(
                *[timestep_request(url, method="GET") for url in urls]
            )

            results = [request.json() for request in timestep_requests]

            # Combine results into a single response
            datetime_strs = [d["datetime"] for d in query]

            return TimeseriesTileJSON(
                timeseries_tilejsons=dict(zip(datetime_strs, results))
            )

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
            ] = None,
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
            # Construct the base URL for the original endpoint
            base_url = str(
                factory.url_for(
                    request,
                    "bbox_image",
                    minx=minx,
                    miny=miny,
                    maxx=maxx,
                    maxy=maxy,
                    width=image_params.width,
                    height=image_params.height,
                    format="png",
                )
            )

            # Create a list of URLs for each time interval
            urls = []
            for timeseries_query_params in query:
                url = f"{base_url}?{urlencode({**request.query_params, **timeseries_query_params})}"
                urls.append(url)

            # Fetch all URLs concurrently
            timestep_requests = await asyncio.gather(
                *[timestep_request(url, method="GET", timeout=60) for url in urls]
            )

            pngs = [
                Image.open(io.BytesIO(request.content)) for request in timestep_requests
            ]

            # Create a BytesIO object to hold the GIF
            gif_bytes = io.BytesIO()

            # Save images as a GIF
            pngs[0].save(
                gif_bytes,
                format="GIF",
                save_all=True,
                append_images=pngs[1:],
                loop=0,
                duration=1000 // fps,
            )

            # Seek to the start
            gif_bytes.seek(0)

            # Create a streaming response to return the gif
            return StreamingResponse(gif_bytes, media_type=TimeseriesMediaType.gif)
