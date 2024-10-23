"""titiler.cmr.factory: router factories."""

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional, Tuple, Type, Union
from urllib.parse import urlencode

import jinja2
import numpy
import orjson
from fastapi import Body, Depends, Path, Query
from fastapi.responses import ORJSONResponse
from geojson_pydantic import Feature, FeatureCollection
from morecantile.defaults import TileMatrixSets
from morecantile.defaults import tms as default_tms
from pydantic import conint
from rio_tiler.constants import MAX_THREADS, WGS84_CRS
from rio_tiler.io import BaseReader, rasterio
from starlette.requests import Request
from starlette.responses import HTMLResponse, Response
from starlette.routing import compile_path, replace_params
from starlette.templating import Jinja2Templates, _TemplateResponse
from typing_extensions import Annotated

from titiler.cmr import models
from titiler.cmr.backend import CMRBackend
from titiler.cmr.dependencies import (
    OutputType,
    RasterioParams,
    ReaderParams,
    ZarrParams,
    cmr_query,
)
from titiler.cmr.enums import MediaType
from titiler.cmr.reader import MultiFilesBandsReader, ZarrReader
from titiler.core.algorithm import algorithms as available_algorithms
from titiler.core.dependencies import (
    CoordCRSParams,
    DefaultDependency,
    DstCRSParams,
    HistogramParams,
    PartFeatureParams,
    StatisticsParams,
)
from titiler.core.factory import BaseTilerFactory, img_endpoint_params
from titiler.core.models.mapbox import TileJSON
from titiler.core.models.responses import MultiBaseStatisticsGeoJSON
from titiler.core.resources.enums import ImageType, OptionalHeader
from titiler.core.resources.responses import GeoJSONResponse
from titiler.core.utils import render_image

jinja2_env = jinja2.Environment(
    loader=jinja2.ChoiceLoader([jinja2.PackageLoader(__package__, "templates")])
)
DEFAULT_TEMPLATES = Jinja2Templates(env=jinja2_env)

MOSAIC_THREADS = int(os.getenv("MOSAIC_CONCURRENCY", MAX_THREADS))


def create_html_response(
    request: Request,
    data: str,
    templates: Jinja2Templates,
    template_name: str,
    router_prefix: Optional[str] = None,
) -> _TemplateResponse:
    """Create Template response."""
    urlpath = request.url.path
    if root_path := request.app.root_path:
        urlpath = re.sub(r"^" + root_path, "", urlpath)

    crumbs = []
    baseurl = str(request.base_url).rstrip("/")

    crumbpath = str(baseurl)
    for crumb in urlpath.split("/"):
        crumbpath = crumbpath.rstrip("/")
        part = crumb
        if part is None or part == "":
            part = "Home"
        crumbpath += f"/{crumb}"
        crumbs.append({"url": crumbpath.rstrip("/"), "part": part.capitalize()})

    if router_prefix:
        baseurl += router_prefix

    return templates.TemplateResponse(
        request=request,
        name=f"{template_name}.html",
        context={
            "response": orjson.loads(data),
            "template": {
                "api_root": baseurl,
                "params": request.query_params,
                "title": "",
            },
            "crumbs": crumbs,
            "url": str(request.url),
            "baseurl": baseurl,
            "urlpath": str(request.url.path),
            "urlparams": str(request.url.query),
        },
    )


def parse_reader_options(
    rasterio_params: RasterioParams,
    zarr_params: ZarrParams,
    reader_params: ReaderParams,
) -> Tuple[Type[BaseReader], Dict[str, Any], Dict[str, Any]]:
    """Convert rasterio and zarr parameters into a reader and a set of reader_options and read_options"""

    read_options: Dict[str, Any]
    reader_options: Dict[str, Any]
    options: Dict[str, Any]
    reader: Type[BaseReader]

    resampling_method = rasterio_params.resampling_method or "nearest"

    if reader_params.backend == "xarray":
        reader = ZarrReader
        read_options = {}

        options = {
            "variable": zarr_params.variable,
            "decode_times": zarr_params.decode_times,
            "drop_dim": zarr_params.drop_dim,
            "time_slice": zarr_params.time_slice,
        }
        reader_options = {k: v for k, v in options.items() if v is not None}
    else:
        if rasterio_params.bands_regex:
            assert (
                rasterio_params.bands
            ), "`bands=` option must be provided when using Multi bands collections."

            reader = MultiFilesBandsReader
            options = {
                "expression": rasterio_params.expression,
                "bands": rasterio_params.bands,
                "unscale": rasterio_params.unscale,
                "resampling_method": rasterio_params.resampling_method,
                "bands_regex": rasterio_params.bands_regex,
            }
            read_options = {k: v for k, v in options.items() if v is not None}
            reader_options = {}

        else:
            assert (
                rasterio_params.bands
            ), "Can't use `bands=` option without `bands_regex`"

            reader = rasterio.Reader
            options = {
                "indexes": rasterio_params.indexes,
                "expression": rasterio_params.expression,
                "unscale": rasterio_params.unscale,
                "resampling_method": resampling_method,
            }
            read_options = {k: v for k, v in options.items() if v is not None}
            reader_options = {}

    return reader, read_options, reader_options


@dataclass
class Endpoints(BaseTilerFactory):
    """Endpoints Factory."""

    reader: Optional[Type[BaseReader]] = field(default=None)  # type: ignore
    supported_tms: TileMatrixSets = default_tms

    zarr_dependency: Type[DefaultDependency] = ZarrParams
    rasterio_dependency: Type[DefaultDependency] = RasterioParams
    reader_dependency: Type[DefaultDependency] = ReaderParams
    stats_dependency: Type[DefaultDependency] = StatisticsParams
    histogram_dependency: Type[DefaultDependency] = HistogramParams
    img_part_dependency: Type[DefaultDependency] = PartFeatureParams

    templates: Jinja2Templates = DEFAULT_TEMPLATES

    title: str = "TiTiler-CMR"

    def _create_html_response(
        self,
        request: Request,
        data: str,
        template_name: str,
    ) -> _TemplateResponse:
        return create_html_response(
            request,
            data,
            templates=self.templates,
            template_name=template_name,
            router_prefix=self.router_prefix,
        )

    def register_routes(self):
        """Post Init: register routes."""

        self.register_landing()
        self.register_conformance()
        self.register_tilematrixsets()
        self.register_tiles()
        self.register_map()
        self.register_statistics()
        self.register_parts()

    def register_landing(self) -> None:
        """register landing page endpoint."""

        @self.router.get(
            "/",
            response_model=models.Landing,
            response_model_exclude_none=True,
            response_class=ORJSONResponse,
            responses={
                200: {
                    "content": {
                        MediaType.json.value: {},
                        MediaType.html.value: {},
                    }
                },
            },
            operation_id="getLandingPage",
            summary="landing page",
            tags=["Landing Page"],
        )
        def landing(
            request: Request,
            output_type: Annotated[Optional[MediaType], Depends(OutputType)] = None,
        ):
            """The landing page provides links to the API definition, the conformance statements and to the feature collections in this dataset."""
            data = models.Landing(
                title=self.title,
                links=[
                    models.Link(
                        title="Landing Page",
                        href=self.url_for(request, "landing"),
                        type=MediaType.html,
                        rel="self",
                    ),
                    models.Link(
                        title="the API definition (JSON)",
                        href=str(request.url_for("openapi")),
                        type=MediaType.openapi30_json,
                        rel="service-desc",
                    ),
                    models.Link(
                        title="the API documentation",
                        href=str(request.url_for("swagger_ui_html")),
                        type=MediaType.html,
                        rel="service-doc",
                    ),
                    models.Link(
                        title="Conformance",
                        href=self.url_for(request, "conformance"),
                        type=MediaType.json,
                        rel="conformance",
                    ),
                    models.Link(
                        title="TiTiler-CMR Documentation (external link)",
                        href="https://developmentseed.org/titiler-cmr/",
                        type=MediaType.html,
                        rel="doc",
                    ),
                    models.Link(
                        title="TiTiler-CMR source code (external link)",
                        href="https://github.com/developmentseed/titiler-cmr",
                        type=MediaType.html,
                        rel="doc",
                    ),
                ],
            )

            if output_type == MediaType.html:
                return self._create_html_response(
                    request,
                    data.model_dump_json(exclude_none=True),
                    template_name="landing",
                )

            return data

    def register_conformance(self) -> None:
        """Register conformance endpoint."""

        @self.router.get(
            "/conformance",
            response_model=models.Conformance,
            response_model_exclude_none=True,
            response_class=ORJSONResponse,
            responses={
                200: {
                    "content": {
                        MediaType.json.value: {},
                        MediaType.html.value: {},
                    }
                },
            },
            operation_id="getConformanceDeclaration",
            summary="information about specifications that this API conforms to",
            tags=["Conformance"],
        )
        def conformance(
            request: Request,
            output_type: Annotated[Optional[MediaType], Depends(OutputType)] = None,
        ):
            """A list of all conformance classes specified in a standard that the server conforms to."""
            data = models.Conformance(
                # TODO: Validate / Update
                conformsTo=[
                    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
                    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/landing-page",
                    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/json",
                    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/html",
                    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/oas30",
                    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/core",
                    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/oas30",
                    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tileset",
                    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tilesets-list",
                ]
            )

            if output_type == MediaType.html:
                return self._create_html_response(
                    request,
                    data.model_dump_json(exclude_none=True),
                    template_name="conformance",
                )

            return data

    def register_tilematrixsets(self):
        """Register Tiling Schemes endpoints."""

        @self.router.get(
            r"/tileMatrixSets",
            response_model=models.TileMatrixSetList,
            response_model_exclude_none=True,
            summary="retrieve the list of available tiling schemes (tile matrix sets)",
            operation_id="getTileMatrixSetsList",
            responses={
                200: {
                    "content": {
                        MediaType.html.value: {},
                        MediaType.json.value: {},
                    },
                },
            },
            tags=["Tiling Schemes"],
        )
        async def tilematrixsets(
            request: Request,
            output_type: Annotated[Optional[MediaType], Depends(OutputType)] = None,
        ):
            """Retrieve the list of available tiling schemes (tile matrix sets)."""
            data = models.TileMatrixSetList(
                tileMatrixSets=[
                    models.TileMatrixSetRef(
                        id=tms_id,
                        title=f"Definition of {tms_id} tileMatrixSets",
                        links=[
                            models.TileMatrixSetLink(
                                href=self.url_for(
                                    request,
                                    "tilematrixset",
                                    tileMatrixSetId=tms_id,
                                ),
                                rel="http://www.opengis.net/def/rel/ogc/1.0/tiling-schemes",
                                type=MediaType.json,
                            )
                        ],
                    )
                    for tms_id in self.supported_tms.list()
                ]
            )

            if output_type == MediaType.html:
                return self._create_html_response(
                    request,
                    data.model_dump_json(exclude_none=True),
                    template_name="tilematrixsets",
                )

            return data

        @self.router.get(
            "/tileMatrixSets/{tileMatrixSetId}",
            response_model=models.TileMatrixSet,
            response_model_exclude_none=True,
            summary="retrieve the definition of the specified tiling scheme (tile matrix set)",
            operation_id="getTileMatrixSet",
            responses={
                200: {
                    "content": {
                        MediaType.html.value: {},
                        MediaType.json.value: {},
                    },
                },
            },
            tags=["Tiling Schemes"],
        )
        async def tilematrixset(
            request: Request,
            tileMatrixSetId: Annotated[
                Literal[tuple(self.supported_tms.list())],
                Path(description="Identifier for a supported TileMatrixSet."),
            ],
            output_type: Annotated[Optional[MediaType], Depends(OutputType)] = None,
        ):
            """Retrieve the definition of the specified tiling scheme (tile matrix set)."""
            # Morecantile TileMatrixSet should be the same as `models.TileMatrixSet`
            tms = self.supported_tms.get(tileMatrixSetId)
            data = models.TileMatrixSet.model_validate(tms.model_dump())

            if output_type == MediaType.html:
                return self._create_html_response(
                    request,
                    # For visualization purpose we add the tms bbox
                    json.dumps(
                        {
                            **tms.model_dump(exclude_none=True, mode="json"),
                            "bbox": tms.bbox,  # morecantile attribute
                        },
                    ),
                    template_name="tilematrixset",
                )

            return data

    def register_tiles(self):  # noqa: C901
        """Register tileset endpoints."""

        @self.router.get(
            "/tiles/{tileMatrixSetId}/{z}/{x}/{y}",
            **img_endpoint_params,
            tags=["Raster Tiles"],
        )
        @self.router.get(
            "/tiles/{tileMatrixSetId}/{z}/{x}/{y}.{format}",
            **img_endpoint_params,
            tags=["Raster Tiles"],
        )
        @self.router.get(
            "/tiles/{tileMatrixSetId}/{z}/{x}/{y}@{scale}x",
            **img_endpoint_params,
            tags=["Raster Tiles"],
        )
        @self.router.get(
            "/tiles/{tileMatrixSetId}/{z}/{x}/{y}@{scale}x.{format}",
            **img_endpoint_params,
            tags=["Raster Tiles"],
        )
        def tiles_endpoint(
            request: Request,
            tileMatrixSetId: Annotated[
                Literal[tuple(self.supported_tms.list())],
                Path(description="Identifier for a supported TileMatrixSet"),
            ],
            z: Annotated[
                int,
                Path(
                    description="Identifier (Z) selecting one of the scales defined in the TileMatrixSet and representing the scaleDenominator the tile.",
                ),
            ],
            x: Annotated[
                int,
                Path(
                    description="Column (X) index of the tile on the selected TileMatrix. It cannot exceed the MatrixHeight-1 for the selected TileMatrix.",
                ),
            ],
            y: Annotated[
                int,
                Path(
                    description="Row (Y) index of the tile on the selected TileMatrix. It cannot exceed the MatrixWidth-1 for the selected TileMatrix.",
                ),
            ],
            scale: Annotated[  # type: ignore
                conint(gt=0, le=4), "Tile size scale. 1=256x256, 2=512x512..."
            ] = 1,
            format: Annotated[
                Optional[ImageType],
                "Default will be automatically defined if the output image needs a mask (png) or not (jpeg).",
            ] = None,
            query=Depends(cmr_query),
            zarr_params=Depends(self.zarr_dependency),
            rasterio_params=Depends(self.rasterio_dependency),
            reader_params=Depends(self.reader_dependency),
            post_process=Depends(self.process_dependency),
            rescale=Depends(self.rescale_dependency),
            color_formula=Depends(self.color_formula_dependency),
            colormap=Depends(self.colormap_dependency),
            render_params=Depends(self.render_dependency),
        ) -> Response:
            """Create map tile from a dataset."""
            reproject_method = reader_params.reproject_method or "nearest"
            nodata = (
                (
                    numpy.nan
                    if reader_params.nodata == "nan"
                    else float(reader_params.nodata)
                )
                if reader_params.nodata
                else None
            )

            tms = self.supported_tms.get(tileMatrixSetId)

            reader, read_options, reader_options = parse_reader_options(
                rasterio_params=rasterio_params,
                zarr_params=zarr_params,
                reader_params=reader_params,
            )

            with CMRBackend(
                tms=tms,
                reader=reader,
                reader_options=reader_options,
                auth=request.app.state.cmr_auth,
            ) as src_dst:
                image, _ = src_dst.tile(
                    x,
                    y,
                    z,
                    tilesize=scale * 256,
                    cmr_query=query,
                    nodata=nodata,
                    reproject_method=reproject_method,
                    **read_options,
                )

            if post_process:
                image = post_process(image)

            if rescale:
                image.rescale(rescale)

            if color_formula:
                image.apply_color_formula(color_formula)

            content, media_type = render_image(
                image,
                output_format=format,
                colormap=colormap,
                **render_params,
            )

            return Response(content, media_type=media_type)

        @self.router.get(
            "/{tileMatrixSetId}/tilejson.json",
            response_model=TileJSON,
            responses={200: {"description": "Return a tilejson"}},
            response_model_exclude_none=True,
            tags=["TileJSON"],
        )
        def tilejson_endpoint(  # type: ignore
            request: Request,
            tileMatrixSetId: Annotated[
                Literal[tuple(self.supported_tms.list())],
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
            query=Depends(cmr_query),
            zarr_params=Depends(self.zarr_dependency),
            rasterio_params=Depends(self.rasterio_dependency),
            reader_params=Depends(self.reader_dependency),
            post_process=Depends(available_algorithms.dependency),
            rescale=Depends(self.rescale_dependency),
            color_formula=Depends(self.color_formula_dependency),
            colormap=Depends(self.colormap_dependency),
            render_params=Depends(self.render_dependency),
        ) -> Dict:
            """Return TileJSON document for a dataset."""
            route_params = {
                "z": "{z}",
                "x": "{x}",
                "y": "{y}",
                "scale": tile_scale,
                "tileMatrixSetId": tileMatrixSetId,
            }
            if tile_format:
                route_params["format"] = tile_format.value

            tiles_url = self.url_for(request, "tiles_endpoint", **route_params)

            qs_key_to_remove = [
                "tilematrixsetid",
                "tile_format",
                "tile_scale",
                "minzoom",
                "maxzoom",
            ]
            qs = [
                (key, value)
                for (key, value) in request.query_params._list
                if key.lower() not in qs_key_to_remove
            ]
            if qs:
                tiles_url += f"?{urlencode(qs)}"

            tms = self.supported_tms.get(tileMatrixSetId)

            # TODO: can we get metadata from the CMR dataset?
            with CMRBackend(
                tms=tms,
                auth=request.app.state.cmr_auth,
            ) as src_dst:
                minx, miny, maxx, maxy = zip(
                    [-180, -90, 180, 90], list(src_dst.geographic_bounds)
                )
                bounds = [max(minx), max(miny), min(maxx), min(maxy)]

                return {
                    "bounds": bounds,
                    "minzoom": minzoom if minzoom is not None else src_dst.minzoom,
                    "maxzoom": maxzoom if maxzoom is not None else src_dst.maxzoom,
                    "tiles": [tiles_url],
                }

    def register_map(self):  # noqa: C901
        """Register map endpoints."""

        @self.router.get(
            "/{tileMatrixSetId}/map",
            response_class=HTMLResponse,
            responses={200: {"description": "Return a Map document"}},
            tags=["Map"],
        )
        def map_endpoint(  # type: ignore
            request: Request,
            tileMatrixSetId: Annotated[
                Literal[tuple(self.supported_tms.list())],
                Path(description="Identifier for a supported TileMatrixSet"),
            ],
            minzoom: Annotated[
                Optional[int],
                Query(description="Overwrite default minzoom."),
            ] = None,
            maxzoom: Annotated[
                Optional[int],
                Query(description="Overwrite default maxzoom."),
            ] = None,
            query=Depends(cmr_query),
            zarr_params=Depends(self.zarr_dependency),
            rasterio_params=Depends(self.rasterio_dependency),
            reader_params=Depends(self.reader_dependency),
            ###################################################################
            # Rendering Options
            ###################################################################
            post_process=Depends(self.process_dependency),
            rescale=Depends(self.rescale_dependency),
            color_formula=Depends(self.color_formula_dependency),
            colormap=Depends(self.colormap_dependency),
            render_params=Depends(self.render_dependency),
        ) -> _TemplateResponse:
            """Return Map document."""
            tilejson_url = self.url_for(
                request,
                "tilejson_endpoint",
                tileMatrixSetId=tileMatrixSetId,
            )
            if request.query_params._list:
                tilejson_url += f"?{urlencode(request.query_params._list)}"

            tms = self.supported_tms.get(tileMatrixSetId)

            base_url = str(request.base_url).rstrip("/")
            if self.router_prefix:
                prefix = self.router_prefix.lstrip("/")
                # If we have prefix with custom path param we check and replace them with
                # the path params provided
                if "{" in prefix:
                    _, path_format, param_convertors = compile_path(prefix)
                    prefix, _ = replace_params(
                        path_format, param_convertors, request.path_params.copy()
                    )
                base_url += prefix

            return self.templates.TemplateResponse(
                request=request,
                name="map.html",
                context={
                    "tilejson_endpoint": tilejson_url,
                    "tms": tms,
                    "resolutions": [matrix.cellSize for matrix in tms],
                    "template": {
                        "api_root": base_url,
                        "params": request.query_params,
                        "title": "Map",
                    },
                },
                media_type="text/html",
            )

    def register_parts(self):  # noqa: C901
        """Register /bbox and /feature endpoint."""

        # GET endpoints
        @self.router.get(
            "/bbox/{minx},{miny},{maxx},{maxy}.{format}",
            tags=["images"],
            **img_endpoint_params,
        )
        @self.router.get(
            "/bbox/{minx},{miny},{maxx},{maxy}/{width}x{height}.{format}",
            tags=["images"],
            **img_endpoint_params,
        )
        def bbox_image(
            request: Request,
            minx: Annotated[float, Path(description="Bounding box min X")],
            miny: Annotated[float, Path(description="Bounding box min Y")],
            maxx: Annotated[float, Path(description="Bounding box max X")],
            maxy: Annotated[float, Path(description="Bounding box max Y")],
            format: Annotated[
                ImageType,
                "Default will be automatically defined if the output image needs a mask (png) or not (jpeg).",
            ] = None,
            coord_crs=Depends(CoordCRSParams),
            dst_crs=Depends(DstCRSParams),
            query=Depends(cmr_query),
            rasterio_params=Depends(self.rasterio_dependency),
            zarr_params=Depends(self.zarr_dependency),
            reader_params=Depends(self.reader_dependency),
            post_process=Depends(self.process_dependency),
            image_params=Depends(self.img_part_dependency),
            rescale=Depends(self.rescale_dependency),
            color_formula=Depends(self.color_formula_dependency),
            colormap=Depends(self.colormap_dependency),
            render_params=Depends(self.render_dependency),
        ):
            """Create image from a bbox."""
            reader, read_options, reader_options = parse_reader_options(
                rasterio_params=rasterio_params,
                zarr_params=zarr_params,
                reader_params=reader_params,
            )

            with CMRBackend(
                reader=reader,
                reader_options=reader_options,
                auth=request.app.state.cmr_auth,
            ) as src_dst:
                if reader_params.backend == "rasterio":
                    read_options.update(
                        {
                            "threads": MOSAIC_THREADS,
                            "align_bounds_with_dataset": True,
                        }
                    )

                    read_options.update(image_params)

                image, assets = src_dst.part(
                    bbox=[minx, miny, maxx, maxy],
                    cmr_query=query,
                    bounds_crs=coord_crs or WGS84_CRS,
                    dst_crs=dst_crs,
                    **read_options,
                )

                dst_colormap = getattr(src_dst, "colormap", None)

            if post_process:
                image = post_process(image)

            if rescale:
                image.rescale(rescale)

            if color_formula:
                image.apply_color_formula(color_formula)

            content, media_type = render_image(
                image,
                output_format=format,
                colormap=colormap or dst_colormap,
                **render_params,
            )

            headers: Dict[str, str] = {}
            if OptionalHeader.x_assets in self.optional_headers:
                ids = [x["id"] for x in assets]
                headers["X-Assets"] = ",".join(ids)

            return Response(content, media_type=media_type, headers=headers)

        @self.router.post(
            "/feature",
            tags=["images"],
            **img_endpoint_params,
        )
        @self.router.post(
            "/feature.{format}",
            tags=["images"],
            **img_endpoint_params,
        )
        @self.router.post(
            "/feature/{width}x{height}.{format}",
            tags=["images"],
            **img_endpoint_params,
        )
        def feature_image(
            request: Request,
            geojson: Annotated[
                Union[FeatureCollection, Feature],
                Body(description="GeoJSON Feature or FeatureCollection."),
            ],
            format: Annotated[
                ImageType,
                "Default will be automatically defined if the output image needs a mask (png) or not (jpeg).",
            ] = None,
            coord_crs=Depends(CoordCRSParams),
            dst_crs=Depends(DstCRSParams),
            query=Depends(cmr_query),
            rasterio_params=Depends(self.rasterio_dependency),
            zarr_params=Depends(self.zarr_dependency),
            reader_params=Depends(self.reader_dependency),
            post_process=Depends(self.process_dependency),
            rescale=Depends(self.rescale_dependency),
            image_params=Depends(self.img_part_dependency),
            color_formula=Depends(self.color_formula_dependency),
            colormap=Depends(self.colormap_dependency),
            render_params=Depends(self.render_dependency),
        ):
            """Create image from a geojson feature."""
            reader, read_options, reader_options = parse_reader_options(
                rasterio_params=rasterio_params,
                zarr_params=zarr_params,
                reader_params=reader_params,
            )

            with CMRBackend(
                reader=reader,
                reader_options=reader_options,
                auth=request.app.state.cmr_auth,
            ) as src_dst:
                if reader_params.backend == "rasterio":
                    read_options.update(
                        {
                            "threads": MOSAIC_THREADS,
                            "align_bounds_with_dataset": True,
                        }
                    )

                    read_options.update(image_params)

                image, assets = src_dst.feature(
                    geojson.model_dump(exclude_none=True),
                    cmr_query=query,
                    shape_crs=coord_crs or WGS84_CRS,
                    dst_crs=dst_crs,
                    **read_options,
                )

            if post_process:
                image = post_process(image)

            if rescale:
                image.rescale(rescale)

            if color_formula:
                image.apply_color_formula(color_formula)

            content, media_type = render_image(
                image,
                output_format=format,
                colormap=colormap,
                **render_params,
            )

            headers: Dict[str, str] = {}
            if OptionalHeader.x_assets in self.optional_headers:
                ids = [x["id"] for x in assets]
                headers["X-Assets"] = ",".join(ids)

            return Response(content, media_type=media_type, headers=headers)

    def register_statistics(self):
        """Register /statistics endpoint."""

        @self.router.post(
            "/statistics",
            response_model=MultiBaseStatisticsGeoJSON,
            response_model_exclude_none=True,
            response_class=GeoJSONResponse,
            responses={
                200: {
                    "content": {"application/geo+json": {}},
                    "description": "Return statistics for geojson features.",
                }
            },
            tags=["Statistics"],
        )
        def geojson_statistics(
            request: Request,
            geojson: Annotated[
                Union[FeatureCollection, Feature],
                Body(description="GeoJSON Feature or FeatureCollection."),
            ],
            query=Depends(cmr_query),
            coord_crs=Depends(CoordCRSParams),
            dst_crs=Depends(DstCRSParams),
            rasterio_params=Depends(self.rasterio_dependency),
            zarr_params=Depends(self.zarr_dependency),
            reader_params=Depends(self.reader_dependency),
            post_process=Depends(self.process_dependency),
            stats_params=Depends(self.stats_dependency),
            histogram_params=Depends(self.histogram_dependency),
            image_params=Depends(self.img_part_dependency),
        ):
            """Get Statistics from a geojson feature or featureCollection."""
            fc = geojson
            if isinstance(fc, Feature):
                fc = FeatureCollection(type="FeatureCollection", features=[geojson])

            reader, read_options, reader_options = parse_reader_options(
                rasterio_params=rasterio_params,
                zarr_params=zarr_params,
                reader_params=reader_params,
            )

            with CMRBackend(
                reader=reader,
                reader_options=reader_options,
                auth=request.app.state.cmr_auth,
            ) as src_dst:
                for feature in fc:
                    shape = feature.model_dump(exclude_none=True)

                    if reader_params.backend == "rasterio":
                        read_options.update(
                            {
                                "threads": MOSAIC_THREADS,
                                "align_bounds_with_dataset": True,
                            }
                        )

                        read_options.update(image_params)

                    image, _ = src_dst.feature(
                        shape,
                        cmr_query=query,
                        shape_crs=coord_crs or WGS84_CRS,
                        dst_crs=dst_crs,
                        **read_options,
                    )

                    coverage_array = image.get_coverage_array(
                        shape,
                        shape_crs=coord_crs or WGS84_CRS,
                    )

                    if post_process:
                        image = post_process(image)

                    # set band name for statistics method
                    if not image.band_names and reader_params.backend == "xarray":
                        image.band_names = [zarr_params.variable]

                    stats = image.statistics(
                        **stats_params,
                        hist_options={**histogram_params},
                        coverage=coverage_array,
                    )

                    feature.properties = feature.properties or {}
                    feature.properties.update({"statistics": stats})

            return fc.features[0] if isinstance(geojson, Feature) else fc
