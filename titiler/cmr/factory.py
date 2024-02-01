"""titiler.cmr.factory: router factories."""

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Union
from urllib.parse import urlencode

import jinja2
import numpy
import orjson
from fastapi import APIRouter, Depends, Path, Query
from fastapi.responses import ORJSONResponse
from morecantile import tms as default_tms
from morecantile.defaults import TileMatrixSets
from pydantic import conint
from rio_tiler.io import Reader
from rio_tiler.types import RIOResampling, WarpResampling
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import compile_path, replace_params
from starlette.templating import Jinja2Templates, _TemplateResponse
from typing_extensions import Annotated

from titiler.cmr import models
from titiler.cmr.backend import CMRBackend
from titiler.cmr.dependencies import OutputType, cmr_query
from titiler.cmr.enums import MediaType
from titiler.cmr.reader import ZarrReader
from titiler.core import dependencies
from titiler.core.algorithm import algorithms as available_algorithms
from titiler.core.factory import img_endpoint_params
from titiler.core.models.mapbox import TileJSON
from titiler.core.resources.enums import ImageType
from titiler.core.utils import render_image

jinja2_env = jinja2.Environment(
    loader=jinja2.ChoiceLoader([jinja2.PackageLoader(__package__, "templates")])
)
DEFAULT_TEMPLATES = Jinja2Templates(env=jinja2_env)


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
        f"{template_name}.html",
        {
            "request": request,
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


@dataclass
class Endpoints:
    """Endpoints Factory."""

    # FastAPI router
    router: APIRouter = field(default_factory=APIRouter)

    supported_tms: TileMatrixSets = default_tms

    # Router Prefix is needed to find the path for routes when prefixed
    # e.g if you mount the route with `/foo` prefix, set router_prefix to foo
    router_prefix: str = ""

    templates: Jinja2Templates = DEFAULT_TEMPLATES

    title: str = "TiTiler-CMR"

    def url_for(self, request: Request, name: str, **path_params: Any) -> str:
        """Return full url (with prefix) for a specific handler."""
        url_path = self.router.url_path_for(name, **path_params)

        base_url = str(request.base_url)
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

        return str(url_path.make_absolute_url(base_url=base_url))

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

    def __post_init__(self):
        """Post Init: register routes."""

        self.register_landing()
        self.register_conformance()
        self.register_tilematrixsets()
        self.register_tiles()

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
            "/collections/{collectionId}/tiles/{tileMatrixSetId}/{z}/{x}/{y}",
            **img_endpoint_params,
            tags=["Raster Tiles"],
        )
        @self.router.get(
            "/collections/{collectionId}/tiles/{tileMatrixSetId}/{z}/{x}/{y}.{format}",
            **img_endpoint_params,
            tags=["Raster Tiles"],
        )
        @self.router.get(
            "/collections/{collectionId}/tiles/{tileMatrixSetId}/{z}/{x}/{y}@{scale}x",
            **img_endpoint_params,
            tags=["Raster Tiles"],
        )
        @self.router.get(
            "/collections/{collectionId}/tiles/{tileMatrixSetId}/{z}/{x}/{y}@{scale}x.{format}",
            **img_endpoint_params,
            tags=["Raster Tiles"],
        )
        def tiles_endpoint(
            request: Request,
            collectionId: Annotated[
                str,
                Path(
                    description="A CMR concept id, in the format <concept-type-prefix> <unique-number> '-' <provider-id>"
                ),
            ],
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
                ImageType,
                "Default will be automatically defined if the output image needs a mask (png) or not (jpeg).",
            ] = None,
            ###################################################################
            # CMR options
            query=Depends(cmr_query),
            ###################################################################
            backend: Annotated[
                Literal["cog", "xarray"],
                Query(description="Backend to read the CMR dataset"),
            ] = "cog",
            ###################################################################
            # ZarrReader Options
            ###################################################################
            variable: Annotated[
                Optional[str],
                Query(description="Xarray Variable"),
            ] = None,
            drop_dim: Annotated[
                Optional[str],
                Query(description="Dimension to drop"),
            ] = None,
            time_slice: Annotated[
                Optional[str], Query(description="Slice of time to read (if available)")
            ] = None,
            decode_times: Annotated[
                Optional[bool],
                Query(
                    title="decode_times",
                    description="Whether to decode times",
                ),
            ] = None,
            ###################################################################
            # COG Reader Options
            ###################################################################
            indexes: Annotated[
                Optional[List[int]],
                Query(
                    title="Band indexes",
                    alias="bidx",
                    description="Dataset band indexes",
                ),
            ] = None,
            expression: Annotated[
                Optional[str],
                Query(
                    title="Band Math expression",
                    description="rio-tiler's band math expression",
                ),
            ] = None,
            unscale: Annotated[
                Optional[bool],
                Query(
                    title="Apply internal Scale/Offset",
                    description="Apply internal Scale/Offset. Defaults to `False`.",
                ),
            ] = None,
            resampling_method: Annotated[
                Optional[RIOResampling],
                Query(
                    alias="resampling",
                    description="RasterIO resampling algorithm. Defaults to `nearest`.",
                ),
            ] = None,
            ###################################################################
            # Reader options
            ###################################################################
            nodata: Annotated[
                Optional[Union[str, int, float]],
                Query(
                    title="Nodata value",
                    description="Overwrite internal Nodata value",
                ),
            ] = None,
            reproject_method: Annotated[
                Optional[WarpResampling],
                Query(
                    alias="reproject",
                    description="WarpKernel resampling algorithm (only used when doing re-projection). Defaults to `nearest`.",
                ),
            ] = None,
            ###################################################################
            # Rendering Options
            ###################################################################
            post_process=Depends(available_algorithms.dependency),
            rescale=Depends(dependencies.RescalingParams),
            color_formula=Depends(dependencies.ColorFormulaParams),
            colormap=Depends(dependencies.ColorMapParams),
            render_params=Depends(dependencies.ImageRenderingParams),
        ) -> Response:
            """Create map tile from a dataset."""
            resampling_method = resampling_method or "nearest"
            reproject_method = reproject_method or "nearest"
            if nodata is not None:
                nodata = numpy.nan if nodata == "nan" else float(nodata)

            tms = self.supported_tms.get(tileMatrixSetId)

            read_options: Dict[str, Any] = {}
            reader_options: Dict[str, Any] = {}

            if backend != "cog":
                reader = ZarrReader
                read_options = {}

                options = {
                    "variable": variable,
                    "decode_times": decode_times,
                    "drop_dim": drop_dim,
                    "time_slice": time_slice,
                }
                reader_options = {k: v for k, v in options.items() if v is not None}
            else:
                reader = Reader
                options = {
                    "indexes": indexes,  # type: ignore
                    "expression": expression,
                    "unscale": unscale,
                    "resampling_method": resampling_method,
                }
                read_options = {k: v for k, v in options.items() if v is not None}

                reader_options = {}

            with CMRBackend(
                collectionId,
                tms=tms,
                reader=reader,
                reader_options=reader_options,
                auth=request.app.cmr_auth,
            ) as src_dst:
                image = src_dst.tile(
                    x,
                    y,
                    z,
                    tilesize=scale * 256,
                    cmr_query=cmr_query,
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
            "/collections/{collectionId}/{tileMatrixSetId}/tilejson.json",
            response_model=TileJSON,
            responses={200: {"description": "Return a tilejson"}},
            response_model_exclude_none=True,
            tags=["TileJSON"],
        )
        def tilejson_endpoint(  # type: ignore
            request: Request,
            collectionId: Annotated[
                str,
                Path(
                    description="A CMR concept id, in the format <concept-type-prefix> <unique-number> '-' <provider-id>"
                ),
            ],
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
            ###################################################################
            # CMR options
            query=Depends(cmr_query),
            ###################################################################
            backend: Annotated[
                Literal["cog", "xarray"],
                Query(description="Backend to read the CMR dataset"),
            ] = "cog",
            ###################################################################
            # ZarrReader Options
            ###################################################################
            variable: Annotated[
                Optional[str],
                Query(description="Xarray Variable"),
            ] = None,
            drop_dim: Annotated[
                Optional[str],
                Query(description="Dimension to drop"),
            ] = None,
            time_slice: Annotated[
                Optional[str], Query(description="Slice of time to read (if available)")
            ] = None,
            decode_times: Annotated[
                Optional[bool],
                Query(
                    title="decode_times",
                    description="Whether to decode times",
                ),
            ] = None,
            ###################################################################
            # COG Reader Options
            ###################################################################
            indexes: Annotated[
                Optional[List[int]],
                Query(
                    title="Band indexes",
                    alias="bidx",
                    description="Dataset band indexes",
                ),
            ] = None,
            expression: Annotated[
                Optional[str],
                Query(
                    title="Band Math expression",
                    description="rio-tiler's band math expression",
                ),
            ] = None,
            unscale: Annotated[
                Optional[bool],
                Query(
                    title="Apply internal Scale/Offset",
                    description="Apply internal Scale/Offset. Defaults to `False`.",
                ),
            ] = None,
            resampling_method: Annotated[
                Optional[RIOResampling],
                Query(
                    alias="resampling",
                    description="RasterIO resampling algorithm. Defaults to `nearest`.",
                ),
            ] = None,
            ###################################################################
            # Reader options
            ###################################################################
            nodata: Annotated[
                Optional[Union[str, int, float]],
                Query(
                    title="Nodata value",
                    description="Overwrite internal Nodata value",
                ),
            ] = None,
            reproject_method: Annotated[
                Optional[WarpResampling],
                Query(
                    alias="reproject",
                    description="WarpKernel resampling algorithm (only used when doing re-projection). Defaults to `nearest`.",
                ),
            ] = None,
            ###################################################################
            # Rendering Options
            ###################################################################
            post_process=Depends(available_algorithms.dependency),
            rescale=Depends(dependencies.RescalingParams),
            color_formula=Depends(dependencies.ColorFormulaParams),
            colormap=Depends(dependencies.ColorMapParams),
            render_params=Depends(dependencies.ImageRenderingParams),
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

            # TODO: can we get metadata from the collection?
            with CMRBackend(
                collectionId,
                auth=request.app.cmr_auth,
                tms=tms,
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
