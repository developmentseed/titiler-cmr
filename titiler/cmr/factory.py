"""tipg.factory: router factories."""

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Literal, Optional

import jinja2
import orjson
from fastapi import APIRouter, Depends, Path
from fastapi.responses import ORJSONResponse
from morecantile import tms as default_tms
from morecantile.defaults import TileMatrixSets
from starlette.datastructures import QueryParams
from starlette.requests import Request
from starlette.routing import compile_path, replace_params
from starlette.templating import Jinja2Templates, _TemplateResponse
from typing_extensions import Annotated

from titiler.cmr import models
from titiler.cmr.dependencies import CollectionParams, CollectionsParams, OutputType
from titiler.cmr.enums import MediaType

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

    # collection dependency
    collections_dependency: Callable[..., models.CollectionList] = CollectionsParams
    collection_dependency: Callable[..., models.Collection] = CollectionParams

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
        self.register_collections()
        self.register_collection()
        self.register_tilematrixsets()

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
                        title="Collections",
                        href=self.url_for(request, "collections"),
                        type=MediaType.json,
                        rel="data",
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
                    "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/collections",
                    "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/simple-query",
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

    def register_collections(self) -> None:
        """register collections endpoint."""

        @self.router.get(
            "/collections",
            response_model=models.Collections,
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
            summary="list the collections in the dataset",
            operation_id="getCollections",
            tags=["Data Collections"],
        )
        def collections(
            request: Request,
            collection_list: Annotated[
                models.CollectionList,
                Depends(self.collections_dependency),
            ],
            output_type: Annotated[
                Optional[MediaType],
                Depends(OutputType),
            ] = None,
        ):
            """List the collections in the dataset."""
            links: list = [
                models.Link(
                    href=self.url_for(request, "collections"),
                    rel="self",
                    type=MediaType.json,
                ),
            ]

            if next_token := collection_list.get("next"):
                query_params = QueryParams(
                    {**request.query_params, "offset": next_token}
                )
                url = self.url_for(request, "collections") + f"?{query_params}"
                links.append(
                    models.Link(
                        href=url,
                        rel="next",
                        type=MediaType.json,
                        title="Next page",
                    ),
                )

            if collection_list.get("prev") is not None:
                prev_token = collection_list["prev"]
                qp = dict(request.query_params)
                qp.pop("offset", None)
                query_params = QueryParams({**qp, "offset": prev_token})
                url = self.url_for(request, "collections")
                if query_params:
                    url += f"?{query_params}"

                links.append(
                    models.Link(
                        href=url,
                        rel="prev",
                        type=MediaType.json,
                        title="Previous page",
                    ),
                )

            collections = [
                collection.copy() for collection in collection_list["collections"]
            ]
            for collection in collections:
                collection.links = [
                    models.Link(
                        href=self.url_for(
                            request,
                            "collection",
                            collectionId=collection.id,
                        ),
                        rel="collection",
                        type=MediaType.json,
                    ),
                ]

            data = models.Collections(
                links=links,
                numberMatched=collection_list.get("matched"),
                numberReturned=len(collection_list["collections"]),
                collections=collections,
            )

            if output_type == MediaType.html:
                return self._create_html_response(
                    request,
                    data.model_dump_json(exclude_none=True),
                    template_name="collections",
                )

            return data

    def register_collection(self) -> None:
        """register collection endpoint."""

        @self.router.get(
            "/collections/{collectionId}",
            response_model=models.Collection,
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
            summary="describe the collection with id `collectionId`",
            operation_id="describeCollection",
            tags=["Data Collections"],
        )
        def collection(
            request: Request,
            collection: Annotated[
                models.Collection, Depends(self.collection_dependency)
            ],
            output_type: Annotated[Optional[MediaType], Depends(OutputType)] = None,
        ):
            """Describe the collection with id `collectionId`"""
            data = collection.copy()
            data.links = [
                models.Link(
                    href=self.url_for(
                        request,
                        "collection",
                        collectionId=collection.id,
                    ),
                    rel="collection",
                    type=MediaType.json,
                ),
            ]

            if output_type == MediaType.html:
                return self._create_html_response(
                    request,
                    data.model_dump_json(exclude_none=True),
                    template_name="collection",
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
