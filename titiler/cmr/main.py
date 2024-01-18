"""TiTiler+cmr FastAPI application."""

import datetime
import pathlib
from contextlib import asynccontextmanager

import jinja2
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from starlette.templating import Jinja2Templates

from titiler.cmr import __version__ as titiler_cmr_version
from titiler.cmr import models
from titiler.cmr.factory import Endpoints
from titiler.cmr.settings import ApiSettings
from titiler.core.middleware import CacheControlMiddleware

jinja2_env = jinja2.Environment(
    loader=jinja2.ChoiceLoader(
        [
            jinja2.PackageLoader(__package__, "templates"),
        ]
    ),
)
templates = Jinja2Templates(env=jinja2_env)

settings = ApiSettings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI Lifespan."""

    def parse(path: pathlib.Path) -> models.Collection:
        with path.open() as f:
            return models.Collection.model_validate_json(f.read())

    collections = [
        parse(collection)
        for collection in pathlib.Path(__file__).parent.joinpath("data").glob("*.json")
    ]

    app.state.collection_catalog = models.Catalog(
        collections={collection.id: collection for collection in collections},
        last_updated=datetime.datetime.now(),
    )

    yield


app = FastAPI(
    title=settings.name,
    openapi_url="/api",
    docs_url="/api.html",
    description="""Connect Common Metadata Repository (CMR) and TiTiler.

---

**Documentation**: <a href="https://developmentseed.org/titiler-cmr/" target="_blank">https://developmentseed.org/titiler-cmr/</a>

**Source Code**: <a href="https://github.com/developmentseed/titiler-cmr" target="_blank">https://github.com/developmentseed/titiler-cmr</a>

---
    """,
    version=titiler_cmr_version,
    root_path=settings.root_path,
    lifespan=lifespan,
)


# Set all CORS enabled origins
if settings.cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["GET"],
        allow_headers=["*"],
    )

app.add_middleware(CacheControlMiddleware, cachecontrol=settings.cachecontrol)

###############################################################################
# application endpoints
endpoints = Endpoints(
    title=settings.name,
    templates=templates,
)
app.include_router(endpoints.router)