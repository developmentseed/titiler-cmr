"""TiTiler+cmr FastAPI application."""

from contextlib import asynccontextmanager

import earthaccess
import jinja2
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from starlette.templating import Jinja2Templates

from titiler.cmr import __version__ as titiler_cmr_version
from titiler.cmr.errors import DEFAULT_STATUS_CODES as CMR_STATUS_CODES
from titiler.cmr.factory import Endpoints
from titiler.cmr.settings import ApiSettings, AuthSettings
from titiler.cmr.timeseries import TimeseriesExtension
from titiler.core.errors import DEFAULT_STATUS_CODES, add_exception_handlers
from titiler.core.middleware import CacheControlMiddleware, LoggerMiddleware
from titiler.mosaic.errors import MOSAIC_STATUS_CODES

jinja2_env = jinja2.Environment(
    loader=jinja2.ChoiceLoader(
        [
            jinja2.PackageLoader(__package__, "templates"),
        ]
    ),
)
templates = Jinja2Templates(env=jinja2_env)

settings = ApiSettings()
auth_config = AuthSettings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI Lifespan."""
    if auth_config.strategy == "environment" and auth_config.access == "direct":
        app.state.cmr_auth = earthaccess.login(strategy="environment")
    else:
        app.state.cmr_auth = None

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

add_exception_handlers(app, DEFAULT_STATUS_CODES)
add_exception_handlers(app, MOSAIC_STATUS_CODES)
add_exception_handlers(app, CMR_STATUS_CODES)

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

if settings.debug:
    app.add_middleware(LoggerMiddleware, headers=True, querystrings=True)

###############################################################################
# application endpoints
endpoints = Endpoints(
    title=settings.name,
    templates=templates,
    extensions=[TimeseriesExtension()],
)
app.include_router(endpoints.router)
