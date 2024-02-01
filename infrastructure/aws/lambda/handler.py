"""AWS Lambda handler."""

import asyncio
import logging
import os

import earthaccess
from mangum import Mangum

from titiler.cmr.main import app
from titiler.cmr.settings import AuthSettings

auth_config = AuthSettings()

logging.getLogger("mangum.lifespan").setLevel(logging.ERROR)
logging.getLogger("mangum.http").setLevel(logging.ERROR)


@app.on_event("startup")
async def startup_event() -> None:
    """startup."""
    if auth_config.strategy == "environment":
        app.state.cmr_auth = earthaccess.login(strategy="environment")
    else:
        app.state.cmr_auth = None


handler = Mangum(app, lifespan="off")

if "AWS_EXECUTION_ENV" in os.environ:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.router.startup())
