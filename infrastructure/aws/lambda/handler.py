"""AWS Lambda handler optimized for container runtime with OTEL instrumentation."""

import asyncio
import logging
import os
import warnings

from mangum import Mangum

from titiler.cmr.logger import configure_logging
from titiler.cmr.main import app

configure_logging()

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
logging.getLogger("numexpr").setLevel(logging.WARNING)

lambda_handler = Mangum(
    app,
    lifespan="off" if "AWS_EXECUTION_ENV" in os.environ else "auto",
    text_mime_types=[
        "application/json",
        "application/javascript",
        "application/xml",
        "application/vnd.api+json",
    ],
)


# Run lifespan only during cold starts.
if not hasattr(app.state, "startup_done"):

    async def run_lifespan():
        """Manually trigger lifecyle startup/shutdown."""
        async with app.router.lifespan_context(app):
            app.state.startup_done = True

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_lifespan())
