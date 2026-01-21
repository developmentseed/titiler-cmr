"""AWS Lambda handler optimized for container runtime with OTEL instrumentation."""

import asyncio
import logging
import os
import warnings
from typing import Any

from mangum import Mangum
from mangum.types import LambdaContext, LambdaEvent

from titiler.cmr.logger import configure_logging
from titiler.cmr.main import app

configure_logging()

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
logging.getLogger("numexpr").setLevel(logging.WARNING)


handler = Mangum(
    app,
    lifespan="on",
    text_mime_types=[
        "application/json",
        "application/javascript",
        "application/xml",
        "application/vnd.api+json",
    ],
)

if "AWS_EXECUTION_ENV" in os.environ:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.router.startup())


def lambda_handler(event: LambdaEvent, context: LambdaContext) -> dict[str, Any]:
    """Lambda handler with container-specific optimizations and OTEL tracing."""
    return handler(event, context)
