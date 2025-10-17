"""AWS Lambda handler optimized for container runtime with OTEL instrumentation."""

import asyncio
import os
import warnings
from typing import Any, Dict

import earthaccess
from mangum import Mangum

from titiler.cmr.logger import configure_logging
from titiler.cmr.main import app
from titiler.cmr.settings import AuthSettings

configure_logging()

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

auth_config = AuthSettings()


@app.on_event("startup")
async def startup_event() -> None:
    """startup."""
    if auth_config.strategy == "environment":
        app.state.cmr_auth = earthaccess.login(strategy="environment")
    else:
        app.state.cmr_auth = None


handler = Mangum(
    app,
    lifespan="off",
    api_gateway_base_path=None,
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


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler with container-specific optimizations and OTEL tracing."""
    return handler(event, context)
