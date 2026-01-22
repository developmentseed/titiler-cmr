"""AWS Lambda handler optimized for container runtime with OTEL instrumentation."""

import logging
import warnings

from mangum import Mangum

from titiler.cmr.logger import configure_logging, logger
from titiler.cmr.main import app, startup

configure_logging()

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
logging.getLogger("numexpr").setLevel(logging.WARNING)

logger.info("Cold starting Lambda")
startup(app)

lambda_handler = Mangum(
    app,
    # Prevent mangum from running lifespan because it will do so on every
    # invocation, rather than only during cold starts.
    lifespan="off",
    text_mime_types=[
        "application/json",
        "application/javascript",
        "application/xml",
        "application/vnd.api+json",
    ],
)
