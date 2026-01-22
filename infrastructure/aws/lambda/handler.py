"""AWS Lambda handler optimized for container runtime with OTEL instrumentation."""

import logging
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
    lifespan="on",
    text_mime_types=[
        "application/json",
        "application/javascript",
        "application/xml",
        "application/vnd.api+json",
    ],
)
