"""AWS Lambda handler."""

import asyncio
import logging
import os

import earthaccess
from mangum import Mangum
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION, Resource
from opentelemetry.sdk.trace import TracerProvider

from titiler.cmr.main import app
from titiler.cmr.settings import AuthSettings

auth_config = AuthSettings()

logging.getLogger("mangum.lifespan").setLevel(logging.ERROR)
logging.getLogger("mangum.http").setLevel(logging.ERROR)


LoggingInstrumentor().instrument(set_logging_format=True)
FastAPIInstrumentor.instrument_app(app)

resource = Resource.create(
    {
        SERVICE_NAME: "titiler-cmr",
        SERVICE_VERSION: "0.1.0",
    }
)

provider = TracerProvider(resource=resource)

# uses the OTEL_EXPORTER_OTLP_ENDPOINT env var
# processor = BatchSpanProcessor(OTLPSpanExporter())
# provider.add_span_processor(processor)

trace.set_tracer_provider(provider)


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
