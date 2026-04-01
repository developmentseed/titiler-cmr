"""AWS Lambda handler for zip deployment with OTEL/X-Ray instrumentation."""

import logging
import os
import warnings

# Must be set before any import that triggers GDAL or PROJ context creation.
# GDAL_DATA: schema/SRS files from our custom GDAL build (bundled at /var/task/gdal_data).
# PROJ_DATA: CRS database (proj.db) bundled at /var/task/pyproj/proj_dir/share/proj.
os.environ.setdefault("GDAL_DATA", "/var/task/gdal_data")
os.environ.setdefault("PROJ_DATA", "/var/task/pyproj/proj_dir/share/proj")

import pyproj
import rasterio
from mangum import Mangum

from titiler.cmr.logger import configure_logging
from titiler.cmr.main import app, startup

configure_logging()

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
logging.getLogger("numexpr").setLevel(logging.WARNING)

# Configure OTEL with X-Ray when running in Lambda.
# Skipped outside Lambda so local dev/testing is unaffected.
if "AWS_EXECUTION_ENV" in os.environ:
    import hashlib
    from urllib.parse import urlparse

    import botocore.auth
    import botocore.awsrequest
    import requests
    from botocore.session import Session as BotocoreSession
    from opentelemetry import propagate, trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.instrumentation.logging import LoggingInstrumentor
    from opentelemetry.propagators.aws import AwsXRayPropagator
    from opentelemetry.sdk.extension.aws.trace import AwsXRayIdGenerator
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    _region = os.environ.get("AWS_REGION", "us-east-1")

    class _SigV4Auth(requests.auth.AuthBase):
        """Sign OTLP HTTP requests with AWS SigV4.

        A fresh BotocoreSession is created on every request so that Lambda
        credential rotations are always picked up and the full provider chain
        is available as a fallback (e.g. during SnapStart init before env vars
        are injected).
        """

        def __init__(self, service: str, region: str) -> None:
            self._service = service
            self._region = region

        def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
            """Add SigV4 Authorization header to the request."""
            credentials = BotocoreSession().get_credentials().get_frozen_credentials()
            parsed = urlparse(r.url)
            body = r.body if r.body is not None else b""
            body_bytes = body if isinstance(body, bytes) else body.encode("utf-8")
            body_hash = hashlib.sha256(body_bytes).hexdigest()

            # Only pass headers that are stable at signing time. Botocore will
            # sign every header it receives; headers added or modified by the
            # requests transport layer after this point (e.g. Content-Length set
            # by urllib3) would cause a signature mismatch at the server.
            sign_headers: dict[str, str] = {
                "Content-Type": str(
                    r.headers.get("Content-Type", "application/x-protobuf")
                ),
                "Host": str(parsed.hostname or ""),
                "X-Amz-Content-Sha256": body_hash,
            }
            if "Content-Encoding" in r.headers:
                sign_headers["Content-Encoding"] = str(r.headers["Content-Encoding"])

            aws_req = botocore.awsrequest.AWSRequest(
                method=r.method,
                url=r.url,
                data=body_bytes,
                headers=sign_headers,
            )
            botocore.auth.SigV4Auth(credentials, self._service, self._region).add_auth(
                aws_req
            )
            # Copy only the auth headers botocore added — do not overwrite the
            # rest of the prepared request headers.
            for key in (
                "Authorization",
                "X-Amz-Date",
                "X-Amz-Security-Token",
                "X-Amz-Content-Sha256",
            ):
                if key in aws_req.headers:
                    r.headers[key] = aws_req.headers[key]
            return r

    _exporter = OTLPSpanExporter(
        endpoint=f"https://xray.{_region}.amazonaws.com/v1/traces",
    )
    _exporter._session.auth = _SigV4Auth("xray", _region)
    # After SnapStart restore the session's connection pool contains stale TCP
    # connections. requests does not retry POSTs by default, so a stale
    # connection causes the first post-restore export to fail silently.
    _exporter._session.mount(
        "https://",
        HTTPAdapter(max_retries=Retry(total=2, allowed_methods={"POST"})),
    )

    def _log_otlp_response(r: requests.Response, *args, **kwargs) -> None:
        """Log non-2xx OTLP export responses to diagnose signing/auth failures."""
        if r.status_code >= 300:
            logging.getLogger(__name__).error(
                "OTLP export failed: status=%s body=%r signed_headers=%s",
                r.status_code,
                r.text[:500],
                r.request.headers.get("Authorization", "")[:200],
            )

    _exporter._session.hooks["response"].append(_log_otlp_response)

    class _LambdaXRayPropagator(AwsXRayPropagator):
        """AwsXRayPropagator that falls back to the Lambda _X_AMZN_TRACE_ID env var.

        API Gateway HTTP API (v2) does not inject X-Amzn-Trace-Id into the
        Lambda event headers, so the standard propagator finds nothing and
        creates a new root span with a fresh trace ID. Reading from the env
        var ensures OTEL continues the same trace as Lambda's native X-Ray
        segment, keeping trace IDs consistent between OTEL spans, CloudWatch
        log entries, and the X-Ray console.
        """

        def extract(self, carrier, context=None, getter=None):
            """Extract trace context, falling back to _X_AMZN_TRACE_ID."""
            ctx = super().extract(carrier, context=context, getter=getter)
            if not trace.get_current_span(context=ctx).get_span_context().is_valid:
                trace_header = os.environ.get("_X_AMZN_TRACE_ID", "")
                if trace_header:
                    ctx = super().extract(
                        {"X-Amzn-Trace-Id": trace_header},
                        context=context,
                    )
            return ctx

    # AwsXRayIdGenerator produces trace IDs whose first 8 hex chars encode the
    # epoch timestamp — the format X-Ray requires for time-indexed trace lookup.
    # BatchSpanProcessor (not Simple) is used here so that span exports happen on
    # a background thread and never block the asyncio event loop. SimpleSpanProcessor
    # calls OTLPSpanExporter.export() synchronously inside span.end(), which runs
    # inline on the event loop thread and blocks it for the duration of the OTLP
    # HTTP round-trip. For endpoints like /timeseries/bbox that fan out N concurrent
    # httpx sub-requests via asyncio.gather, this produces N sequential blocking
    # exports that can exhaust the Lambda timeout before all responses are collected.
    # Spans are flushed explicitly in the handler wrapper below before Lambda returns.
    _provider = TracerProvider(id_generator=AwsXRayIdGenerator())
    _provider.add_span_processor(BatchSpanProcessor(_exporter))
    trace.set_tracer_provider(_provider)
    propagate.set_global_textmap(_LambdaXRayPropagator())

    # Instrument before startup() creates the httpx client so it is captured.
    # set_logging_format=True is required to inject otelTraceID/otelSpanID into
    # log records. Despite the name, it does NOT override the XRayJsonFormatter —
    # it calls logging.basicConfig() internally, which is a no-op when handlers
    # already exist (configure_logging() has already run).
    LoggingInstrumentor().instrument(set_logging_format=True)
    HTTPXClientInstrumentor().instrument()
    FastAPIInstrumentor.instrument_app(app)

startup(app)

# ── SnapStart pre-warming ──────────────────────────────────────────────────────
# Build the Starlette/OTEL middleware stack now so the first post-restore request
# doesn't pay the assembly cost.
app.middleware_stack = app.build_middleware_stack()

# Open a rasterio.Env and leave it open (do not call __exit__).  This calls
# gdal.AllRegister() and keeps the reference count at 1 so the GDAL driver
# manager is not torn down.  gdal.AllRegister() is not re-entrant-safe so
# request handlers that use their own rasterio.Env() push on top of this one
# rather than re-initializing from scratch.
_gdal_env = rasterio.Env()
_gdal_env.__enter__()

# Warm the PROJ CRS cache so the first CRS lookup after restore hits the
# in-memory cache rather than the SQLite database.
pyproj.CRS.from_epsg(4326)
# ──────────────────────────────────────────────────────────────────────────────

_mangum = Mangum(
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


def lambda_handler(event: dict, context: object) -> dict:
    """Lambda entry point with explicit OTEL flush after each invocation.

    BatchSpanProcessor exports spans on a background thread during the
    request. We flush here — after the response is fully assembled but
    before returning — so spans are not silently dropped when Lambda
    freezes the execution environment between invocations.

    The flush timeout (5 s) is intentionally shorter than the Lambda
    timeout so a slow X-Ray endpoint cannot push the invocation over
    the function's configured limit.
    """
    result = _mangum(event, context)
    _provider.force_flush(timeout_millis=5_000)
    return result
