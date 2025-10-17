"""titiler-cmr logger."""

import json
import logging
import os
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """JSON log formatter for local development."""

    def format(self, record):
        """Format log record as JSON."""
        log_entry = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S.%fZ"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "filename": record.filename,
            "lineno": record.lineno,
        }

        # Add any extra fields passed via the extra parameter
        if hasattr(record, "__dict__"):
            for key, value in record.__dict__.items():
                if key not in log_entry and not key.startswith("_"):
                    # Only add if it's not a standard logging attribute
                    if not hasattr(logging.LogRecord("", 0, "", 0, "", (), None), key):
                        log_entry[key] = value

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)


def otel_trace_id_to_xray_format(otel_trace_id: str) -> str:
    """
    Convert OpenTelemetry trace ID to X-Ray format.

    OTEL format: 32 hex chars (e.g., "68eeb2ec45b07caf760899f308d34ab6")
    X-Ray format: "1-{first 8 chars}-{remaining 24 chars}" (e.g., "1-68eeb2ec-45b07caf760899f308d34ab6")

    The first 8 hex chars represent the Unix timestamp, which is how X-Ray generates compatible IDs.
    """
    if len(otel_trace_id) == 32:
        return f"1-{otel_trace_id[:8]}-{otel_trace_id[8:]}"
    return otel_trace_id


class XRayJsonFormatter(logging.Formatter):
    """
    Custom JSON formatter that includes X-Ray trace ID for log correlation.

    This formatter outputs logs as JSON and includes:
    - Standard log fields (timestamp, level, message, logger)
    - X-Ray trace ID (converted from OTEL format)
    - OTEL trace context fields (if present)
    - Any extra fields passed via logger.info("msg", extra={...})
    """

    RESERVED_ATTRS = {
        "name",
        "msg",
        "args",
        "created",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "thread",
        "threadName",
        "exc_info",
        "exc_text",
        "stack_info",
        "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:  # noqa: C901
        """Format log record as JSON with X-Ray trace ID."""
        log_object = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }

        xray_trace_id = None

        trace_header = os.environ.get("_X_AMZN_TRACE_ID", "")
        if trace_header:
            for part in trace_header.split(";"):
                if part.startswith("Root="):
                    xray_trace_id = part.split("=", 1)[1]
                    break

        if not xray_trace_id and hasattr(record, "otelTraceID"):
            xray_trace_id = otel_trace_id_to_xray_format(record.otelTraceID)

        if xray_trace_id:
            log_object["xray_trace_id"] = xray_trace_id

        if record.exc_info:
            log_object["exception"] = self.formatException(record.exc_info)

        for attr in [
            "otelSpanID",
            "otelTraceID",
            "otelTraceSampled",
            "otelServiceName",
        ]:
            if hasattr(record, attr):
                log_object[attr] = getattr(record, attr)

        if hasattr(record, "aws_request_id"):
            log_object["requestId"] = record.aws_request_id

        for key, value in record.__dict__.items():
            if key not in self.RESERVED_ATTRS and key not in log_object:
                log_object[key] = value

        return json.dumps(log_object)


def configure_logging():
    """
    Configure logging for the application.

    This should be called once at application startup.
    Automatically detects the environment and configures appropriately:
    - AWS Lambda: Uses XRayJsonFormatter with X-Ray trace correlation
    - Local dev: Uses JSONFormatter for simple JSON logs

    All log levels for application and third-party libraries are configured here.
    """
    is_lambda = "AWS_EXECUTION_ENV" in os.environ

    if is_lambda:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.WARN)

        for log_handler in root_logger.handlers[:]:
            root_logger.removeHandler(log_handler)

        json_handler = logging.StreamHandler()
        json_handler.setFormatter(XRayJsonFormatter())
        root_logger.addHandler(json_handler)
    else:
        formatter = JSONFormatter()
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logging.root.addHandler(handler)

    logging.getLogger("titiler").setLevel(logging.INFO)
    logging.getLogger("titiler-cmr").setLevel(logging.INFO)

    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("aiobotocore").setLevel(logging.WARNING)
    logging.getLogger("earthaccess").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("numexpr").setLevel(logging.WARNING)

    if is_lambda:
        logging.getLogger("mangum.lifespan").setLevel(logging.ERROR)
        logging.getLogger("mangum.http").setLevel(logging.ERROR)


logger = logging.getLogger("titiler-cmr")
