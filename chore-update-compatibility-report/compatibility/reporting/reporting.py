"""Shared helpers for compatibility assessment reports."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

LOGGER = logging.getLogger(__name__)

DEFAULT_ASSESSMENT_PATH = Path(__file__).with_name(
    "assessment-results-2026-07-01.parquet"
)

REQUIRED_COLUMNS = ("tiling_compatible", "assessment_status")
OPTIONAL_COLUMNS = (
    "collection_concept_id",
    "collection_short_name_and_version",
    "concept_id",
    "data_center",
    "data_url",
    "backend",
    "format",
    "extension",
    "num_granules",
    "processing_level",
    "groups",
    "incompatible_reason",
    "error_message",
    "tiles_url",
    "variable",
    "data_variables",
    "failure_stage",
    "failure_category",
    "failure_subcategory",
    "error_code",
    "error_detail",
    "failure_http_status_code",
    "failure_endpoint",
    "failure_url",
    "raw_error_body",
    "granule_concept_id",
    "granule_ur",
    "assessed_asset_href",
    "assessed_asset_extension",
    "assessed_asset_scheme",
    "group",
    "variables",
    "selected_variable_source",
    "compatible_groups",
    "tested_groups",
    "granule_bbox",
    "probe_bbox",
    "bbox_status_code",
    "bbox_error_body",
    "bbox_error_snippet",
    "bbox_url",
    "bbox_attempt_count",
    "bbox_attempts",
    "bbox_probe_limited",
    "compatibility_status_code",
    "compatibility_error_body",
    "compatibility_response",
)

CATEGORY_LABELS = {
    "compatible": "Compatible",
    "unsupported_asset": "Unsupported asset",
    "data_incompatible": "Data incompatibility",
    "metadata_incomplete": "Metadata gap",
    "inaccessible": "Inaccessible or authentication",
    "service_error": "Service error",
    "render_error": "Render error",
    "request_error": "Request error",
}

REASON_LABELS = {
    "unsupported_media_type": "Unsupported media type",
    "missing_asset_href_or_extension": "Missing asset href or extension",
    "missing_xy_spatial_coordinates": "Missing spatial coordinate metadata",
    "missing_y_coordinate": "Missing spatial coordinate metadata",
    "no_tileable_variables": "No tileable variables",
    "unsupported_dimensionality": "Unsupported dimensionality",
    "unsupported_file_signature": "Unsupported file signature",
    "no_granule_found": "No granule found",
    "invalid_or_missing_granule_bbox": "Invalid or missing granule bbox",
    "s3_credential_lookup_failed": "S3 credential lookup failed",
    "compatibility_http_500": "Compatibility endpoint HTTP 500",
    "bbox_http_500": "Bbox endpoint HTTP 500",
    "compatibility_http_503": "Compatibility endpoint HTTP 503",
    "bbox_http_503": "Bbox endpoint HTTP 503",
    "bbox_http_404": "Bbox endpoint HTTP 404",
    "bbox_probe_attempt_limit_exceeded": "Bbox probe attempt limit exceeded",
    "decode_error": "Decode error",
    "cant_open_file": "Cannot open file",
}


def load_assessment_results(
    path: str | Path = DEFAULT_ASSESSMENT_PATH, *, validate: bool = True
) -> pd.DataFrame:
    """Load compatibility assessment results from a parquet file."""
    assessment_path = Path(path)
    df = pd.read_parquet(assessment_path)
    if validate:
        validate_assessment_schema(df)
    return df


def validate_assessment_schema(df: pd.DataFrame) -> dict[str, list[str]]:
    """Validate the report schema and return missing required/optional columns."""
    missing_required = [
        column for column in REQUIRED_COLUMNS if column not in df.columns
    ]
    missing_optional = [
        column for column in OPTIONAL_COLUMNS if column not in df.columns
    ]

    if missing_required:
        joined = ", ".join(missing_required)
        msg = f"Compatibility assessment is missing required columns: {joined}"
        raise ValueError(msg)

    if missing_optional:
        LOGGER.warning(
            "Compatibility assessment is missing optional columns: %s",
            ", ".join(missing_optional),
        )

    return {
        "missing_required": missing_required,
        "missing_optional": missing_optional,
    }


def classify_assessment(df: pd.DataFrame, *, validate: bool = True) -> pd.DataFrame:
    """Return a dataframe with normalized report classification columns added."""
    if validate:
        validate_assessment_schema(df)

    classified = df.copy()
    normalized = classified.apply(classify_row, axis=1, result_type="expand")
    for column in normalized.columns:
        classified[column] = normalized[column]
    return classified


def classify_row(row: pd.Series | dict[str, Any]) -> dict[str, str]:
    """Classify one assessment row into stable human-facing report fields."""
    status = _clean(row.get("assessment_status"))
    compatible = _bool_value(row.get("tiling_compatible"))
    failure_category = _clean(row.get("failure_category"))
    failure_subcategory = _clean(row.get("failure_subcategory"))
    error_code = _clean(row.get("error_code")) or failure_subcategory
    raw_error_body = _clean(row.get("raw_error_body"))

    if compatible is True or status == "compatible":
        return {
            "report_status": "compatible",
            "report_category": CATEGORY_LABELS["compatible"],
            "report_reason": "Compatible",
            "report_reason_detail": "Assessment completed successfully",
        }

    report_status = status if status in {"incompatible", "inconclusive"} else "unknown"
    report_category = CATEGORY_LABELS.get(failure_category, "Unknown")
    report_reason = REASON_LABELS.get(error_code, _humanize(error_code))
    report_reason_detail = _reason_detail(error_code, raw_error_body)

    if report_category == "Unknown" and report_status == "inconclusive":
        report_category = "Inconclusive"

    if not report_reason:
        report_reason = "Unclassified assessment outcome"
    if not report_reason_detail:
        report_reason_detail = _fallback_detail(failure_category, error_code)

    return {
        "report_status": report_status,
        "report_category": report_category,
        "report_reason": report_reason,
        "report_reason_detail": report_reason_detail,
    }


def _bool_value(value: Any) -> bool | None:
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return bool(value)


def _clean(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _humanize(value: str) -> str:
    return value.replace("_", " ").capitalize() if value else ""


def _reason_detail(error_code: str, raw_error_body: str) -> str:
    body = raw_error_body.lower()

    if error_code == "s3_credential_lookup_failed":
        return _s3_credential_detail(body)

    if error_code in {"compatibility_http_500", "bbox_http_500"}:
        return _http_500_detail(body)

    if error_code in {"compatibility_http_503", "bbox_http_503"}:
        return "Service unavailable response"

    if error_code == "decode_error":
        return "Asset could not be decoded by the selected backend"

    if error_code in {
        "unsupported_media_type",
        "unsupported_file_signature",
        "unsupported_dimensionality",
        "missing_asset_href_or_extension",
        "missing_xy_spatial_coordinates",
        "missing_y_coordinate",
        "no_tileable_variables",
        "no_granule_found",
        "invalid_or_missing_granule_bbox",
        "bbox_probe_attempt_limit_exceeded",
        "bbox_http_404",
        "cant_open_file",
    }:
        return REASON_LABELS[error_code]

    return ""


def _s3_credential_detail(body: str) -> str:
    if "169.254.169.254" in body or "imds" in body or "latest/api/token" in body:
        return "IMDS credential token unavailable"
    return "S3 credential lookup failed"


def _http_500_detail(body: str) -> str:
    body_patterns = {
        ("nonetype", "get"): "Internal NoneType access error",
        ("to_authority",): "Internal CRS authority lookup error",
        (
            "requires string as left operand",
        ): "Internal bytes/string media-type check error",
        ("invalid media type",): "Invalid media type surfaced as service error",
        ("could not encode array",): "Tile encoding failed",
        ("unable to allocate",): "Memory allocation failure",
        (
            "cannot find coordinate operations",
        ): "CRS coordinate operation lookup failed",
    }
    for patterns, detail in body_patterns.items():
        if all(pattern in body for pattern in patterns):
            return detail
    return "HTTP 500 response"


def _fallback_detail(failure_category: str, error_code: str) -> str:
    if error_code:
        return _humanize(error_code)
    if failure_category:
        return CATEGORY_LABELS.get(failure_category, _humanize(failure_category))
    return "No structured failure detail was provided"
