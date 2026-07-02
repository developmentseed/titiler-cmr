"""Tests for compatibility assessment report categorization."""

from pathlib import Path

import pandas as pd
import pytest

from docs.compatibility.generate_table import generate_searchable_table
from docs.compatibility.reporting import (
    classify_assessment,
    classify_row,
    load_assessment_results,
    validate_assessment_schema,
)


def row(**overrides: object) -> pd.Series:
    """Build a representative assessment row for classification tests."""
    values: dict[str, object] = {
        "tiling_compatible": False,
        "assessment_status": "incompatible",
        "failure_category": None,
        "failure_subcategory": None,
        "error_code": None,
        "raw_error_body": None,
    }
    values.update(overrides)
    return pd.Series(values)


def test_compatible_row_classifies_as_compatible() -> None:
    """Compatible rows receive a compatible report status and category."""
    classified = classify_row(
        row(tiling_compatible=True, assessment_status="compatible")
    )

    assert classified["report_status"] == "compatible"
    assert classified["report_category"] == "Compatible"
    assert classified["report_reason"] == "Compatible"


def test_unsupported_media_type_classifies_as_unsupported_asset() -> None:
    """Unsupported media type errors get a readable unsupported-asset reason."""
    classified = classify_row(
        row(
            failure_category="unsupported_asset",
            failure_subcategory="unsupported_media_type",
            error_code="unsupported_media_type",
        )
    )

    assert classified["report_status"] == "incompatible"
    assert classified["report_category"] == "Unsupported asset"
    assert classified["report_reason"] == "Unsupported media type"


def test_s3_imds_error_classifies_as_credential_failure() -> None:
    """S3 IMDS token lookup failures classify as inaccessible credential failures."""
    classified = classify_row(
        row(
            failure_category="inaccessible",
            failure_subcategory="s3_credential_lookup_failed",
            error_code="s3_credential_lookup_failed",
            raw_error_body=(
                '{"detail":"Generic S3 error: Error performing PUT '
                'http://169.254.169.254/latest/api/token after 10 retries"}'
            ),
        )
    )

    assert classified["report_category"] == "Inaccessible or authentication"
    assert classified["report_reason"] == "S3 credential lookup failed"
    assert classified["report_reason_detail"] == "IMDS credential token unavailable"


@pytest.mark.parametrize(
    "error_code",
    ["missing_xy_spatial_coordinates", "missing_y_coordinate"],
)
def test_missing_spatial_coordinate_errors_classify_as_data_incompatible(
    error_code: str,
) -> None:
    """Missing X/Y spatial coordinates classify as data incompatibilities."""
    classified = classify_row(
        row(failure_category="data_incompatible", error_code=error_code)
    )

    assert classified["report_category"] == "Data incompatibility"
    assert classified["report_reason"] == "Missing spatial coordinate metadata"


@pytest.mark.parametrize(
    ("error_code", "expected_reason"),
    [
        ("no_granule_found", "No granule found"),
        ("invalid_or_missing_granule_bbox", "Invalid or missing granule bbox"),
    ],
)
def test_metadata_gaps_classify_as_metadata_or_inconclusive(
    error_code: str, expected_reason: str
) -> None:
    """Granule and bbox metadata failures classify as metadata gaps."""
    classified = classify_row(
        row(
            assessment_status="inconclusive",
            failure_category="metadata_incomplete",
            error_code=error_code,
        )
    )

    assert classified["report_status"] == "inconclusive"
    assert classified["report_category"] == "Metadata gap"
    assert classified["report_reason"] == expected_reason


def test_unknown_http_500_stays_service_error() -> None:
    """Unknown compatibility 500 bodies stay operational instead of data incompatible."""
    classified = classify_row(
        row(
            failure_category="service_error",
            error_code="compatibility_http_500",
            raw_error_body='{"detail":"something new"}',
        )
    )

    assert classified["report_category"] == "Service error"
    assert classified["report_reason"] == "Compatibility endpoint HTTP 500"
    assert classified["report_reason_detail"] == "HTTP 500 response"


def test_null_fields_do_not_crash_and_get_fallback() -> None:
    """Null failure fields produce an explicit fallback classification."""
    classified = classify_row(row(assessment_status=None))

    assert classified["report_status"] == "unknown"
    assert classified["report_category"] == "Unknown"
    assert classified["report_reason"] == "Unclassified assessment outcome"


def test_classify_assessment_adds_no_missing_categories() -> None:
    """Dataframe classification populates normalized columns for every row."""
    df = pd.DataFrame(
        [
            row(tiling_compatible=True, assessment_status="compatible"),
            row(
                failure_category="unsupported_asset",
                error_code="unsupported_media_type",
            ),
            row(
                assessment_status="inconclusive", failure_category="metadata_incomplete"
            ),
        ]
    )

    classified = classify_assessment(df)

    assert classified["report_category"].isna().sum() == 0
    assert classified["report_reason"].isna().sum() == 0


def test_schema_validation_requires_minimal_columns() -> None:
    """Missing required schema fields raise a clear validation error."""
    with pytest.raises(ValueError, match="tiling_compatible"):
        validate_assessment_schema(pd.DataFrame({"assessment_status": ["compatible"]}))


def test_missing_optional_columns_are_reported_not_failed() -> None:
    """Optional probe columns can be absent while validation still succeeds."""
    report = validate_assessment_schema(
        pd.DataFrame(
            {
                "tiling_compatible": [True],
                "assessment_status": ["compatible"],
            }
        )
    )

    assert not report["missing_required"]
    assert "failure_stage" in report["missing_optional"]


def test_full_assessment_schema_validates_cleanly() -> None:
    """The checked-in 2026 assessment has the required reporting schema."""
    path = Path("docs/compatibility/assessment-results-2026-07-01.parquet")
    if not path.exists():
        pytest.skip("assessment parquet is not available")

    df = load_assessment_results(path)
    report = validate_assessment_schema(df)

    assert not report["missing_required"]
    assert classify_assessment(df)["report_category"].isna().sum() == 0


def test_generate_table_includes_normalized_categories(tmp_path: Path) -> None:
    """The searchable table renders normalized category fields and status counts."""
    output_path = tmp_path / "results_table.html"
    df = pd.DataFrame(
        [
            {
                "collection_short_name_and_version": "A v1",
                "data_center": "NSIDC",
                "backend": "xarray",
                "format": "HDF5",
                "extension": ".h5",
                "tiling_compatible": True,
                "assessment_status": "compatible",
            },
            {
                "collection_short_name_and_version": "B v1",
                "data_center": "LP DAAC",
                "backend": "rasterio",
                "format": "HDF5",
                "extension": ".h5",
                "tiling_compatible": False,
                "assessment_status": "inconclusive",
                "failure_category": "metadata_incomplete",
                "error_code": "no_granule_found",
            },
        ]
    )

    generate_searchable_table(dataframe=df, output_path=output_path)
    html = output_path.read_text()

    assert "report_category" in html
    assert "Metadata gap" in html
    assert "Inconclusive datasets:</strong> 1" in html


def test_generate_table_skips_missing_optional_columns(tmp_path: Path) -> None:
    """Table generation tolerates minimal new-schema fixtures."""
    output_path = tmp_path / "results_table.html"
    df = pd.DataFrame([{"tiling_compatible": True, "assessment_status": "compatible"}])

    generate_searchable_table(dataframe=df, output_path=output_path)

    assert output_path.exists()
