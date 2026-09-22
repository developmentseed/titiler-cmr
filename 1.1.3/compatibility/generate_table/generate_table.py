#!/usr/bin/env python3
"""Generate an HTML table from compatibility assessment parquet results."""

from __future__ import annotations

import importlib
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

try:
    reporting = importlib.import_module("docs.compatibility.reporting")
except (
    ModuleNotFoundError
):  # pragma: no cover - supports running from docs/compatibility
    reporting = importlib.import_module("reporting")

DEFAULT_ASSESSMENT_PATH = reporting.DEFAULT_ASSESSMENT_PATH
classify_assessment = reporting.classify_assessment

LOGGER = logging.getLogger(__name__)

DISPLAY_COLUMNS = [
    "collection_short_name_and_version",
    "data_center",
    "backend",
    "format",
    "extension",
    "tiling_compatible",
    "assessment_status",
    "report_status",
    "report_category",
    "report_reason",
    "report_reason_detail",
    "error_code",
    "failure_stage",
    "failure_category",
    "failure_subcategory",
    "failure_http_status_code",
    "processing_level",
    "num_granules",
]

FILTER_COLUMNS = [
    "report_category",
    "report_reason",
    "assessment_status",
    "data_center",
    "backend",
    "tiling_compatible",
]


def generate_searchable_table(
    input_path: str | Path = DEFAULT_ASSESSMENT_PATH,
    output_path: str | Path | None = None,
    *,
    dataframe: pd.DataFrame | None = None,
) -> Path:
    """Convert assessment results to a searchable DataTables HTML page."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

    if dataframe is None:
        LOGGER.info("Reading compatibility assessment from %s", input_path)
        df = pd.read_parquet(input_path)
    else:
        df = dataframe.copy()

    df = classify_assessment(df)

    if "collection_short_name_and_version" in df.columns:
        df = df[df["collection_short_name_and_version"].notna()]

    available_columns = [col for col in DISPLAY_COLUMNS if col in df.columns]
    display_df = df[available_columns].copy()
    records = _json_records(display_df)
    columns = [
        {"data": col, "title": col.replace("_", " ").title()}
        for col in available_columns
    ]
    counts = _status_counts(df)

    output = (
        Path(output_path)
        if output_path is not None
        else Path(__file__).with_name("results_table.html")
    )
    output.write_text(_render_html(records, columns, counts), encoding="utf-8")
    LOGGER.info("Generated %s", output)
    return output


def _status_counts(df: pd.DataFrame) -> dict[str, Any]:
    total = len(df)
    compatible = int(df["tiling_compatible"].fillna(False).sum()) if total else 0
    status_counts = df.get(
        "assessment_status", pd.Series(dtype="object")
    ).value_counts()
    inconclusive = int(status_counts.get("inconclusive", 0))
    incompatible = int(status_counts.get("incompatible", 0))
    rate = compatible / total * 100 if total else 0
    return {
        "total": total,
        "compatible": compatible,
        "incompatible": incompatible,
        "inconclusive": inconclusive,
        "rate": rate,
    }


def _json_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    sanitized = df.astype(object).where(pd.notna(df), None)
    return sanitized.to_dict("records")


def _render_html(
    records: list[dict[str, Any]],
    columns: list[dict[str, str]],
    counts: dict[str, Any],
) -> str:
    filter_columns = json.dumps(FILTER_COLUMNS)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TiTiler-CMR Dataset Compatibility Results</title>
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.7/css/jquery.dataTables.min.css">
    <style>
        body {{
            font-family: 'Roboto', Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1600px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #ff5722;
            padding-bottom: 10px;
        }}
        .info-box {{
            background-color: #e3f2fd;
            border-left: 4px solid #2196f3;
            padding: 15px;
            margin: 20px 0;
            border-radius: 4px;
        }}
        table.dataTable {{
            width: 100% !important;
            font-size: 13px;
        }}
        .dataTables_wrapper {{
            margin-top: 20px;
        }}
        .compatible-true {{
            color: #2e7d32;
            font-weight: bold;
        }}
        .compatible-false {{
            color: #c62828;
            font-weight: bold;
        }}
        .status-inconclusive {{
            color: #ef6c00;
            font-weight: bold;
        }}
        .back-link {{
            display: inline-block;
            margin-bottom: 20px;
            color: #2196f3;
            text-decoration: none;
            font-weight: 500;
        }}
        .back-link:hover {{
            text-decoration: underline;
        }}
        select {{
            display: block;
            max-width: 180px;
            margin-top: 4px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <a href="index.html" class="back-link">&larr; Back to Compatibility Report</a>

        <h1>TiTiler-CMR Dataset Compatibility Results</h1>

        <div class="info-box">
            <strong>Total datasets:</strong> {counts["total"]}<br>
            <strong>Compatible datasets:</strong> {counts["compatible"]}<br>
            <strong>Incompatible datasets:</strong> {counts["incompatible"]}<br>
            <strong>Inconclusive datasets:</strong> {counts["inconclusive"]}<br>
            <strong>Compatibility rate:</strong> {counts["rate"]:.2f}%
        </div>

        <p>Use the search box and column filters below to explore the dataset compatibility results.
        The table includes raw assessment fields and normalized report category fields derived from
        the 2026-07-01 assessment schema.</p>

        <table id="resultsTable" class="display" style="width:100%">
        </table>
    </div>

    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
    <script>
        const data = {json.dumps(records, default=str, allow_nan=False)};
        const columns = {json.dumps(columns)};
        const filterColumns = {filter_columns};

        $(document).ready(function() {{
            $('#resultsTable').DataTable({{
                data: data,
                columns: columns,
                pageLength: 25,
                order: [[0, 'asc']],
                deferRender: true,
                columnDefs: [
                    {{
                        targets: columns.findIndex(col => col.data === 'tiling_compatible'),
                        render: function(data, type, row) {{
                            if (type === 'display') {{
                                const className = data ? 'compatible-true' : 'compatible-false';
                                return '<span class="' + className + '">' + data + '</span>';
                            }}
                            return data;
                        }}
                    }},
                    {{
                        targets: columns.findIndex(col => col.data === 'assessment_status'),
                        render: function(data, type, row) {{
                            if (type === 'display' && data === 'inconclusive') {{
                                return '<span class="status-inconclusive">' + data + '</span>';
                            }}
                            return data;
                        }}
                    }},
                    {{
                        targets: '_all',
                        render: function(data, type, row) {{
                            if (data === null || data === undefined || data === '') {{
                                return type === 'display' ? '<em style="color: #999;">N/A</em>' : '';
                            }}
                            return data;
                        }}
                    }}
                ],
                initComplete: function() {{
                    this.api().columns().every(function() {{
                        const column = this;
                        if (filterColumns.includes(column.dataSrc())) {{
                            const select = $('<select><option value="">All</option></select>')
                                .appendTo($(column.header()))
                                .on('change', function() {{
                                    const val = $.fn.dataTable.util.escapeRegex($(this).val());
                                    column.search(val ? '^' + val + '$' : '', true, false).draw();
                                }});

                            column.data().unique().sort().each(function(d) {{
                                if (d !== null && d !== undefined && d !== '') {{
                                    select.append('<option value="' + d + '">' + d + '</option>');
                                }}
                            }});
                        }}
                    }});
                }}
            }});
        }});
    </script>
</body>
</html>"""


if __name__ == "__main__":
    generate_searchable_table()
