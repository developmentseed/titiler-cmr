#!/usr/bin/env python3
"""Generate HTML table from parquet file for mkdocs site."""

import json
import pandas as pd
from pathlib import Path


def generate_searchable_table():
    """Convert parquet to JSON and generate HTML with DataTables."""

    # Read the parquet file
    df = pd.read_parquet("tiling_results-11-24-2025.parquet")

    # Filter out rows with n/a collection short name and version
    df = df[df["collection_short_name_and_version"].notna()]

    # Select key columns for display
    display_columns = [
        "collection_short_name_and_version",
        "data_center",
        "format",
        "extension",
        "tiling_compatible",
        "incompatible_reason",
        "processing_level",
        "num_granules",
    ]

    # Filter to only include columns that exist
    available_columns = [col for col in display_columns if col in df.columns]
    display_df = df[available_columns]

    # Convert to records for JSON serialization
    records = display_df.to_dict("records")

    # Create column definitions for DataTables
    columns = [
        {"data": col, "title": col.replace("_", " ").title()}
        for col in available_columns
    ]

    # Generate HTML
    html_content = f"""<!DOCTYPE html>
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
            max-width: 1400px;
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
            font-size: 14px;
        }}
        .dataTables_wrapper {{
            margin-top: 20px;
        }}
        .compatible-true {{
            color: #4caf50;
            font-weight: bold;
        }}
        .compatible-false {{
            color: #f44336;
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
    </style>
</head>
<body>
    <div class="container">
        <a href="index.html" class="back-link">&larr; Back to Compatibility Report</a>

        <h1>TiTiler-CMR Dataset Compatibility Results</h1>

        <div class="info-box">
            <strong>Total datasets:</strong> {len(df)}<br>
            <strong>Compatible datasets:</strong> {len(df[df.tiling_compatible])}<br>
            <strong>Compatibility rate:</strong> {len(df[df.tiling_compatible]) / len(df) * 100:.2f}%
        </div>

        <p>Use the search box and column filters below to explore the dataset compatibility results.
        Click on column headers to sort.</p>

        <table id="resultsTable" class="display" style="width:100%">
        </table>
    </div>

    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
    <script>
        const data = {json.dumps(records, default=str)};
        const columns = {json.dumps(columns)};

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
                        // Handle null values for all columns
                        targets: '_all',
                        render: function(data, type, row) {{
                            if (data === null || data === undefined) {{
                                return type === 'display' ? '<em style="color: #999;">N/A</em>' : '';
                            }}
                            return data;
                        }}
                    }}
                ],
                initComplete: function() {{
                    // Add column filters
                    this.api().columns().every(function() {{
                        const column = this;
                        const header = $(column.header());

                        // Add filter for specific columns
                        const filterColumns = ['data_center', 'format', 'tiling_compatible', 'incompatible_reason'];
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

    # Write the HTML file
    output_path = Path("results_table.html")
    output_path.write_text(html_content)
    print(f"Generated {output_path}")


if __name__ == "__main__":
    generate_searchable_table()
