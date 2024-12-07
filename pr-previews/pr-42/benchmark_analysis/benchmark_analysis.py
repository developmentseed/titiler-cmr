"""Functions for processing and plotting the time series benchmark results"""

import json
from collections import defaultdict
from typing import Any, Dict, List

import pandas as pd
import plotly.express as px


def process_benchmark_data() -> Dict[str, Dict[str, pd.DataFrame]]:
    """Read the benchmark json and convert into a dictionary of dataframes"""
    with open("../benchmark.json", "r") as f:
        benchmarks = json.load(f)["benchmarks"]

    records: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for bench in benchmarks:
        dataset = bench["extra_info"].pop("concept_config_id")
        record = {
            "mean_time": bench["stats"]["mean"],
            "stddev": bench["stats"]["stddev"],
            **bench["extra_info"],
            **bench["params"],
        }
        record.update(bench["params"])
        records[bench["group"]][dataset].append(record)

    dfs: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)
    for group, dataset_records in records.items():
        for dataset, _records in dataset_records.items():
            df = pd.DataFrame(_records).sort_values(by="bbox_size")

            bbox_dims = sorted(
                df["bbox_dims"].unique(), key=lambda x: float(x.split("x")[0]) * -1
            )

            df["bbox_dims"] = pd.Categorical(
                df["bbox_dims"], categories=bbox_dims, ordered=True
            )

            dfs[group][dataset] = df

    return dfs


def plot_line_with_error_bars(df: pd.DataFrame, **kwargs):
    """Create line plot with vertical error bars"""
    fig = px.line(
        df,
        x="num_timepoints",
        y="mean_time",
        error_y="stddev",
        labels={
            "mean_time": "mean response time (seconds)",
            "num_timepoints": "number of points in time series",
        },
        **kwargs,
    )

    return fig


def plot_error_rate_heatmap(
    df: pd.DataFrame,
    x: str,
    y: str,
    z: str,
    labels: Dict[str, str],
    title: str,
):
    """Plot error rate as a heatmap"""
    # Create the pivot table for heatmap
    data = df[[x, y, z]].pivot(index=y, columns=x, values=z)

    # Create the faceted heatmap using plotly
    fig = px.imshow(
        data,
        labels=labels,
        title=title,
    )

    return fig


# Load and process the data
dfs = process_benchmark_data()
