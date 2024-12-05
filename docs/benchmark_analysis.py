"""Functions for processing and plotting the time series benchmark results"""

import json
from collections import defaultdict
from typing import Dict

import pandas as pd
import plotly.express as px


def process_benchmark_data() -> Dict[str, pd.DataFrame]:
    """Read the benchmark json and convert into a dictionary of dataframes"""
    with open("../benchmark.json", "r") as f:
        benchmarks = json.load(f)["benchmarks"]

    records = defaultdict(list)
    for bench in benchmarks:
        record = {
            "mean_time": bench["stats"]["mean"],
            "stddev": bench["stats"]["stddev"],
            **bench["extra_info"],
            **bench["params"],
        }
        record.update(bench["params"])
        records[bench["group"]].append(record)

    dfs = {
        group: pd.DataFrame(records).sort_values(by="bbox_size")
        for group, records in records.items()
    }

    for group, df in dfs.items():
        bbox_dims = sorted(
            df["bbox_dims"].unique(), key=lambda x: float(x.split("x")[0]) * -1
        )

        df["bbox_dims"] = pd.Categorical(
            df["bbox_dims"], categories=bbox_dims, ordered=True
        )

        dfs[group] = df

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
