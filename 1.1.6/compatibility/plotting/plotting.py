"""
Utilities for plotting
"""

import matplotlib.pyplot as plt
import numpy as np


def create_pie_chart(
    labels: list[str],
    sizes: list[float],
    title: str = "Pie Chart",
    legend: bool = False,
    top_n: int = -1,
    legend_position: str = "best",
):
    """
    Create a pie chart with automatic color generation

    Parameters:
    labels: list of category names
    sizes: list of values/counts
    title: chart title (optional)
    legend: whether to show legend
    top_n: number of top categories to show percentages on (-1 for all)
    """
    # Generate distinct categorical colors. Set3 only has 12 colors, so sampling
    # it for larger pies repeats adjacent-looking colors. Interleave tab20 so
    # neighboring slices stay visually distinct for charts with many categories.
    n = len(labels)
    tab20 = plt.get_cmap("tab20").colors
    color_order = list(range(0, len(tab20), 2)) + list(range(1, len(tab20), 2))
    colors = [tab20[color_order[i % len(tab20)]] for i in range(n)]

    # Find the top N values - handle the case when top_n=-1
    if top_n == -1 or top_n >= n:
        top_indices = set(range(n))  # All indices
    else:
        top_indices = set(np.argsort(sizes)[::-1][:top_n])

    # Create the pie chart. When a legend is shown, avoid duplicating long labels
    # around the pie itself because tight layout can shrink the chart to make room.
    figure_width = 11 if legend else 8
    figure_height = max(6, n * 0.25) if legend else 6
    _, ax = plt.subplots(figsize=(figure_width, figure_height))

    common_args = {
        "x": sizes,
        "colors": colors,
        "autopct": "%1.1f%%",
        "startangle": 90,
        "textprops": {"fontsize": 11},
        "labels": None if legend else labels,
    }

    _, texts, autotexts = ax.pie(**common_args)

    if legend:
        # Add legend with labels and counts
        legend_labels = [f"{label}: {size}" for label, size in zip(labels, sizes)]
        legend_loc = "center left" if legend_position == "best" else legend_position
        ax.legend(
            legend_labels,
            loc=legend_loc,
            bbox_to_anchor=(1.02, 0.5),
            fontsize=10,
            title="Counts",
        )

    # Hide percentages for non-top N categories
    for i, (text, autotext) in enumerate(zip(texts, autotexts)):
        if i not in top_indices:
            text.set_text("")
            autotext.set_text("")

    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.axis("equal")
    plt.tight_layout()
    plt.show()
