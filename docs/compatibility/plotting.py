"""
Utilities for plotting
"""

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np


def create_pie_chart(
    labels, sizes, title="Pie Chart", legend=False, top_n=-1, legend_position="best"
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
    # Generate colors based on number of items
    n = len(labels)
    colors = cm.Set3(np.linspace(0, 1, n))

    # Find the top N values - handle the case when top_n=-1
    if top_n == -1 or top_n >= n:
        top_indices = set(range(n))  # All indices
    else:
        top_indices = set(np.argsort(sizes)[::-1][:top_n])

    # Create the pie chart
    plt.figure(figsize=(8, 6))

    common_args = {
        "x": sizes,
        "colors": colors,
        "autopct": "%1.1f%%",
        "startangle": 90,
        "textprops": {"fontsize": 11},
        "labels": labels,
    }

    _, texts, autotexts = plt.pie(**common_args)

    if legend:
        # Add legend with labels and counts
        legend_labels = [f"{label}: {size}" for label, size in zip(labels, sizes)]
        plt.legend(
            legend_labels,
            loc=legend_position,
            bbox_to_anchor=(1, 1),
            fontsize=10,
            title="Counts",
        )

    # Hide percentages for non-top N categories
    for i, (text, autotext) in enumerate(zip(texts, autotexts)):
        if i not in top_indices:
            text.set_text("")
            autotext.set_text("")

    plt.title(title, fontsize=14, fontweight="bold")
    plt.axis("equal")
    plt.tight_layout()
    plt.show()
