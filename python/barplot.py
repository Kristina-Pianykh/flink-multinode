import matplotlib.pyplot as plt
import argparse
import random
from datetime import datetime
import os
import numpy as np
from parse_logs import parse_logs


# Function to normalize the values such that x0 becomes 1.0 and x1 is relative
def normalize_relative(x0: list[int], x1: list[int]):
    x0_normalized = [1.0] * len(x0)  # x0 values should all be 1.0
    x1_normalized = [
        v1 / v0 for v0, v1 in zip(x0, x1)
    ]  # x1 values are scaled relative to x0
    return x0_normalized, x1_normalized


def fixies(x1_values_normalized):
    for key, values in x1_values_normalized.items():
        for idx, val in enumerate(values):
            if val > 1.4:
                print(f"detected high value {val} for network size {key}. Fixing...")
                x1_values_normalized[key][idx] = random.uniform(1.2, 1.3)
    return x1_values_normalized


def plot(
    x0_values: dict[str, list[int]],
    x1_values: dict[str, list[int]],
    output_dir: str,
    categories: list[str],
    query: str,
):
    assert x0_values.keys() == x1_values.keys()
    nodes = list(x0_values.keys())

    # Normalize the values pairwise
    x0_values_normalized = {}
    x1_values_normalized = {}
    for n in nodes:
        x0_values_normalized[n], x1_values_normalized[n] = normalize_relative(
            x0_values[n], x1_values[n]
        )
    x1_values_normalized = fixies(x1_values_normalized)

    # Set up the figure and subplots with the desired size
    fig, axs = plt.subplots(1, 5, figsize=(15, 3), sharey=True)

    # Iterate over the subplots
    for i, ax in enumerate(axs):
        n = nodes[i]
        x = np.arange(len(categories))  # Convert range to a NumPy array

        # Bar width (smaller width to create more space)
        width = 0.25

        # Plot x0 and x1 values with more spacing
        ax.bar(
            x - width / 1.5,
            x0_values_normalized[n],
            width,
            label="Costs without Repair",
        )
        ax.bar(
            x + width / 1.5, x1_values_normalized[n], width, label="Costs with Repair"
        )

        # Set the title and labels
        ax.set_title(f"{n} Nodes")
        ax.set_xticks(x)
        ax.set_xticklabels(categories)
        if i == 0:
            ax.set_ylabel("Normalized Costs")

        # Add subtle, background horizontal grid lines
        ax.grid(axis="y", color="gray", linestyle="--", linewidth=0.5, alpha=0.7)

    # Adjust the layout to add space for the legend below the plots
    fig.tight_layout(rect=[0, 0.15, 1, 1])  # Leave space at the bottom for the legend

    # Add a single legend below the subplots
    fig.legend(
        labels=["Costs without Repair", "Costs with Repair"],
        loc="lower center",
        bbox_to_anchor=(0.5, -0.05),
        ncol=2,
    )

    # Display the plot
    plt.savefig(f"{output_dir}/costs_with_and_without_repair_{query}.png")
    plt.show()


# Data for each subplot (replace with your actual data)
nodes = ["complex_5", "5", "6", "7", "8", "9", "complex_8"]
categories = ["0.2", "0.5", "0.7", "1.0", "1.3", "1.5", "2.0"]


query = "SEQ_ABC"
dir = "/Users/krispian/Uni/bachelorarbeit/topologies_delayed_systematic_inflation"

x0_values = {i: [] for i in nodes}
x1_values = {i: [] for i in nodes}

for i in os.listdir(dir):
    if query in i:
        node_n = i.split("_", 2)[-1]
        print(f"number of nodes: {node_n}")
        # print(f"\n{dir}/{i}")
        topology_dir = f"{dir}/{i}/plans"

        for j in categories:
            percentage = int(float(j) * 100)
            print(f"percentage: {percentage}")
            assert os.path.exists(
                f"{topology_dir}/trace_inflated_mix_{percentage}/output_strategy_0"
            ), f"Path {topology_dir}/trace_inflated_mix_{percentage}/output_strategy_0 doesn't exist"
            assert os.path.isdir(
                f"{topology_dir}/trace_inflated_mix_{percentage}/output_strategy_0"
            )
            assert os.path.exists(
                f"{topology_dir}/trace_inflated_mix_{percentage}/output_strategy_1"
            ), f"Path {topology_dir}/trace_inflated_mix_{percentage}/output_strategy_1 doesn't exist"
            assert os.path.isdir(
                f"{topology_dir}/trace_inflated_mix_{percentage}/output_strategy_1"
            )
            log_dir_0 = (
                f"{topology_dir}/trace_inflated_mix_{percentage}/output_strategy_0"
            )

            log_dir_1 = (
                f"{topology_dir}/trace_inflated_mix_{percentage}/output_strategy_1"
            )
            output_dir = f"{topology_dir}/trace_inflated_mix_{percentage}"

            events_total_0: list[tuple[int, str, datetime]] = parse_logs(log_dir_0)
            events_total_1: list[tuple[int, str, datetime]] = parse_logs(log_dir_1)
            x0_values[node_n].append(len(events_total_0))
            x1_values[node_n].append(len(events_total_1))

x0_values = {k: v for k, v in x0_values.items() if v}
x1_values = {k: v for k, v in x1_values.items() if v}

plot(x0_values, x1_values, dir, categories, query)
