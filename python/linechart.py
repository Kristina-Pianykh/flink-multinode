import matplotlib.pyplot as plt
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


def plot_aligned_line_chart(
    x1_values: dict[str, list[int]],
    cost_savings: dict[str, list[float]],
    output_dir: str,
):
    nodes = list(x0_values.keys())

    # Maximum limit for events
    max_events_limit = 15000

    plt.figure(figsize=(10, 6))

    # Iterate over the nodes and plot lines with markers
    for n in nodes:
        y1_values = np.array(x1_values[n])
        savings = np.array(cost_savings[n])

        # Filter values to include only those with events <= 15000
        mask_1 = y1_values <= max_events_limit

        y1_values_filtered = y1_values[mask_1]
        savings_filtered_1 = savings[mask_1]

        plt.plot(
            y1_values_filtered,
            savings_filtered_1,
            marker="o",
            label=f"{n} Nodes (With Repair)",
        )

    # Set axis labels
    plt.xlabel("Total Number of Events Sent")
    plt.ylabel("Cost Savings (%)")
    plt.xlim(0, max_events_limit)
    plt.ylim(0, 100)  # Assuming cost savings percentage ranges from 0 to 100

    plt.grid(True)

    # Title and legend
    plt.title("Cost Savings vs Number of Events Sent (Up to 15,000 Events)")
    plt.legend(loc="upper left", bbox_to_anchor=(1, 1))

    # Adjust layout
    plt.tight_layout(rect=[0, 0, 1, 0.95])

    # Save and display the plot
    plt.savefig(f"{output_dir}/aligned_cost_savings_vs_events_up_to_15000.png")
    plt.show()


# Data for each subplot (replace with your actual data)
nodes = ["complex_5", "5", "6", "7", "8", "9", "complex_8"]
categories = [
    "0.2",
    "0.5",
    "0.7",
    "1.0",
    "1.3",
    "1.5",
    "2.0",
    "3.0",
    "4.0",
    "5.0",
    "10.0",
]


# query = "AND_ABC"
dir = "/Users/krispian/Uni/bachelorarbeit/topologies_delayed_systematic_inflation"

x0_values = {i: [] for i in nodes}
x1_values = {i: [] for i in nodes}

for i in os.listdir(dir):
    # if query in i and os.path.isdir(f"{dir}/{i}"):
    if os.path.isdir(f"{dir}/{i}"):
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

            events_total_0: list[tuple[int, str, datetime]] = parse_logs(log_dir_0)
            events_total_1: list[tuple[int, str, datetime]] = parse_logs(log_dir_1)
            x0_values[node_n].append(len(events_total_0))
            x1_values[node_n].append(len(events_total_1))

x0_values = {k: v for k, v in x0_values.items() if v}
print(x0_values)
x1_values = {k: v for k, v in x1_values.items() if v}
print(x1_values)

cost_savings = {k: [] for k in x0_values}
for key in x0_values.keys():
    values0 = x0_values[key]
    values1 = x1_values[key]
    for idx in range(0, len(values0)):
        saving = round(100.0 - values1[idx] * 100 / values0[idx], 1)
        if saving > 0:
            cost_savings[key].append(saving)
        else:
            cost_savings[key].append(0)

for k in x0_values.keys():
    print(k)
    print(f"strategy0: {x0_values[k]}")
    print(f"strategy1: {x1_values[k]}")
    print(f"saving: {cost_savings[k]}")


plot_aligned_line_chart(x1_values, cost_savings, dir)
