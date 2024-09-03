import matplotlib.pyplot as plt
from typing import Union
import os
import numpy as np
from parse_logs import parse_logs_by_min


def plot(data: list[dict[str, dict[int, list[Union[int, float]]]]], output_dir: str):
    plt.figure(figsize=(10, 6))

    all_cumulative_events = []
    all_latencies = []

    for sim in data:
        # Extract events and latencies
        events = sim["events"]
        latencies = sim["latencies"]

        # Calculate cumulative events
        cumulative_events = np.cumsum(list(events.values()))

        # Accumulate valid data points for scatter plot
        for minute, latency in latencies.items():
            if minute in events:
                all_cumulative_events.append(cumulative_events[minute])
                all_latencies.append(latency)

    # Create a single scatter plot with all valid data points
    plt.scatter(all_cumulative_events, all_latencies, alpha=0.7, label="Data Points")

    # Perform linear regression to get the trend line
    if all_cumulative_events and all_latencies:  # Ensure lists are not empty
        coefficients = np.polyfit(all_cumulative_events, all_latencies, 1)
        poly = np.poly1d(coefficients)
        trend_line = poly(np.unique(all_cumulative_events))

        # Plot the trend line
        plt.plot(
            np.unique(all_cumulative_events),
            trend_line,
            color="red",
            label="Linear Fit",
        )

    plt.title("Latency vs. Cumulative Number of Events (All Experiments Combined)")
    plt.xlabel("Cumulative Number of Events")
    plt.ylabel("Median Latency (ms)")
    plt.legend()
    plt.grid(True)
    plt.savefig(f"{output_dir}/latency_cumulative.png")
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


dir = "/Users/krispian/Uni/bachelorarbeit/topologies_delayed_systematic_inflation"
# query = "AND_ABC"

time_minutes = np.arange(0, 10)
res = []

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
            log_dir_0 = (
                f"{topology_dir}/trace_inflated_mix_{percentage}/output_strategy_0"
            )

            n = int(node_n.split("_")[-1].strip())
            events_per_simulation, latencies_per_simulation = parse_logs_by_min(
                log_dir_0, n
            )
            res.append(
                {"events": events_per_simulation, "latencies": latencies_per_simulation}
            )

plot(res, dir)
