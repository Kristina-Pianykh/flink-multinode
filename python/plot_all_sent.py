import argparse
import os
from typing import Optional
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
from datetime import datetime
from parse_logs import (
    parse_logs,
    parse_args,
    parse_events_arg,
)


def plot(df0, df1, output_dir: str, query: str, node_n: str):
    interval = 30

    # Round timestamps to the nearest n seconds to group events in n-second intervals
    df0["timestamp"] = df0["timestamp"].dt.floor(f"{interval}s")
    df1["timestamp"] = df1["timestamp"].dt.floor(f"{interval}s")

    # Align the start time of both datasets to the minimum of their start times
    common_start_time = min(df0["timestamp"].min(), df1["timestamp"].min())
    df0["timestamp"] = df0["timestamp"] - (df0["timestamp"].min() - common_start_time)
    df1["timestamp"] = df1["timestamp"] - (df1["timestamp"].min() - common_start_time)

    # Group by timestamp and sum counts for all event types
    df0 = df0.groupby("timestamp").size()
    df1 = df1.groupby("timestamp").size()

    # Generate a complete time index covering the full range in n-second intervals
    full_time_index = pd.date_range(
        start=common_start_time.floor("min"),
        end=common_start_time.floor("min") + pd.Timedelta(minutes=10),
        freq=f"{interval}s",
    )

    # Reindex both dataframes to the common time index
    df0 = df0.reindex(full_time_index, fill_value=0)
    df1 = df1.reindex(full_time_index, fill_value=0)

    # Plotting the total event count for both datasets
    plt.figure(figsize=(15, 6))  # Adjusted width for better readability

    plt.plot(
        df0.index, df0, label="Total Events (No Strategy)", marker="o", color="blue"
    )
    plt.plot(
        df1.index, df1, label="Total Events (With Strategy)", marker="o", color="orange"
    )

    plt.title(f"Total Event Count Per {interval} Seconds")
    plt.ylabel("Event Count")
    plt.xlabel("Time (HH:MM:SS)")

    # Set x-axis major ticks to show each minute
    plt.gca().xaxis.set_major_locator(mdates.MinuteLocator(interval=1))

    # Set minor ticks for every n seconds
    plt.gca().xaxis.set_minor_locator(mdates.SecondLocator(interval=interval))

    # Adjust the x-axis labels to display time starting from 00:00:00
    def format_func(x, _):
        # Calculate the elapsed time in seconds from the start_time
        elapsed_seconds = (x - mdates.date2num(full_time_index.min())) * 86400
        # Format the elapsed time as HH:MM:SS
        return pd.Timestamp("00:00:00") + pd.to_timedelta(elapsed_seconds, unit="s")

    plt.gca().xaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, _: format_func(x, _).strftime("%H:%M:%S"))
    )

    plt.legend(title="Event Source")
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)

    # Add a title to the entire plot
    plt.suptitle(f"Node num: {node_n}, Query: {query}", fontsize=16)

    plt.tight_layout(
        rect=[0, 0, 1, 0.95]
    )  # Adjust layout to make space for the suptitle
    plt.savefig(f"{output_dir}/transmission_rates.png")
    plt.show()


def main(
    dir0: str,
    dir1: str,
    output_dir: str,
    query: str,
    node_n: str,
    events: Optional[list[str]],
):
    res0: list[tuple[int, str, datetime]] = parse_logs(dir0)
    res1: list[tuple[int, str, datetime]] = parse_logs(dir1)
    # assert all(len(el[1]) == 1 for el in res0)
    # assert all(len(el[1]) == 1 for el in res1)

    # Convert parsed results into a DataFrame for easier manipulation
    df0 = pd.DataFrame(res0, columns=["node_id", "event_type", "timestamp"])
    df1 = pd.DataFrame(res1, columns=["node_id", "event_type", "timestamp"])

    # Filter the DataFrame by the specified event types
    if events:
        df0 = df0[df0["event_type"].isin(events)]
        df1 = df1[df1["event_type"].isin(events)]

    plot(df0, df1, output_dir, query, node_n)


if __name__ == "__main__":
    args = parse_args()
    print(args.dir0)
    print(args.dir1)
    print(args.output_dir)
    event_types = parse_events_arg(args.events)
    assert args.node_n
    assert args.node_n.isdigit()

    assert args.query
    main(args.dir0, args.dir1, args.output_dir, args.query, args.node_n, event_types)
