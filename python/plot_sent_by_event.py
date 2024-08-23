from typing import Optional
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from parse_logs import (
    parse_logs,
    parse_args,
    parse_events_arg,
)
import matplotlib.ticker as ticker


def plot(df0, df1, query: str, node_n: str, output_dir: str):
    interval = 60

    def prepare_data(df):
        # Round timestamps to the nearest n seconds to group events in n-second intervals
        df["timestamp"] = df["timestamp"].dt.floor("10s")

        # Group by timestamp and event_type, and sum counts
        df = df.groupby(["timestamp", "event_type"]).size().unstack(fill_value=0)

        # Generate a complete time index covering the full 10 minutes in 10-second intervals
        full_time_index = pd.date_range(
            start=df.index.min().floor("min"),
            end=df.index.min().floor("min") + pd.Timedelta(minutes=10),
            freq=f"{interval}s",
        )

        # Reindex the dataframe to include all n-second intervals
        df = df.reindex(full_time_index, fill_value=0)
        return df

    # Prepare data for both scenarios
    df0 = prepare_data(df0)
    df1 = prepare_data(df1)

    # Calculate the maximum y-axis value across both dataframes
    max_y = max(df0.max().max(), df1.max().max())

    # Create a figure with two subplots side by side
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))

    # Add a title to the entire plot
    fig.suptitle(f"Node num: {node_n}, Query: {query}", fontsize=16)

    def format_func(x, min_timestamp):
        # Calculate the elapsed time in minutes from the start_time
        elapsed_seconds = (x - mdates.date2num(min_timestamp)) * 86400
        return int(elapsed_seconds // 60)

    # Plotting for the first scenario (Adaptive Strategy Disabled)
    for event_type in df0.columns:
        axes[0].plot(df0.index, df0[event_type], label=event_type, marker="o")

    axes[0].set_title("Adaptive Strategy Disabled")
    axes[0].set_ylabel("Event Count")
    axes[0].set_xlabel("Minutes Elapsed")
    axes[0].xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
    axes[0].xaxis.set_minor_locator(mdates.SecondLocator(interval=interval))
    axes[0].set_ylim(0, max_y + 10)  # Set y-axis limits with a little extra space

    axes[0].xaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, _: f"{format_func(x, df0.index.min())}")
    )
    axes[0].legend(title="Event Type")
    axes[0].grid(True, which="both", linestyle="--", linewidth=0.5)

    # Plotting for the second scenario (Adaptive Strategy Enabled)
    for event_type in df1.columns:
        axes[1].plot(df1.index, df1[event_type], label=event_type, marker="o")

    axes[1].set_title("Adaptive Strategy Enabled")
    axes[1].set_ylabel("Event Count")
    axes[1].set_xlabel("Minutes Elapsed")
    axes[1].xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
    axes[1].xaxis.set_minor_locator(mdates.SecondLocator(interval=interval))
    axes[1].set_ylim(0, max_y + 10)  # Set y-axis limits with a little extra space

    axes[1].xaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, _: f"{format_func(x, df1.index.min())}")
    )
    axes[1].legend(title="Event Type")
    axes[1].grid(True, which="both", linestyle="--", linewidth=0.5)

    plt.tight_layout()
    plt.savefig(f"{output_dir}/transmission_rates_per_event.png")
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
    res_no_node_id0 = [el[1:] for el in res0]
    res_no_node_id1 = [el[1:] for el in res1]

    df0 = pd.DataFrame(res_no_node_id0, columns=["event_type", "timestamp"])
    df1 = pd.DataFrame(res_no_node_id1, columns=["event_type", "timestamp"])

    # Filter the DataFrames by the specified event types
    if events:
        df0 = df0[df0["event_type"].isin(events)]
        df1 = df1[df1["event_type"].isin(events)]

    plot(df0, df1, query, node_n, output_dir)


if __name__ == "__main__":
    args = parse_args()
    event_types = parse_events_arg(args.events)
    assert args.node_n
    assert args.node_n.isdigit()

    assert args.query

    main(args.dir0, args.dir1, args.output_dir, args.query, args.node_n, event_types)
