import math
import argparse
import os
from typing import Optional
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
from parse_logs import (
    parse_logs,
    parse_events_arg,
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=str, help="Log dir with *.log files to parse")
    parser.add_argument("--output_dir", type=str, help="Output dir for saving the plot")
    parser.add_argument("--events", type=str, help="Event types to plot")
    args = parser.parse_args()

    assert args.output_dir
    assert os.path.exists(args.output_dir)
    if args.output_dir.endswith("/"):
        args.output_dir = args.output_dir[:-1]

    assert args.dir
    assert os.path.exists(args.dir)
    if args.dir.endswith("/"):
        args.dir = args.dir[:-1]

    assert args.events
    assert isinstance(args.events, str)

    return args


def format_func(x, min_timestamp):
    # Calculate the elapsed time in seconds from the start_time
    elapsed_seconds = (x - mdates.date2num(min_timestamp)) * 86400
    # Format the elapsed time as HH:MM:SS
    return pd.Timestamp("00:00:00") + pd.to_timedelta(elapsed_seconds, unit="s")


def plot(dfs: dict[int, pd.DataFrame], output_dir: str, interval: int = 60):
    num_nodes = len(dfs)
    num_cols = 3
    num_rows = math.ceil(num_nodes / num_cols)

    fig, axes = plt.subplots(num_rows, num_cols, figsize=(15, 10))

    # Flatten the axes array for easier iteration
    axes = axes.flatten()

    for idx, (node_id, df) in enumerate(dfs.items()):
        df["timestamp"] = df["timestamp"].dt.floor("10s")
        df = df.groupby(["timestamp", "event_type"]).size().unstack(fill_value=0)
        full_time_index = pd.date_range(
            start=df.index.min().floor("min"),
            end=df.index.min().floor("min") + pd.Timedelta(minutes=10),
            freq=f"{interval}s",
        )
        df = df.reindex(full_time_index, fill_value=0)

        ax = axes[idx]
        for event_type in df.columns:
            ax.plot(df.index, df[event_type], label=event_type, marker="o")

        ax.set_title(f"Node {node_id}")
        ax.set_ylabel("Event Count")
        ax.grid(True, which="both", linestyle="--", linewidth=0.5)

        # Ensure start_time is timezone-naive
        start_time = df.index.min().replace(tzinfo=None)

        # Set major ticks to every minute
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
        ax.xaxis.set_minor_locator(mdates.SecondLocator(interval=interval))

        # Correctly format the x-axis to show minutes elapsed from the start
        ax.xaxis.set_major_formatter(
            ticker.FuncFormatter(
                lambda x,
                pos: f"{int((mdates.num2date(x).replace(tzinfo=None) - start_time).total_seconds() // 60):.0f}"
            )
        )

        ax.legend(title="Event Type")

    # Hide any unused subplots
    for i in range(len(dfs), len(axes)):
        fig.delaxes(axes[i])

    plt.tight_layout()
    plt.savefig(f"{output_dir}/transmission_rates_per_event_per_node.png")
    # plt.show()


def main(dir: str, output_dir: str, events: Optional[list[str]]):
    dfs: dict[int, pd.DataFrame] = parse_logs(dir)

    # Filter the DataFrame by the specified event types
    if events:
        for node_id, df in dfs.items():
            dfs[node_id] = df[df["event_type"].isin(events)]

    plot(dfs, output_dir)


if __name__ == "__main__":
    args = parse_args()
    print(args.dir)
    event_types = parse_events_arg(args.events)
    main(args.dir, args.output_dir, event_types)
