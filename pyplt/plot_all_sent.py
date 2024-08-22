import argparse
import os
from typing import Optional
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator
import matplotlib.ticker as ticker
import numpy as np
from datetime import datetime


# Function to parse the timestamp string to a datetime object, ignoring milliseconds
def parse_timestamp(timestamp_str) -> datetime:
    return datetime.strptime(timestamp_str, "%H:%M:%S:%f")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dir0", type=str, help="Log dir with *.log files with NO strategy enabled"
    )
    parser.add_argument(
        "--dir1", type=str, help="Log dir with *.log files with strategy enabled"
    )
    parser.add_argument("--output_dir", type=str, help="Output dir for saving the plot")
    parser.add_argument("--events", type=str, help="Event types to plot")
    args = parser.parse_args()

    assert args.dir0
    assert os.path.exists(args.dir0)
    if args.dir0.endswith("/"):
        args.dir0 = args.dir0[:-1]

    assert args.dir1
    assert os.path.exists(args.dir1)
    if args.dir1.endswith("/"):
        args.dir1 = args.dir1[:-1]

    assert args.output_dir
    assert os.path.exists(args.output_dir)
    if args.output_dir.endswith("/"):
        args.output_dir = args.output_dir[:-1]

    assert args.events
    assert isinstance(args.events, str)

    return args


def parse_events_arg(
    events: str,
) -> list[str]:  # events are passed as a cli arg in the form "A,B,C"
    return [event.strip() for event in events.split(";")]


def extract_event_type(line: str) -> str:
    res = [el.strip() for el in line.split("|")][3]
    # print(res)
    # assert len(res) == 1
    # assert all([not el.isdigit() for el in res])
    # assert res.isalpha()
    return res


def extract_timestamp(line: str) -> str:
    res = [el.strip() for el in line.split("|")][2]
    # print(res)
    assert ":" in res
    assert all([el.strip().isdigit() for el in res.split(":")])
    return res


def parse_log(log_file_path: str, node_id: int) -> list[tuple[int, str, datetime]]:
    event_timestamp_lst: list[tuple[int, str, datetime]] = []
    f = open(log_file_path, "r")
    for line in f:
        if "sendTo():" in line:
            event = extract_event_type(line)
            timestamp = parse_timestamp(extract_timestamp(line))
            event_timestamp_lst.append((node_id, event, timestamp))

    return event_timestamp_lst


def parse_logs(dir: str) -> list[tuple[int, str, datetime]]:
    res: list[tuple[int, str, datetime]] = []
    max_num_nodes = 20

    for i in range(max_num_nodes):
        log_file = f"{dir}/{i}.log"

        try:
            assert os.path.exists(log_file)
        except AssertionError:
            print(f"Warning: file {log_file} doesn't exist")
            break

        event_timestamp_lst = parse_log(log_file, i)
        print(f"sent events on node {i}: {len(event_timestamp_lst)}")
        res.extend(event_timestamp_lst)

    print(f"sent total events: {len(res)}")
    return res


def plot(df0, df1, output_dir: str):
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
    plt.tight_layout()
    # Save the plot if a save path is provided

    plt.savefig(f"{output_dir}/transmission_rates.png")
    plt.show()


def main(dir0: str, dir1: str, output_dir: str, events: Optional[list[str]]):
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

    plot(df0, df1, output_dir)


if __name__ == "__main__":
    args = parse_args()
    print(args.dir0)
    print(args.dir1)
    print(args.output_dir)
    event_types = parse_events_arg(args.events)
    main(args.dir0, args.dir1, args.output_dir, event_types)
