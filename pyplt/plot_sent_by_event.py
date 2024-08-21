import argparse
import os
from typing import Optional
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator
import numpy as np
from datetime import datetime


# Function to parse the timestamp string to a datetime object, ignoring milliseconds
def parse_timestamp(timestamp_str) -> datetime:
    return datetime.strptime(timestamp_str, "%H:%M:%S:%f")
    # return datetime.strptime(
    #     timestamp_str.split(":")[0]
    #     + ":"
    #     + timestamp_str.split(":")[1]
    #     + ":"
    #     + timestamp_str.split(":")[2],
    #     "%H:%M:%S",
    # )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=str, help="Log dir with *.log files to parse")
    parser.add_argument("--events", type=str, help="Event types to plot")
    args = parser.parse_args()

    assert args.dir
    assert os.path.exists(args.dir)
    if args.dir.endswith("/"):
        args.dir = args.dir[:-1]

    assert args.events
    assert isinstance(args.events, str)

    return args


def parse_events_arg(
    events: str,
) -> list[str]:  # events are passed as a cli arg in the form "A,B,C"
    return [event.strip() for event in events.split(",")]


def extract_event_type(line: str) -> str:
    res = [el.strip() for el in line.split("|")][3]
    # print(res)
    assert len(res) == 1
    assert res.isalpha()
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


import matplotlib.ticker as ticker


def plot(df):
    interval = 30

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

    # Plotting the data
    plt.figure(figsize=(15, 6))  # Adjusted width for better readability over 10 minutes

    for event_type in df.columns:
        plt.plot(df.index, df[event_type], label=event_type, marker="o")

    plt.title(f"Event Rates Per {interval} Seconds Over 10 Minutes")
    plt.ylabel("Event Count")
    plt.xlabel("Time (HH:MM:SS)")

    # Set x-axis major ticks to show each minute
    plt.gca().xaxis.set_major_locator(mdates.MinuteLocator(interval=1))

    # Set minor ticks for every n seconds
    plt.gca().xaxis.set_minor_locator(mdates.SecondLocator(interval=interval))

    # Adjust the x-axis labels to display time starting from 00:00:00
    def format_func(x, _):
        # Calculate the elapsed time in seconds from the start_time
        elapsed_seconds = (x - mdates.date2num(df.index.min())) * 86400
        # Format the elapsed time as HH:MM:SS
        return pd.Timestamp("00:00:00") + pd.to_timedelta(elapsed_seconds, unit="s")

    plt.gca().xaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, _: format_func(x, _).strftime("%H:%M:%S"))
    )

    plt.legend(title="Event Type")
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.show()


def main(dir: str, events: Optional[list[str]]):
    # res: list[tuple[int, str, datetime]] = parse_logs(dir)
    id = 4
    res: list[tuple[int, str, datetime]] = parse_log(f"{dir}/{id}.log", id)
    if not res:
        print(f"Node {id} doesn't emit any data")
        os._exit(0)
    assert all(el[0] == id for el in res)
    assert all(len(el[1]) == 1 for el in res)

    # Convert parsed results into a DataFrame for easier manipulation
    df = pd.DataFrame(res, columns=["node_id", "event_type", "timestamp"])

    # Filter the DataFrame by the specified event types
    if events:
        df = df[df["event_type"].isin(events)]

    plot(df)


if __name__ == "__main__":
    args = parse_args()
    print(args.dir)
    event_types = parse_events_arg(args.events)
    main(args.dir, event_types)
