import argparse
import os
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


# Function to parse the timestamp string to a datetime object, ignoring milliseconds
def parse_timestamp(timestamp_str):
    return datetime.strptime(
        timestamp_str.split(":")[0]
        + ":"
        + timestamp_str.split(":")[1]
        + ":"
        + timestamp_str.split(":")[2],
        "%H:%M:%S",
    )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=str, help="dir with the CSV files")
    parser.add_argument("--events", type=str, help="Event types to plot")
    args = parser.parse_args()

    assert args.dir
    assert os.path.exists(args.dir)
    assert args.events
    assert isinstance(args.events, str)

    return args


def parse_events_arg(
    events: str,
) -> list[str]:  # events are passed as a cli arg in the form "A,B,C"
    return [event.strip() for event in events.split(",")]


# Load the data from CSV
args = parse_args()
dir = args.dir
print(dir)

event_types = parse_events_arg(args.events)

# Get list of CSV files
csv_files = [f for f in os.listdir(dir) if f.endswith(".csv")]


# Determine the layout for subplots
num_files = len(csv_files)
num_cols = 2  # Number of columns in the subplot grid
num_rows = (num_files + 1) // num_cols  # Calculate number of rows needed

# Create a figure with subplots
fig, axs = plt.subplots(num_rows, num_cols, figsize=(15, 5 * num_rows))

# Flatten the axs array for easy iteration if there are multiple rows
axs = axs.flatten()

# Loop over all files in the directory and corresponding subplot axis
for i, (filename, ax) in enumerate(zip(csv_files, axs)):
    file_path = os.path.join(dir, filename)

    # Load the data from the current file
    df = pd.read_csv(
        file_path,
        header=0,
        names=["event_type", "timestamp", "timestamp_long"],
        dtype=str,
        quotechar='"',
    )

    # Convert the 'timestamp' to a pandas datetime object
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%H:%M:%S:%f")

    # Create a new column for 10-second intervals
    df["time_bin"] = (df["timestamp"].astype(np.int64) // 10**9 // 10) * 10

    # Group by event type and 10-second intervals, then count the occurrences
    event_counts = df.groupby(["event_type", "time_bin"]).size().unstack(fill_value=0)

    # Plot on the specific subplot axis
    # event_types = df["event_type"].unique()

    for event_type in event_types:
        if event_type in event_counts.index:
            ax.plot(
                event_counts.columns,
                event_counts.loc[event_type],
                label=f"Event {event_type}",
            )

    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Event Frequency")
    ax.set_title(f"{filename}")
    ax.legend()
    ax.grid(True)

# Hide any empty subplots if the number of files is less than the grid size
for j in range(i + 1, len(axs)):
    fig.delaxes(axs[j])

# Adjust layout to prevent overlap
plt.tight_layout()

# Save the full figure as a single image file
output_file = os.path.join(dir, "all_event_frequencies.png")
plt.savefig(output_file)

# Show the full figure with subplots
plt.show()
