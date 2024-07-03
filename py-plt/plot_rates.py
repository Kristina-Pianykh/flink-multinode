import pandas as pd
import argparse
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from datetime import timedelta


def seconds_to_hhmmss(seconds):
    return str(timedelta(seconds=seconds))


def plot_event_frequencies(csv_files):
    num_files = len(csv_files)
    fig, axs = plt.subplots(
        num_files, 1, figsize=(12, 6 * num_files), sharex=True, sharey=True
    )

    if num_files == 1:
        axs = [axs]

    for i, csv_file in enumerate(csv_files):
        # Read the CSV file into a DataFrame
        df = pd.read_csv(
            csv_file,
            header=0,
            names=["EventType", "Timestamp", "Rate"],
            dtype={"EventType": str, "Timestamp": float, "Rate": float},
        )

        # Drop any rows where Rate is NaN
        df = df.dropna(subset=["Rate"])

        # Pivot the DataFrame to have EventType as columns and Timestamp as the index
        pivot_df = df.pivot(index="Timestamp", columns="EventType", values="Rate")

        # Plot each event type as a line over time
        for event_type in pivot_df.columns:
            axs[i].plot(
                pivot_df.index, pivot_df[event_type], marker="o", label=event_type
            )

        # Customize the subplot
        axs[i].set_title(f"Event Rates Over Time - {csv_file}")
        axs[i].set_xlabel("Time (hh:mm:ss)")
        axs[i].set_ylabel("Rate")
        axs[i].legend(title="Event Types")
        axs[i].grid(True)

        # Set the x-axis labels to hh:mm:ss
        axs[i].xaxis.set_major_formatter(
            ticker.FuncFormatter(lambda x, _: seconds_to_hhmmss(x))
        )

    plt.tight_layout()
    plt.show()


def parse_args():
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("--files", nargs="+", type=str, help="paths to the CSV files")
    args = parser.parse_args()
    return args.files


if __name__ == "__main__":
    csv_files = parse_args()
    plot_event_frequencies(csv_files)
