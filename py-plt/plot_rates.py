import pandas as pd
import argparse
import matplotlib.pyplot as plt


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
            header=None,
            names=["EventType", "Rate"],
            dtype={"EventType": str, "Rate": str},
        )

        # Convert the Rate column to numeric, setting errors='coerce' will turn non-numeric values into NaN
        df["Rate"] = pd.to_numeric(df["Rate"], errors="coerce")

        # Drop any rows where Rate is NaN
        df = df.dropna(subset=["Rate"])

        # Transform data to plot each event type as a line over time
        # Create a column for time based on the index
        df["Time"] = df.groupby("EventType").cumcount()

        # Pivot the DataFrame to have EventType as columns and Time as the index
        pivot_df = df.pivot(index="Time", columns="EventType", values="Rate")

        # Plot each event type as a line over time
        for event_type in pivot_df.columns:
            axs[i].plot(
                pivot_df.index, pivot_df[event_type], marker="o", label=event_type
            )

        # Customize the subplot
        axs[i].set_title(f"Event Rates Over Time - {csv_file}")
        axs[i].set_xlabel("Time (seconds)")
        axs[i].set_ylabel("Rate")
        axs[i].legend(title="Event Types")
        axs[i].grid(True)

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
