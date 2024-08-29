import pandas as pd
import os
import argparse


# Function to convert timestamp to seconds
def timestamp_to_seconds(timestamp):
    h, m, s = map(int, timestamp.split(":"))
    return h * 3600 + m * 60 + s


def check_dir(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)
    else:
        assert os.path.isdir(dir)
        for i in os.listdir(dir):
            os.remove(f"{dir}/{i}")


def generate_mixed_trace(dir0, dir1, dir2):
    for f in os.listdir(dir0):
        assert os.path.exists(f"{dir0}/{f}")
        assert os.path.exists(f"{dir1}/{f}")

        # print(f"Processing {f}")

        # Load the regular rate trace file
        regular_trace = pd.read_csv(
            f"{dir0}/{f}", header=None, names=["timestamp", "event_type", "event_id"]
        )
        # Load the inflated frequency trace file
        inflated_trace = pd.read_csv(
            f"{dir1}/{f}", header=None, names=["timestamp", "event_type", "event_id"]
        )
        # Duplicate the original timestamp column to create a new column for conversion
        regular_trace["timestamp_seconds"] = regular_trace["timestamp"]
        inflated_trace["timestamp_seconds"] = inflated_trace["timestamp"]

        # Convert the duplicated timestamp column to seconds for easy comparison
        regular_trace["timestamp_seconds"] = regular_trace["timestamp_seconds"].apply(
            timestamp_to_seconds
        )
        inflated_trace["timestamp_seconds"] = inflated_trace["timestamp_seconds"].apply(
            timestamp_to_seconds
        )

        # Filter events: first 3 minutes (0 to 180 seconds) from regular trace
        regular_part = regular_trace[regular_trace["timestamp_seconds"] < 180]

        # Filter events: from 3 minutes (180 seconds) onwards from inflated trace
        inflated_part = inflated_trace[inflated_trace["timestamp_seconds"] >= 180]

        # Combine the two parts
        combined_trace = pd.concat([regular_part, inflated_part]).sort_values(
            by="timestamp_seconds"
        )

        # Drop the auxiliary 'timestamp_seconds' column
        combined_trace = combined_trace.drop(columns=["timestamp_seconds"])

        # Save the combined trace to a new CSV file
        combined_trace.to_csv(f"{dir2}/{f}", index=False, header=False)
        print(f"Saved {f} to {dir2}")


def size_of_traces(dir):
    cnt = 0
    for i in os.listdir(dir):
        with open(f"{dir}/{i}", "r") as f:
            cnt += len(f.readlines())
    return cnt


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--topology_dir",
        type=str,
        help="Dir to the topology root",
    )
    args = parser.parse_args()

    assert args.topology_dir
    assert os.path.exists(args.topology_dir)
    if args.topology_dir.endswith("/"):
        args.topology_dir = args.topology_dir[:-1]
    return args.topology_dir


if __name__ == "__main__":
    topology_dir = parse_args()
    dir0 = f"{topology_dir}/plans/trace_inflated_0"
    plans_dir = f"{topology_dir}/plans"
    # dir1 = f"{topology_dir}/plans/trace_inflated_1"
    for dir in os.listdir(plans_dir):
        # print(dir)
        # print(os.path.isdir(f"{plans_dir}/{dir}"))
        dir_path = f"{plans_dir}/{dir}"
        if (
            os.path.isdir(dir_path)
            and dir.startswith("trace_inflated_")
            and dir != "trace_inflated_0"
            and not dir.startswith("trace_inflated_mix")
        ):
            dir1 = f"{plans_dir}/{dir}"
            # print(f"Input inflated dir: {dir1}")
            suffix = dir.split("trace_inflated_")[1]
            dir2 = f"{plans_dir}/trace_inflated_mix_{suffix}"
            # print(f"Output dir: {dir2}")

            check_dir(dir2)

            generate_mixed_trace(dir0, dir1, dir2)

            size0 = size_of_traces(dir0)
            size1 = size_of_traces(dir1)
            size2 = size_of_traces(dir2)

            # print(f"\nSize of traces in {dir0}: {size0}")
            # print(f"Size of traces in {dir1}: {size1}")
            # print(f"Size of traces in {dir2}: {size2}\n")

            try:
                assert size0 < size1
            except AssertionError as err:
                print(f"\nAssertionError: assert size0 < size1")
                print(f"\nSize of traces in {dir0}: {size0}")
                print(f"Size of traces in {dir1}: {size1}")

            try:
                assert (size2 > size0) and (size2 < size1)
            except AssertionError as err:
                print(f"\nAssertionError: assert (size2 > size0) and (size2 < size1)")
                print(f"\nSize of traces in {dir0}: {size0}")
                print(f"Size of traces in {dir1}: {size1}")
                print(f"Size of traces in {dir2}: {size2}\n")
