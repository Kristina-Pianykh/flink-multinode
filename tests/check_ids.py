import argparse
import os
import csv


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace", type=str)
    parser.add_argument("--log", type=str)
    args = parser.parse_args()
    assert args.trace
    assert os.path.exists(args.trace)

    assert args.log
    assert os.path.exists(args.log)
    return args


def is_found(id, f):
    for line in f:
        if id in line:
            return True
    # print(id)
    return False


if __name__ == "__main__":
    args = parse_args()
    f = open(args.log, "r")

    with open(args.trace, "r") as trace:
        reader = csv.DictReader(trace, fieldnames=["timestamp", "event", "id"])
        fail_count = 0

        for row in reader:
            if not is_found(row["id"], f):
                fail_count += 1

    print(fail_count)
    f.close()
