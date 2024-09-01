import argparse
import os
from datetime import datetime


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
    parser.add_argument("--node_n", type=str, help="Number of nodes in the network")
    parser.add_argument("--query", type=str, help="Query")
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


def parse_timestamp(timestamp_str) -> datetime:
    return datetime.strptime(timestamp_str, "%H:%M:%S:%f")


def parse_events_arg(
    events: str,
) -> list[str]:  # events are passed as a cli arg in the form "A,B,C"
    return [event.strip() for event in events.split(";")]


def extract_event_type(line: str) -> str:
    res = [el.strip() for el in line.split("|")][3]
    # print(res)
    # assert len(res) == 1
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
