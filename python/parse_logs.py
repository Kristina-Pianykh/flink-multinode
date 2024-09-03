import argparse
import os
from statistics import StatisticsError
from statistics import mean, median
from datetime import datetime, timedelta


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
    # return datetime.strptime(timestamp_str, "%H:%M:%S")


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


def parse_by_min(log_file_path: str):
    events_by_minute = {i: 0 for i in range(0, 11)}
    # latencies_by_minute = {i: [] for i in range(0, 11)}  # averages latencies per minute
    latencies_by_minute = {}
    f = open(log_file_path, "r")
    first_timestamp = None
    previous_minute_timestamp = None
    in_a_minute = None
    current_minute = 0

    for line in f:
        if not previous_minute_timestamp:
            try:
                previous_minute_timestamp = parse_timestamp(extract_timestamp(line))
                first_timestamp = previous_minute_timestamp
                print(f"First timestamp: {first_timestamp}")
                in_a_minute = first_timestamp + timedelta(minutes=1)
            except Exception as err:
                continue

        if "sendTo():" in line:
            timestamp = parse_timestamp(extract_timestamp(line))
            if timestamp >= previous_minute_timestamp and timestamp <= in_a_minute:
                events_by_minute[current_minute] += 1
            else:
                print(
                    f"timestamp {timestamp} is newer than {previous_minute_timestamp} but sooner than {in_a_minute}"
                )
                delta = int((timestamp - in_a_minute).seconds / 60)
                in_a_minute = first_timestamp + timedelta(
                    minutes=current_minute + (delta + 1)
                )
                previous_minute_timestamp = first_timestamp + timedelta(
                    minutes=delta + 1
                )

                current_minute += delta + 1
                try:
                    events_by_minute[current_minute] += 1
                except KeyError:
                    print(
                        f"current_minute = {current_minute}, timestamp = {timestamp}, events_by_minute.keys() = {events_by_minute.keys()}, line: {line}"
                    )
            # previous_line = line
        if "LATENCYYYYYYYYYYYYYYYYYYYY" in line:
            ms = int([i.strip() for i in line.split(" ")][-1])
            if current_minute not in latencies_by_minute:
                latencies_by_minute[current_minute] = []
            latencies_by_minute[current_minute].append(ms)
            # print(
            #     f"LATENCYYYYYYYYYYYYYYYYYYYY current_minute {current_minute}, ms: {ms}"
            # )

    latencies_avg = {}
    for k, v in latencies_by_minute.items():
        if v:
            # latencies_avg[k] = median(v)
            latencies_avg[k] = mean(v)
        else:
            latencies_avg[k] = 0
    print(latencies_avg)
    # print(events_by_minute)
    # print(latencies_avg)
    return (events_by_minute, latencies_avg)


def parse_logs_by_min(dir: str, node_num):
    events_per_file = {i: {} for i in range(node_num)}
    latencies_per_file = {i: {} for i in range(node_num)}

    for i in range(node_num):
        log_file = f"{dir}/{i}.log"
        events_per_file[i] = {}
        latencies_per_file[i] = {}
        print(log_file)

        try:
            assert os.path.exists(log_file)
        except AssertionError:
            print(f"Warning: file {log_file} doesn't exist")
            break

        events_by_minute, latencies_avg = parse_by_min(log_file)

        events_by_minute[9] = events_by_minute[9] + events_by_minute[10]
        del events_by_minute[10]
        events_per_file[i] = events_by_minute

        latencies_per_file[i] = latencies_avg

    events_per_simulation = {}
    latencies_per_simulation = {}

    for i in range(10):
        events_per_simulation[i] = sum(
            [node_events[i] for node_events in events_per_file.values()]
        )
        # print(
        #     f"latencies mean: {mean([node_latencies[i] for node_latencies in latencies_per_file.values() if node_latencies])}, median: {median([node_latencies[i] for node_latencies in latencies_per_file.values() if node_latencies])}"
        # )
        tmp = []
        for node_latencies in latencies_per_file.values():
            if node_latencies:
                if i in node_latencies.keys():
                    tmp.append(node_latencies[i])
        if tmp:
            latencies_per_simulation[i] = mean(tmp)

        # try:
        #     latencies_per_simulation[i] = mean(
        #         [node_latencies[i] for node_latencies in latencies_per_file.values()]
        #     )
        # except StatisticsError:
        #     pass

    return (events_per_simulation, latencies_per_simulation)


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
