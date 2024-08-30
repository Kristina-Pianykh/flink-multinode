import argparse
import json
import os
from copy import deepcopy


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_dir",
        type=str,
        help="Input dir with matrix.txt, inequality_inputs.json, plans/, etc.",
    )
    args = parser.parse_args()

    assert args.input_dir
    assert os.path.exists(args.input_dir)
    assert os.path.isdir(args.input_dir)
    if args.input_dir.endswith("/"):
        args.input_dir = args.input_dir[:-1]

    return args


def parse_matrix(matrix_path: str):
    try:
        with open(matrix_path, "r") as file:
            return file.readlines()
    except Exception as err:
        print(err)
        os._exit(1)


def parse_rates(line: str):
    event_rate_map = {}
    alphabet = ["A", "B", "C"]
    rates = [e.strip() for e in line.split(" ")]
    assert len(alphabet) == len(rates)

    for event_type, rate in zip(alphabet, rates):
        event_rate_map[event_type] = float(rate)

    return event_rate_map


def parse_inequality_inputs(ineq_inputs_path: str):
    try:
        with open(ineq_inputs_path, "r") as file:
            ineq_inputs = json.load(file)
            return ineq_inputs
    except Exception as err:
        print(err)


if __name__ == "__main__":
    args = parse_args()
    matrix_path = f"{args.input_dir}/matrix.txt"
    ineq_path = f"{args.input_dir}/inequality_inputs.json"
    output_dir = args.input_dir

    matrix = parse_matrix(matrix_path)
    print(matrix)
    event_rate_map = parse_rates(matrix[-1])
    print(event_rate_map)
    ineq_inputs = parse_inequality_inputs(ineq_path)
    assert ineq_inputs, f"failed to parse {ineq_inputs}"

    part_event_rate = [
        v for k, v in event_rate_map.items() if k == ineq_inputs["partitioningInput"]
    ]
    assert (
        len(part_event_rate) == 1
    ), f"Failed to find the partitioning input rate for {ineq_inputs['partitioningInput']} in {event_rate_map}"
    part_event_rate = part_event_rate[0]

    print(f"Partition event rate: {part_event_rate}")
    print(f"20% of {part_event_rate}: {part_event_rate * 0.2}")
    print(f"50% of {part_event_rate}: {part_event_rate * 0.5}")
    print(f"70% of {part_event_rate}: {part_event_rate * 0.7}")
    print(f"100% of {part_event_rate}: {part_event_rate}")
    print(f"130% of {part_event_rate}: {part_event_rate * 1.3}")
    print(f"150% of {part_event_rate}: {part_event_rate * 1.5}")
    print(f"200% of {part_event_rate}: {part_event_rate * 2.0}")

    ratios = [0.2, 0.5, 0.7, 1.0, 1.3, 1.5, 2.0]
    nonpart_events = set(event_rate_map.keys()).difference(
        set([ineq_inputs["partitioningInput"]])
    )
    highest_nonpart_rate = max(
        [v for k, v in event_rate_map.items() if k in nonpart_events]
    )
    print(highest_nonpart_rate)

    for ratio in ratios:
        new_ratios = []
        for event, rate in event_rate_map.items():
            if event in nonpart_events:
                part_event_rate_20_percent = part_event_rate * ratio
                factor = part_event_rate_20_percent / highest_nonpart_rate
                if (rate * factor) < rate:
                    new_ratios.append(rate)
                else:
                    new_ratios.append(rate * factor)
            else:
                new_ratios.append(rate)

        new_matrix = deepcopy(matrix)
        new_matrix[-1] = " ".join([str(round(i, 1)) for i in new_ratios])
        print(new_matrix)

        with open(f"{output_dir}/matrix_{int(ratio * 100)}.txt", "w") as out:
            print("".join(new_matrix))
            out.write("".join(new_matrix))
