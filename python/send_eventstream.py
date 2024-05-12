import argparse
import json
import logging
import string
from datetime import datetime, timedelta
import pathlib
import socket
import time


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    input_file = (
        args.input_file
        or str(pathlib.Path(__file__).parent.resolve()) + f"/trace_{args.nodeId}.csv"
    )
    node_id = str(args.nodeId)
    logging.info(f"Using input file: {input_file}")
    logging.info(f"Using node id: {args.nodeId}")
    logging.info(f"Using address book: {args.address_book}")

    address_book = args.address_book
    with open(args.address_book) as f:
        address_book = json.load(f)

    for key in address_book.keys():
        # logging.info(type(key))
        logging.info(f"Node {key} at {address_book[key]}")

    if node_id not in address_book.keys():
        logging.error("Node not found in address book")
        exit(1)

    host_port = address_book[node_id].split(":")
    if len(host_port) != 2:
        logging.error("Invalid address book format")
        exit(1)

    client_ip = address_book[node_id].split(":")[0]
    client_port = int(address_book[node_id].split(":")[1])

    # client_ip = args.host
    # client_port = 5500 + args.nodeId

    # read event stream input txt
    with open(input_file) as f:
        event_stream = f.readlines()

    print("read:", input_file)
    read_and_send_event_stream(event_stream, client_ip, client_port, args.nodeId)


# send data with this function
def send_event(socket, eventtype, event_id, creation_timestamp, attribute_values):
    timestamp_string = creation_timestamp.strftime("%H:%M:%S:%f")
    message = "simple | %s | %s | %s" % (event_id, timestamp_string, eventtype)
    for attr in attribute_values:
        message += "|" + attr
    message += " \n"
    print(message)

    try:
        socket.send(message.encode(encoding="UTF-8"))
        print("Message sent!")
    except Exception as error:
        print("Error - message not sent!", error)


def send_greeting_message(socket, node_id):
    socket.send(f"I am {node_id}\n".encode(encoding="UTF-8"))


def send_end_of_the_stream_message(socket):
    try:
        socket.send("end-of-the-stream\n".encode(encoding="UTF-8"))
        print("end-of-the-stream!!")
    except Exception as error:
        print("Error - message not sent!", error)


def read_and_send_event_stream(event_stream, client_ip, client_port, node_id):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((str(client_ip), client_port))
    send_greeting_message(client_socket, node_id)

    event_type_universe = string.ascii_uppercase[0:25]

    timestamp_offset = start_of_next_minute(datetime.now())

    for event in event_stream:
        event = event.strip()
        attributes = event.split(",")
        timestamp = attributes[0]
        event_type = attributes[1]

        if event_type not in event_type_universe:
            continue
        try:
            hours, minutes, seconds, us = map(int, timestamp.split(":"))
        except Exception:
            hours, minutes, seconds = map(int, timestamp.split(":"))
            us = 0

        event_time = timedelta(
            days=hours // 24,
            hours=(hours - (hours // 24) * 24),
            minutes=minutes,
            seconds=seconds,
            microseconds=us,
        )

        target_timestamp = event_time + timestamp_offset
        # sleep until it is time
        delay = (target_timestamp - datetime.now()).total_seconds()
        if delay > 0:
            time.sleep(delay)

        event_id = attributes[2]
        attribute_values = attributes[3:]
        send_event(
            client_socket, event_type, event_id, target_timestamp, attribute_values
        )
    # signal the end of the stream by sending a "end-of-the-stream" message
    send_end_of_the_stream_message(client_socket)


def start_of_next_minute(timestamp: datetime) -> datetime:
    if timestamp.microsecond == 0 and timestamp.second == 0:
        return timestamp
    else:
        # add one minute - increments minutes counter by one, handling any roll-over that may be needed (e.g. when minute=59, and perhaps, day=31, etc)
        ts = timestamp + timedelta(minutes=1)
        # set all components of the timedelta below "minutes" to zero
        return ts.replace(second=0, microsecond=0)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Send event data to the given tcp address."
    )
    parser.add_argument(
        "nodeId",
        type=int,
        default=0,
        help="Node id to use. Will use config_id, trace_id, port 5500+id and localhost, unless overridden by long options",
    )
    parser.add_argument(
        "-c", "--config-file", help="Path to config file", type=str, required=False
    )
    parser.add_argument(
        "-f",
        "--input-file",
        help="Path to the input data file (override). ",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-a", "--address-book", type=str, help="Address book file", required=True
    )
    # parser.add_argument(
    #     "-p", "--port", type=int, help="Client port", required=False, default=None
    # )
    # parser.add_argument(
    #     "-H", "--host", type=str, default="localhost", required=False, help="The host"
    # )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
