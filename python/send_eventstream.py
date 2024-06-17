import argparse
import json
import logging
import string
from datetime import datetime, timedelta
import pathlib
import socket, errno
import time


CLIENT_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
TARGET_NODE_ID = ""


def main():
    global CLIENT_SOCKET
    global TARGET_NODE_ID

    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    input_file = (
        args.input_file
        or str(pathlib.Path(__file__).parent.resolve()) + f"/trace_{args.nodeId}.csv"
    )
    TARGET_NODE_ID = str(args.nodeId)
    assert TARGET_NODE_ID

    logging.info(f"Using input file: {input_file}")
    logging.info(f"Using node id: {args.nodeId}")
    logging.info(f"Using address book: {args.address_book}")

    address_book = args.address_book
    with open(args.address_book) as f:
        address_book = json.load(f)

    for key in address_book.keys():
        # logging.info(type(key))
        logging.info(f"Node {key} at {address_book[key]}")

    if TARGET_NODE_ID not in address_book.keys():
        logging.error("Node not found in address book")
        exit(1)

    host_port = address_book[TARGET_NODE_ID].split(":")
    if len(host_port) != 2:
        logging.error("Invalid address book format")
        exit(1)

    client_ip = address_book[TARGET_NODE_ID].split(":")[0]
    client_port = int(address_book[TARGET_NODE_ID].split(":")[1])

    retries = 0
    while retries <= 5:
        if retries == 5:
            logging.error(
                f"Failed to connect to client {client_ip}:{client_port} after 5 attempts."
            )
            break
        try:
            print(f"Connecting to {client_ip}:{client_port}")
            CLIENT_SOCKET.connect((str(client_ip), client_port))
            break
        except Exception as e:
            logging.error(f"Failed to connect to {client_ip}:{client_port}")
            retries += 1
            time.sleep(1)

    # client_ip = args.host
    # client_port = 5500 + args.nodeId

    # read event stream input txt
    with open(input_file) as f:
        event_stream = f.readlines()

    print("read:", input_file)
    read_and_send_event_stream(event_stream, client_ip, client_port)
    CLIENT_SOCKET.close()


# send data with this function
def send_event(
    eventtype,
    event_id,
    creation_timestamp,
    attribute_values,
    attempt_num,
    client_ip,
    client_port,
):
    global CLIENT_SOCKET
    global TARGET_NODE_ID

    timestamp_string = creation_timestamp.strftime("%H:%M:%S:%f")
    message = "simple | %s | %s | %s" % (event_id, timestamp_string, eventtype)
    if attempt_num > 5:
        print(
            f"Failed to send message {message} after 5 attempts to node {TARGET_NODE_ID}"
        )
        return
    for attr in attribute_values:
        message += "|" + attr
    message += " \n"
    print(message)

    try:
        CLIENT_SOCKET.send(message.encode(encoding="UTF-8"))
        print("Message sent!")
    except socket.error as error:
        print("Detected remote disconnect")
        if error.errno == errno.EPIPE:
            # recreate the socket.
            # CLIENT_SOCKET.close()
            CLIENT_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            CLIENT_SOCKET.connect((str(client_ip), client_port))
            send_event(
                eventtype,
                event_id,
                creation_timestamp,
                attribute_values,
                attempt_num + 1,
                client_ip,
                client_port,
            )
        else:
            print("Error - message not sent!", error)
            # determine and handle different error
            pass
    except Exception as error:
        print("Error - message not sent!", error)


def send_greeting_message(attempt_num, client_ip, client_port):
    global CLIENT_SOCKET
    global TARGET_NODE_ID

    if attempt_num > 5:
        print(
            f"Failed to send greeting message after 5 attempts to node {TARGET_NODE_ID}"
        )
        return

    try:
        CLIENT_SOCKET.send(f"I am {TARGET_NODE_ID}\n".encode(encoding="UTF-8"))
    except socket.error as error:
        print("Detected remote disconnect")
        if error.errno == errno.EPIPE:
            # recreate the socket.
            # CLIENT_SOCKET.close()
            CLIENT_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            CLIENT_SOCKET.connect((str(client_ip), client_port))
            send_greeting_message(attempt_num + 1, client_ip, client_port)
        else:
            print("Error - message not sent!", error)
            # determine and handle different error
            pass


def send_end_of_the_stream_message(attempt_num, client_ip, client_port):
    global CLIENT_SOCKET

    if attempt_num > 5:
        print(
            f"Failed to send end-of-the-stream message after 5 attempts to node {TARGET_NODE_ID}"
        )
        return

    try:
        CLIENT_SOCKET.send("end-of-the-stream\n".encode(encoding="UTF-8"))
        print("end-of-the-stream!!")
    except socket.error as error:
        print("Detected remote disconnect")
        if error.errno == errno.EPIPE:
            # recreate the socket.
            # CLIENT_SOCKET.close()
            CLIENT_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            CLIENT_SOCKET.connect((str(client_ip), client_port))
            send_end_of_the_stream_message(attempt_num + 1, client_ip, client_port)
        else:
            print("Error - message not sent!", error)
            # determine and handle different error
            pass
    except Exception as error:
        print("Error - message not sent!", error)


def read_and_send_event_stream(event_stream, client_ip, client_port):
    global CLIENT_SOCKET
    global TARGET_NODE_ID

    send_greeting_message(attempt_num=1, client_ip=client_ip, client_port=client_port)

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
            event_type,
            event_id,
            target_timestamp,
            attribute_values,
            1,
            client_ip,
            client_port,
        )
    # signal the end of the stream by sending a "end-of-the-stream" message
    send_end_of_the_stream_message(
        attempt_num=1, client_ip=client_ip, client_port=client_port
    )


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
