import os
import logging
import csv
import socket
import signal

from common import protocol

INPUT_FILE = os.environ["INPUT_FILE"]
OUTPUT_FILE = os.environ["OUTPUT_FILE"]
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])


class Client:

    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Recieved SIGTERM signal")
        self.closed = True
        self.disconnect()

        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def connect(self, server_host, server_port):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((server_host, server_port))

    def disconnect(self):
        if self.server_socket:
            self.server_socket.shutdown(socket.SHUT_RDWR)

    def send_tran_records(self, input_file):
        logging.info("Sending transactions records")
        with open(input_file, newline="\n") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            for row in csv_reader:
                [timestamp, from_bank, to_bank, _, _, amount, currency, format, _] = row
                protocol.external.send_msg(
                    self.server_socket, protocol.external.MsgType.TRAN_RECORD,
                    timestamp, from_bank, to_bank, amount, currency, format
                )
                protocol.external.recv_msg(self.server_socket)

        protocol.external.send_msg(
            self.server_socket, protocol.external.MsgType.END_OF_RECODS
        )
        protocol.external.recv_msg(self.server_socket)

    def recv_results(self, output_file):
        logging.info("Receiving count")
        count_message = protocol.external.recv_msg(self.server_socket)
        protocol.external.send_msg(
            self.server_socket, protocol.external.MsgType.ACK
        )

        if count_message[0] != protocol.external.MsgType.RESULT_COUNT:
            raise TypeError("Expected a RESULT_COUNT message")

        with open(output_file, "w") as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=",", quotechar='"')
            for count_item in count_message[1]:
                csv_writer.writerow(count_item)


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()

    try:
        client.connect(SERVER_HOST, SERVER_PORT)
        client.send_tran_records(INPUT_FILE)
        client.recv_results(OUTPUT_FILE)
    except socket.error:
        if not client.closed:
            logging.error("The connection with the server was lost")
            return 1
    except Exception as e:
        logging.error(e)
        return 2
    finally:
        if not client.closed:
            client.disconnect()

    return 0


if __name__ == "__main__":
    main()
