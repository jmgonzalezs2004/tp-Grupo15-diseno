import os
import logging
import csv
import socket
import signal
import time
from typing import TextIO
from _csv import Writer as CsvWriter

from common import protocol

INPUT_FILE = os.environ["INPUT_FILE"]
OUTPUT_FILE_PREFIX = os.environ["OUTPUT_FILE_PREFIX"]
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

# TODO increase to 5
_QUERIES_COUNT = 3

class Client:

    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.finished_queries = 0
        self.output_files: list[TextIO] = []
        self.csv_writers: list[CsvWriter] = []

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
            next(csv_reader, None) # Ignore header
            for row in csv_reader:
                [timestamp, from_bank, from_account, to_bank, to_account, _, _, amount, currency, format, _] = row
                protocol.external.send_msg(
                    self.server_socket, protocol.external.MsgType.TRAN_RECORD,
                    timestamp, from_bank, from_account, to_bank, to_account, amount, currency, format
                )
                protocol.external.recv_msg(self.server_socket)

        protocol.external.send_msg(
            self.server_socket, protocol.external.MsgType.END_OF_RECORDS
        )
        protocol.external.recv_msg(self.server_socket)

    def initialize_output_files(self):
        for i in range(_QUERIES_COUNT):
            self.output_files.append(open(f"{OUTPUT_FILE_PREFIX}{i+1}.csv", "w"))
            self.csv_writers.append(csv.writer(self.output_files[i], delimiter=",", quotechar='"'))
    
    def close_output_files(self):
        for file in self.output_files:
            file.close()
        self.output_files.clear()
        self.csv_writers.clear()
    
    def process_q1_tran(self, tran):
        logging.info("Receiving Q1 transaction")
        
        from_bank_id, from_account, to_bank_id, to_account, amount = tran
        from_account_hex = format(from_account, "X")
        to_account_hex = format(to_account, "X")
        output_row = [from_bank_id, from_account_hex, to_bank_id, to_account_hex, amount]
        
        csv_writer = self.csv_writers[0]
        csv_writer.writerow(output_row)

    def process_q1_end(self):
        logging.info("Receiving Q1 end")
        self.finished_queries += 1
    
    def process_q2_results(self, bank_max):
        logging.info("Receiving Q2 bank max results")
        
        from_bank_name, from_account, amount = bank_max
        from_account_hex = format(from_account, "X")
        output_row = [from_bank_name, from_account_hex, amount]
        
        csv_writer = self.csv_writers[1]
        csv_writer.writerow(output_row)

    def process_q2_end(self):
        logging.info("Receiving Q2 end")
        self.finished_queries += 1

    def process_q3_result_tran(self, tran):
        logging.info("Receiving Q3 transaction result")
        
        from_bank_id, from_account, payment_format_id, amount = tran
        from_account_hex = format(from_account, "X")
        payment_format_str = protocol.common_enums.PaymentFormat.to_str(payment_format_id)
        output_row = [from_bank_id, from_account_hex, payment_format_str, amount]
        
        csv_writer = self.csv_writers[2]
        csv_writer.writerow(output_row)

    def process_q3_end(self):
        logging.info("Receiving Q3 end")
        self.finished_queries += 1

    def recv_results(self):
        while self.finished_queries < _QUERIES_COUNT:
            logging.info("Receiving result")
            msg_type, content = protocol.external.recv_msg(self.server_socket)
            protocol.external.send_msg(
                self.server_socket, protocol.external.MsgType.ACK
            )

            if msg_type == protocol.external.MsgType.Q1_TRAN:
                self.process_q1_tran(content)
            elif msg_type == protocol.external.MsgType.Q1_END:
                self.process_q1_end()
            elif msg_type == protocol.external.MsgType.Q2_RESULT:
                self.process_q2_results(content)
            elif msg_type == protocol.external.MsgType.Q2_END:
                self.process_q2_end()
            elif msg_type == protocol.external.MsgType.Q3_RESULT_TRAN:
                self.process_q3_result_tran(content)
            elif msg_type == protocol.external.MsgType.Q3_END:
                self.process_q3_end()
            # TODO: Complete for Q4 and Q5
            else:
                raise TypeError(f"Message type {msg_type} not supported")


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()

    # client start too fast, before gateway is ready
    # This will be not needed when we implement the full instances tree
    time.sleep(1)
    try:
        client.connect(SERVER_HOST, SERVER_PORT)
        client.initialize_output_files()
        client.send_tran_records(INPUT_FILE)
        client.recv_results()
    except socket.error:
        if not client.closed:
            logging.error("The connection with the server was lost")
            return 1
    except Exception as e:
        logging.exception(e)
        return 2
    finally:
        client.close_output_files()
        if not client.closed:
            client.disconnect()

    return 0


if __name__ == "__main__":
    main()
