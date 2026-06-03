from dataclasses import dataclass
import os
import logging
import csv
import queue
import socket
import signal
import threading
import time
from typing import TextIO
from _csv import Writer as CsvWriter

from common import protocol
from common.protocol.common_enums import PaymentFormat

INPUT_FILE = os.environ["INPUT_FILE"]
ACCOUNTS_FILE = os.environ["ACCOUNTS_FILE"]
OUTPUT_FILE_PREFIX = os.environ["OUTPUT_FILE_PREFIX"]
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

_QUERIES_COUNT = 5

@dataclass
class OutboundMessage:
    msg_type: protocol.external.MsgType
    data: list | None = None

class Client:
    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.finished_queries = 0
        self.output_files: list[TextIO] = []
        self.csv_writers: list[CsvWriter] = []
        self.output_files_headers = {
            1: ["From Bank", "From Account", "To Bank", "To Account", "Amount"],
            2: ["Bank ID", "Account", "Bank Name", "Amount"],
            3: ["Bank", "Account", "Payment Format", "Amount"],
            4: ["Bank", "Account"],
            5: ["Count"]
        }
        self.in_ack_event = threading.Event()
        self.outbound_queue: queue.Queue[OutboundMessage] = queue.Queue()
        self.data_feed_thread = threading.Thread(
            target=self._data_feed_loop,
            daemon=True,
            name=f"data-feed",
        )
        self.send_thread = threading.Thread(
            target=self._send_loop,
            daemon=True,
            name=f"socket-writer",
        )

    def _send_loop(self):
        try:
            while not self.closed:
                outbound_message = self.outbound_queue.get()
                if outbound_message.data is None:
                    protocol.external.send_msg(
                        self.server_socket, outbound_message.msg_type,
                    )
                else:
                    protocol.external.send_msg(
                        self.server_socket, outbound_message.msg_type, 
                        outbound_message.data
                    )
        except socket.error:
            logging.error(f"Socket write error for gateway")
        except Exception as e:
            logging.exception(e)
    
    def _data_feed_loop(self):
        self.send_acoount_records()
        self.send_tran_records()

    def handle_sigterm(self, signum, frame):
        logging.info("Recieved SIGTERM signal")
        self.closed = True
        self.disconnect()

        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)
    
    def start(self):
        self.connect(SERVER_HOST, SERVER_PORT)
        self.send_thread.start()
        self.initialize_output_files()
        self.data_feed_thread.start()
        self.recv_results()

    def connect(self, server_host, server_port):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((server_host, server_port))

    def disconnect(self):
        if self.server_socket:
            try:
                self.server_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

            self.server_socket.close()
            self.server_socket = None

    def enqueue_message(self, msg_type: protocol.external.MsgType, data: list | None = None):
        '''Thread-safe method that enqueues a message to be sent to gateway'''
        if self.closed:
            return
        if data is None:
            self.outbound_queue.put(OutboundMessage(msg_type))
        else:
            self.outbound_queue.put(OutboundMessage(msg_type, data.copy()))

    def send_acoount_records(self):
        logging.info("Sending accounts records")
        ACC_BATCH_SIZE = 75 # 80 bytes (estimated) per record. Payload near 6KB
        with open(ACCOUNTS_FILE, newline="\n") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(csv_reader, None) # Ignore header
            current_batch = []
            for row in csv_reader:
                [bank_name, bank_id, account_number, entity_id, entity_name] = row
                current_batch.append((bank_name, bank_id, account_number, entity_id, entity_name))
                if len(current_batch) >= ACC_BATCH_SIZE:
                    self._send_records_batch(protocol.external.MsgType.ACCOUNT_RECORD, current_batch)
                    current_batch.clear()
            
            if len(current_batch) > 0:
                self._send_records_batch(protocol.external.MsgType.ACCOUNT_RECORD, current_batch)
                current_batch.clear()

        self.in_ack_event.clear()
        self.enqueue_message(protocol.external.MsgType.END_OF_RECORDS)
        ack_received = self.in_ack_event.wait(timeout=60)
        if not ack_received:
            raise RuntimeError("Timeout waiting ACK from gateway")
        
    def _send_records_batch(self, msg_type, records: list):
        self.in_ack_event.clear()
        self.enqueue_message(msg_type, records)
        ack_received = self.in_ack_event.wait(timeout=60)
        if not ack_received:
            raise RuntimeError("Timeout waiting ACK from gateway")

    def send_tran_records(self):
        logging.info("Sending transactions records")
        TRAN_BATCH_SIZE = 210 # 38 bytes per record. Payload = 7980 B
        with open(INPUT_FILE, newline="\n") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(csv_reader, None) # Ignore header
            current_batch = []
            for row in csv_reader:
                [timestamp, from_bank, from_account, to_bank, to_account, _, _, amount, currency, format, _] = row
                current_batch.append((timestamp, from_bank, from_account, to_bank, to_account, amount, currency, format))
                if len(current_batch) >= TRAN_BATCH_SIZE:
                    self._send_records_batch(protocol.external.MsgType.TRAN_RECORD, current_batch)
                    current_batch.clear()

            if len(current_batch) > 0:
                self._send_records_batch(protocol.external.MsgType.TRAN_RECORD, current_batch)
                current_batch.clear()

        self.in_ack_event.clear()
        self.enqueue_message(protocol.external.MsgType.END_OF_RECORDS)
        ack_received = self.in_ack_event.wait(timeout=60)
        if not ack_received:
            raise RuntimeError("Timeout waiting ACK from gateway")

    def initialize_output_files(self):
        output_dir = os.path.dirname(OUTPUT_FILE_PREFIX)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        for i in range(_QUERIES_COUNT):
            q_num = i + 1
            self.output_files.append(open(f"{OUTPUT_FILE_PREFIX}{q_num}.csv", "w"))
            self.csv_writers.append(csv.writer(self.output_files[i], delimiter=",", quotechar='"'))
            self.csv_writers[i].writerow(self.output_files_headers[q_num])

    def close_output_files(self):
        for file in self.output_files:
            file.flush()
            file.close()
        self.output_files.clear()
        self.csv_writers.clear()
    
    def process_q1_tran(self, tran):
        logging.debug("Receiving Q1 transaction")
        
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
        
        from_bank_id, from_account, from_bank_name, amount = bank_max
        from_account_hex = format(from_account, "X")
        output_row = [from_bank_id, from_account_hex, from_bank_name, amount]
        
        csv_writer = self.csv_writers[1]
        csv_writer.writerow(output_row)

    def process_q2_end(self):
        logging.info("Receiving Q2 end")
        self.finished_queries += 1

    def process_q3_result_tran(self, tran):
        logging.info("Receiving Q3 transaction result")
        from_bank_id, from_account, payment_format_id, amount = tran
        from_account_hex = format(from_account, "X")
        payment_format_str = PaymentFormat.to_str(payment_format_id)
        output_row = [from_bank_id, from_account_hex, payment_format_str, amount]
        
        csv_writer = self.csv_writers[2]
        csv_writer.writerow(output_row)

    def process_q3_end(self):
        logging.info("Receiving Q3 end")
        self.finished_queries += 1

    def _process_q4_laundering_acc(self, laundering_acc):
        logging.info("Receiving Q4 laundering account result")
        
        bank_id, account = laundering_acc
        account_hex = format(account, "X")
        output_row = [bank_id, account_hex]
        
        csv_writer = self.csv_writers[3]
        csv_writer.writerow(output_row)
    
    def _process_q4_end(self):
        logging.info("Receiving Q4 end")
        self.finished_queries += 1

    def _process_q5_result(self, count):
        logging.info("Receiving Q5 count result")
        output_row = [count]
        csv_writer = self.csv_writers[4]
        csv_writer.writerow(output_row)
        self.finished_queries += 1

    def recv_results(self):
        while self.finished_queries < _QUERIES_COUNT:
            logging.debug("Receiving result")
            msg_type, content = protocol.external.recv_msg(self.server_socket)
            self.enqueue_message(protocol.external.MsgType.ACK)

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
            elif msg_type == protocol.external.MsgType.Q4_LAUNDERING_ACC:
                self._process_q4_laundering_acc(content)
            elif msg_type == protocol.external.MsgType.Q4_END:
                self._process_q4_end()
            elif msg_type == protocol.external.MsgType.Q5_RESULT:
                self._process_q5_result(content)
            elif msg_type == protocol.external.MsgType.ACK:
                self.in_ack_event.set()
            else:
                raise TypeError(f"Message type {msg_type} not supported")


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()

    # client start too fast, before gateway is ready
    # This will be not needed when we implement the full instances tree
    time.sleep(1)
    try:
        client.start()
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
