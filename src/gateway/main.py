import logging
import os
import queue
import signal
import socket
import threading
from dataclasses import dataclass

import message_handler
from common import middleware, protocol
from message_handler.client_id_generator import ClientIdGenerator

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
BANKS_AMOUNT = int(os.environ["BANKS_AMOUNT"])
BANKS_PREFIX = os.environ["BANKS_PREFIX"]
DISTRIBUTOR_AMOUNT = int(os.environ["DISTRIBUTOR_AMOUNT"])
DISTRIBUTOR_PREFIX = os.environ["DISTRIBUTOR_PREFIX"]
QUERIES_COUNT = 5

@dataclass
class OutboundMessage:
    msg_type: protocol.external.MsgType
    data: bytes | None = None


class ClientSession:
    def __init__(
        self,
        client_id: str,
        client_socket: socket.socket,
        message_handler_instance: message_handler.MessageHandler,
    ):
        self.client_id = client_id
        self.socket = client_socket
        self.message_handler = message_handler_instance
        self.ack_event = threading.Event()
        self.closed = threading.Event()
        self.all_query_ends_sent = threading.Event()

        self.outbound_queue: queue.Queue[OutboundMessage] = queue.Queue()

        self.distributor_exchanges: list[middleware.MessageMiddlewareExchangeRabbitMQ] = []
        for i in range(DISTRIBUTOR_AMOUNT):
            exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, DISTRIBUTOR_PREFIX, [f"{DISTRIBUTOR_PREFIX}_{i}"],
            )
            self.distributor_exchanges.append(exchange)

        self.banks_exchanges: list[middleware.MessageMiddlewareExchangeRabbitMQ] = []
        for i in range(BANKS_AMOUNT):
            exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, BANKS_PREFIX, [f"{BANKS_PREFIX}_{i}"],
            )
            self.banks_exchanges.append(exchange)

        self.writer_thread = threading.Thread(
            target=self._write_loop,
            daemon=True,
            name=f"writer-{client_id}",
        )

    def start(self):
        self.writer_thread.start()
        self._read_loop()
        self.close()

    def close(self):
        if self.closed.is_set():
            return

        self.closed.set()
        logging.info(f"Closing session for client {self.client_id}")

        self.socket.shutdown(socket.SHUT_RDWR)
        for exchange in self.distributor_exchanges:
            exchange.close()
        for exchange in self.banks_exchanges:
            exchange.close()

    def enqueue_message(self, msg_type: protocol.external.MsgType, data: bytes | None = None):
        '''Thread-safe method that enqueues a message to be sent to client'''
        if self.closed.is_set():
            return
        self.outbound_queue.put(OutboundMessage(msg_type, data))

    def _write_loop(self):
        sent_query_ends = 0
        query_end_msg_types = [protocol.external.MsgType.Q1_END,
                               protocol.external.MsgType.Q2_END,
                               protocol.external.MsgType.Q3_END,
                               protocol.external.MsgType.Q4_END,
                               protocol.external.MsgType.Q5_RESULT]
        try:
            while not self.closed.is_set():
                outbound_message = self.outbound_queue.get()
                self.ack_event.clear()

                msg_type = outbound_message.msg_type
                if msg_type in query_end_msg_types:
                    sent_query_ends += 1
                    if sent_query_ends >= QUERIES_COUNT:
                        self.all_query_ends_sent.set()
                if outbound_message.data is None:
                    # Used for EOF and ACK messages
                    protocol.external.send_msg(
                        self.socket, msg_type,
                    )
                else:
                    protocol.external.forward_msg(
                        self.socket, msg_type, outbound_message.data,
                    )

                if msg_type != protocol.external.MsgType.ACK:
                    ack_received = self.ack_event.wait(timeout=30)
                    if not ack_received:
                        raise RuntimeError("Timeout waiting ACK from client")
                    
        except socket.error:
            logging.error(f"Socket write error for client {self.client_id}")
            self.close()
        except Exception as e:
            logging.exception(e)
            self.close()

    def _read_loop(self):
        in_accounts_mode = True
        dst_distributor = 0
        try:
            while not self.closed.is_set():
                msg_type, content = protocol.external.recv_msg(self.socket)
                if msg_type == protocol.external.MsgType.ACCOUNT_RECORD:
                    if not in_accounts_mode:
                        raise RuntimeError("ACCOUNT_RECORD received after END_OF_RECORDS")

                    self.enqueue_message(protocol.external.MsgType.ACK)
                    serialized_messages = self.message_handler.prepare_account_batch(content, BANKS_AMOUNT)
                    for bank_idx in range(len(serialized_messages)):
                        if serialized_messages[bank_idx] is None:
                            continue
                        self.banks_exchanges[bank_idx].send(serialized_messages[bank_idx])

                elif msg_type == protocol.external.MsgType.TRAN_RECORD:
                    if in_accounts_mode:
                        raise RuntimeError("TRAN_RECORD received before Accounts END_OF_RECORDS")

                    self.enqueue_message(protocol.external.MsgType.ACK)
                    serialized_message = self.message_handler.prepare_tran_batch(content)
                    self.distributor_exchanges[dst_distributor].send(serialized_message)
                    dst_distributor = (dst_distributor + 1) % DISTRIBUTOR_AMOUNT

                elif msg_type == protocol.external.MsgType.END_OF_RECORDS:
                    serialized_message = self.message_handler.serialize_eof_message(content)
                    if in_accounts_mode:
                        for exchange in self.banks_exchanges:
                            exchange.send(serialized_message)

                        in_accounts_mode = False
                        self.enqueue_message(protocol.external.MsgType.ACK)
                    else:
                        for distributor in self.distributor_exchanges:
                            distributor.send(serialized_message)
                        self.enqueue_message(protocol.external.MsgType.ACK)
                        logging.info(f"Client {self.client_id} finished upload")
                
                elif msg_type == protocol.external.MsgType.ACK:
                    self.ack_event.set()
                    if self.all_query_ends_sent.is_set():
                        logging.info(f"Client {self.client_id} read finished")
                        return

                else:
                    raise RuntimeError(f"Unknown message type: {msg_type}")

        except socket.error:
            logging.error(f"Socket read error for client {self.client_id}")
        except Exception as e:
            logging.exception(e)


class Gateway:
    def __init__(self):
        self.client_id_generator = ClientIdGenerator()

        self.clients: dict[int, ClientSession] = {}
        self.clients_lock = threading.Lock()

        self.shutdown_event = threading.Event()
        self.query_results_thread = threading.Thread(
            target=self._query_results_loop,
            daemon=True,
            name="query-results-consumer",
        )

    def start(self):
        logging.info("Starting gateway")
        self.query_results_thread.start()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((SERVER_HOST, SERVER_PORT))
            server_socket.listen()
            logging.info(f"Listening to connections on {SERVER_HOST}:{SERVER_PORT}")
            signal.signal(
                signal.SIGTERM,
                lambda signum, frame: self.stop(server_socket),
            )

            while not self.shutdown_event.is_set():
                try:
                    client_socket, addr = server_socket.accept()
                    logging.info(f"New client connected from {addr}")

                    client_id = self.client_id_generator.generate()
                    handler = message_handler.MessageHandler(client_id)

                    session = ClientSession(client_id, client_socket, handler)

                    with self.clients_lock:
                        self.clients[client_id] = session

                    threading.Thread(
                        target=self._run_client_session,
                        args=(session,),
                        daemon=True,
                        name=f"client-{client_id}",
                    ).start()

                except socket.error:
                    if self.shutdown_event.is_set():
                        break
                    logging.error("The connection with the client was lost")
                except Exception as e:
                    logging.error(e)

        logging.info("Gateway stopped")

    def stop(self, server_socket: socket.socket):
        if self.shutdown_event.is_set():
            return

        logging.info("Stopping gateway")
        self.shutdown_event.set()
        
        server_socket.shutdown(socket.SHUT_RDWR)

        if self.query_results_thread and self.query_results_thread.is_alive():
            try:
                self.input_queue.stop_consuming()
            except Exception as e:
                logging.exception(e)
            self.query_results_thread.join()
        with self.clients_lock:
            clients = list(self.clients.values())

        for session in clients:
            session.close()

    def _run_client_session(self, session: ClientSession):
        try:
            session.start()
        finally:
            with self.clients_lock:
                self.clients.pop(session.client_id, None)

    def _query_results_loop(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE,
        )

        def _consume_result(message, ack, nack):
            try:
                matched = False
                with self.clients_lock:
                    sessions = list(self.clients.values())

                for session in sessions:
                    deserialized_message = session.message_handler.deserialize_result_message(message)
                    if not deserialized_message:
                        continue

                    matched = True
                    msg_type = deserialized_message.msg_type
                    raw_data = deserialized_message.raw_data
                    if msg_type == protocol.internal.MsgType.Q1_TRAN:
                        session.enqueue_message(protocol.external.MsgType.Q1_TRAN, raw_data)
                    elif msg_type == protocol.internal.MsgType.Q1_END:
                        session.enqueue_message(protocol.external.MsgType.Q1_END)
                    elif msg_type == protocol.internal.MsgType.Q2_RESULT:
                        session.enqueue_message(protocol.external.MsgType.Q2_RESULT, raw_data)
                    elif msg_type == protocol.internal.MsgType.Q2_END:
                        session.enqueue_message(protocol.external.MsgType.Q2_END)
                    elif msg_type == protocol.internal.MsgType.Q3_RESULT_TRAN:
                        session.enqueue_message(protocol.external.MsgType.Q3_RESULT_TRAN, raw_data)
                    elif msg_type == protocol.internal.MsgType.Q3_END:
                        session.enqueue_message(protocol.external.MsgType.Q3_END)
                    elif msg_type == protocol.internal.MsgType.Q4_LAUNDERING_ACC:
                        session.enqueue_message(protocol.external.MsgType.Q4_LAUNDERING_ACC, raw_data)
                    elif msg_type == protocol.internal.MsgType.Q4_END:
                        session.enqueue_message(protocol.external.MsgType.Q4_END)
                    elif msg_type == protocol.internal.MsgType.Q5_COUNT:
                        session.enqueue_message(protocol.external.MsgType.Q5_RESULT, raw_data)
                    else:
                        raise RuntimeError(f"Unknown internal msg type: {msg_type}")
                    break

                if not matched:
                    logging.warning("Message from RabbitMQ did not match any client")

                ack()
            except Exception as e:
                logging.exception(e)
                nack()
                self.input_queue.stop_consuming()

        self.input_queue.start_consuming(_consume_result, 0)
        self.input_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARN)
    gateway = Gateway()
    gateway.start()
    return 0


if __name__ == "__main__":
    main()
