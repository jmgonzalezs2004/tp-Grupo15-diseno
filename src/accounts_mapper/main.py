import os
import logging
import queue
import threading
import signal

from common import middleware
from common.protocol.internal import MsgType, MsgEnvelope
from common.protocol.memory_reader import MemoryReader
from common.protocol.internal_msgs.q4_msgs import Transaction2Accounts


ID = int(os.environ["ID"])
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
MOM_HOST = os.environ["MOM_HOST"]
ACCOUNTS_MAPPER_AMOUNT = int(os.environ["ACCOUNTS_MAPPER_AMOUNT"])
ACCOUNTS_MAPPER_PREFIX = os.environ["ACCOUNTS_MAPPER_PREFIX"]
ACCOUNTS_MAPPER_CONTROL_EXCHANGE = "ACCOUNTS_MAPPER_CONTROL_EXCHANGE"
THREE_CHAIN_AMOUNT = int(os.environ["THREE_CHAIN_AMOUNT"])
THREE_CHAIN_PREFIX = os.environ["THREE_CHAIN_PREFIX"]

class AccountsMapper:
    def __init__(self):
        self._input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self._control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, ACCOUNTS_MAPPER_CONTROL_EXCHANGE, [ACCOUNTS_MAPPER_PREFIX]
        )
        self._data_output_exchanges = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, THREE_CHAIN_PREFIX, [f"{THREE_CHAIN_PREFIX}_{i}"] 
            ) for i in range(THREE_CHAIN_AMOUNT)
        ]

        self._sender_working_queue = queue.Queue()

        self._running = True

        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self._running = False
        self._stop_consuming_messages()
        self._sender_working_queue.put(None)

    def _stop_consuming_messages(self):
        logging.info("Stopping consuming messages")
        try: 
            self._input_queue.stop_consuming()
            self._control_exchange.stop_consuming()
        except middleware.MessageMiddlewareDisconnectedError as e:
            logging.error(f"Error middleware disconnected: {e}")
        except Exception as e:
            logging.error(f"Error stopping consuming messages: {e}")

    def _route(self, client_id, account):
        key = f"{client_id}:{account}".encode()
        hash_int = int.from_bytes(key, byteorder='big')
        return hash_int % THREE_CHAIN_AMOUNT

    def _process_data(self, client_id, data: bytes):
        """
        Process a transaction of 2 accounts. Route the transaction by both the source 
        and destination accounts to the corresponding data output exchange.
        """

        logging.info(f"Processing transaction data")
        transaction_2acc = Transaction2Accounts.deserialize(MemoryReader(data))

        exchange_index_source = self._route(client_id, transaction_2acc.source_acc)
        exchange_index_dest = self._route(client_id, transaction_2acc.dest_acc)

        msg = MsgEnvelope(client_id, MsgType.Q4_TRAN_2ACC, transaction_2acc.serialize()).serialize()
        self._sender_working_queue.put((self._data_output_exchanges[exchange_index_source], msg))
        self._sender_working_queue.put((self._data_output_exchanges[exchange_index_dest], msg))

    def _process_eof(self, client_id):
        logging.info(f"Received EOF")
        self._publish_eof(client_id)
    
    def _publish_eof(self, client_id):
        """
        Publish an EOF message to the control exchange to notify that all 
        transaction records of a client were processed.
        """

        logging.info(f"Publishing EOF message")
        msg = MsgEnvelope(client_id, MsgType.END_OF_RECORDS_NOTIF, b"").serialize()
        self._sender_working_queue.put((self._control_exchange, msg))

    def _process_eof_notif(self, client_id):
        """
        Broadcast EOF message to the data output exchanges.
        """

        logging.info(f"Process EOF notification: broadcasting EOF message")
        for data_output_exchange in self._data_output_exchanges:
            msg = MsgEnvelope(client_id, MsgType.END_OF_RECORDS, b"").serialize()
            self._sender_working_queue.put((data_output_exchange, msg))

    def _process_data_message(self, message, ack, nack):
        try:
            msg = MsgEnvelope.deserialize(message)
            if msg.msg_type == MsgType.Q4_TRAN_2ACC:
                self._process_data(msg.client_id, msg.raw_data)
            elif msg.msg_type == MsgType.END_OF_RECORDS:
                self._process_eof(msg.client_id)
            elif msg.msg_type == MsgType.END_OF_RECORDS_NOTIF:
                self._process_eof_notif(msg.client_id)
            else:
                logging.error(f"Unknown message type: {msg.msg_type}")
            ack()
        except Exception as e:
            if self._running:
                logging.error(f"Unexpected error: {e}")
                nack()
                self._stop_consuming_messages()
                self._sender_working_queue.put(None)

    def _sender_loop(self):
        """
        Loop for sending messages asynchronously to the exchanges. Messages to be sent 
        will be received from the sender working queue.
        """

        while True:
            task = self._sender_working_queue.get()
            try:
                if task is None:  # EXIT
                    break
                exchange, message = task
                exchange.send(message)
            except middleware.MessageMiddlewareDisconnectedError as e:
                if self._running:
                    logging.error(f"Connection with RabbitMQ server lost: {e}")
            except middleware.MessageMiddlewareMessageError as e:
                if self._running:
                    logging.error(f"Error processing message: {e}")
            finally:
                self._sender_working_queue.task_done()

    def start(self):
        sender_thread = threading.Thread(target=self._sender_loop)
        sender_thread.start()

        control_consumer_thread = threading.Thread(target=self._control_exchange.start_consuming, 
                                                   args=(self._process_data_message,))
        control_consumer_thread.start()

        self._input_queue.start_consuming(self._process_data_message)

        control_consumer_thread.join()

        exit_code = 0
        if self._running:
            self._running = False
            exit_code = 1

        sender_thread.join()

        try:
            self._input_queue.close()
            self._control_exchange.close()
            for data_output_exchange in self._data_output_exchanges:
                data_output_exchange.close()
        except middleware.MessageMiddlewareCloseError as e:
            logging.error(f"Error closing RabbitMQ connections: {e}")

        return exit_code

def main():
    logging.basicConfig(level=logging.INFO)
    accounts_mapper = AccountsMapper()
    return accounts_mapper.start()

if __name__ == "__main__":
    main()
