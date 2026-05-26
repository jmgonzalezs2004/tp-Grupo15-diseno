import os
import logging
import queue
import threading
import signal

from common.middleware.middleware import MessageMiddlewareCloseError
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ, MessageMiddlewareQueueRabbitMQ
from common.protocol.internal import MsgType, MsgEnvelope
from common.protocol.internal_messages import Q4Transaction2Acc


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
        self._input_queue = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self._control_exchange_sender = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, ACCOUNTS_MAPPER_CONTROL_EXCHANGE, [ACCOUNTS_MAPPER_PREFIX]
        )
        self._queue_data_output_exchanges = queue.Queue()

        self._running = True
        self._lock_running = threading.Lock()
        self._lock_processing_message = threading.Lock()

        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        with self._lock_running:
            self._running = False
        try: 
            self._input_queue.stop_consuming()
        except Exception as e:
            logging.error(f"Error stopping consuming messages: {e}")

    def _route(self, client_id, account):
        key = f"{client_id}:{account.bank_id}:{account.account_id}".encode()
        hash_int = int.from_bytes(key, byteorder='big')
        return hash_int % THREE_CHAIN_AMOUNT

    def _process_data(self, client_id, data: bytes):
        """
        Process a transaction of 2 accounts. Route the transaction by both the source 
        and destination accounts to the corresponding data output exchange.
        """

        logging.info(f"Processing transaction data")
        transaction_2acc = Q4Transaction2Acc.deserialize(data)

        exchange_index_source = self._route(client_id, transaction_2acc.from_acc)
        exchange_index_dest = self._route(client_id, transaction_2acc.to_acc)

        msg = MsgEnvelope(client_id, MsgType.Q4_TRAN_2ACC, transaction_2acc.serialize()).serialize()
        self._queue_data_output_exchanges.put((msg, list(set([exchange_index_source, exchange_index_dest]))))

    def _process_eof(self, client_id):
        logging.info(f"Received EOF")
        self._queue_data_output_exchanges.join()
        self._publish_eof(client_id)
    
    def _publish_eof(self, client_id):
        """
        Publish an EOF message to the control exchange to notify that all 
        transaction records of a client were processed.
        """

        logging.info(f"Publishing EOF message")
        msg = MsgEnvelope(client_id, MsgType.END_OF_RECORDS_NOTIFY, b"").serialize()
        self._control_exchange_sender.send(msg)

    def _process_data_message(self, message, ack, nack):
        with self._lock_processing_message:
            try:
                msg = MsgEnvelope.deserialize(message)
                if msg.msg_type == MsgType.Q4_TRAN_2ACC:
                    self._process_data(msg.client_id, msg.raw_data)
                elif msg.msg_type == MsgType.END_OF_RECORDS:
                    self._process_eof(msg.client_id)
                else:
                    logging.error(f"Unknown message type: {msg.msg_type}")
                ack()
            except Exception as e:
                if self._running:
                    logging.error(f"Unexpected error: {e}")
                    nack()
                    self._input_queue.stop_consuming()

    def _process_eof_notif(self, client_id):
        """
        Broadcast EOF message to the data output exchanges.
        """

        logging.info(f"Process EOF notification: broadcasting EOF message")
        msg = MsgEnvelope(client_id, MsgType.END_OF_RECORDS, b"").serialize()
        self._queue_data_output_exchanges.put((msg, [i for i in range(THREE_CHAIN_AMOUNT)]))

    def _process_control_message(self, message, ack, nack):
        with self._lock_processing_message:
            try:
                msg = MsgEnvelope.deserialize(message)
                if msg.msg_type == MsgType.END_OF_RECORDS_NOTIFY:
                    self._process_eof_notif(msg.client_id)
                else:
                    logging.error(f"Unknown control message type: {msg.msg_type}")
                ack()
            except Exception as e:
                if self._running:
                    logging.error(f"Unexpected error in control message processing: {e}")
                    nack()
                    self._control_exchange_consumer.stop_consuming()

    def _control_consumer_thread(self):
        """
        Thread for consuming messages from the control exchange.
        """

        self._control_exchange_consumer = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, ACCOUNTS_MAPPER_CONTROL_EXCHANGE, [ACCOUNTS_MAPPER_PREFIX]
        )
        self._control_exchange_consumer.start_consuming(self._process_control_message)
        self._control_exchange_consumer.close()

    def _data_output_exchange_sender_thread(self):
        """
        Thread for sending messages to the data output exchanges.
        """

        self._data_output_exchanges = [
            MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, THREE_CHAIN_PREFIX, [f"{THREE_CHAIN_PREFIX}_{i}"]
            ) for i in range(THREE_CHAIN_AMOUNT)
        ]

        while self._running or not self._queue_data_output_exchanges.empty():
            msg, idx_exchanges = self._queue_data_output_exchanges.get()
            try:
                for i in idx_exchanges:
                    self._data_output_exchanges[i].send(msg)
            except Exception as e:
                logging.error(f"Error sending message to data output exchange: {e}")
                break
            finally:
                self._queue_data_output_exchanges.task_done()
        
        for data_output_exchange in self._data_output_exchanges:
            try:
                data_output_exchange.close()
            except MessageMiddlewareCloseError as e:
                logging.error(f"Error closing RabbitMQ connections: {e}")

    def start(self):
        control_consumer_thread = threading.Thread(target=self._control_consumer_thread)
        control_consumer_thread.start()
        data_output_exchange_sender_thread = threading.Thread(target=self._data_output_exchange_sender_thread)
        data_output_exchange_sender_thread.start()
        self._input_queue.start_consuming(self._process_data_message)

        control_consumer_thread.join()
        data_output_exchange_sender_thread.join()

        exit_code = 0
        if self._running:
            with self._lock_running:
                self._running = False
            exit_code = 1

        try:
            self._input_queue.close()
            self._control_exchange_sender.close()
        except MessageMiddlewareCloseError as e:
            logging.error(f"Error closing RabbitMQ connections: {e}")

        return exit_code

def main():
    logging.basicConfig(level=logging.INFO)
    accounts_mapper = AccountsMapper()
    return accounts_mapper.start()

if __name__ == "__main__":
    main()
