import os
import logging
import queue
import threading
import signal

from datetime import datetime, UTC
from common.middleware.middleware import MessageMiddlewareCloseError
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ, MessageMiddlewareQueueRabbitMQ
from common.protocol.internal import MsgType, MsgEnvelope
from common.protocol.internal_messages import Q3Transaction, Q3TransactionPreceding, Q3TransactionSubsequent


ID = int(os.environ["ID"])
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
MOM_HOST = os.environ["MOM_HOST"]
MAPPER_AND_DISTRIBUTOR_AMOUNT = int(os.environ["MAPPER_AND_DISTRIBUTOR_AMOUNT"])
MAPPER_AND_DISTRIBUTOR_PREFIX = os.environ["MAPPER_AND_DISTRIBUTOR_PREFIX"]
MAPPER_AND_DISTRIBUTOR_CONTROL_EXCHANGE = "MAPPER_AND_DISTRIBUTOR_CONTROL_EXCHANGE"
PAYMENT_FORMAT_AVG_AMOUNT = int(os.environ["PAYMENT_FORMAT_AVG_AMOUNT"])
PAYMENT_FORMAT_AVG_PREFIX = os.environ["PAYMENT_FORMAT_AVG_PREFIX"]
AMOUNT_FILTER_AMOUNT = int(os.environ["AMOUNT_FILTER_AMOUNT"])
AMOUNT_FILTER_PREFIX = os.environ["AMOUNT_FILTER_PREFIX"]


class MapperAndDistributor:
    def __init__(self):
        self._input_queue = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self._control_exchange_sender = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, MAPPER_AND_DISTRIBUTOR_CONTROL_EXCHANGE, [MAPPER_AND_DISTRIBUTOR_PREFIX]
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

    def _route(self, client_id, routing_key, nodes_amount):
        key = f"{client_id}:{routing_key}".encode()
        hash_int = int.from_bytes(key, byteorder='big')
        return hash_int % nodes_amount

    def _process_data(self, client_id, data: bytes):
        """
        Process transaction data for a client. 
        If the transaction is historical, we send it to node 'payment_format_avg' for 
        average amount calculation.
        If the transaction is not historical, we send it to node 'amount_filter' for 
        filtering by amount.
        """
        logging.debug(f"Processing transaction for client {client_id}")
        transaction = Q3Transaction.deserialize(data)

        preceding_from_dt = int(datetime(2022, 9, 1, tzinfo=UTC).timestamp())
        preceding_to_dt = int(datetime(2022, 9, 5, 23, 59, 59, tzinfo=UTC).timestamp())
        if preceding_from_dt <= transaction.timestamp <= preceding_to_dt:
            logging.debug(f"Transaction is historical: sending to payment_format_avg")
            tran_preceding = Q3TransactionPreceding(transaction.payment_format_id, transaction.amount)
            msg = MsgEnvelope(client_id, MsgType.Q3_TRAN_PRECEDING, tran_preceding.serialize()).serialize()
            exch_idx = self._route(client_id, transaction.payment_format_id, PAYMENT_FORMAT_AVG_AMOUNT)
            self._queue_data_output_exchanges.put((msg, PAYMENT_FORMAT_AVG_PREFIX, [exch_idx]))

        subsequent_from_dt = int(datetime(2022, 9, 6, tzinfo=UTC).timestamp())
        subsequent_to_dt = int(datetime(2022, 9, 15, 23, 59, 59, tzinfo=UTC).timestamp())
        if subsequent_from_dt <= transaction.timestamp <= subsequent_to_dt:
            logging.debug(f"Transaction is subsequent: sending to amount_filter")
            tran_subsequent = Q3TransactionSubsequent(transaction.from_bank_id, 
                                                      transaction.from_account, 
                                                      transaction.payment_format_id, 
                                                      transaction.amount)
            msg = MsgEnvelope(client_id, MsgType.Q3_TRAN_SUBSEQUENT, tran_subsequent.serialize()).serialize()
            exch_idx = self._route(client_id, transaction.payment_format_id, AMOUNT_FILTER_AMOUNT)
            self._queue_data_output_exchanges.put((msg, AMOUNT_FILTER_PREFIX, [exch_idx]))

    def _process_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
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
                if msg.msg_type == MsgType.Q3_TRAN:
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
        Broadcast EOF message to the data output exchanges of the node 'payment_format_avg'
        """

        logging.info(f"Process EOF notification: broadcasting EOF message")
        msg = MsgEnvelope(client_id, MsgType.END_OF_RECORDS, b"").serialize()
        self._queue_data_output_exchanges.put((msg, PAYMENT_FORMAT_AVG_PREFIX, 
                                               [i for i in range(PAYMENT_FORMAT_AVG_AMOUNT)]))

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
            MOM_HOST, MAPPER_AND_DISTRIBUTOR_CONTROL_EXCHANGE, [MAPPER_AND_DISTRIBUTOR_PREFIX]
        )
        self._control_exchange_consumer.start_consuming(self._process_control_message)
        self._control_exchange_consumer.close()

    def _data_output_exchange_sender_thread(self):
        """
        Thread for sending messages to the data output exchanges.
        """

        self._data_output_exchanges = {
            PAYMENT_FORMAT_AVG_PREFIX: [MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, PAYMENT_FORMAT_AVG_PREFIX, [f"{PAYMENT_FORMAT_AVG_PREFIX}_{i}"]
            ) for i in range(PAYMENT_FORMAT_AVG_AMOUNT)],
            AMOUNT_FILTER_PREFIX: [MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AMOUNT_FILTER_PREFIX, [f"{AMOUNT_FILTER_PREFIX}_{i}"]
            ) for i in range(AMOUNT_FILTER_AMOUNT)]
        }

        while self._running or not self._queue_data_output_exchanges.empty():
            msg, type_exch, idxs_exch = self._queue_data_output_exchanges.get()
            try:
                for i in idxs_exch:
                    self._data_output_exchanges[type_exch][i].send(msg)
            except Exception as e:
                logging.error(f"Error sending message to data output exchange: {e}")
                break
            finally:
                self._queue_data_output_exchanges.task_done()

        for data_output_exchange in self._data_output_exchanges.values():
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
    logging.getLogger("pika").setLevel(logging.WARN)
    mapper_and_distributor = MapperAndDistributor()
    return mapper_and_distributor.start()

if __name__ == "__main__":
    main()
