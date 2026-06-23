import os
import logging
import queue
import signal
import threading

from common.middleware.middleware import MessageMiddlewareCloseError
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ, MessageMiddlewareQueueRabbitMQ
from common.protocol.internal import MsgType, MsgEnvelope
from common.protocol.internal_messages import Q3TransactionSubsequent, Q3Average, Q3ResultTransaction


ID = int(os.environ["ID"])
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
MOM_HOST = os.environ["MOM_HOST"]
AMOUNT_FILTER_AMOUNT = int(os.environ["AMOUNT_FILTER_AMOUNT"])
AMOUNT_FILTER_PREFIX = os.environ["AMOUNT_FILTER_PREFIX"]
AMOUNT_FILTER_CONTROL_EXCHANGE = "AMOUNT_FILTER_CONTROL_EXCHANGE"
PAYMENT_FORMAT_AVG_AMOUNT = int(os.environ["PAYMENT_FORMAT_AVG_AMOUNT"])


class AmountFilter:
    def __init__(self):
        self._input_exchange = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AMOUNT_FILTER_PREFIX, [f"{AMOUNT_FILTER_PREFIX}_{ID}"]
        )
        self._control_exchange_sender = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AMOUNT_FILTER_CONTROL_EXCHANGE, [AMOUNT_FILTER_PREFIX]
        )

        self._tran_per_payment_format: dict[int, dict] = {} # Dict[client_id, Dict[payment_format_id, List[Q3TransactionSubsequent]]]]
        self._avg_eof_received = {} # Dict[client_id, int]
        self._eof_received = {} # Dict[client_id, int]

        self._queue_data_output = queue.Queue()

        self._running = True
        self._lock_running = threading.Lock()
        self._lock_processing_message = threading.Lock()

        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def _is_leader(self):
        # TODO Implement leader election
        return ID == 0

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        with self._lock_running:
            self._running = False
        try: 
            self._input_exchange.stop_consuming()
        except Exception as e:
            logging.error(f"Error stopping consuming messages: {e}")

    def _process_transaction(self, client_id, data: bytes):
        """
        Process transaction data for a client.
        Store the subsequent transactions per payment format.
        """
        logging.debug(f"Processing transaction for client {client_id}")
        transaction = Q3TransactionSubsequent.deserialize(data)
        if client_id not in self._tran_per_payment_format:
            self._tran_per_payment_format[client_id] = {}
        if transaction.payment_format_id not in self._tran_per_payment_format[client_id]:
            self._tran_per_payment_format[client_id][transaction.payment_format_id] = []
        self._tran_per_payment_format[client_id][transaction.payment_format_id].append(transaction)

    def _process_avg(self, client_id, data: bytes):
        """
        Process average data for a client.
        Filter subsequent transactions per payment format by the average amount / 100 and 
        send the valid transactions to the output queue.
        """
        logging.info(f"Processing average data")
        average = Q3Average.deserialize(data)
        if client_id not in self._tran_per_payment_format or average.payment_format_id not in self._tran_per_payment_format[client_id]:
            logging.info(f"No subsequent transactions for client and payment format")
            return
        
        logging.info(f"Filtering transactions for client and payment format")
        valid_transactions: list[Q3TransactionSubsequent] = []
        for transaction in self._tran_per_payment_format[client_id][average.payment_format_id]:
            if transaction.amount < average.avg / 100:
                valid_transactions.append(transaction)

        logging.info(f"Sending valid transactions to the output queue")
        for transaction in valid_transactions:
            result_tran = Q3ResultTransaction(transaction.from_bank_id,
                                              transaction.from_account,
                                              transaction.payment_format_id,
                                              transaction.amount)
            msg = MsgEnvelope(client_id, MsgType.Q3_RESULT_TRAN, result_tran.serialize()).serialize()
            self._queue_data_output.put(msg)

    def _process_avg_eof(self, client_id):
        """
        Handle AVG EOF for a client. When all expected EOF messages are received,
        send the END_OF_RECORDS message to the output queue and clean up any 
        stored state for the client.
        """
        logging.info(f"Received EOF for client {client_id}")
        if client_id not in self._avg_eof_received:
            self._avg_eof_received[client_id] = 0
        self._avg_eof_received[client_id] += 1
        if self._avg_eof_received[client_id] < PAYMENT_FORMAT_AVG_AMOUNT:
            logging.info(f"Waiting for more AVG EOF messages from client")
            return

        logging.info(f"Sending Amount Filter EOF message for client {client_id}")
        self._control_exchange_sender.send(MsgEnvelope(client_id, MsgType.END_OF_RECORDS_NOTIFY, b"").serialize())

        if client_id in self._tran_per_payment_format:
            del self._tran_per_payment_format[client_id]
        del self._avg_eof_received[client_id]

    def _process_data_message(self, message, ack, nack):
        try:
            msg = MsgEnvelope.deserialize(message)
            if msg.msg_type == MsgType.Q3_TRAN_SUBSEQUENT:
                self._process_transaction(msg.client_id, msg.raw_data)
            elif msg.msg_type == MsgType.Q3_AVG:
                self._process_avg(msg.client_id, msg.raw_data)
            elif msg.msg_type == MsgType.END_OF_RECORDS:
                self._process_avg_eof(msg.client_id)
            else:
                logging.error(f"Unknown message type: {msg.msg_type}")
            ack()
        except Exception as e:
            if self._running:
                logging.error(f"Unexpected error: {e}")
                nack()
                self._input_exchange.stop_consuming()

    def _process_eof_notif(self, client_id):
        """
        Handle Amount filter EOF for a client. When all expected EOF messages are received,
        send the END_OF_RECORDS message to the output queue and clean up any 
        stored state for the client.
        """
        logging.info(f"Received Amount Filter EOF for client {client_id}")
        assert self._is_leader()
        if client_id not in self._eof_received:
            self._eof_received[client_id] = 0
        self._eof_received[client_id] += 1
        if self._eof_received[client_id] < AMOUNT_FILTER_AMOUNT:
            logging.info(f"Waiting for more Amount Filter EOF messages from client")
            return

        logging.info(f"Sending Q3_END message for client {client_id}")
        eof_msg = MsgEnvelope(client_id, MsgType.Q3_END, b"").serialize()
        self._queue_data_output.put(eof_msg)
        del self._eof_received[client_id]

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
            MOM_HOST, AMOUNT_FILTER_CONTROL_EXCHANGE, [AMOUNT_FILTER_PREFIX]
        )
        self._control_exchange_consumer.start_consuming(self._process_control_message)
        self._control_exchange_consumer.close()

    def _data_output_queue_sender_thread(self):
        """
        Thread for sending messages to the data output queue.
        """
        self._output_queue = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        while self._running or not self._queue_data_output.empty():
            msg = self._queue_data_output.get()
            try:
                self._output_queue.send(msg)
            except Exception as e:
                logging.error(f"Error sending message to data output exchange: {e}")
                break
            finally:
                self._queue_data_output.task_done()

        try:
            self._output_queue.close()
        except MessageMiddlewareCloseError as e:
            logging.error(f"Error closing RabbitMQ connections: {e}")


    def start(self):
        if self._is_leader():
            control_consumer_thread = threading.Thread(target=self._control_consumer_thread)
            control_consumer_thread.start()
        data_output_exchange_sender_thread = threading.Thread(target=self._data_output_queue_sender_thread)
        data_output_exchange_sender_thread.start()
        self._input_exchange.start_consuming(self._process_data_message)

        if self._is_leader():
            control_consumer_thread.join()
        data_output_exchange_sender_thread.join()

        exit_code = 0
        if self._running:
            with self._lock_running:
                self._running = False
            exit_code = 1

        try:
            self._input_exchange.close()
            self._control_exchange_sender.close()
        except MessageMiddlewareCloseError as e:
            logging.error(f"Error closing RabbitMQ connections: {e}")

        return exit_code

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARN)
    amount_filter = AmountFilter()
    return amount_filter.start()

if __name__ == "__main__":
    main()
