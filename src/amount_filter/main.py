import os
import logging
import signal

from common.middleware.middleware import MessageMiddlewareCloseError
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ, MessageMiddlewareQueueRabbitMQ
from common.protocol.internal import MsgType, MsgEnvelope
from common.protocol.internal_messages import Q3TransactionSubsequent, Q3Average, Q3ResultTransaction


ID = int(os.environ["ID"])
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
MOM_HOST = os.environ["MOM_HOST"]
AMOUNT_FILTER_AMOUNT = int(os.environ["AMOUNT_FILTER_AMOUNT"])
AMOUNT_FILTER_PREFIX = os.environ["AMOUNT_FILTER_PREFIX"]
PAYMENT_FORMAT_AVG_AMOUNT = int(os.environ["PAYMENT_FORMAT_AVG_AMOUNT"])


class AmountFilter:
    def __init__(self):
        self._input_exchange = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AMOUNT_FILTER_PREFIX, [f"{AMOUNT_FILTER_PREFIX}_{ID}"]
        )
        self._output_queue = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self._tran_per_payment_format = {} # Dict[client_id, Dict[payment_format_id, List[Q3TransactionSubsequent]]]]
        self._eof_received = {} # Dict[client_id, int]

        self._running = True

        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
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
        valid_transactions = []
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
            self._output_queue.send(msg)

    def _process_eof(self, client_id):
        """
        Handle EOF for a client. When all expected EOF messages are received,
        send the END_OF_RECORDS message to the output queue and clean up any 
        stored state for the client.
        """
        logging.info(f"Received EOF for client {client_id}")
        if client_id not in self._eof_received:
            self._eof_received[client_id] = 0
        self._eof_received[client_id] += 1
        if self._eof_received[client_id] < PAYMENT_FORMAT_AVG_AMOUNT:
            logging.info(f"Waiting for more EOF messages from client")
            return

        logging.info(f"Sending END_OF_RECORDS message for client {client_id}")
        self._output_queue.send(MsgEnvelope(client_id, MsgType.END_OF_RECORDS, b"").serialize())

        if client_id in self._tran_per_payment_format:
            del self._tran_per_payment_format[client_id]
        del self._eof_received[client_id]

    def _process_data_message(self, message, ack, nack):
        try:
            msg = MsgEnvelope.deserialize(message)
            if msg.msg_type == MsgType.Q3_TRAN_SUBSEQUENT:
                self._process_transaction(msg.client_id, msg.raw_data)
            elif msg.msg_type == MsgType.Q3_AVG:
                self._process_avg(msg.client_id, msg.raw_data)
            elif msg.msg_type == MsgType.END_OF_RECORDS:
                self._process_eof(msg.client_id)
            else:
                logging.error(f"Unknown message type: {msg.msg_type}")
            ack()
        except Exception as e:
            if self._running:
                logging.error(f"Unexpected error: {e}")
                nack()
                self._input_exchange.stop_consuming()

    def start(self):
        self._input_exchange.start_consuming(self._process_data_message)

        try:
            self._input_exchange.close()
            self._output_queue.close()
        except MessageMiddlewareCloseError as e:
            logging.error(f"Error closing RabbitMQ connections: {e}")

        if self._running:
            return 1
        return 0

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARN)
    amount_filter = AmountFilter()
    return amount_filter.start()

if __name__ == "__main__":
    main()
