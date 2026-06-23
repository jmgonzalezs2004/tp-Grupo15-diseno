import os
import logging
import signal

from common.middleware.middleware import MessageMiddlewareCloseError
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ
from common.protocol.internal import MsgType, MsgEnvelope
from common.protocol.internal_messages import Q3TransactionPreceding, Q3Average


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
PAYMENT_FORMAT_AVG_AMOUNT = int(os.environ["PAYMENT_FORMAT_AVG_AMOUNT"])
PAYMENT_FORMAT_AVG_PREFIX = os.environ["PAYMENT_FORMAT_AVG_PREFIX"]
AMOUNT_FILTER_AMOUNT = int(os.environ["AMOUNT_FILTER_AMOUNT"])
AMOUNT_FILTER_PREFIX = os.environ["AMOUNT_FILTER_PREFIX"]
MAPPER_AND_DISTRIBUTOR_AMOUNT = int(os.environ["MAPPER_AND_DISTRIBUTOR_AMOUNT"])


class PaymentFormatAvg:
    def __init__(self):
        self._input_exchange = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, PAYMENT_FORMAT_AVG_PREFIX, [f"{PAYMENT_FORMAT_AVG_PREFIX}_{ID}"]
        )
        self._data_output_exchanges = [
            MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AMOUNT_FILTER_PREFIX, [f"{AMOUNT_FILTER_PREFIX}_{i}"]
            ) for i in range(AMOUNT_FILTER_AMOUNT)
        ]

        self._sum_count_per_payment_format: dict[int, dict] = {} # Dict[client_id, Dict[payment_format_id, tuple(sum, count)]]]
        self._eof_received: dict[int, int] = {} # Dict[client_id, int]

        self._running = True

        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self._running = False
        try: 
            self._input_exchange.stop_consuming()
        except Exception as e:
            logging.error(f"Error stopping consuming messages: {e}")

    def _route(self, client_id, routing_key, nodes_amount):
        key = f"{client_id}:{routing_key}".encode()
        hash_int = int.from_bytes(key, byteorder='big')
        return hash_int % nodes_amount

    def _process_data(self, client_id, data: bytes):
        """
        Process transaction data for a client. 
        Store the sum and count of amounts per payment format.
        """

        logging.debug(f"Processing transaction for client {client_id}")
        transaction = Q3TransactionPreceding.deserialize(data)
        if client_id not in self._sum_count_per_payment_format:
            self._sum_count_per_payment_format[client_id] = {}
        if transaction.payment_format_id not in self._sum_count_per_payment_format[client_id]:
            self._sum_count_per_payment_format[client_id][transaction.payment_format_id] = (0, 0)
        
        sum_amount, count = self._sum_count_per_payment_format[client_id][transaction.payment_format_id]
        sum_amount += transaction.amount
        count += 1
        self._sum_count_per_payment_format[client_id][transaction.payment_format_id] = (sum_amount, count)

    def _process_eof(self, client_id):
        """
        Handle EOF for a client. When all expected EOF messages are received,
        generate and emit all averages per payment format, then send the final
        END_OF_RECORDS messages to the 'amount_filter' nodes.
        """

        logging.info(f"Received EOF")
        if client_id not in self._eof_received:
            self._eof_received[client_id] = 0
        self._eof_received[client_id] += 1
        if self._eof_received[client_id] < MAPPER_AND_DISTRIBUTOR_AMOUNT:
            logging.info(f"Waiting for more EOF messages from client")
            return
        
        logging.info(f"All EOF messages received for client. Sending averages")
        if client_id in self._sum_count_per_payment_format:
            for payment_format_id, (sum_amount, count) in self._sum_count_per_payment_format[client_id].items():
                average = sum_amount / count
                q3_avg = Q3Average(payment_format_id, average).serialize()
                msg = MsgEnvelope(client_id, MsgType.Q3_AVG, q3_avg).serialize()
                exch_idx = self._route(client_id, payment_format_id, AMOUNT_FILTER_AMOUNT)
                self._data_output_exchanges[exch_idx].send(msg)

        logging.info(f"Sending END_OF_RECORDS message for client")
        for data_output_exchange in self._data_output_exchanges:
            data_output_exchange.send(MsgEnvelope(client_id, MsgType.END_OF_RECORDS, b"").serialize())

        if client_id in self._sum_count_per_payment_format:
            del self._sum_count_per_payment_format[client_id]
        del self._eof_received[client_id]

    def _process_data_message(self, message, ack, nack):
        try:
            msg = MsgEnvelope.deserialize(message)
            if msg.msg_type == MsgType.Q3_TRAN_PRECEDING:
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
                self._input_exchange.stop_consuming()

    def start(self):
        self._input_exchange.start_consuming(self._process_data_message)

        try:
            self._input_exchange.close()
            for data_output_exchanges in self._data_output_exchanges:
                data_output_exchanges.close()
        except MessageMiddlewareCloseError as e:
            logging.error(f"Error closing RabbitMQ connections: {e}")

        if self._running:
            return 1
        return 0

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARN)
    payment_format_avg = PaymentFormatAvg()
    return payment_format_avg.start()

if __name__ == "__main__":
    main()
