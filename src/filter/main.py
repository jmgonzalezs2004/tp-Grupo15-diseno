import os
import logging
import signal

from common import middleware
import common.protocol.internal as protocol
from common.protocol.internal_messages import Transaction
from criteria.criteria import CurrencyCriteria, build_criteria_from_kind

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
Q2_OUTPUT_QUEUE = os.environ["Q2_OUTPUT_QUEUE"]
FILTER_KIND = os.environ["FILTER_KIND"]


# Unconvenient naming convention
class FilterFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.q2_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, Q2_OUTPUT_QUEUE
        )
        try:
            self.criteria = build_criteria_from_kind(FILTER_KIND)
        except ValueError:
            self.stop()
            raise

    def _process_tran(self, client_id, transaction: Transaction) -> bool:
        logging.info(f"Received transaction for client {client_id}")
        if self.criteria.check(transaction):
            logging.info(f"Transaction filter passed for client {client_id}")
            return True
        else:
            logging.info(f"Transaction filter refused for client {client_id}")
            return False

    
    def _process_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
        logging.info(f"Sending EOF for client {client_id}")

    def process_messsage(self, message, ack, nack):
        envelope = protocol.MsgEnvelope.deserialize(message)
        if envelope.msg_type == protocol.MsgType.TRAN_RECORD:
            tran = Transaction.deserialize(envelope.raw_data)
            if self._process_tran(envelope.client_id, tran):
                self.output_queue.send(message)
                self.q2_output_queue.send(message)
        elif envelope.msg_type == protocol.MsgType.END_OF_RECORDS:
            self._process_eof(envelope.client_id)
            self.output_queue.send(message)
            self.q2_output_queue.send(message)
        else:
            raise RuntimeError(f"msg_type {envelope.msg_type} not supported")
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)
        self.stop()

    def stop(self):
        logging.info("Stopping FilterFilter...")
        self.input_queue.close()
        self.output_queue.close()
        self.q2_output_queue.close()

def handle_sigterm(filter_filter: FilterFilter):
    logging.info("SIGTERM received")
    try:
        filter_filter.input_queue.stop_consuming()
    except Exception as e:
        logging.error(e)

def main():
    logging.basicConfig(level=logging.INFO)
    try:
        filter_filter = FilterFilter()
    except ValueError as e:
        logging.error(e)
        return 1
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(filter_filter))
    filter_filter.start()

    return 0


if __name__ == "__main__":
    main()
