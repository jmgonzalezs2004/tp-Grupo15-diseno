import os
import logging
import signal

from common import middleware
from common.protocol import serialization
import common.protocol.internal as protocol
from common.protocol.internal_messages import Q2BankMax, Q2Result

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]


class Q2JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

    def _process_partial_max(self, client_id, partial_max: list[Q2BankMax]):
        logging.info(f"Received partial bank max for client {client_id}")
        # TODO Add bank names
        logging.info(f"Sending query 2 result for client {client_id}")
        # TODO Send individual result instead of a list
        results = [Q2Result(f"Bank {item.from_bank_id}", item.from_account, item.amount) for item in partial_max]
        raw_data = serialization.serialize_list(results, Q2Result.serialize)
        out_result_msg = protocol.MsgEnvelope(client_id, protocol.MsgType.Q2_RESULT, raw_data)
        self.output_queue.send(out_result_msg.serialize())

    def process_messsage(self, message, ack, nack):
        envelope = protocol.MsgEnvelope.deserialize(message)
        if envelope.msg_type == protocol.MsgType.Q2_BANK_MAX:
            partial_max = serialization.deserialize_list(envelope.raw_data, Q2BankMax.deserialize_reader)
            self._process_partial_max(envelope.client_id, partial_max)
        else:
            raise RuntimeError(f"msg_type {envelope.msg_type} not supported")
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)
        self.stop()

    def stop(self):
        logging.info("Stopping JoinFilter...")
        self.input_queue.close()
        self.output_queue.close()

def handle_sigterm(q2_join_filter: Q2JoinFilter):
    logging.info("SIGTERM received")
    try:
        q2_join_filter.input_queue.stop_consuming()
    except Exception as e:
        logging.error(e)

def main():
    logging.basicConfig(level=logging.INFO)
    q2_join_filter = Q2JoinFilter()
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(q2_join_filter))
    q2_join_filter.start()

    return 0


if __name__ == "__main__":
    main()
