import os
import logging
import signal

from common import middleware
from common.protocol import serialization
import common.protocol.internal as protocol
from common.protocol.internal_messages import Q2BankMax, Q2Result, Q5Count

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
CONV_FILTER_AMOUNT = int(os.environ["CONV_FILTER_AMOUNT"])


class Q5Join:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self._count_num_by_client: dict[int, int] = {}
        self._count_accum_by_client: dict[int, int] = {}

    def _process_count(self, client_id, count: int):
        logging.info(f"Received count for client {client_id}")
        if client_id not in self._count_num_by_client:
            self._count_num_by_client[client_id] = 0
            self._count_accum_by_client[client_id] = 0
        self._count_num_by_client[client_id] += 1
        self._count_accum_by_client[client_id] += count

        if self._count_num_by_client[client_id] < CONV_FILTER_AMOUNT:
            logging.info(f"Waiting for more COUNT messages from client {client_id}")
            return
        
        logging.info(f"Sending COUNT for client {client_id}")
        count_msg = Q5Count(self._count_accum_by_client.get(client_id, 0))
        out_count_msg = protocol.MsgEnvelope(client_id, protocol.MsgType.Q5_COUNT, count_msg.serialize())
        self.output_queue.send(out_count_msg.serialize())

        del self._count_num_by_client[client_id]
        del self._count_accum_by_client[client_id]

    def process_messsage(self, message, ack, nack):
        envelope = protocol.MsgEnvelope.deserialize(message)
        if envelope.msg_type == protocol.MsgType.Q5_COUNT:
            count_msg = Q5Count.deserialize(envelope.raw_data)
            self._process_count(envelope.client_id, count_msg.count)
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

def handle_sigterm(q5_join: Q5Join):
    logging.info("SIGTERM received")
    try:
        q5_join.input_queue.stop_consuming()
    except Exception as e:
        logging.error(e)

def main():
    logging.basicConfig(level=logging.INFO)
    q5_join = Q5Join()
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(q5_join))
    q5_join.start()

    return 0


if __name__ == "__main__":
    main()
