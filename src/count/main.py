import os
import logging
import signal

from common import middleware
from common.protocol import serialization
import common.protocol.internal as protocol
from common.protocol.internal_messages import Q1Transaction

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.count_by_client: dict[str, int] = {}

    def _process_tran(self, client_id, transaction: Q1Transaction):
        logging.info(f"Received transaction for client {client_id}")
        if not client_id in self.count_by_client:
            self.count_by_client[client_id] = 0
        self.count_by_client[client_id] += 1
    
    def _process_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
        logging.info(f"Sending count result for client {client_id}")
        count = self.count_by_client.get(client_id, 0)
        raw_data = serialization.serialize_uint32(count)
        out_count_msg = protocol.MsgEnvelope(client_id, protocol.MsgType.COUNT_RESULT, raw_data)
        self.output_queue.send(out_count_msg.serialize())
        del self.count_by_client[client_id]

    def process_messsage(self, message, ack, nack):
        envelope = protocol.MsgEnvelope.deserialize(message)
        if envelope.msg_type == protocol.MsgType.Q1_TRAN:
            tran = Q1Transaction.deserialize(envelope.raw_data)
            self._process_tran(envelope.client_id, tran)
        elif envelope.msg_type == protocol.MsgType.END_OF_RECORDS:
            self._process_eof(envelope.client_id)
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

def handle_sigterm(join_filter: JoinFilter):
    logging.info("SIGTERM received")
    try:
        join_filter.input_queue.stop_consuming()
    except Exception as e:
        logging.error(e)

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(join_filter))
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
