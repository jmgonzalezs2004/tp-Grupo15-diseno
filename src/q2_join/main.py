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
BANK_MAX_AMOUNT = int(os.environ["BANK_MAX_AMOUNT"])


class Q2JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self._eof_received = {} # Dict[client_id, int]

    def _process_bank_max(self, client_id, bank_max: Q2BankMax):
        logging.info(f"Received bank max for client {client_id}")
        # TODO Add bank names
        logging.info(f"Sending query 2 result for client {client_id}")
        result = Q2Result(f"Bank {bank_max.from_bank_id}", bank_max.from_account, bank_max.amount)
        out_result_msg = protocol.MsgEnvelope(client_id, protocol.MsgType.Q2_RESULT, result.serialize())
        self.output_queue.send(out_result_msg.serialize())
    
    def _process_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
        if client_id not in self._eof_received:
            self._eof_received[client_id] = 0
        self._eof_received[client_id] += 1
        if self._eof_received[client_id] < BANK_MAX_AMOUNT:
            logging.info(f"Waiting for more EOF messages from client")
            return
        
        logging.info(f"Sending END for client {client_id}")
        eof_msg = protocol.MsgEnvelope(client_id, protocol.MsgType.Q2_END, b"")
        self.output_queue.send(eof_msg.serialize())

        del self._eof_received[client_id]

    def process_messsage(self, message, ack, nack):
        envelope = protocol.MsgEnvelope.deserialize(message)
        if envelope.msg_type == protocol.MsgType.Q2_BANK_MAX:
            bank_max = Q2BankMax.deserialize(envelope.raw_data)
            self._process_bank_max(envelope.client_id, bank_max)
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
