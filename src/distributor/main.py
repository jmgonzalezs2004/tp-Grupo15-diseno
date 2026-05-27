import os
import logging
import signal

from common import middleware
import common.protocol.internal as protocol
from common.protocol.internal_messages import Q1Transaction, Q2Transaction, Q3Transaction, Q4Transaction2Acc, Q5Transaction, SerializableMessage, Transaction
from criteria.criteria import build_criteria_for_query

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
Q1_QUEUE = os.environ["Q1_QUEUE"]
Q2_QUEUE = os.environ["Q2_QUEUE"]
Q3_QUEUE = os.environ["Q3_QUEUE"]
Q4_QUEUE = os.environ["Q4_QUEUE"]
Q5_QUEUE = os.environ["Q5_QUEUE"]


class Distributor:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.q1_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, Q1_QUEUE
        )
        self.q2_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, Q2_QUEUE
        )
        self.q3_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, Q3_QUEUE
        )
        self.q4_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, Q4_QUEUE
        )
        self.q5_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, Q5_QUEUE
        )
        try:
            self.q1_criteria = build_criteria_for_query(1)
            self.q2_criteria = build_criteria_for_query(2)
            self.q3_criteria = build_criteria_for_query(3)
            self.q4_criteria = build_criteria_for_query(4)
            self.q5_criteria = build_criteria_for_query(5)
        except ValueError:
            self.stop()
            raise

    def _distribute_tran(self, client_id, tran: SerializableMessage, target_queue: middleware.MessageMiddlewareQueueRabbitMQ):
        message = protocol.MsgEnvelope(client_id, tran.MESSAGE_TYPE, tran.serialize())
        target_queue.send(message.serialize())

    def _process_tran(self, client_id, transaction: Transaction) -> bool:
        logging.debug(f"Received transaction for client {client_id}")
        if self.q1_criteria.check(transaction):
            logging.debug(f"Sending transaction to query 1 for client {client_id}")
            q_tran = Q1Transaction.from_transaction(transaction)
            self._distribute_tran(client_id, q_tran, self.q1_output_queue)
        if self.q2_criteria.check(transaction):
            logging.debug(f"Sending transaction to query 2 for client {client_id}")
            q_tran = Q2Transaction.from_transaction(transaction)
            self._distribute_tran(client_id, q_tran, self.q2_output_queue)
        if self.q3_criteria.check(transaction):
            logging.debug(f"Sending transaction to query 3 for client {client_id}")
            q_tran = Q3Transaction.from_transaction(transaction)
            self._distribute_tran(client_id, q_tran, self.q3_output_queue)
        if self.q4_criteria.check(transaction):
            logging.debug(f"Sending transaction to query 4 for client {client_id}")
            q_tran = Q4Transaction2Acc.from_transaction(transaction)
            self._distribute_tran(client_id, q_tran, self.q4_output_queue)
        if self.q5_criteria.check(transaction):
            logging.debug(f"Sending transaction to query 5 for client {client_id}")
            q_tran = Q5Transaction.from_transaction(transaction)
            self._distribute_tran(client_id, q_tran, self.q5_output_queue)

    
    def _process_eof(self, client_id, message):
        logging.info(f"Received EOF for client {client_id}")
        logging.info(f"Sending EOF for client {client_id}")
        self.q1_output_queue.send(message)
        self.q2_output_queue.send(message)
        self.q3_output_queue.send(message)
        self.q4_output_queue.send(message)
        self.q5_output_queue.send(message)

    def process_messsage(self, message, ack, nack):
        envelope = protocol.MsgEnvelope.deserialize(message)
        if envelope.msg_type == protocol.MsgType.TRAN_RECORD:
            tran = Transaction.deserialize(envelope.raw_data)
            self._process_tran(envelope.client_id, tran)
        elif envelope.msg_type == protocol.MsgType.END_OF_RECORDS:
            self._process_eof(envelope.client_id, message)
        else:
            raise RuntimeError(f"msg_type {envelope.msg_type} not supported")
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)
        self.stop()

    def stop(self):
        logging.info("Stopping Distributor...")
        self.input_queue.close()
        self.q1_output_queue.close()
        self.q2_output_queue.close()
        self.q3_output_queue.close()
        self.q4_output_queue.close()
        self.q5_output_queue.close()

def handle_sigterm(distributor: Distributor):
    logging.info("SIGTERM received")
    try:
        distributor.input_queue.stop_consuming()
    except Exception as e:
        logging.error(e)

def main():
    logging.basicConfig(level=logging.WARN)
    try:
        distributor = Distributor()
    except ValueError as e:
        logging.error(e)
        return 1
    logging.getLogger().setLevel(logging.INFO)
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(distributor))
    distributor.start()

    return 0


if __name__ == "__main__":
    main()
