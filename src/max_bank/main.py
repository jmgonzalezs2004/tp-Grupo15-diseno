import os
import logging
import signal

from common import middleware
from common.protocol import serialization
import common.protocol.internal as protocol
from common.protocol.internal_messages import Q2BankMax, Q2Transaction

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]


class MaxBankFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.max_by_bank_client: dict[str, dict[int, Q2BankMax]] = {}

    def _process_tran(self, client_id, transaction: Q2Transaction) -> bool:
        logging.info(f"Received transaction for client {client_id}")
        if not client_id in self.max_by_bank_client:
            self.max_by_bank_client[client_id] = {}
        
        max_by_bank = self.max_by_bank_client[client_id]
        if not transaction.from_bank_id in max_by_bank or max_by_bank[transaction.from_bank_id].amount < transaction.amount:
            max_by_bank[transaction.from_bank_id] = Q2BankMax.from_transaction(transaction)
    
    def _process_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")

        logging.info(f"Sending partial MAX for client {client_id}")
        client_max_results = list(self.max_by_bank_client.pop(client_id, {}).values())
        raw_data = serialization.serialize_list(client_max_results, Q2BankMax.serialize)
        message = protocol.MsgEnvelope(client_id, protocol.MsgType.Q2_BANK_MAX, raw_data)
        self.output_queue.send(message.serialize())

    def process_messsage(self, message, ack, nack):
        envelope = protocol.MsgEnvelope.deserialize(message)
        if envelope.msg_type == protocol.MsgType.Q2_TRAN:
            tran = Q2Transaction.deserialize(envelope.raw_data)
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

def handle_sigterm(max_bank_filter: MaxBankFilter):
    logging.info("SIGTERM received")
    try:
        max_bank_filter.input_queue.stop_consuming()
    except Exception as e:
        logging.error(e)

def main():
    logging.basicConfig(level=logging.INFO)
    max_bank_filter = MaxBankFilter()
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(max_bank_filter))
    max_bank_filter.start()

    return 0


if __name__ == "__main__":
    main()
