import os
import logging
import signal

from common import middleware
import common.protocol.internal as protocol
from common.protocol.internal_messages import Q2BankMax, Q2Transaction

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
BANK_MAX_AMOUNT = int(os.environ["BANK_MAX_AMOUNT"])
BANK_MAX_PREFIX = os.environ["BANK_MAX_PREFIX"]
BANK_MAPPER_AMOUNT = int(os.environ["BANK_MAPPER_AMOUNT"])


class BankMaxFilter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, BANK_MAX_PREFIX, [f"{BANK_MAX_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.max_by_bank_client: dict[int, dict[int, Q2BankMax]] = {}
        self._eofs_by_client: dict[int, int] = {}

    def _process_tran(self, client_id, transaction: Q2Transaction) -> bool:
        logging.debug(f"Received transaction for client {client_id}")
        if not client_id in self.max_by_bank_client:
            self.max_by_bank_client[client_id] = {}
        
        max_by_bank = self.max_by_bank_client[client_id]
        if (not transaction.from_bank_id in max_by_bank or 
            transaction.amount > max_by_bank[transaction.from_bank_id].amount or
            (transaction.amount == max_by_bank[transaction.from_bank_id].amount and
            transaction.from_account < max_by_bank[transaction.from_bank_id].from_account)):
            max_by_bank[transaction.from_bank_id] = Q2BankMax.from_transaction(transaction)
    
    def _process_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
        if client_id not in self._eofs_by_client:
            self._eofs_by_client[client_id] = 0
        self._eofs_by_client[client_id] += 1
        if self._eofs_by_client[client_id] < BANK_MAPPER_AMOUNT:
            logging.debug(f"Waiting for more EOF messages from client")
            return

        client_max_results = list(self.max_by_bank_client.pop(client_id, {}).values())
        logging.info(f"Sending partial MAX of {len(client_max_results)} banks for client {client_id}")
        for bank_max_entry in client_max_results:
            message = protocol.MsgEnvelope(client_id, protocol.MsgType.Q2_BANK_MAX, bank_max_entry.serialize())
            self.output_queue.send(message.serialize())
        
        logging.info(f"Sending EOF for client {client_id}")
        eof_msg = protocol.MsgEnvelope(client_id, protocol.MsgType.END_OF_RECORDS, b"")
        self.output_queue.send(eof_msg.serialize())
        del self._eofs_by_client[client_id]

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
        self.input_exchange.start_consuming(self.process_messsage)
        self.stop()

    def stop(self):
        logging.info("Stopping BankMaxFilter...")
        self.input_exchange.close()
        self.output_queue.close()

def handle_sigterm(bank_max_filter: BankMaxFilter):
    logging.info("SIGTERM received")
    try:
        bank_max_filter.input_exchange.stop_consuming()
    except Exception as e:
        logging.error(e)

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARN)
    bank_max_filter = BankMaxFilter()
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(bank_max_filter))
    bank_max_filter.start()

    return 0


if __name__ == "__main__":
    main()
