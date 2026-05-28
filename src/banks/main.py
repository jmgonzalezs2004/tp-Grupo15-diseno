from dataclasses import dataclass
import os
import logging
import signal

from common import middleware
import common.protocol.internal as protocol
from common.protocol.internal_messages import BankNameRequest, BankNameResponse, BankRecord

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
BANKS_AMOUNT = int(os.environ["BANKS_AMOUNT"])
BANKS_PREFIX = os.environ["BANKS_PREFIX"]

class Banks:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, BANKS_PREFIX, [f"{BANKS_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.bank_names_by_client: dict[str, dict[int, str]] = {}

    def _process_bank_record(self, client_id, bank_record: BankRecord) -> bool:
        logging.debug(f"Received bank record for client {client_id}")
        if not client_id in self.bank_names_by_client:
            self.bank_names_by_client[client_id] = {}
        self.bank_names_by_client[client_id][bank_record.bank_id] = bank_record.bank_name
    
    def _process_eof(self, client_id) -> bool:
        logging.info(f"Received banks EOF for client {client_id}")
        # TODO Implement a fence to defer requests until receiving banks data

    def _process_request(self, client_id, bank_id: int) -> bool:
        logging.info(f"Received bank name request for client {client_id}")
        if not client_id in self.bank_names_by_client:
            self.bank_names_by_client[client_id] = {}
        
        bank_name = self.bank_names_by_client[client_id].get(bank_id, "")
        logging.info(f"Sending bank name for client {client_id}")
        bank_name_msg = BankNameResponse(bank_id, bank_name)
        out_bank_name_msg = protocol.MsgEnvelope(client_id, protocol.MsgType.BANK_NAME_RESPONSE, bank_name_msg.serialize())
        self.output_queue.send(out_bank_name_msg.serialize())

    def process_messsage(self, message, ack, nack):
        # TODO Implement BANK_PRUNE
        envelope = protocol.MsgEnvelope.deserialize(message)
        if envelope.msg_type == protocol.MsgType.BANK_RECORD:
            bank_record = BankRecord.deserialize(envelope.raw_data)
            self._process_bank_record(envelope.client_id, bank_record)
        elif envelope.msg_type == protocol.MsgType.END_OF_RECORDS:
            self._process_eof(envelope.client_id)
        elif envelope.msg_type == protocol.MsgType.BANK_NAME_REQUEST:
            request = BankNameRequest.deserialize(envelope.raw_data)
            self._process_request(envelope.client_id, request.bank_id)
        else:
            raise RuntimeError(f"msg_type {envelope.msg_type} not supported")
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)
        self.stop()

    def stop(self):
        logging.info("Stopping Banks...")
        self.input_exchange.close()
        self.output_queue.close()

def handle_sigterm(banks_filter: Banks):
    logging.info("SIGTERM received")
    try:
        banks_filter.input_exchange.stop_consuming()
    except Exception as e:
        logging.error(e)

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARN)
    banks_filter = Banks()
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(banks_filter))
    banks_filter.start()

    return 0


if __name__ == "__main__":
    main()
