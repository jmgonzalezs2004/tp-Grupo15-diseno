import os
import logging
import signal

from common import middleware
import common.protocol.internal as protocol
from common.protocol.internal_messages import Q2Transaction

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
BANK_MAX_AMOUNT = int(os.environ["BANK_MAX_AMOUNT"])
BANK_MAX_PREFIX = os.environ["BANK_MAX_PREFIX"]


class BankMapper:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges: list[middleware.MessageMiddlewareExchangeRabbitMQ] = []
        for i in range(BANK_MAX_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, BANK_MAX_PREFIX, [f"{BANK_MAX_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

    def _hash_bank(self, bank_id: int):
        return bank_id % BANK_MAX_AMOUNT

    def _process_tran(self, client_id, tran_data: bytes) -> bool:
        logging.debug(f"Received transaction for client {client_id}")
        tran = Q2Transaction.deserialize(tran_data)
        message = protocol.MsgEnvelope(client_id, protocol.MsgType.Q2_TRAN, tran_data)
        dst_bank_max = self.data_output_exchanges[self._hash_bank(tran.from_bank_id)]
        dst_bank_max.send(message.serialize())
    
    def _process_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
        message = protocol.MsgEnvelope(client_id, protocol.MsgType.END_OF_RECORDS, b"")
        for dst_bank_max in self.data_output_exchanges:
            dst_bank_max.send(message.serialize())

    def process_messsage(self, message, ack, nack):
        envelope = protocol.MsgEnvelope.deserialize(message)
        if envelope.msg_type == protocol.MsgType.Q2_TRAN:
            self._process_tran(envelope.client_id, envelope.raw_data)
        elif envelope.msg_type == protocol.MsgType.END_OF_RECORDS:
            self._process_eof(envelope.client_id)
        else:
            raise RuntimeError(f"msg_type {envelope.msg_type} not supported")
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)
        self.stop()

    def stop(self):
        logging.info("Stopping BankMapper...")
        self.input_queue.close()
        for exchange in self.data_output_exchanges:
            exchange.close()

def handle_sigterm(bank_mapper: BankMapper):
    logging.info("SIGTERM received")
    try:
        bank_mapper.input_queue.stop_consuming()
    except Exception as e:
        logging.error(e)

def main():
    logging.basicConfig(level=logging.WARN)
    bank_mapper = BankMapper()
    logging.getLogger().setLevel(logging.INFO)
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(bank_mapper))
    bank_mapper.start()

    return 0


if __name__ == "__main__":
    main()
