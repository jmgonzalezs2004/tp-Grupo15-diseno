import os
import logging
import signal

from common import middleware
from common.protocol import serialization
import common.protocol.internal as protocol
from common.protocol.internal_messages import BankNameRequest, BankNameResponse, Q2BankMax, Q2Result

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
BANK_MAX_AMOUNT = int(os.environ["BANK_MAX_AMOUNT"])
BANKS_AMOUNT = int(os.environ["BANKS_AMOUNT"])
BANKS_PREFIX = os.environ["BANKS_PREFIX"]


class Q2Join:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.banks_exchanges: list[middleware.MessageMiddlewareExchangeRabbitMQ] = []
        for i in range(BANKS_AMOUNT):
            banks_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, BANKS_PREFIX, [f"{BANKS_PREFIX}_{i}"]
            )
            self.banks_exchanges.append(banks_exchange)

        self._eofs_by_client: dict[int, int] = {}
        self._bank_max_by_client: dict[int, dict[int, Q2BankMax]] = {}

    def _hash_bank(self, bank_id: int):
        return bank_id % BANKS_AMOUNT
    
    def _process_bank_max(self, client_id, bank_max: Q2BankMax):
        logging.info(f"Received bank max for client {client_id}")
        bank_id = bank_max.from_bank_id
        if client_id not in self._bank_max_by_client:
            self._bank_max_by_client[client_id] = {}
        assert bank_max.from_bank_id not in self._bank_max_by_client[client_id]
        self._bank_max_by_client[client_id][bank_id] = bank_max

        logging.info(f"Requesting bank name for client {client_id}")
        request_msg = BankNameRequest(bank_id)
        out_request_msg = protocol.MsgEnvelope(client_id, protocol.MsgType.BANK_NAME_REQUEST, request_msg.serialize())
        dst_banks = self.banks_exchanges[self._hash_bank(bank_id)]
        dst_banks.send(out_request_msg.serialize())

    def _process_bank_name_response(self, client_id, response: BankNameResponse):
        logging.info(f"Received bank name response for client {client_id}")
        bank_max = self._bank_max_by_client[client_id].pop(response.bank_id)

        if response.bank_name != "":
            logging.info(f"Sending query 2 result for client {client_id}")
            result = Q2Result(response.bank_id, bank_max.from_account, response.bank_name, bank_max.amount)
            out_result_msg = protocol.MsgEnvelope(client_id, protocol.MsgType.Q2_RESULT, result.serialize())
            self.output_queue.send(out_result_msg.serialize())

        if self._eofs_by_client.get(client_id, 0) >= BANK_MAX_AMOUNT and len(self._bank_max_by_client[client_id]) == 0:
            # Send deferred END, as every EOF have been received
            self._send_query_end(client_id)
    
    def _process_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
        if client_id not in self._eofs_by_client:
            self._eofs_by_client[client_id] = 0
        self._eofs_by_client[client_id] += 1
        if self._eofs_by_client[client_id] < BANK_MAX_AMOUNT:
            logging.info(f"Waiting for more EOF messages from client")
            return
        if client_id in self._bank_max_by_client and len(self._bank_max_by_client[client_id]) > 0:
            # Defer sending END until receiving pending bank names
            return
        self._send_query_end(client_id)

    def _send_query_end(self, client_id):
        logging.info(f"Sending END for client {client_id}")
        eof_msg = protocol.MsgEnvelope(client_id, protocol.MsgType.Q2_END, b"")
        self.output_queue.send(eof_msg.serialize())
        del self._eofs_by_client[client_id]
        self._bank_max_by_client.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        envelope = protocol.MsgEnvelope.deserialize(message)
        if envelope.msg_type == protocol.MsgType.Q2_BANK_MAX:
            bank_max = Q2BankMax.deserialize(envelope.raw_data)
            self._process_bank_max(envelope.client_id, bank_max)
        elif envelope.msg_type == protocol.MsgType.BANK_NAME_RESPONSE:
            response = BankNameResponse.deserialize(envelope.raw_data)
            self._process_bank_name_response(envelope.client_id, response)
        elif envelope.msg_type == protocol.MsgType.END_OF_RECORDS:
            self._process_eof(envelope.client_id)
        else:
            raise RuntimeError(f"msg_type {envelope.msg_type} not supported")
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)
        self.stop()

    def stop(self):
        logging.info("Stopping Q2Join...")
        self.input_queue.close()
        self.output_queue.close()

def handle_sigterm(q2_join: Q2Join):
    logging.info("SIGTERM received")
    try:
        q2_join.input_queue.stop_consuming()
    except Exception as e:
        logging.error(e)

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARN)
    q2_join = Q2Join()
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(q2_join))
    q2_join.start()

    return 0


if __name__ == "__main__":
    main()
