import os
import logging
import signal

from common import middleware
import common.protocol.internal as protocol
from common.protocol.internal_messages import Q5Count, Q5Transaction
from utils.usd_converter import USDConverter

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
MAX_USD_AMOUNT = float(os.environ["MAX_USD_AMOUNT"])


class ConvertionFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.usd_converter = USDConverter()
        self.count_by_client: dict[int, int] = {}

    def _process_tran(self, client_id, tran_data: bytes) -> bool:
        logging.debug(f"Received transaction for client {client_id}")
        tran = Q5Transaction.deserialize(tran_data)
        usd_amount = self.usd_converter.convert_to_usd(tran.timestamp, tran.currency_id, tran.amount)
        if usd_amount < MAX_USD_AMOUNT:
            if not client_id in self.count_by_client:
                self.count_by_client[client_id] = 0
            self.count_by_client[client_id] += 1
    
    def _process_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
        count_msg = Q5Count(self.count_by_client.get(client_id, 0))
        out_count_msg = protocol.MsgEnvelope(client_id, protocol.MsgType.Q5_COUNT, count_msg.serialize())
        self.output_queue.send(out_count_msg.serialize())
        self.count_by_client.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        envelope = protocol.MsgEnvelope.deserialize(message)
        if envelope.msg_type == protocol.MsgType.Q5_TRAN:
            self._process_tran(envelope.client_id, envelope.raw_data)
        elif envelope.msg_type == protocol.MsgType.END_OF_RECORDS:
            self._process_eof(envelope.client_id)
        else:
            raise RuntimeError(f"msg_type {envelope.msg_type} not supported")
        ack()

    def start(self):
        # API requests too slow... disabled
        # self.usd_converter.warmup("2022-09-01", "2022-09-05")
        self.input_queue.start_consuming(self.process_messsage)
        self.stop()

    def stop(self):
        logging.info("Stopping ConvertionFilter...")
        self.input_queue.close()
        self.output_queue.close()

def handle_sigterm(conversion_filter: ConvertionFilter):
    logging.info("SIGTERM received")
    try:
        conversion_filter.input_queue.stop_consuming()
    except Exception as e:
        logging.error(e)

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARN)
    conversion_filter = ConvertionFilter()
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(conversion_filter))
    conversion_filter.start()

    return 0


if __name__ == "__main__":
    main()
