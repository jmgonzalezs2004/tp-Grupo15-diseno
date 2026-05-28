from dataclasses import dataclass
import os
import logging
import queue
import signal
import threading

from common import middleware
from common.middleware.middleware import MessageMiddlewareCloseError
import common.protocol.internal as protocol
from common.protocol.internal_messages import Q2Transaction

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
BANK_MAPPER_AMOUNT = int(os.environ["BANK_MAPPER_AMOUNT"])
BANK_MAPPER_PREFIX = os.environ["BANK_MAPPER_PREFIX"]
CONTROL_EXCHANGE = "bank_mapper_control"
BANK_MAX_AMOUNT = int(os.environ["BANK_MAX_AMOUNT"])
BANK_MAX_PREFIX = os.environ["BANK_MAX_PREFIX"]

@dataclass
class OutboundMessage:
    dst_idx : int
    msg: protocol.MsgEnvelope

class BankMapper:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self._control_exchange_sender = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, CONTROL_EXCHANGE, [BANK_MAPPER_PREFIX]
        )
        self.outbound_queue: queue.Queue[OutboundMessage] = queue.Queue()

        self._running = True
        self._lock_running = threading.Lock()
        self._lock_processing_message = threading.Lock()

    def _hash_bank(self, bank_id: int):
        return bank_id % BANK_MAX_AMOUNT

    def _process_tran(self, client_id, tran_data: bytes) -> bool:
        logging.debug(f"Received transaction for client {client_id}")
        tran = Q2Transaction.deserialize(tran_data)
        message = protocol.MsgEnvelope(client_id, protocol.MsgType.Q2_TRAN, tran_data)
        dst_bank_max_idx = self._hash_bank(tran.from_bank_id)
        self.outbound_queue.put(OutboundMessage(dst_bank_max_idx, message))
    
    def _process_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
        self.outbound_queue.join()
        msg = protocol.MsgEnvelope(client_id, protocol.MsgType.END_OF_RECORDS_NOTIFY, b"")
        self._control_exchange_sender.send(msg.serialize())

    def process_messsage(self, message, ack, nack):
        envelope = protocol.MsgEnvelope.deserialize(message)
        if envelope.msg_type == protocol.MsgType.Q2_TRAN:
            self._process_tran(envelope.client_id, envelope.raw_data)
        elif envelope.msg_type == protocol.MsgType.END_OF_RECORDS:
            self._process_eof(envelope.client_id)
        else:
            raise RuntimeError(f"msg_type {envelope.msg_type} not supported")
        ack()

    def _process_eof_notif(self, client_id):
        """
        Broadcast EOF message to the data output exchanges of the node 'payment_format_avg'
        """
        logging.info(f"Process EOF notification: broadcasting EOF message")
        msg = protocol.MsgEnvelope(client_id, protocol.MsgType.END_OF_RECORDS, b"")
        for dts_idx in range(BANK_MAX_AMOUNT):
            self.outbound_queue.put(OutboundMessage(dts_idx, msg))
    
    def _process_control_message(self, message, ack, nack):
        with self._lock_processing_message:
            try:
                msg = protocol.MsgEnvelope.deserialize(message)
                if msg.msg_type == protocol.MsgType.END_OF_RECORDS_NOTIFY:
                    self._process_eof_notif(msg.client_id)
                else:
                    logging.error(f"Unknown control message type: {msg.msg_type}")
                ack()
            except Exception as e:
                if self._running:
                    logging.error(f"Unexpected error in control message processing: {e}")
                    nack()
                    self._control_exchange_consumer.stop_consuming()


    def _control_consumer_thread(self):
        """
        Thread for consuming messages from the control exchange.
        """
        self._control_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, CONTROL_EXCHANGE, [BANK_MAPPER_PREFIX]
        )
        self._control_exchange_consumer.start_consuming(self._process_control_message)
        self._control_exchange_consumer.close()
    
    def _data_output_sender_thread(self):
        """
        Thread for sending messages to the data output exchanges.
        """
        self._data_output_exchanges: list[middleware.MessageMiddlewareExchangeRabbitMQ] = []
        for i in range(BANK_MAX_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, BANK_MAX_PREFIX, [f"{BANK_MAX_PREFIX}_{i}"]
            )
            self._data_output_exchanges.append(data_output_exchange)

        while self._running or not self.outbound_queue.empty():
            item = self.outbound_queue.get()
            try:
                self._data_output_exchanges[item.dst_idx].send(item.msg.serialize())
            except Exception as e:
                logging.error(f"Error sending message to data output exchange: {e}")
                break
            finally:
                self.outbound_queue.task_done()

        for data_output_exchange in self._data_output_exchanges:
            try:
                data_output_exchange.close()
            except MessageMiddlewareCloseError as e:
                logging.error(f"Error closing RabbitMQ connections: {e}")

    def start(self):
        control_consumer_thread = threading.Thread(target=self._control_consumer_thread)
        control_consumer_thread.start()
        data_output_exchange_sender_thread = threading.Thread(target=self._data_output_sender_thread)
        data_output_exchange_sender_thread.start()
        self.input_queue.start_consuming(self.process_messsage)
        self.stop()
        control_consumer_thread.join()
        data_output_exchange_sender_thread.join()

    def stop(self):
        logging.info("Stopping BankMapper...")
        with self._lock_running:
            self._running = False
        try:
            self.input_queue.close()
            self._control_exchange_sender.close()
        except MessageMiddlewareCloseError as e:
            logging.error(f"Error closing RabbitMQ connections: {e}")

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
