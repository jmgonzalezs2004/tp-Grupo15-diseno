from dataclasses import dataclass
import os
import logging
import queue
import signal
import threading

from common import middleware
from common.middleware.middleware import MessageMiddlewareCloseError
import common.protocol.internal as protocol
from common.protocol.internal_messages import Q1Transaction, Q2Transaction, Q3Transaction, Q4Transaction2Acc, Q5Transaction, SerializableMessage, Transaction
from criteria.criteria import build_criteria_for_query

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
CONTROL_EXCHANGE = "distributor_control"
DISTRIBUTOR_PREFIX = os.environ["DISTRIBUTOR_PREFIX"]
DISTRIBUTOR_AMOUNT = int(os.environ["DISTRIBUTOR_AMOUNT"])
Q1_QUEUE = os.environ["Q1_QUEUE"]
Q2_QUEUE = os.environ["Q2_QUEUE"]
Q3_QUEUE = os.environ["Q3_QUEUE"]
Q4_QUEUE = os.environ["Q4_QUEUE"]
Q5_QUEUE = os.environ["Q5_QUEUE"]

@dataclass
class OutboundMessage:
    q_num : int
    msg: protocol.MsgEnvelope

class Distributor:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, DISTRIBUTOR_PREFIX, [f"{DISTRIBUTOR_PREFIX}_{ID}"]
        )
        self._control_exchange_sender = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, CONTROL_EXCHANGE, [DISTRIBUTOR_PREFIX]
        )
        
        self._running = True
        self._msg_outbound_queue: queue.Queue[OutboundMessage] = queue.Queue()
        self.eofs_by_client: dict[int, int] = {}
        self.eofs_by_client_lock = threading.Lock()
        self._lock_processing_message = threading.Lock()
        try:
            self.q1_criteria = build_criteria_for_query(1)
            self.q2_criteria = build_criteria_for_query(2)
            self.q3_criteria = build_criteria_for_query(3)
            self.q4_criteria = build_criteria_for_query(4)
            self.q5_criteria = build_criteria_for_query(5)
        except ValueError:
            self.stop()
            raise
    
    def _is_leader(self):
        # TODO Implement leader election, or improve our queues
        return ID == 0

    def _distribute_tran(self, client_id, tran: SerializableMessage, query_num: int):
        message = protocol.MsgEnvelope(client_id, tran.MESSAGE_TYPE, tran.serialize())
        self._msg_outbound_queue.put(OutboundMessage(query_num, message))

    def _distribute_eof(self, client_id):
        logging.info(f"Sending EOF for client {client_id}")
        message = protocol.MsgEnvelope(client_id, protocol.MsgType.END_OF_RECORDS, b"")
        for i in range(5):
            self._msg_outbound_queue.put(OutboundMessage(i+1, message))

    def _process_tran(self, client_id, transaction: Transaction) -> bool:
        logging.debug(f"Received transaction for client {client_id}")
        if self.q1_criteria.check(transaction):
            logging.debug(f"Sending transaction to query 1 for client {client_id}")
            q_tran = Q1Transaction.from_transaction(transaction)
            self._distribute_tran(client_id, q_tran, 1)
        if self.q2_criteria.check(transaction):
            logging.debug(f"Sending transaction to query 2 for client {client_id}")
            q_tran = Q2Transaction.from_transaction(transaction)
            self._distribute_tran(client_id, q_tran, 2)
        if self.q3_criteria.check(transaction):
            logging.debug(f"Sending transaction to query 3 for client {client_id}")
            q_tran = Q3Transaction.from_transaction(transaction)
            self._distribute_tran(client_id, q_tran, 3)
        if self.q4_criteria.check(transaction):
            logging.debug(f"Sending transaction to query 4 for client {client_id}")
            q_tran = Q4Transaction2Acc.from_transaction(transaction)
            self._distribute_tran(client_id, q_tran, 4)
        if self.q5_criteria.check(transaction):
            logging.debug(f"Sending transaction to query 5 for client {client_id}")
            q_tran = Q5Transaction.from_transaction(transaction)
            self._distribute_tran(client_id, q_tran, 5)

    def _evaluate_eofs(self, client_id):
        # Called from control_consumer_thread and main thread
        with self.eofs_by_client_lock:
            self.eofs_by_client[client_id] = self.eofs_by_client.get(client_id, 0) + 1
            if self.eofs_by_client[client_id] >= DISTRIBUTOR_AMOUNT:
                self._distribute_eof(client_id)
                del self.eofs_by_client[client_id]
    
    def _process_eof(self, client_id, message):
        logging.info(f"Received EOF for client {client_id}")
        if self._is_leader():
            self._evaluate_eofs(client_id)
        else:
            self._msg_outbound_queue.join()
            logging.info(f"Sending EOF NOTIFY message for client {client_id}")
            msg = protocol.MsgEnvelope(client_id, protocol.MsgType.END_OF_RECORDS_NOTIFY, b"").serialize()
            self._control_exchange_sender.send(msg)

    def process_messsage(self, message, ack, nack):
        with self._lock_processing_message:
            envelope = protocol.MsgEnvelope.deserialize(message)
            if envelope.msg_type == protocol.MsgType.TRAN_RECORD:
                tran = Transaction.deserialize(envelope.raw_data)
                self._process_tran(envelope.client_id, tran)
            elif envelope.msg_type == protocol.MsgType.END_OF_RECORDS:
                self._process_eof(envelope.client_id, message)
            else:
                raise RuntimeError(f"msg_type {envelope.msg_type} not supported")
            ack()

    def _process_eof_notif(self, client_id):
        logging.info(f"Process EOF notification for client {client_id}")
        assert self._is_leader()
        self._evaluate_eofs(client_id)

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
            MOM_HOST, CONTROL_EXCHANGE, [DISTRIBUTOR_PREFIX]
        )
        self._control_exchange_consumer.start_consuming(self._process_control_message)
        self._control_exchange_consumer.close()

    def _data_output_sender_thread(self):
        """
        Thread for sending messages to the data output queues.
        """
        self.q_out_queues = [
            middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, Q1_QUEUE),
            middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, Q2_QUEUE),
            middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, Q3_QUEUE),
            middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, Q4_QUEUE),
            middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, Q5_QUEUE),
        ]

        while self._running or not self._msg_outbound_queue.empty():
            item = self._msg_outbound_queue.get()
            try:
                self.q_out_queues[item.q_num-1].send(item.msg.serialize())
            except Exception as e:
                logging.error(f"Error sending message to data output queue: {e}")
                break
            finally:
                self._msg_outbound_queue.task_done()
        
        for queue in self.q_out_queues:
            try:
                queue.close()
            except MessageMiddlewareCloseError as e:
                logging.error(f"Error closing RabbitMQ connections: {e}")

    def start(self):
        if self._is_leader():
            control_consumer_thread = threading.Thread(target=self._control_consumer_thread)
            control_consumer_thread.start()
        data_output_sender_thread = threading.Thread(target=self._data_output_sender_thread)
        data_output_sender_thread.start()
        self.input_exchange.start_consuming(self.process_messsage)
        #control_consumer_thread.join()
        self.stop()

    def stop(self):
        logging.info("Stopping Distributor...")
        self.input_exchange.close()
        self._running = False

def handle_sigterm(distributor: Distributor):
    logging.info("SIGTERM received")
    try:
        distributor.input_exchange.stop_consuming()
    except Exception as e:
        logging.error(e)

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARN)
    try:
        distributor = Distributor()
    except ValueError as e:
        logging.error(e)
        return 1
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(distributor))
    distributor.start()

    return 0


if __name__ == "__main__":
    main()
