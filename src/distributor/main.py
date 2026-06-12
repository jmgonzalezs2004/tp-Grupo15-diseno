from dataclasses import dataclass
import os
import logging
import signal

from common.middleware.cluster_config import ClusterConfig
from common.middleware.cluster_middleware import ClusterMiddleware
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
Q_COUNT = 5

class Distributor:
    def __init__(self):
        config = ClusterConfig(
            node_id=ID,
            cluster_name=DISTRIBUTOR_PREFIX,
            cluster_size=DISTRIBUTOR_AMOUNT
        )
        
        # --- NUEVA ARQUITECTURA: ClusterMiddleware maneja TODO ---
        self.middleware = ClusterMiddleware(
            cluster_config=config,
            host=MOM_HOST,
            input_exchange=(DISTRIBUTOR_PREFIX, [f"{DISTRIBUTOR_PREFIX}_{ID}"]),
            output_queues={
                1: Q1_QUEUE,
                2: Q2_QUEUE,
                3: Q3_QUEUE,
                4: Q4_QUEUE,
                5: Q5_QUEUE
            }
        )
        self.middleware.on_phase_complete = self._on_phase_complete
        
        """
        CÓDIGO VIEJO:
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(...)
        self._control_exchange_sender = middleware.MessageMiddlewareExchangeRabbitMQ(...)
        self._msg_outbound_queue: queue.Queue[OutboundMessage] = queue.Queue()
        self.eofs_by_client: dict[int, int] = {}
        self.eofs_by_client_lock = threading.Lock()
        self._lock_processing_message = threading.Lock()
        """

        try:
            self.q1_criteria = build_criteria_for_query(1)
            self.q2_criteria = build_criteria_for_query(2)
            self.q3_criteria = build_criteria_for_query(3)
            self.q4_criteria = build_criteria_for_query(4)
            self.q5_criteria = build_criteria_for_query(5)
        except ValueError:
            self.stop()
            raise
    
    """
    CÓDIGO VIEJO:
    def _is_leader(self):
        return ID == 0
    """

    def _distribute_tran_batch(self, client_id, batch: list[SerializableMessage], query_num: int):
        msg_cls = type(batch[0])
        message = protocol.MsgEnvelope(client_id, msg_cls.MESSAGE_TYPE, msg_cls.serialize_batch(batch))
        
        # --- NUEVA ARQUITECTURA ---
        self.middleware.send_raw(message.serialize(), output_key=query_num, count=len(batch))
        
        """
        CÓDIGO VIEJO:
        self._msg_outbound_queue.put(OutboundMessage(query_num, message))
        """

    """
    CÓDIGO VIEJO:
    def _distribute_eof(self, client_id):
        ...
    """

    def _process_tran(self, transaction: Transaction, dst_q_lists: list[list]) -> bool:
        if self.q1_criteria.check(transaction):
            q_tran = Q1Transaction.from_transaction(transaction)
            dst_q_lists[0].append(q_tran)
        if self.q2_criteria.check(transaction):
            q_tran = Q2Transaction.from_transaction(transaction)
            dst_q_lists[1].append(q_tran)
        if self.q3_criteria.check(transaction):
            q_tran = Q3Transaction.from_transaction(transaction)
            dst_q_lists[2].append(q_tran)
        if self.q4_criteria.check(transaction):
            q_tran = Q4Transaction2Acc.from_transaction(transaction)
            dst_q_lists[3].append(q_tran)
        if self.q5_criteria.check(transaction):
            q_tran = Q5Transaction.from_transaction(transaction)
            dst_q_lists[4].append(q_tran)
    
    def _process_tran_batch(self, client_id, batch: list[Transaction]):
        dst_q_lists = [[] for _ in range(Q_COUNT)]
        logging.debug(f"Received transaction batch for client {client_id}")
        for tran in batch:
            self._process_tran(tran, dst_q_lists)

        for q_idx in range(len(dst_q_lists)):
            if len(dst_q_lists[q_idx]) > 0:
                q_num = q_idx+1
                logging.debug(f"Sending transactions to query {q_num} for client {client_id}")
                self._distribute_tran_batch(client_id, dst_q_lists[q_idx], q_num)

    """
    CÓDIGO VIEJO (Lógica de Coordinación manual eliminada):
    def _evaluate_eofs(self, client_id): ...
    def _process_eof(self, client_id, message): ...
    def _process_eof_notif(self, client_id): ...
    def _process_control_message(self, message, ack, nack): ...
    def _control_consumer_thread(self): ...
    def _data_output_sender_thread(self): ...
    """

    def process_messsage(self, message, ack, nack):
        try:
            envelope = protocol.MsgEnvelope.deserialize(message)
            if envelope.msg_type == protocol.MsgType.TRAN_RECORD:
                tran_batch = Transaction.deserialize_batch(envelope.raw_data)
                self._process_tran_batch(envelope.client_id, tran_batch)
            else:
                raise RuntimeError(f"msg_type {envelope.msg_type} not supported")
            ack()
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            nack()

    def _on_phase_complete(self, client_id, total_sent: dict):
        # --- NUEVA ARQUITECTURA: Emisión de EOF delegada ---
        logging.info(f"Phase complete for client {client_id}. Emitting EOF to 5 queries.")
        for i in range(1, 6):
            count_for_q = total_sent.get(str(i), 0)
            from common.protocol.serialization import serialize_uint32
            eof_payload = serialize_uint32(count_for_q)
            eof_msg = protocol.MsgEnvelope(client_id, protocol.MsgType.END_OF_RECORDS, eof_payload)
            self.middleware.send_raw(eof_msg.serialize(), output_key=i, count=0)

    def start(self):
        # --- NUEVA ARQUITECTURA: Sin multithreading explícito ---
        self.middleware.start_consuming(self.process_messsage)

        """
        CÓDIGO VIEJO:
        if self._is_leader():
            control_consumer_thread = threading.Thread(target=self._control_consumer_thread)
            control_consumer_thread.start()
        data_output_sender_thread = threading.Thread(target=self._data_output_sender_thread)
        data_output_sender_thread.start()
        self.input_exchange.start_consuming(self.process_messsage)
        self.stop()
        """

    def stop(self):
        logging.info("Stopping Distributor...")
        self.middleware.close()

def handle_sigterm(distributor: Distributor):
    logging.info("SIGTERM received")
    try:
        distributor.middleware.stop_consuming()
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
