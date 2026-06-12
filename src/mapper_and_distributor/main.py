import os
import logging
import signal

from datetime import datetime, UTC
from common.middleware.cluster_config import ClusterConfig
from common.middleware.cluster_middleware import ClusterMiddleware
from common.protocol.internal import MsgType, MsgEnvelope
from common.protocol.internal_messages import Q3Transaction, Q3TransactionPreceding, Q3TransactionSubsequent


ID = int(os.environ["ID"])
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
MOM_HOST = os.environ["MOM_HOST"]
MAPPER_AND_DISTRIBUTOR_AMOUNT = int(os.environ["MAPPER_AND_DISTRIBUTOR_AMOUNT"])
MAPPER_AND_DISTRIBUTOR_PREFIX = os.environ["MAPPER_AND_DISTRIBUTOR_PREFIX"]
MAPPER_AND_DISTRIBUTOR_CONTROL_EXCHANGE = "MAPPER_AND_DISTRIBUTOR_CONTROL_EXCHANGE"
PAYMENT_FORMAT_AVG_AMOUNT = int(os.environ["PAYMENT_FORMAT_AVG_AMOUNT"])
PAYMENT_FORMAT_AVG_PREFIX = os.environ["PAYMENT_FORMAT_AVG_PREFIX"]
AMOUNT_FILTER_AMOUNT = int(os.environ["AMOUNT_FILTER_AMOUNT"])
AMOUNT_FILTER_PREFIX = os.environ["AMOUNT_FILTER_PREFIX"]


class MapperAndDistributor:
    def __init__(self):
        config = ClusterConfig(
            node_id=ID,
            cluster_name=MAPPER_AND_DISTRIBUTOR_PREFIX,
            cluster_size=MAPPER_AND_DISTRIBUTOR_AMOUNT
        )
        
        # --- NUEVA ARQUITECTURA: ClusterMiddleware maneja TODO ---
        self.middleware = ClusterMiddleware(
            cluster_config=config,
            host=MOM_HOST,
            input_queue=INPUT_QUEUE,
            output_exchanges={
                PAYMENT_FORMAT_AVG_PREFIX: (PAYMENT_FORMAT_AVG_PREFIX, []),
                AMOUNT_FILTER_PREFIX: (AMOUNT_FILTER_PREFIX, [])
            }
        )
        self.middleware.on_phase_complete = self._on_phase_complete

        """
        CÓDIGO VIEJO:
        self._input_queue = MessageMiddlewareQueueRabbitMQ(...)
        self._control_exchange_sender = MessageMiddlewareExchangeRabbitMQ(...)
        self._queue_data_output_exchanges = queue.Queue()
        self._running = True
        self._lock_running = threading.Lock()
        self._lock_processing_message = threading.Lock()
        """

    def _route(self, client_id, routing_key, nodes_amount):
        key = f"{client_id}:{routing_key}".encode()
        hash_int = int.from_bytes(key, byteorder='big')
        return hash_int % nodes_amount

    def _process_tran(self, client_id, transaction: Q3Transaction):
        preceding_from_dt = int(datetime(2022, 9, 1, tzinfo=UTC).timestamp())
        preceding_to_dt = int(datetime(2022, 9, 5, 23, 59, 59, tzinfo=UTC).timestamp())
        if preceding_from_dt <= transaction.timestamp <= preceding_to_dt:
            logging.debug(f"Transaction is historical: sending to payment_format_avg")
            tran_preceding = Q3TransactionPreceding(transaction.payment_format_id, transaction.amount)
            msg = MsgEnvelope(client_id, MsgType.Q3_TRAN_PRECEDING, tran_preceding.serialize()).serialize()
            exch_idx = self._route(client_id, transaction.payment_format_id, PAYMENT_FORMAT_AVG_AMOUNT)
            
            # --- NUEVA ARQUITECTURA: Routing key dinámico ---
            self.middleware.send_raw(
                msg, 
                output_key=PAYMENT_FORMAT_AVG_PREFIX, 
                routing_key=f"{PAYMENT_FORMAT_AVG_PREFIX}_{exch_idx}"
            )
            
            """
            CÓDIGO VIEJO:
            self._queue_data_output_exchanges.put((msg, PAYMENT_FORMAT_AVG_PREFIX, [exch_idx]))
            """

        subsequent_from_dt = int(datetime(2022, 9, 6, tzinfo=UTC).timestamp())
        subsequent_to_dt = int(datetime(2022, 9, 15, 23, 59, 59, tzinfo=UTC).timestamp())
        if subsequent_from_dt <= transaction.timestamp <= subsequent_to_dt:
            logging.debug(f"Transaction is subsequent: sending to amount_filter")
            tran_subsequent = Q3TransactionSubsequent(transaction.from_bank_id, 
                                                      transaction.from_account, 
                                                      transaction.payment_format_id, 
                                                      transaction.amount)
            msg = MsgEnvelope(client_id, MsgType.Q3_TRAN_SUBSEQUENT, tran_subsequent.serialize()).serialize()
            exch_idx = self._route(client_id, transaction.payment_format_id, AMOUNT_FILTER_AMOUNT)
            
            # --- NUEVA ARQUITECTURA: Routing key dinámico ---
            self.middleware.send_raw(
                msg, 
                output_key=AMOUNT_FILTER_PREFIX, 
                routing_key=f"{AMOUNT_FILTER_PREFIX}_{exch_idx}"
            )
            
            """
            CÓDIGO VIEJO:
            self._queue_data_output_exchanges.put((msg, AMOUNT_FILTER_PREFIX, [exch_idx]))
            """

    def _process_tran_batch(self, client_id, batch: list[Q3Transaction]):
        logging.debug(f"Received transaction batch for client {client_id}")
        for tran in batch:
            self._process_tran(client_id, tran)

    """
    CÓDIGO VIEJO (Lógica de Coordinación manual y ruteo eliminada):
    def _process_eof(self, client_id): ...
    def _publish_eof(self, client_id): ...
    def _process_eof_notif(self, client_id): ...
    def _process_control_message(self, message, ack, nack): ...
    def _control_consumer_thread(self): ...
    def _data_output_exchange_sender_thread(self): ...
    """

    def _process_data_message(self, message, ack, nack):
        try:
            msg = MsgEnvelope.deserialize(message)
            if msg.msg_type == MsgType.Q3_TRAN:
                batch = Q3Transaction.deserialize_batch(msg.raw_data)
                self._process_tran_batch(msg.client_id, batch)
            else:
                raise RuntimeError(f"msg_type {msg.msg_type} not supported")
            ack()
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            nack()

    def _on_phase_complete(self, client_id, total_sent):
        # --- NUEVA ARQUITECTURA: OPCIÓN A (El emisor elige un solo líder) ---
        logging.info(f"Phase complete for client {client_id}. Emitting EOF to partition 0 only.")
        msg = MsgEnvelope(client_id, MsgType.END_OF_RECORDS, b"").serialize()
        
        # Mandamos el EOF a la partición 0 del AVERAGE
        self.middleware.send_raw(msg, output_key=PAYMENT_FORMAT_AVG_PREFIX, 
                                 routing_key=f"{PAYMENT_FORMAT_AVG_PREFIX}_0")
                                 
        # Mandamos el EOF a la partición 0 del FILTER
        self.middleware.send_raw(msg, output_key=AMOUNT_FILTER_PREFIX, 
                                 routing_key=f"{AMOUNT_FILTER_PREFIX}_0")

    def start(self):
        # --- NUEVA ARQUITECTURA: Flujo principal ---
        self.middleware.start_consuming(self._process_data_message)

        """
        CÓDIGO VIEJO:
        control_consumer_thread = threading.Thread(target=self._control_consumer_thread)
        control_consumer_thread.start()
        data_output_exchange_sender_thread = threading.Thread(target=self._data_output_exchange_sender_thread)
        data_output_exchange_sender_thread.start()
        self._input_queue.start_consuming(self._process_data_message)
        control_consumer_thread.join()
        data_output_exchange_sender_thread.join()
        ...
        """

    def stop(self):
        logging.info("Stopping MapperAndDistributor...")
        self.middleware.close()

def handle_sigterm(mapper_and_distributor: MapperAndDistributor):
    logging.info("SIGTERM received")
    try:
        mapper_and_distributor.middleware.stop_consuming()
    except Exception as e:
        logging.error(e)

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARN)
    mapper_and_distributor = MapperAndDistributor()
    signal.signal(signal.SIGTERM, lambda s, f: handle_sigterm(mapper_and_distributor))
    return mapper_and_distributor.start()

if __name__ == "__main__":
    main()
