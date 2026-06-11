from common.middleware.middleware import MessageMiddleware
from common.middleware.cluster_config import ClusterConfig
from common.middleware.wal import WriteAheadLog
from common.middleware.cluster_coordinator import ClusterCoordinator
from common.protocol.internal import MsgType
from common.protocol.serialization import deserialize_uint
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ, MessageMiddlewareExchangeRabbitMQ
import logging

def _get_client_id_from_envelope(body: bytes) -> int:
    if len(body) >= 4:
        return int.from_bytes(body[:4], "big")
    return 0

def _is_eof_message(body: bytes) -> bool:
    if len(body) >= 8:
        msg_type = int.from_bytes(body[4:8], "big")
        return msg_type == MsgType.END_OF_RECORDS
    return False

def _get_expected_count_from_eof(body: bytes) -> int:
    if len(body) >= 12:
        return deserialize_uint(body[8:])
    return 0

class ClusterMiddleware:
    def __init__(self, 
                 cluster_config: ClusterConfig, 
                 host: str,
                 input_queue: str = None,
                 input_exchange: tuple = None,
                 output_queue: str = None,
                 output_exchange: tuple = None):
        
        self.wal = WriteAheadLog(cluster_config) if cluster_config else None
        self.coordinator = ClusterCoordinator(cluster_config, host) if cluster_config else None
        
        self.input_middleware = None
        self.output_middleware = None

        if input_queue:
            self.input_middleware = MessageMiddlewareQueueRabbitMQ(host, input_queue)
        elif input_exchange:
            self.input_middleware = MessageMiddlewareExchangeRabbitMQ(host, input_exchange[0], input_exchange[1])

        if output_queue:
            self.output_middleware = MessageMiddlewareQueueRabbitMQ(host, output_queue)
        elif output_exchange:
            self.output_middleware = MessageMiddlewareExchangeRabbitMQ(host, output_exchange[0], output_exchange[1])

    def start_consuming(self, on_message_callback, *args, **kwargs):
        if not self.input_middleware:
            raise ValueError("No input configured for this node")

        def _interceptor(body: bytes, ack_func, nack_func):
            entry_id = None
            if self.wal:
                entry_id = self.wal.write(body)
            ack_func()

            client_id = _get_client_id_from_envelope(body)
            if self.coordinator:
                if _is_eof_message(body):
                    expected = _get_expected_count_from_eof(body)
                    self.coordinator.on_eof(client_id, expected)
                    if self.wal:
                        self.wal.mark_done(entry_id)
                    return
                else:
                    self.coordinator.log_input(client_id)

            try:
                on_message_callback(body)
                if self.wal:
                    self.wal.mark_done(entry_id)
            except Exception as e:
                logging.error(f"Error procesando mensaje: {e}")

        self.input_middleware.start_consuming(_interceptor, *args, **kwargs)

    def stop_consuming(self):
        if self.input_middleware:
            self.input_middleware.stop_consuming()

    def send(self, message: bytes):
        if not self.output_middleware:
            raise ValueError("No output configured for this node")
            
        self.output_middleware.send(message)
        
        if self.coordinator:
            client_id = _get_client_id_from_envelope(message)
            self.coordinator.log_output(client_id)

    def send_raw(self, message: bytes):
        if not self.output_middleware:
            raise ValueError("No output configured for this node")
        if hasattr(self.output_middleware, 'send_raw'):
            self.output_middleware.send_raw(message)
        else:
            self.output_middleware.send(message)

    def close(self):
        if self.coordinator:
            self.coordinator.stop()
        if self.input_middleware:
            self.input_middleware.close()
        if self.output_middleware:
            self.output_middleware.close()
