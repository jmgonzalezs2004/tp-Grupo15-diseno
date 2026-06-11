import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic
from pika.frame import Method
from pika.exceptions import AMQPConnectionError
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange
from common.protocol.internal import MsgType
from common.protocol.serialization import deserialize_uint


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

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def _on_message_received(self, ch: BlockingChannel, method: Basic.Deliver, properties, body: bytes):
        entry_id = None
        if hasattr(self, 'wal') and self.wal:
            entry_id = self.wal.write(body)
            # Damos el ACK temprano a RabbitMQ porque ya está seguro en nuestro disco
            ch.basic_ack(delivery_tag = method.delivery_tag)

        if hasattr(self, 'coordinator') and self.coordinator:
            client_id = _get_client_id_from_envelope(body)
            if _is_eof_message(body):
                expected = _get_expected_count_from_eof(body)
                self.coordinator.on_eof(client_id, expected)
                if hasattr(self, 'wal') and self.wal:
                    self.wal.mark_done(entry_id)
                elif not hasattr(self, 'wal') or not self.wal:
                    ch.basic_ack(delivery_tag = method.delivery_tag)
                return
            else:
                self.coordinator.log_input(client_id)

        def ack_func():
            if hasattr(self, 'wal') and self.wal:
                self.wal.mark_done(entry_id)
            else:
                ch.basic_ack(delivery_tag = method.delivery_tag)
                
        def nack_func():
            if not hasattr(self, 'wal') or not self.wal:
                ch.basic_nack(delivery_tag = method.delivery_tag)
                
        self.on_message_callback(body, ack_func, nack_func)

    def __init__(self, host, queue_name, cluster_config=None, enable_wal=False, heartbeat=0, coordinator=None):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host, heartbeat=heartbeat))
        self.channel = self.connection.channel()
        self.queue_name = queue_name

        self.channel.queue_declare(queue=self.queue_name)
        
        if enable_wal and cluster_config:
            from common.middleware.wal import WriteAheadLog
            self.wal = WriteAheadLog(cluster_config)
        
        if coordinator:
            self.coordinator = coordinator
        elif cluster_config:
            from common.middleware.cluster_coordinator import ClusterCoordinator
            self.coordinator = ClusterCoordinator(cluster_config, host)
    
    def start_consuming(self, on_message_callback, prefectch=1):
        self.on_message_callback = on_message_callback
        

        try:
            if prefectch > 0:
                self.channel.basic_qos(prefetch_count=prefectch)
            self.channel.basic_consume(queue=self.queue_name, 
                                       on_message_callback=self._on_message_received, 
                                       auto_ack=False)
            self.channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e
	
    def stop_consuming(self):
        try:
            self.connection.add_callback_threadsafe(self.channel.stop_consuming)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='',
                                       routing_key=self.queue_name,
                                       body=message)
            if hasattr(self, 'coordinator') and self.coordinator:
                client_id = _get_client_id_from_envelope(message)
                self.coordinator.log_output(client_id)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def send_raw(self, message):
        try:
            self.channel.basic_publish(exchange='',
                                       routing_key=self.queue_name,
                                       body=message)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            if hasattr(self, 'coordinator') and self.coordinator:
                self.coordinator.stop()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def _on_message_received(self, ch: BlockingChannel, method: Basic.Deliver, properties, body: bytes):
        entry_id = None
        if hasattr(self, 'wal') and self.wal:
            entry_id = self.wal.write(body)
            # Damos el ACK temprano a RabbitMQ porque ya está seguro en nuestro disco
            ch.basic_ack(delivery_tag = method.delivery_tag)

        if hasattr(self, 'coordinator') and self.coordinator:
            client_id = _get_client_id_from_envelope(body)
            if _is_eof_message(body):
                expected = _get_expected_count_from_eof(body)
                self.coordinator.on_eof(client_id, expected)
                if hasattr(self, 'wal') and self.wal:
                    self.wal.mark_done(entry_id)
                elif not hasattr(self, 'wal') or not self.wal:
                    ch.basic_ack(delivery_tag = method.delivery_tag)
                return
            else:
                self.coordinator.log_input(client_id)

        def ack_func():
            if hasattr(self, 'wal') and self.wal:
                self.wal.mark_done(entry_id)
            else:
                ch.basic_ack(delivery_tag = method.delivery_tag)
                
        def nack_func():
            if not hasattr(self, 'wal') or not self.wal:
                ch.basic_nack(delivery_tag = method.delivery_tag)
                
        self.on_message_callback(body, ack_func, nack_func)
    
    def __init__(self, host, exchange_name, routing_keys, cluster_config=None, enable_wal=False, heartbeat=0, coordinator=None):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host, heartbeat=heartbeat))
        self.channel = self.connection.channel()
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
        
        if enable_wal and cluster_config:
            from common.middleware.wal import WriteAheadLog
            self.wal = WriteAheadLog(cluster_config)
            
        if coordinator:
            self.coordinator = coordinator
        elif cluster_config:
            from common.middleware.cluster_coordinator import ClusterCoordinator
            self.coordinator = ClusterCoordinator(cluster_config, host)

    def start_consuming(self, on_message_callback):
        self.on_message_callback = on_message_callback
        

        try:
            result: Method = self.channel.queue_declare(queue='', exclusive=True)
            queue_name: str = result.method.queue

            for key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name,
                                        queue=queue_name,
                                        routing_key=key)
            
            self.channel.basic_consume(queue=queue_name, 
                                       on_message_callback=self._on_message_received, 
                                       auto_ack=False)
            self.channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        try:
            self.connection.add_callback_threadsafe(self.channel.stop_consuming)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e

    def send(self, message):
        try:
            for key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name,
                                           routing_key=key,
                                           body=message)
                if hasattr(self, 'coordinator') and self.coordinator:
                    client_id = _get_client_id_from_envelope(message)
                    self.coordinator.log_output(client_id)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e
            
    def send_raw(self, message):
        try:
            for key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name,
                                           routing_key=key,
                                           body=message)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            if hasattr(self, 'coordinator') and self.coordinator:
                self.coordinator.stop()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e

