import os
import sys
import time
import logging
import threading
from unittest.mock import MagicMock

# --- SIMULACIÓN DE PIKA Y RABBITMQ PARA LA PRUEBA SIN DOCKER ---
import sys
import collections

mock_pika = MagicMock()

GLOBAL_CALLBACKS = {}
GLOBAL_EXCHANGES = {}

class MockChannel:
    def __init__(self):
        self.is_open = True
    def queue_declare(self, queue, **kwargs):
        class Result: pass
        res = Result()
        res.method = Result()
        import uuid
        res.method.queue = queue if queue else f"temp_queue_{uuid.uuid4().hex}"
        return res

    def exchange_declare(self, exchange, exchange_type): pass
    def queue_bind(self, exchange, queue, routing_key):
        if routing_key not in GLOBAL_EXCHANGES:
            GLOBAL_EXCHANGES[routing_key] = []
        GLOBAL_EXCHANGES[routing_key].append(queue)
    def basic_qos(self, prefetch_count): pass
    
    def basic_consume(self, queue, on_message_callback, auto_ack):
        GLOBAL_CALLBACKS[queue] = on_message_callback
        
    def start_consuming(self):
        # Para que no bloquee el hilo principal en el test
        while True: time.sleep(1)

    def basic_publish(self, exchange, routing_key, body):
        class MockMethod: pass
        m = MockMethod()
        m.delivery_tag = 1
        
        # Enrutamiento basado en queue_bind
        queues_to_deliver = GLOBAL_EXCHANGES.get(routing_key, [routing_key])
        if type(queues_to_deliver) is not list:
            queues_to_deliver = [queues_to_deliver]
        
        for q, cb in GLOBAL_CALLBACKS.items():
            if q in queues_to_deliver or (routing_key != '' and routing_key in q) or (exchange != '' and exchange in q):
                # Disparamos el callback asincrónicamente
                threading.Thread(target=cb, args=(self, m, None, body)).start()

    def basic_ack(self, delivery_tag): pass
    def basic_nack(self, delivery_tag): pass

class MockConn:
    def __init__(self):
        self.is_open = True
    def channel(self): return MockChannel()
    def close(self): pass

mock_pika.BlockingConnection.return_value = MockConn()
sys.modules['pika'] = mock_pika
sys.modules['pika.adapters.blocking_connection'] = MagicMock()
sys.modules['pika.spec'] = MagicMock()
sys.modules['pika.frame'] = MagicMock()
sys.modules['pika.exceptions'] = MagicMock()
mock_pika.exceptions.AMQPConnectionError = Exception
# ----------------------------------------------------------------

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from common.middleware.cluster_config import ClusterConfig
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ, MessageMiddlewareQueueRabbitMQ
from common.middleware.cluster_middleware import ClusterMiddleware
from common.protocol.internal import MsgType, MsgEnvelope
from common.protocol.serialization import serialize_uint32

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def run_node(node_id):
    config = ClusterConfig("test_cluster", node_id, 3)
    
    # El middleware unificado, 100% puro y multi-output
    middleware = ClusterMiddleware(
        cluster_config=config,
        host='localhost',
        input_exchange=('test_prefix', [f'test_prefix_{node_id}']),
        output_queues={'out': 'test_output'}
    )
    
    # Callback when phase completes
    def on_phase_complete(client_id, total_sent):
        logging.info(f"[Node {node_id}] Phase complete callback called. Total sent: {total_sent}")
        # Enviar EOF a la siguiente fase
        eof_msg = MsgEnvelope(client_id, MsgType.END_OF_RECORDS, serialize_uint32(total_sent))
        middleware.send_raw(eof_msg.serialize())

    middleware.coordinator.set_eof_callback(on_phase_complete)

    def on_message(body):
        envelope = MsgEnvelope.deserialize(body)
        logging.info(f"[Node {node_id}] Received message type {envelope.msg_type}")
        
        # Simulate processing time
        time.sleep(0.1)
        
        # Send an output message (this triggers output count automatically)
        out_msg = MsgEnvelope(envelope.client_id, MsgType.TRAN_RECORD, b"data")
        middleware.send(out_msg.serialize(), output_key='out')
        
        # We don't call ack() manually anymore! 
        # The ClusterMiddleware auto-ACKs and manages WAL on successful return.

    logging.info(f"[Node {node_id}] Starting consuming")
    middleware.start_consuming(on_message)

def simulate_gateway():
    time.sleep(2) # Wait for nodes to start
    
    logging.info("[Gateway] Sending data messages")
    gateway_exchange = MessageMiddlewareExchangeRabbitMQ('localhost', 'test_prefix', [])
    
    client_id = 1
    
    # Send 5 messages to node 0
    for i in range(5):
        msg = MsgEnvelope(client_id, MsgType.TRAN_RECORD, b"data")
        gateway_exchange.channel.basic_publish(exchange='test_prefix', routing_key='test_prefix_0', body=msg.serialize())
        
    # Send 3 messages to node 1
    for i in range(3):
        msg = MsgEnvelope(client_id, MsgType.TRAN_RECORD, b"data")
        gateway_exchange.channel.basic_publish(exchange='test_prefix', routing_key='test_prefix_1', body=msg.serialize())

    # Send 2 messages to node 2
    for i in range(2):
        msg = MsgEnvelope(client_id, MsgType.TRAN_RECORD, b"data")
        gateway_exchange.channel.basic_publish(exchange='test_prefix', routing_key='test_prefix_2', body=msg.serialize())

    # Total messages: 10
    time.sleep(2)
    
    logging.info("[Gateway] Sending EOF to node 0")
    # Gateway sends EOF to node 0 (so node 0 becomes leader)
    eof_msg = MsgEnvelope(client_id, MsgType.END_OF_RECORDS, serialize_uint32(10))
    gateway_exchange.channel.basic_publish(exchange='test_prefix', routing_key='test_prefix_0', body=eof_msg.serialize())
    
    gateway_exchange.close()

if __name__ == '__main__':
    # Clean WAL dir
    os.system("rm -rf data/wal_test_cluster_*")

    threads = []
    for i in range(3):
        t = threading.Thread(target=run_node, args=(i,), daemon=True)
        t.start()
        threads.append(t)
        
    gateway_thread = threading.Thread(target=simulate_gateway)
    gateway_thread.start()
    
    gateway_thread.join()
    # Wait a bit for coordination to finish
    time.sleep(5)
    logging.info("Test finished")
    os._exit(0)
