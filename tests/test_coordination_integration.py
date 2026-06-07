import os
import sys
import time
import logging
import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from common.middleware.cluster_config import ClusterConfig
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ, MessageMiddlewareQueueRabbitMQ
from common.protocol.internal import MsgType, MsgEnvelope
from common.protocol.serialization import serialize_uint32

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def run_node(node_id):
    config = ClusterConfig("test_cluster", node_id, 3)
    
    # Input exchange
    input_exchange = MessageMiddlewareExchangeRabbitMQ(
        'localhost', 'test_prefix', [f'test_prefix_{node_id}'],
        cluster_config=config, enable_wal=True
    )
    
    # Output queue (share coordinator)
    output_queue = MessageMiddlewareQueueRabbitMQ(
        'localhost', 'test_output',
        coordinator=input_exchange.coordinator
    )
    
    # Callback when phase completes
    def on_phase_complete(client_id, total_sent):
        logging.info(f"[Node {node_id}] Phase complete callback called. Total sent: {total_sent}")
        # Enviar EOF a la siguiente fase
        eof_msg = MsgEnvelope(client_id, MsgType.END_OF_RECORDS, serialize_uint32(total_sent))
        output_queue.send_raw(eof_msg.serialize())

    input_exchange.coordinator.set_eof_callback(on_phase_complete)

    def on_message(body, ack, nack):
        envelope = MsgEnvelope.deserialize(body)
        logging.info(f"[Node {node_id}] Received message type {envelope.msg_type}")
        
        # Simulate processing time
        time.sleep(0.1)
        
        # Send an output message (this triggers output count automatically)
        out_msg = MsgEnvelope(envelope.client_id, MsgType.TRAN_RECORD, b"data")
        output_queue.send(out_msg.serialize())
        
        # Ack the message (this marks WAL as done)
        ack()

    logging.info(f"[Node {node_id}] Starting consuming")
    input_exchange.start_consuming(on_message)

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
    os.system("rm -rf /data/wal_test_cluster_*")

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
