import threading
import logging
from common.middleware.cluster_config import ClusterConfig
from common.middleware.control_messages import ControlMsgType, ControlEnvelope, LeaderAnnounce, CounterReport, PhaseComplete
from common.protocol.serialization import MemoryReader
import pika

class ClusterCoordinator:
    def __init__(self, config: ClusterConfig, host: str):
        self.config = config
        self.host = host
        self.control_exchange = f"{config.cluster_name}_control"
        self._lock = threading.Lock()
        
        self.client_states = {}
        
        self.eof_callback = None
        self._running = True
        
        self.control_thread = threading.Thread(target=self._run_control_consumer)
        self.control_thread.start()

    def set_eof_callback(self, callback):
        self.eof_callback = callback

    def _get_or_create_state(self, client_id):
        if client_id not in self.client_states:
            self.client_states[client_id] = {
                "processed": 0,
                "sent": 0,
                "is_leader": False,
                "expected_count": 0,
                "reports": {},
                "leader_announced": False
            }
        return self.client_states[client_id]

    def log_input(self, client_id):
        with self._lock:
            state = self._get_or_create_state(client_id)
            state["processed"] += 1
            if state["leader_announced"]:
                self._send_counter_update(client_id, state)

    def log_output(self, client_id):
        with self._lock:
            state = self._get_or_create_state(client_id)
            state["sent"] += 1
            if state["leader_announced"]:
                self._send_counter_update(client_id, state)

    def on_eof(self, client_id, expected_count):
        logging.info(f"[{self.config.cluster_name}_{self.config.node_id}] Received EOF for client {client_id}. Expected: {expected_count}")
        with self._lock:
            state = self._get_or_create_state(client_id)
            state["is_leader"] = True
            state["leader_announced"] = True
            state["expected_count"] = expected_count
            
            msg = LeaderAnnounce(client_id)
            env = ControlEnvelope(ControlMsgType.LEADER_ANNOUNCE, msg.serialize())
            self._send_control_msg(env)
            
            self._check_phase_complete(client_id, state)

    def _send_counter_update(self, client_id, state):
        msg = CounterReport(client_id, self.config.node_id, state["processed"], state["sent"])
        env = ControlEnvelope(ControlMsgType.COUNTER_REPORT, msg.serialize())
        self._send_control_msg(env)

    def _check_phase_complete(self, client_id, state):
        if not state["is_leader"]:
            return
            
        total_processed = state["processed"]
        total_sent = state["sent"]
        
        for node_id, (proc, sent) in state["reports"].items():
            total_processed += proc
            total_sent += sent
            
        if total_processed >= state["expected_count"]:
            logging.info(f"[{self.config.cluster_name}_{self.config.node_id}] Phase complete for client {client_id}. Total processed: {total_processed}, Sent: {total_sent}")
            msg = PhaseComplete(client_id)
            env = ControlEnvelope(ControlMsgType.PHASE_COMPLETE, msg.serialize())
            self._send_control_msg(env)
            
            if self.eof_callback:
                self.eof_callback(client_id, total_sent)

    def _send_control_msg(self, envelope: ControlEnvelope):
        if hasattr(self, 'control_channel') and self.control_channel and self.control_channel.is_open:
            try:
                self.control_channel.basic_publish(
                    exchange=self.control_exchange,
                    routing_key=self.config.cluster_name,
                    body=envelope.serialize()
                )
            except Exception as e:
                logging.error(f"Error sending control message: {e}")

    def _run_control_consumer(self):
        try:
            self.control_conn = pika.BlockingConnection(pika.ConnectionParameters(self.host, heartbeat=0))
            self.control_channel = self.control_conn.channel()
            self.control_channel.exchange_declare(exchange=self.control_exchange, exchange_type='direct')
            
            result = self.control_channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            self.control_channel.queue_bind(exchange=self.control_exchange, queue=queue_name, routing_key=self.config.cluster_name)
            
            def on_msg(ch, method, properties, body):
                env = ControlEnvelope.deserialize(body)
                with self._lock:
                    state = self._get_or_create_state(env.msg_type)
                    
                    if env.msg_type == ControlMsgType.LEADER_ANNOUNCE:
                        msg = LeaderAnnounce.deserialize_from(MemoryReader(env.raw_data))
                        state = self._get_or_create_state(msg.client_id)
                        state["leader_announced"] = True
                        self._send_counter_update(msg.client_id, state)
                        
                    elif env.msg_type == ControlMsgType.COUNTER_REPORT:
                        msg = CounterReport.deserialize_from(MemoryReader(env.raw_data))
                        state = self._get_or_create_state(msg.client_id)
                        if state["is_leader"] and msg.node_id != self.config.node_id:
                            state["reports"][msg.node_id] = (msg.processed, msg.sent)
                            self._check_phase_complete(msg.client_id, state)
                            
                    elif env.msg_type == ControlMsgType.PHASE_COMPLETE:
                        msg = PhaseComplete.deserialize_from(MemoryReader(env.raw_data))
                        if msg.client_id in self.client_states:
                            del self.client_states[msg.client_id]
                            
                ch.basic_ack(delivery_tag=method.delivery_tag)

            self.control_channel.basic_consume(queue=queue_name, on_message_callback=on_msg, auto_ack=False)
            self.control_channel.start_consuming()
        except Exception as e:
            if self._running:
                logging.error(f"Control consumer error: {e}")

    def stop(self):
        self._running = False
        if hasattr(self, 'control_conn') and self.control_conn and self.control_conn.is_open:
            self.control_conn.add_callback_threadsafe(self.control_conn.close)
