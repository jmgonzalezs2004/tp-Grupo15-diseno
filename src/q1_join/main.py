import os
import logging
import signal

from common.middleware.middleware import MessageMiddlewareCloseError
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from common.protocol.internal import MsgType, MsgEnvelope


INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
MOM_HOST = os.environ["MOM_HOST"]


class Q4Join:
    def __init__(self):
        self._input_queue = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self._output_queue = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self._running = True

        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self._running = False
        try: 
            self._input_queue.stop_consuming()
        except Exception as e:
            logging.error(f"Error stopping consuming messages: {e}")

    def _process_data(self, client_id, data: bytes):
        """
        Process transaction data for a client. As the transaction was already processed 
        we just need to forward it to the output queue.
        """

        logging.info(f"Processing transaction data")
        self._output_queue.send(MsgEnvelope(client_id, MsgType.Q1_TRAN, data).serialize())

    def _process_eof(self, client_id):
        """
        Handle EOF messages for a client. Send a Q1_END message to the output queue 
        to indicate that all data for the client has been processed.
        """

        logging.info(f"Received EOF")
        logging.info(f"Sending Q1_END message for client")
        self._output_queue.send(MsgEnvelope(client_id, MsgType.Q1_END, b"").serialize())

    def _process_data_message(self, message, ack, nack):
        try:
            msg = MsgEnvelope.deserialize(message)
            if msg.msg_type == MsgType.Q1_TRAN:
                self._process_data(msg.client_id, msg.raw_data)
            elif msg.msg_type == MsgType.END_OF_RECORDS:
                self._process_eof(msg.client_id)
            else:
                logging.error(f"Unknown message type: {msg.msg_type}")
            ack()
        except Exception as e:
            if self._running:
                logging.error(f"Unexpected error: {e}")
                nack()
                self._input_queue.stop_consuming()

    def start(self):
        self._input_queue.start_consuming(self._process_data_message)

        try:
            self._input_queue.close()
            self._output_queue.close()
        except MessageMiddlewareCloseError as e:
            logging.error(f"Error closing RabbitMQ connections: {e}")

        if self._running:
            return 1
        return 0

def main():
    logging.basicConfig(level=logging.WARN)
    q4_join = Q4Join()
    logging.getLogger().setLevel(logging.INFO)
    return q4_join.start()

if __name__ == "__main__":
    main()
