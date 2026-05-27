import os
import logging
import signal

from common.middleware.middleware import MessageMiddlewareCloseError
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from common.protocol.internal import MsgType, MsgEnvelope
from common.protocol.internal_messages import Q4Transaction3Acc, Q4LaunderingAcc


INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
MOM_HOST = os.environ["MOM_HOST"]
THREE_CHAIN_AMOUNT = int(os.environ["THREE_CHAIN_AMOUNT"])
SCATTER_GATHER_TRAN_THRESHOLD = int(os.environ["SCATTER_GATHER_TRAN_THRESHOLD"])


class Q4Join:
    def __init__(self):
        self._input_queue = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self._output_queue = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self._scatter_gather = {} # Dict[client_id, Dict[(source_acc, dest_acc), set(middle_acc))]]
        self._eof_received = {} # Dict[client_id, int]

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
        Deserialize an incoming 3-account transaction and update the scatter-gather
        structure by grouping middle accounts per (source_acc, dest_acc) pair.
        """

        logging.info(f"Processing transaction data")
        transaction_3acc = Q4Transaction3Acc.deserialize(data)

        if client_id not in self._scatter_gather:
            self._scatter_gather[client_id] = {}
        key = (transaction_3acc.from_acc, transaction_3acc.to_acc)
        if key not in self._scatter_gather[client_id]:
            self._scatter_gather[client_id][key] = set()
        self._scatter_gather[client_id][key].add(transaction_3acc.mid_acc)

    def _process_eof(self, client_id):
        """
        Handle EOF messages for a client. When all expected EOFs are received,
        finalize processing by analyzing the scatter-gather state and emitting
        Q4 results for detected suspicious (source_acc, dest_acc) pairs. Finally,
        send the Q4_END marker and clean up client state.
        """

        logging.info(f"Received EOF")
        if client_id not in self._eof_received:
            self._eof_received[client_id] = 0
        self._eof_received[client_id] += 1
        if self._eof_received[client_id] < THREE_CHAIN_AMOUNT:
            logging.info(f"Waiting for more EOF messages from client")
            return

        logging.info(f"All EOF messages received for client. Generating results")
        for (source_acc, dest_acc), middle_accs in self._scatter_gather[client_id].items():
            if len(middle_accs) > SCATTER_GATHER_TRAN_THRESHOLD:
                laundering_source_acc = Q4LaunderingAcc(source_acc)
                laundering_dest_acc = Q4LaunderingAcc(dest_acc)
                
                msg_laundering_source_acc = MsgEnvelope(client_id, MsgType.Q4_LAUNDERING_ACC, laundering_source_acc.serialize()).serialize()
                msg_laundering_dest_acc = MsgEnvelope(client_id, MsgType.Q4_LAUNDERING_ACC, laundering_dest_acc.serialize()).serialize()

                self._output_queue.send(msg_laundering_source_acc)
                self._output_queue.send(msg_laundering_dest_acc)

        logging.info(f"Sending Q4_END message for client")
        self._output_queue.send(MsgEnvelope(client_id, MsgType.Q4_END, b"").serialize())

        del self._scatter_gather[client_id]
        del self._eof_received[client_id]

    def _process_data_message(self, message, ack, nack):
        try:
            msg = MsgEnvelope.deserialize(message)
            if msg.msg_type == MsgType.Q4_TRAN_3ACC:
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
