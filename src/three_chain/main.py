import os
import logging
import signal

from common.middleware.middleware import MessageMiddlewareCloseError
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ, MessageMiddlewareQueueRabbitMQ
from common.protocol.internal import MsgType, MsgEnvelope
from common.protocol.internal_messages import Q4Transaction2Acc, Q4Transaction3Acc


ID = int(os.environ["ID"])
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
MOM_HOST = os.environ["MOM_HOST"]
THREE_CHAIN_AMOUNT = int(os.environ["THREE_CHAIN_AMOUNT"])
THREE_CHAIN_PREFIX = os.environ["THREE_CHAIN_PREFIX"]
ACCOUNTS_MAPPER_AMOUNT = int(os.environ["ACCOUNTS_MAPPER_AMOUNT"])


class ThreeChain:
    def __init__(self):
        self._input_exchange = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, THREE_CHAIN_PREFIX, [f"{THREE_CHAIN_PREFIX}_{ID}"]
        )
        self._output_queue = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self._outgoing_tran = {} # Dict[client_id, Dict[source_acc, set(dest_acc))]]
        self._eof_received = {} # Dict[client_id, int]

        self._running = True

        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self._running = False
        try: 
            self._input_exchange.stop_consuming()
        except Exception as e:
            logging.error(f"Error stopping consuming messages: {e}")

    def _process_tran(self, client_id, transaction_2acc: Q4Transaction2Acc):
        """
        Process incoming transaction data by deserializing it and adding it 
        to the client's outgoing transactions set.
        """
        if transaction_2acc.from_acc not in self._outgoing_tran[client_id]:
            self._outgoing_tran[client_id][transaction_2acc.from_acc] = set()
        self._outgoing_tran[client_id][transaction_2acc.from_acc].add(transaction_2acc.to_acc)

    def _process_tran_batch(self, client_id, batch: list[Q4Transaction2Acc]):
        logging.debug(f"Received transaction batch for client {client_id}")
        if client_id not in self._outgoing_tran:
            self._outgoing_tran[client_id] = {}

        for tran in batch:
            self._process_tran(client_id, tran)

    def _process_eof(self, client_id):
        """
        Handle EOF for a client. When all expected EOF messages are received,
        generate and emit all derived 3-account transactions, then send the final
        END_OF_RECORDS message.
        """
        logging.info(f"Received EOF for client {client_id}")
        if client_id not in self._eof_received:
            self._eof_received[client_id] = 0
        self._eof_received[client_id] += 1
        if self._eof_received[client_id] < ACCOUNTS_MAPPER_AMOUNT:
            logging.info(f"Waiting for more EOF messages from client")
            return
        
        logging.info(f"All EOF messages received for client. Sending derived 3-account transactions")
        for source_acc, mid_acc_set in self._outgoing_tran.get(client_id, {}).items():
            for mid_acc in mid_acc_set:
                if mid_acc not in self._outgoing_tran.get(client_id, {}):
                    continue
                for dest_acc in self._outgoing_tran[client_id][mid_acc]:
                    transaction_3acc = Q4Transaction3Acc(source_acc, mid_acc, dest_acc)
                    msg = MsgEnvelope(client_id, MsgType.Q4_TRAN_3ACC, transaction_3acc.serialize()).serialize()
                    self._output_queue.send(msg)
        
        logging.info(f"Sending END_OF_RECORDS message for client {client_id}")
        self._output_queue.send(MsgEnvelope(client_id, MsgType.END_OF_RECORDS, b"").serialize())

        del self._outgoing_tran[client_id]
        del self._eof_received[client_id]

    def _process_data_message(self, message, ack, nack):
        try:
            msg = MsgEnvelope.deserialize(message)
            if msg.msg_type == MsgType.Q4_TRAN_2ACC:
                tran_batch = Q4Transaction2Acc.deserialize_batch(msg.raw_data)
                self._process_tran_batch(msg.client_id, tran_batch)
            elif msg.msg_type == MsgType.END_OF_RECORDS:
                self._process_eof(msg.client_id)
            else:
                logging.error(f"Unknown message type: {msg.msg_type}")
            ack()
        except Exception as e:
            if self._running:
                logging.error(f"Unexpected error: {e}")
                nack()
                self._input_exchange.stop_consuming()

    def start(self):
        self._input_exchange.start_consuming(self._process_data_message)

        try:
            self._input_exchange.close()
            self._output_queue.close()
        except MessageMiddlewareCloseError as e:
            logging.error(f"Error closing RabbitMQ connections: {e}")

        if self._running:
            return 1
        return 0

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARN)
    three_chain = ThreeChain()
    return three_chain.start()

if __name__ == "__main__":
    main()
