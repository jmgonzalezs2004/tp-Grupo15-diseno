import os
import logging
import socket
import signal
import multiprocessing
from message_handler.client_id_generator import ClientIdGenerator
import message_handler
from common import middleware, protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
BANKS_AMOUNT = int(os.environ["BANKS_AMOUNT"])
BANKS_PREFIX = os.environ["BANKS_PREFIX"]


def _hash_bank(bank_id: int):
    return bank_id % BANKS_AMOUNT

def handle_client_request(client_socket: socket.socket, message_handler: message_handler.MessageHandler):
    output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
    banks_exchanges: list[middleware.MessageMiddlewareExchangeRabbitMQ] = []
    for i in range(BANKS_AMOUNT):
        banks_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, BANKS_PREFIX, [f"{BANKS_PREFIX}_{i}"]
        )
        banks_exchanges.append(banks_exchange)
    in_accounts_mode = True

    try:
        while True:
            msg_type, content = protocol.external.recv_msg(client_socket)

            if msg_type == protocol.external.MsgType.ACCOUNT_RECORD:
                assert in_accounts_mode
                serialized_message = message_handler.serialize_account_message(content)
                banks_exchanges[_hash_bank(content[1])].send(serialized_message)
                protocol.external.send_msg(
                    client_socket, protocol.external.MsgType.ACK
                )

            if msg_type == protocol.external.MsgType.TRAN_RECORD:
                assert not in_accounts_mode
                serialized_message = message_handler.serialize_data_message(content)
                output_queue.send(serialized_message)
                protocol.external.send_msg(
                    client_socket, protocol.external.MsgType.ACK
                )

            if msg_type == protocol.external.MsgType.END_OF_RECORDS:
                serialized_message = message_handler.serialize_eof_message(content)
                if in_accounts_mode:
                    for banks_exchange in banks_exchanges:
                        banks_exchange.send(serialized_message)
                else:
                    output_queue.send(serialized_message)
                protocol.external.send_msg(
                    client_socket, protocol.external.MsgType.ACK
                )
                if in_accounts_mode:
                    in_accounts_mode = False
                else:
                    return
            
    except socket.error:
        logging.error("The connection with the server was lost")
    except Exception as e:
        logging.exception(e)
    finally:
        output_queue.close()
        for exchange in banks_exchanges:
            exchange.close()


def handle_client_response(client_list: list[list[message_handler.MessageHandler, socket.socket]]):
    input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

    def _consume_result(message, ack, nack):
        client_index = 0
        try:
            for [message_handler_instance, client_socket] in client_list:
                deserialized_message = (
                    message_handler_instance.deserialize_result_message(message)
                )

                if not deserialized_message:
                    client_index += 1
                    continue

                msg_type = deserialized_message.msg_type
                if msg_type == protocol.internal.MsgType.Q1_TRAN:
                    protocol.external.forward_msg(
                        client_socket,
                        protocol.external.MsgType.Q1_TRAN,
                        deserialized_message.raw_data)
                elif msg_type == protocol.internal.MsgType.Q1_END:
                    protocol.external.send_msg(
                        client_socket,
                        protocol.external.MsgType.Q1_END)
                elif msg_type == protocol.internal.MsgType.Q2_RESULT:
                    protocol.external.forward_msg(
                        client_socket,
                        protocol.external.MsgType.Q2_RESULT,
                        deserialized_message.raw_data)
                elif msg_type == protocol.internal.MsgType.Q2_END:
                    protocol.external.send_msg(
                        client_socket,
                        protocol.external.MsgType.Q2_END)
                elif msg_type == protocol.internal.MsgType.Q3_RESULT_TRAN:
                    protocol.external.forward_msg(
                        client_socket,
                        protocol.external.MsgType.Q3_RESULT_TRAN,
                        deserialized_message.raw_data)
                elif msg_type == protocol.internal.MsgType.Q3_END:
                    protocol.external.send_msg(
                        client_socket,
                        protocol.external.MsgType.Q3_END)
                elif msg_type == protocol.internal.MsgType.Q4_LAUNDERING_ACC:
                    protocol.external.forward_msg(
                        client_socket,
                        protocol.external.MsgType.Q4_LAUNDERING_ACC,
                        deserialized_message.raw_data)
                elif msg_type == protocol.internal.MsgType.Q4_END:
                    protocol.external.send_msg(
                        client_socket,
                        protocol.external.MsgType.Q4_END)
                elif msg_type == protocol.internal.MsgType.Q5_COUNT:
                    protocol.external.forward_msg(
                        client_socket,
                        protocol.external.MsgType.Q5_RESULT,
                        deserialized_message.raw_data)

                protocol.external.recv_msg(client_socket)
                break
            # TODO remove client after sending all results
            #client_list.pop(client_index)
            ack()
        except socket.error:
            logging.error("The connection with the server was lost")
            client_list.pop(client_index)
            ack()
        except Exception as e:
            logging.exception(e)
            nack()
            input_queue.stop_consuming()

    input_queue.start_consuming(_consume_result)
    input_queue.close()


def handle_sigterm(server_socket: socket.socket, client_list, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for [_, client_socket] in client_list:
        client_socket.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1


def main():
    logging.basicConfig(level=logging.INFO)

    with multiprocessing.Manager() as manager:
        client_list = manager.list()
        sigterm_received = manager.Value("c_short", 0)
        client_id_generator = ClientIdGenerator()
        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:
            processes_pool.apply_async(handle_client_response, (client_list,))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                logging.info("Listening to connections")
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: handle_sigterm(
                        server_socket, client_list, sigterm_received
                    ),
                )
                while True:
                    try:
                        client_socket, _ = server_socket.accept()

                        logging.info("A new client has connected")
                        message_handler_instance = message_handler.MessageHandler(client_id_generator.generate())
                        client_list.append([message_handler_instance, client_socket])
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, message_handler_instance),
                        )
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("The connection with the client was lost")
                            return 1
                        else:
                            return 0
                    except Exception as e:
                        logging.error(e)
                        return 2
    return 0


if __name__ == "__main__":
    main()
