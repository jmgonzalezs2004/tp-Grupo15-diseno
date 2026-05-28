from asyncio import IncompleteReadError
from enum import IntEnum
from socket import socket
from datetime import datetime, timezone

from common.protocol import serialization
from common.protocol.common_enums import Currency, PaymentFormat


class MsgType(IntEnum):
    ACCOUNT_RECORD = 1
    TRAN_RECORD = 2
    Q1_TRAN = 3
    Q1_END = 4
    Q2_RESULT = 5
    Q2_END = 6
    Q3_RESULT_TRAN = 7
    Q3_END = 8
    Q4_LAUNDERING_ACC = 9
    Q4_END = 10
    Q5_RESULT = 11
    ACK = 15
    END_OF_RECORDS = 16


def _recv_sized(socket: socket, size):
    """
    Receives exactly 'num_bytes' bytes through the provided socket.
    If no bytes are read from the socket IncompleteReadError is raised
    """
    buf = bytearray(size)
    pos = 0
    while pos < size:
        n = socket.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), size)
        pos += n
    return bytes(buf)

def _recv_uint32(socket: socket):
    return serialization.deserialize_uint(
        _recv_sized(socket, serialization.INT_SIZE)
    )

def _recv_uint64(socket: socket):
    return serialization.deserialize_uint(
        _recv_sized(socket, serialization.INT64_SIZE)
    )

def _recv_float(socket: socket):
    return serialization.deserialize_float(
        _recv_sized(socket, serialization.FLOAT_SIZE)
    )

def _recv_string(socket: socket):
    strlen = _recv_uint32(socket)
    return serialization.buffer_to_string(_recv_sized(socket, strlen))

def _recv_account_record(socket: socket):
    def item_deserializer(reader: serialization.MemoryReader):
        bank_name = reader.read_string()
        bank_id = reader.read_uint32()
        account_number = reader.read_uint64()
        entity_id = reader.read_uint64()
        entity_name = reader.read_string()
        return (bank_name, bank_id, account_number, entity_id, entity_name)
    
    payload_len = _recv_uint32(socket)
    return serialization.deserialize_list(
        _recv_sized(socket, payload_len),
        item_deserializer
    )

def _recv_tran_record(socket: socket):
    def item_deserializer(reader: serialization.MemoryReader):
        timestamp = reader.read_uint32()
        from_bank_id = reader.read_uint32()
        from_account = reader.read_uint64()
        to_bank_id = reader.read_uint32()
        to_account = reader.read_uint64()
        amount = reader.read_float()
        currency = reader.read_uint32()
        payment_format = reader.read_uint32()
        return (timestamp, from_bank_id, from_account, to_bank_id, to_account, amount, currency, payment_format)
    
    payload_len = _recv_uint32(socket)
    return serialization.deserialize_list(
        _recv_sized(socket, payload_len),
        item_deserializer
    )

def _recv_q1_tran(socket: socket):
    from_bank_id = _recv_uint32(socket)
    from_account = _recv_uint64(socket)
    to_bank_id = _recv_uint32(socket)
    to_account = _recv_uint64(socket)
    amount = _recv_float(socket)
    return (from_bank_id, from_account, to_bank_id, to_account, amount)

def _recv_q2_result(socket: socket):
    from_bank_id = _recv_uint32(socket)
    from_account = _recv_uint64(socket)
    from_bank_name = _recv_string(socket)
    amount = _recv_float(socket)
    return (from_bank_id, from_account, from_bank_name, amount)

def _recv_q3_result_tran(socket: socket):
    from_bank_id = _recv_uint32(socket)
    from_account = _recv_uint64(socket)
    payment_format_id = _recv_uint32(socket)
    amount = _recv_float(socket)
    return (from_bank_id, from_account, payment_format_id, amount)

def _recv_q4_laundering_acc(socket: socket):
    bank_id = _recv_uint32(socket)
    account = _recv_uint64(socket)
    return (bank_id, account)

def _recv_q5_result(socket: socket):
    count = _recv_uint32(socket)
    return count

def _recv_empty(socket):
    return None


RECV_MSG_HANDLERS = {
    MsgType.ACCOUNT_RECORD: _recv_account_record,
    MsgType.TRAN_RECORD: _recv_tran_record,
    MsgType.Q1_TRAN: _recv_q1_tran,
    MsgType.Q1_END: _recv_empty,
    MsgType.Q2_RESULT: _recv_q2_result,
    MsgType.Q2_END: _recv_empty,
    MsgType.Q3_RESULT_TRAN: _recv_q3_result_tran,
    MsgType.Q3_END: _recv_empty,
    MsgType.Q4_LAUNDERING_ACC: _recv_q4_laundering_acc,
    MsgType.Q4_END: _recv_empty,
    MsgType.Q5_RESULT: _recv_q5_result,
    MsgType.ACK: _recv_empty,
    MsgType.END_OF_RECORDS: _recv_empty,
}

def recv_msg(socket: socket):
    msg_type = _recv_uint32(socket)
    msg_handler = RECV_MSG_HANDLERS[msg_type]
    return (msg_type, msg_handler(socket))

# Parameters come with the same format as csv dataset
def _serialize_account_record(bank_name, bank_id, account_number, entity_id, entity_name):
    return b"".join(
        [
            serialization.serialize_string(bank_name),
            serialization.serialize_uint32(int(bank_id)),
            serialization.serialize_uint64(int(account_number, 16)),
            serialization.serialize_uint64(int(entity_id, 16)),
            serialization.serialize_string(entity_name),
        ]
    )

def _serialize_account_batch(batch: list[tuple]):
    return b"".join(
        [serialization.serialize_uint32(len(batch))] + [_serialize_account_record(*item) for item in batch]
    )

# Parameters come with the same format as csv dataset
def _serialize_tran_record(timestamp, from_bank_id, from_account, 
                           to_bank_id, to_account, amount, currency, payment_format_id):
    dt = datetime.strptime(timestamp, "%Y/%m/%d %H:%M")
    dt = dt.replace(tzinfo=timezone.utc)
    timestamp = int(dt.timestamp())
    return b"".join(
        [
            serialization.serialize_uint32(timestamp),
            serialization.serialize_uint32(int(from_bank_id)),
            serialization.serialize_uint64(int(from_account, 16)),
            serialization.serialize_uint32(int(to_bank_id)),
            serialization.serialize_uint64(int(to_account, 16)),
            serialization.serialize_float(float(amount)),
            serialization.serialize_uint32(Currency.from_str(currency).value),
            serialization.serialize_uint32(PaymentFormat.from_str(payment_format_id).value)
        ]
    )

def _serialize_tran_batch(batch: list[tuple]):
    return b"".join(
        [serialization.serialize_uint32(len(batch))] + [_serialize_tran_record(*item) for item in batch]
    )

def _send_account_record(socket: socket, batch: list):
    msg = serialization.serialize_uint32(MsgType.ACCOUNT_RECORD)
    payload = _serialize_account_batch(batch)
    msg += serialization.serialize_uint32(len(payload))
    msg += payload
    socket.sendall(msg)

def _send_tran_record(socket: socket, batch: list):
    msg = serialization.serialize_uint32(MsgType.TRAN_RECORD)
    payload = _serialize_tran_batch(batch)
    msg += serialization.serialize_uint32(len(payload))
    msg += payload
    socket.sendall(msg)

def _send_q1_end(socket: socket):
    socket.sendall(serialization.serialize_uint32(MsgType.Q1_END))

def _send_q2_end(socket: socket):
    socket.sendall(serialization.serialize_uint32(MsgType.Q2_END))

def _send_q3_end(socket: socket):
    socket.sendall(serialization.serialize_uint32(MsgType.Q3_END))

def _send_q4_end(socket: socket):
    socket.sendall(serialization.serialize_uint32(MsgType.Q4_END))

def _send_ack(socket: socket):
    socket.sendall(serialization.serialize_uint32(MsgType.ACK))

def _send_end_of_records(socket: socket):
    socket.sendall(serialization.serialize_uint32(MsgType.END_OF_RECORDS))


SEND_MSG_HANDLERS = {
    MsgType.ACCOUNT_RECORD: _send_account_record,
    MsgType.TRAN_RECORD: _send_tran_record,
    MsgType.Q1_END: _send_q1_end,
    MsgType.Q2_END: _send_q2_end,
    MsgType.Q3_END: _send_q3_end,
    MsgType.Q4_END: _send_q4_end,
    MsgType.ACK: _send_ack,
    MsgType.END_OF_RECORDS: _send_end_of_records,
}


def send_msg(socket, msg_type, *args):
    msg_handler = SEND_MSG_HANDLERS[msg_type]
    msg_handler(socket, *args)

def forward_msg(socket: socket, msg_type, raw_data):
    msg = serialization.serialize_uint32(msg_type)
    msg += raw_data
    socket.sendall(msg)
