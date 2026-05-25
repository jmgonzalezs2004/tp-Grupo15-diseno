from asyncio import IncompleteReadError
from enum import IntEnum
from socket import socket
from datetime import datetime, timezone

from common.protocol import serialization
from common.protocol.serialization import MemoryReader
from common.protocol.common_enums import Currency, PaymentFormat


class MsgType(IntEnum):
    TRAN_RECORD = 1
    Q1_TRAN = 2
    Q1_END = 3
    Q2_RESULT = 4
    Q2_END = 5
    Q3_RESULT_TRAN = 6
    Q3_END = 7
    # TODO: Complete for Q4 and Q5
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

def _recv_tran_record(socket: socket):
    timestamp = _recv_uint32(socket)
    from_bank_id = _recv_uint32(socket)
    from_account = _recv_uint64(socket)
    to_bank_id = _recv_uint32(socket)
    to_account = _recv_uint64(socket)
    amount = _recv_float(socket)
    currency = _recv_uint32(socket)
    payment_format = _recv_uint32(socket)
    return (timestamp, from_bank_id, from_account, to_bank_id, to_account, amount, currency, payment_format)

def _recv_q1_tran(socket: socket):
    from_bank_id = _recv_uint32(socket)
    from_account = _recv_uint64(socket)
    to_bank_id = _recv_uint32(socket)
    to_account = _recv_uint64(socket)
    amount = round(_recv_float(socket), 2)
    return (from_bank_id, from_account, to_bank_id, to_account, amount)

def _recv_q2_result(socket: socket):
    from_bank_name = _recv_string(socket)
    from_account = _recv_uint64(socket)
    amount = round(_recv_float(socket), 2)
    return (from_bank_name, from_account, amount)

def _recv_q3_result_tran(socket: socket):
    from_bank_id = _recv_uint32(socket)
    from_account = _recv_uint64(socket)
    payment_format_id = _recv_uint32(socket)
    amount = round(_recv_float(socket), 2)
    return (from_bank_id, from_account, payment_format_id, amount)

def _recv_empty(socket):
    return None


RECV_MSG_HANDLERS = {
    MsgType.TRAN_RECORD: _recv_tran_record,
    MsgType.Q1_TRAN: _recv_q1_tran,
    MsgType.Q1_END: _recv_empty,
    MsgType.Q2_RESULT: _recv_q2_result,
    MsgType.Q2_END: _recv_empty,
    MsgType.Q3_RESULT_TRAN: _recv_q3_result_tran,
    MsgType.Q3_END: _recv_empty,
    MsgType.ACK: _recv_empty,
    MsgType.END_OF_RECORDS: _recv_empty,
}

def recv_msg(socket: socket):
    msg_type = _recv_uint32(socket)
    msg_handler = RECV_MSG_HANDLERS[msg_type]
    return (msg_type, msg_handler(socket))


# Parameters come with the same format as csv datasets
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


def _send_tran_record(socket: socket, timestamp, from_bank_id, from_account, 
                      to_bank_id, to_account, amount, currency, payment_format_id):
    msg = serialization.serialize_uint32(MsgType.TRAN_RECORD)
    msg += _serialize_tran_record(timestamp, from_bank_id, from_account, to_bank_id, 
                                  to_account, amount, currency, payment_format_id)
    socket.sendall(msg)

def _send_q1_end(socket: socket):
    socket.sendall(serialization.serialize_uint32(MsgType.Q1_END))

def _send_q2_end(socket: socket):
    socket.sendall(serialization.serialize_uint32(MsgType.Q2_END))

def _send_q3_end(socket: socket):
    socket.sendall(serialization.serialize_uint32(MsgType.Q3_END))

def _send_ack(socket: socket):
    socket.sendall(serialization.serialize_uint32(MsgType.ACK))

def _send_end_of_records(socket: socket):
    socket.sendall(serialization.serialize_uint32(MsgType.END_OF_RECORDS))


SEND_MSG_HANDLERS = {
    MsgType.TRAN_RECORD: _send_tran_record,
    MsgType.Q1_END: _send_q1_end,
    MsgType.Q2_END: _send_q2_end,
    MsgType.Q3_END: _send_q3_end,
    # TODO: Complete for Q4
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
