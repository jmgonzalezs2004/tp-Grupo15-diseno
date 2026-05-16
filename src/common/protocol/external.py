from asyncio import IncompleteReadError
from socket import socket
from datetime import datetime, timezone

from common.protocol.common_enums import Currency, PaymentFormat

from . import external_serializer


class MsgType:
    TRAN_RECORD = 1
    RESULT_COUNT = 10
    ACK = 15
    END_OF_RECODS = 16


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
    return external_serializer.deserialize_uint(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )

def _recv_uint64(socket: socket):
    return external_serializer.deserialize_uint(
        _recv_sized(socket, external_serializer.UINT64_SIZE)
    )

def _recv_float(socket: socket):
    return external_serializer.deserialize_float(
        _recv_sized(socket, external_serializer.FLOAT_SIZE)
    )

def _recv_tran_record(socket: socket):
    timestamp = _recv_uint32(socket)
    from_bank = _recv_uint32(socket)
    from_account = _recv_uint64(socket)
    to_bank = _recv_uint32(socket)
    to_account = _recv_uint64(socket)
    currency = _recv_uint32(socket)
    format = _recv_uint32(socket)
    amount = _recv_float(socket)
    return (timestamp, from_bank, from_account, to_bank, to_account, currency, format, amount)

def _recv_empty(socket):
    return None


RECV_MSG_HANDLERS = {
    MsgType.TRAN_RECORD: _recv_tran_record,
    MsgType.ACK: _recv_empty,
    MsgType.END_OF_RECODS: _recv_empty,
}

def recv_msg(socket: socket):
    msg_type = _recv_uint32(socket)
    msg_handler = RECV_MSG_HANDLERS[msg_type]
    return (msg_type, msg_handler(socket))


# Parameters come with the same format as csv datasets
def _serialize_tran_record(timestamp, from_bank, from_account, to_bank, to_account, currency, format, amount):
    dt = datetime.strptime(timestamp, "%Y/%m/%d %H:%M")
    dt = dt.replace(tzinfo=timezone.utc)
    timestamp = int(dt.timestamp())
    return b"".join(
        [
            external_serializer.serialize_uint32(timestamp),
            external_serializer.serialize_uint32(int(from_bank)),
            external_serializer.serialize_uint64(int(from_account, 16)),
            external_serializer.serialize_uint32(int(to_bank)),
            external_serializer.serialize_uint64(int(to_account, 16)),
            external_serializer.serialize_uint32(Currency.from_str(currency)),
            external_serializer.serialize_uint32(PaymentFormat.from_str(format)),
            external_serializer.serialize_float(float(amount))
        ]
    )

def _serialize_count_result(count):
    return b"".join(
        [
            external_serializer.serialize_uint32(count)
        ]
    )


def _send_tran_record(socket: socket, timestamp, from_bank, from_account, to_bank, to_account, currency, format, amount):
    msg = external_serializer.serialize_uint32(MsgType.TRAN_RECORD)
    msg += _serialize_tran_record(timestamp, from_bank, from_account, to_bank, to_account, currency, format, amount)
    socket.sendall(msg)

def _send_count_result(socket: socket, count):
    msg = external_serializer.serialize_uint32(MsgType.RESULT_COUNT)
    msg += _serialize_count_result(count)
    socket.sendall(msg)

def _send_ack(socket: socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.ACK))

def _send_end_of_records(socket: socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.END_OF_RECODS))


SEND_MSG_HANDLERS = {
    MsgType.TRAN_RECORD: _send_tran_record,
    MsgType.RESULT_COUNT: _send_count_result,
    MsgType.ACK: _send_ack,
    MsgType.END_OF_RECODS: _send_end_of_records,
}


def send_msg(socket, msg_type, *args):
    msg_handler = SEND_MSG_HANDLERS[msg_type]
    msg_handler(socket, *args)

def forward_msg(socket: socket, msg_type, raw_data):
    msg = external_serializer.serialize_uint32(msg_type)
    msg += raw_data
    socket.sendall(msg)
