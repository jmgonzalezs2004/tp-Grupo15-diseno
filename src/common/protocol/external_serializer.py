import struct

_BIG_ENDIAN = '>'

FLOAT_SIZE = 4
UINT32_SIZE = 4
UINT64_SIZE = 8
BOOL_SIZE = 1


def serialize_bool(u):
    return int(u).to_bytes(BOOL_SIZE, "big")

def deserialize_bool(b):
    return int.from_bytes(b, byteorder="big", signed=False)


def serialize_uint32(u: int):
    return u.to_bytes(UINT32_SIZE, "big")

def serialize_uint64(u: int):
    return u.to_bytes(UINT64_SIZE, "big")

def deserialize_uint(b):
    return int.from_bytes(b, byteorder="big", signed=False)


def serialize_float(u: float):
    return struct.pack(_BIG_ENDIAN + 'f', u)

def deserialize_float(b):
    return struct.unpack(_BIG_ENDIAN + 'f', b)[0]


def deserialize_string(b):
    return b.decode("utf-8")

def serialize_string(s):
    return s.encode("utf-8")
