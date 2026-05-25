import struct

_BIG_ENDIAN = '>'
BOOL_SIZE = 1
INT_SIZE = 4
INT64_SIZE = 8
FLOAT_SIZE = 4

def deserialize_bool(b):
    return int.from_bytes(b, byteorder="big", signed=False)

def deserialize_uint(b):
    return int.from_bytes(b, byteorder="big", signed=False)

def deserialize_float(b):
    return struct.unpack(_BIG_ENDIAN + 'f', b)[0]

def buffer_to_string(b: bytes):
    return b.decode("utf-8")

class MemoryReader:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def skip(self, amount: int):
        self.pos += amount

    def get_remaining(self) -> bytes:
        return self.data[self.pos:]

    def read_bytes(self, size: int) -> bytes:
        if self.pos + size > len(self.data):
            raise EOFError("Buffer overflow")

        data = self.data[self.pos:self.pos + size]
        self.pos += size
        return data

    def read_byte(self):
        value = self.data[self.pos]
        self.pos += 1
        return value

    def read_int32(self):
        data = self.read_bytes(INT_SIZE)
        return int.from_bytes(data, "big", signed=True)

    def read_uint32(self):
        data = self.read_bytes(INT_SIZE)
        return int.from_bytes(data, "big", signed=False)
    
    def read_uint64(self):
        data = self.read_bytes(INT64_SIZE)
        return int.from_bytes(data, "big", signed=False)

    def read_float(self) -> float:
        data = self.read_bytes(FLOAT_SIZE)
        return struct.unpack(_BIG_ENDIAN + 'f', data)[0]
    
    def read_string(self) -> str:
        length = self.read_uint32()
        return self.read_bytes(length).decode("utf-8")


def serialize_bool(u):
    return int(u).to_bytes(BOOL_SIZE, "big")

def serialize_uint32(u: int):
    return u.to_bytes(INT_SIZE, "big")

def serialize_uint64(u: int):
    return u.to_bytes(INT64_SIZE, "big")

def serialize_float(u: float):
    return struct.pack(_BIG_ENDIAN + 'f', u)

def serialize_list(l: list, item_serializer):
    return b"".join(
        [serialize_uint32(len(l))] + [item_serializer(item) for item in l]
    )

def serialize_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return (
        serialize_uint32(len(encoded))
        + encoded
    )


def deserialize_list(b, item_deserializer):
    reader = MemoryReader(b)
    length = reader.read_uint32()
    return [item_deserializer(reader) for i in range(length)]


