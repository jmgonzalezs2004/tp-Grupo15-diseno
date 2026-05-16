import struct

_BIG_ENDIAN = '>'
_INT_SIZE = 4
_INT64_SIZE = 8
_FLOAT_SIZE = 4

class MemoryReader:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def skip(self, amount: int):
        self.pos += amount

    def _read_bytes(self, size: int) -> bytes:
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
        data = self._read_bytes(_INT_SIZE)
        return int.from_bytes(data, "big", signed=True)

    def read_uint32(self):
        data = self._read_bytes(_INT_SIZE)
        return int.from_bytes(data, "big", signed=False)
    
    def read_uint64(self):
        data = self._read_bytes(_INT64_SIZE)
        return int.from_bytes(data, "big", signed=False)

    def read_float(self):
        data = self._read_bytes(_FLOAT_SIZE)
        return struct.unpack(_BIG_ENDIAN + 'f', data)[0]