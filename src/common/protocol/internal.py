from common.protocol import external_serializer
from common.protocol.memory_reader import MemoryReader

class MsgType:
    TRAN_RECORD = 1
    COUNT_RESULT = 2
    Q2_PARTIAL_MAX = 3
    Q2_RESULT = 4
    END_OF_RECODS = 16

class MsgEnvelope:
    def __init__(self, client_id: str, msg_type: MsgType, raw_data: bytes):
        self.client_id = client_id
        self.msg_type = msg_type
        self.raw_data = raw_data
    
    def serialize(self):
        return b"".join(
            [
                external_serializer.serialize_uint32(self.client_id),
                external_serializer.serialize_uint32(self.msg_type),
                self.raw_data
            ]
        )
    
    @staticmethod
    def deserialize(data):
        reader = MemoryReader(data)
        return MsgEnvelope(
            reader.read_uint32(), # client_id
            reader.read_uint32(), # msg_type
            data[reader.pos:]     # raw_data
        )
