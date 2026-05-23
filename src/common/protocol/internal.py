from enum import IntEnum

from common.protocol import serialization
from common.protocol.serialization import MemoryReader

class MsgType(IntEnum):
    # GENERAL
    TRAN_RECORD = 1
    END_OF_RECORDS = 2
    BANK_RECORD = 3
    BANK_PRUNE = 4
    END_OF_RECORDS_NOTIFY = 5
    # QUERY 1
    Q1_TRAN = 6
    Q1_END = 7
    # QUERY 2
    Q2_TRAN = 8
    Q2_BANK_MAX = 9
    Q2_RESULT = 10
    Q2_END = 11
    BANK_NAME_REQUEST = 12
    BANK_NAME_RESPONSE = 13
    # QUERY 3
    Q3_TRAN = 14
    Q3_TRAN_PRECEDING = 15
    Q3_TRAN_SUBSEQUENT = 16
    Q3_RESULT_TRAN = 17
    Q3_AVG = 18
    Q3_END = 19
    # QUERY 4
    # TODO
    # QUERY 5
    Q5_TRAN = 50
    Q5_RATE_REQUEST = 51
    Q5_RATE_RESPONSE = 52
    Q5_COUNT = 53
    # TODO REMOVE THIS
    COUNT_RESULT = 100
    TEMP_Q2_RESULT = 101

class MsgEnvelope:
    def __init__(self, client_id: str, msg_type: MsgType, raw_data: bytes):
        self.client_id = client_id
        self.msg_type = msg_type
        self.raw_data = raw_data
    
    def serialize(self):
        return b"".join(
            [
                serialization.serialize_uint32(self.client_id),
                serialization.serialize_uint32(self.msg_type),
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
