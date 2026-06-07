from dataclasses import dataclass
from enum import IntEnum
from common.protocol import serialization
from common.protocol.serialization import MemoryReader

class ControlMsgType(IntEnum):
    LEADER_ANNOUNCE = 30
    COUNTER_REPORT = 31
    PHASE_COMPLETE = 32

@dataclass
class LeaderAnnounce:
    client_id: int

    def serialize(self) -> bytes:
        return serialization.serialize_uint32(self.client_id)

    @classmethod
    def deserialize_from(cls, reader: MemoryReader):
        return cls(client_id=reader.read_uint32())

@dataclass
class CounterReport:
    client_id: int
    node_id: int
    processed: int
    sent: int

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.client_id),
            serialization.serialize_uint32(self.node_id),
            serialization.serialize_uint32(self.processed),
            serialization.serialize_uint32(self.sent),
        ])

    @classmethod
    def deserialize_from(cls, reader: MemoryReader):
        return cls(
            client_id=reader.read_uint32(),
            node_id=reader.read_uint32(),
            processed=reader.read_uint32(),
            sent=reader.read_uint32(),
        )

@dataclass
class PhaseComplete:
    client_id: int

    def serialize(self) -> bytes:
        return serialization.serialize_uint32(self.client_id)

    @classmethod
    def deserialize_from(cls, reader: MemoryReader):
        return cls(client_id=reader.read_uint32())

class ControlEnvelope:
    def __init__(self, msg_type: ControlMsgType, raw_data: bytes):
        self.msg_type = msg_type
        self.raw_data = raw_data
    
    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.msg_type),
            self.raw_data
        ])

    @staticmethod
    def deserialize(data: bytes):
        reader = MemoryReader(data)
        return ControlEnvelope(
            msg_type=ControlMsgType(reader.read_uint32()),
            raw_data=reader.get_remaining()
        )
