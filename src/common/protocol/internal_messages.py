from dataclasses import dataclass
from typing import ClassVar

from common.protocol import serialization
from common.protocol.common_enums import Currency, PaymentFormat
from common.protocol.internal import MsgType
from common.protocol.serialization import MemoryReader

class SerializableMessage:
    MESSAGE_TYPE: ClassVar[int]

    def serialize(self) -> bytes:
        raise NotImplementedError

    @classmethod
    def deserialize(cls, data: bytes):
        raise NotImplementedError

# ----------------
# GENERAL MESSAGES
# ----------------
    
@dataclass
class Transaction(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.TRAN_RECORD
    timestamp: int
    from_bank_id: int
    from_account: int
    to_bank_id: int
    to_account: int
    currency_id: Currency
    payment_format_id: PaymentFormat
    amount: float

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.timestamp),
            serialization.serialize_uint32(self.from_bank_id),
            serialization.serialize_uint64(self.from_account),
            serialization.serialize_uint32(self.to_bank_id),
            serialization.serialize_uint64(self.to_account),
            serialization.serialize_uint32(self.currency_id),
            serialization.serialize_uint32(self.payment_format_id),
            serialization.serialize_float(self.amount),
        ])

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls(
            timestamp=reader.read_uint32(),
            from_bank_id=reader.read_uint32(),
            from_account=reader.read_uint64(),
            to_bank_id=reader.read_uint32(),
            to_account=reader.read_uint64(),
            currency_id=Currency(reader.read_uint32()),
            payment_format_id=PaymentFormat(reader.read_uint32()),
            amount=reader.read_float(),
        )
    
@dataclass
class BankRecord(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.BANK_RECORD
    bank_id: int
    bank_name: str

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.bank_id),
            serialization.serialize_string(self.bank_name),
        ])

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls(
            bank_id=reader.read_uint32(),
            bank_name=reader.read_string(),
        )

# ----------------
# QUERY 1 MESSAGES
# ----------------

@dataclass
class Q1Transaction(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.Q1_TRAN
    from_bank_id: int
    from_account: int
    to_bank_id: int
    to_account: int
    amount: float

    @staticmethod
    def from_transaction(src_transaction: Transaction):
        return Q1Transaction(src_transaction.from_bank_id,
                             src_transaction.from_account,
                             src_transaction.to_bank_id,
                             src_transaction.to_account,
                             src_transaction.amount)

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.from_bank_id),
            serialization.serialize_uint64(self.from_account),
            serialization.serialize_uint32(self.to_bank_id),
            serialization.serialize_uint64(self.to_account),
            serialization.serialize_float(self.amount),
        ])

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls(
            from_bank_id=reader.read_uint32(),
            from_account=reader.read_uint64(),
            to_bank_id=reader.read_uint32(),
            to_account=reader.read_uint64(),
            amount=reader.read_float(),
        )

# ----------------
# QUERY 2 MESSAGES
# ----------------

@dataclass
class Q2Transaction(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.Q2_TRAN
    from_bank_id: int
    from_account: int
    amount: float

    @staticmethod
    def from_transaction(src_transaction: Transaction):
        return Q2Transaction(src_transaction.from_bank_id,
                             src_transaction.from_account,
                             src_transaction.amount)

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.from_bank_id),
            serialization.serialize_uint64(self.from_account),
            serialization.serialize_float(self.amount),
        ])

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls(
            from_bank_id=reader.read_uint32(),
            from_account=reader.read_uint64(),
            amount=reader.read_float(),
        )

@dataclass
class Q2BankMax(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.Q2_BANK_MAX
    from_bank_id: int
    from_account: int
    amount: float

    @staticmethod
    def from_transaction(src_transaction: Q2Transaction):
        return Q2BankMax(src_transaction.from_bank_id,
                         src_transaction.from_account,
                         src_transaction.amount)

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.from_bank_id),
            serialization.serialize_uint64(self.from_account),
            serialization.serialize_float(self.amount),
        ])

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls.deserialize_reader(reader)
    
    @classmethod
    def deserialize_reader(cls, reader: MemoryReader):
        return cls(
            from_bank_id=reader.read_uint32(),
            from_account=reader.read_uint64(),
            amount=reader.read_float(),
        )
    
@dataclass
class Q2Result(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.Q2_RESULT
    from_bank_name: str
    from_account: int
    amount: float

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_string(self.from_bank_name),
            serialization.serialize_uint64(self.from_account),
            serialization.serialize_float(self.amount),
        ])
    
    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls.deserialize_reader(reader)
    
    @classmethod
    def deserialize_reader(cls, reader: MemoryReader):
        return cls(
            from_bank_name=reader.read_string(),
            from_account=reader.read_uint64(),
            amount=reader.read_float(),
        )

@dataclass
class BankNameRequest(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.BANK_NAME_REQUEST
    bank_id: int

    def serialize(self) -> bytes:
        return serialization.serialize_uint32(self.bank_id)

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls(
            bank_id=reader.read_uint32(),
        )

@dataclass
class BankNameResponse(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.BANK_NAME_RESPONSE
    bank_id: int
    bank_name: str

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.bank_id),
            serialization.serialize_string(self.bank_name),
        ])

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls(
            bank_id=reader.read_uint32(),
            bank_name=reader.read_string(),
        )
    
# ----------------
# QUERY 3 MESSAGES
# ----------------

@dataclass
class Q3Transaction(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.Q3_TRAN
    timestamp: int
    from_bank_id: int
    from_account: int
    payment_format_id: PaymentFormat
    amount: float

    @staticmethod
    def from_transaction(src_transaction: Transaction):
        return Q3Transaction(src_transaction.timestamp,
                             src_transaction.from_bank_id,
                             src_transaction.from_account,
                             src_transaction.payment_format_id,
                             src_transaction.amount)

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.timestamp),
            serialization.serialize_uint32(self.from_bank_id),
            serialization.serialize_uint64(self.from_account),
            serialization.serialize_uint32(self.payment_format_id),
            serialization.serialize_float(self.amount),
        ])

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls(
            timestamp=reader.read_uint32(),
            from_bank_id=reader.read_uint32(),
            from_account=reader.read_uint64(),
            payment_format_id=PaymentFormat(reader.read_uint32()),
            amount=reader.read_float(),
        )

@dataclass
class Q3TransactionPreceding(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.Q3_TRAN_PRECEDING
    payment_format_id: PaymentFormat
    amount: float

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.payment_format_id),
            serialization.serialize_float(self.amount),
        ])

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls(
            payment_format_id=PaymentFormat(reader.read_uint32()),
            amount=reader.read_float(),
        )
    
@dataclass
class Q3TransactionSubsequent(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.Q3_TRAN_SUBSEQUENT
    from_bank_id: int
    from_account: int
    payment_format_id: PaymentFormat
    amount: float

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.from_bank_id),
            serialization.serialize_uint64(self.from_account),
            serialization.serialize_uint32(self.payment_format_id),
            serialization.serialize_float(self.amount),
        ])

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls(
            from_bank_id=reader.read_uint32(),
            from_account=reader.read_uint64(),
            payment_format_id=PaymentFormat(reader.read_uint32()),
            amount=reader.read_float(),
        )

@dataclass
class Q3Average(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.Q3_AVG
    payment_format_id: PaymentFormat
    avg: float

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.payment_format_id),
            serialization.serialize_float(self.avg),
        ])

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls(
            payment_format_id=PaymentFormat(reader.read_uint32()),
            avg=reader.read_float(),
        )

@dataclass
class Q3ResultTransaction(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.Q3_RESULT_TRAN
    from_bank_id: int
    from_account: int
    payment_format_id: PaymentFormat
    amount: float

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.from_bank_id),
            serialization.serialize_uint64(self.from_account),
            serialization.serialize_uint32(self.payment_format_id),
            serialization.serialize_float(self.amount),
        ])

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls(
            from_bank_id=reader.read_uint32(),
            from_account=reader.read_uint64(),
            payment_format_id=PaymentFormat(reader.read_uint32()),
            amount=reader.read_float(),
        )


# ----------------
# QUERY 4 MESSAGES
# ----------------

# TODO

# ----------------
# QUERY 5 MESSAGES
# ----------------

@dataclass
class Q5Transaction(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.Q5_TRAN
    timestamp: int
    currency_id: Currency
    amount: float

    @staticmethod
    def from_transaction(src_transaction: Transaction):
        return Q5Transaction(src_transaction.timestamp,
                             src_transaction.currency_id,
                             src_transaction.amount)

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.timestamp),
            serialization.serialize_uint32(self.currency_id),
            serialization.serialize_float(self.amount),
        ])

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls(
            timestamp=reader.read_uint32(),
            currency_id=Currency(reader.read_uint32()),
            amount=reader.read_float(),
        )

@dataclass
class Q5RateRequest(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.Q5_RATE_REQUEST
    timestamp: int
    currency_id: Currency

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.timestamp),
            serialization.serialize_uint32(self.currency_id),
        ])

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls(
            timestamp=reader.read_uint32(),
            currency_id=Currency(reader.read_uint32()),
        )
    
@dataclass
class Q5RateResponse(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.Q5_RATE_RESPONSE
    timestamp: int
    currency_id: Currency
    dollar_exchange_rate: float

    def serialize(self) -> bytes:
        return b"".join([
            serialization.serialize_uint32(self.timestamp),
            serialization.serialize_uint32(self.currency_id),
            serialization.serialize_float(self.dollar_exchange_rate),
        ])

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls(
            timestamp=reader.read_uint32(),
            currency_id=Currency(reader.read_uint32()),
            dollar_exchange_rate=reader.read_float(),
        )

@dataclass
class Q5Count(SerializableMessage):
    MESSAGE_TYPE: ClassVar[int] = MsgType.Q5_COUNT
    count: int

    def serialize(self) -> bytes:
        return serialization.serialize_uint32(self.count)

    @classmethod
    def deserialize(cls, data: bytes):
        reader = MemoryReader(data)
        return cls(
            count=reader.read_uint32(),
        )

