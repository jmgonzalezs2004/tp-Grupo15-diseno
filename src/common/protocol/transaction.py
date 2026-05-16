from common.protocol import external_serializer
from common.protocol.common_enums import Currency, PaymentFormat
from common.protocol.memory_reader import MemoryReader

# NOTE: Probably we wish different kind of Transaction classes
class Transaction:
    def __init__(self, timestamp: int, from_bank, from_account, to_bank, to_account, 
                 currency: Currency, format: PaymentFormat, amount: float):
        self.timestamp = timestamp
        self.from_bank = from_bank
        self.from_account = from_account
        self.to_bank = to_bank
        self.to_account = to_account
        self.currency = currency
        self.format = format
        self.amount = amount

    def serialize(self):
        return b"".join(
            [
                external_serializer.serialize_uint32(self.timestamp),
                external_serializer.serialize_uint32(self.from_bank),
                external_serializer.serialize_uint64(self.from_account),
                external_serializer.serialize_uint32(self.to_bank),
                external_serializer.serialize_uint64(self.to_account),
                external_serializer.serialize_uint32(self.currency),
                external_serializer.serialize_uint32(self.format),
                external_serializer.serialize_float(self.amount)
            ]
        )
    
    @staticmethod
    def deserialize(data):
        reader = MemoryReader(data)
        return Transaction(
            reader.read_uint32(), # timestamp
            reader.read_uint32(), # from_bank
            reader.read_uint64(), # from_account
            reader.read_uint32(), # to_bank
            reader.read_uint64(), # to_account
            Currency(reader.read_uint32()),
            PaymentFormat(reader.read_uint32()),
            reader.read_float(),  # amount
        )


