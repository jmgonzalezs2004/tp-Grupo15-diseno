from common.protocol import external_serializer
from common.protocol.internal_messages import Transaction
from common.protocol.memory_reader import MemoryReader

class MaxBankResult:
    def __init__(self, from_bank, from_account, amount):
        self.from_bank = from_bank
        self.from_account = from_account
        self.amount = amount

    @staticmethod
    def from_transaction(src_transaction: Transaction):
        return MaxBankResult(src_transaction.from_bank_id,
                             src_transaction.from_account,
                             src_transaction.amount)
    
    def serialize(self):
        return b"".join(
            [
                external_serializer.serialize_uint32(self.from_bank),
                external_serializer.serialize_uint64(self.from_account),
                external_serializer.serialize_float(self.amount)
            ]
        )
    
    @staticmethod
    def deserialize(reader: MemoryReader):
        return MaxBankResult(
            reader.read_uint32(), # from_bank
            reader.read_uint64(), # from_account
            reader.read_float(),  # amount
        )