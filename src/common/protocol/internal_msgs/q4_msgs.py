from common.protocol.memory_reader import MemoryReader
from common.protocol import external_serializer


class Account: 
    def __init__(self, bank_id: int, account_id: int):
        self.bank_id = bank_id
        self.account_id = account_id

    def __eq__(self, other):
        if not isinstance(other, Account):
            return False
        return self.bank_id == other.bank_id and self.account_id == other.account_id

    def __hash__(self):
        return hash((self.bank_id, self.account_id))
    
    def serialize(self):
        return b"".join(
            [
                external_serializer.serialize_uint32(self.bank_id),
                external_serializer.serialize_uint64(self.account_id)
            ]
        )
    
    @staticmethod
    def deserialize(reader: MemoryReader):
        return Account(
            reader.read_uint32(), # bank_id
            reader.read_uint64()  # account_id
        )


class Transaction2Accounts:
    def __init__(self, source_acc: Account, dest_acc: Account):
        self.source_acc = source_acc
        self.dest_acc = dest_acc

    def serialize(self):
        return b"".join(
            [
                self.source_acc.serialize(),
                self.dest_acc.serialize()
            ]
        )

    @staticmethod
    def deserialize(reader: MemoryReader):
        source_acc = Account.deserialize(reader)
        dest_acc = Account.deserialize(reader)
        return Transaction2Accounts(source_acc, dest_acc)


class Transaction3Accounts:
    def __init__(self, source_acc: Account, middle_acc: Account, dest_acc: Account):
        self.source_acc = source_acc
        self.middle_acc = middle_acc
        self.dest_acc = dest_acc

    def serialize(self):
        return b"".join(
            [
                self.source_acc.serialize(),
                self.middle_acc.serialize(),
                self.dest_acc.serialize()
            ]
        )

    @staticmethod
    def deserialize(reader: MemoryReader):
        source_acc = Account.deserialize(reader)
        middle_acc = Account.deserialize(reader)
        dest_acc = Account.deserialize(reader)
        return Transaction3Accounts(source_acc, middle_acc, dest_acc)
