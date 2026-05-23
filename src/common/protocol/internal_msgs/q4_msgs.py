from common.protocol.memory_reader import MemoryReader


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


class Transaction2Accounts:
    def __init__(self, source_acc: Account, dest_acc: Account):
        self.source_acc = source_acc
        self.dest_acc = dest_acc

    def __eq__(self, other):
        if not isinstance(other, Transaction2Accounts):
            return False
        return (self.source_acc == other.source_acc and
                self.dest_acc == other.dest_acc)

    def __hash__(self):
        return hash((self.source_acc, self.dest_acc))

    @staticmethod
    def deserialize(reader: MemoryReader):
        from_bank_id = reader.read_uint32()
        from_account = reader.read_uint64()
        to_bank_id = reader.read_uint32()
        to_account = reader.read_uint64()

        return Transaction2Accounts(
            Account(from_bank_id, from_account),
            Account(to_bank_id, to_account)
        )


class Transaction3Accounts:
    def __init__(self, source_acc: Account, middle_acc: Account, dest_acc: Account):
        self.source_acc = source_acc
        self.middle_acc = middle_acc
        self.dest_acc = dest_acc

    @staticmethod
    def deserialize(reader: MemoryReader):
        from_bank_id = reader.read_uint32()
        from_account = reader.read_uint64()
        mid_bank_id = reader.read_uint32()
        mid_account = reader.read_uint64()
        to_bank_id = reader.read_uint32()
        to_account = reader.read_uint64()
        
        return Transaction3Accounts(
            Account(from_bank_id, from_account),
            Account(mid_bank_id, mid_account),
            Account(to_bank_id, to_account)
        )
