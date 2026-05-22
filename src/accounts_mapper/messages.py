from common.protocol.memory_reader import MemoryReader


class Transaction2Accounts:
    def __init__(self, from_bank_id: int, from_account: int, to_bank_id: int, to_account: int):
        self.from_bank_id = from_bank_id
        self.from_account = from_account
        self.to_bank_id = to_bank_id
        self.to_account = to_account

    @staticmethod
    def deserialize(reader: MemoryReader):
        return Transaction2Accounts(
            reader.read_uint32(), # from_bank_id
            reader.read_uint64(), # from_account
            reader.read_uint32(), # to_bank_id
            reader.read_uint64()  # to_account
        )
