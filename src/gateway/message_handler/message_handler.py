from common.protocol.internal import MsgEnvelope, MsgType
from common.protocol.internal_messages import BankRecord, Transaction



class MessageHandler:
    def __init__(self, client_id):
        self.client_id = client_id

    def _hash_bank(bank_id: int, banks_amount):
        return bank_id % banks_amount
    
    def prepare_account_batch(self, data: list[tuple], banks_amount):
        bank_records = [[] for i in range(banks_amount)]
        for item in data:
            bank_name, bank_id, _, _, _ = item
            bank_records[MessageHandler._hash_bank(bank_id, banks_amount)].append(BankRecord(bank_id, bank_name))
        
        out_messages = []
        for bank_idx in range(len(bank_records)):   
            if len(bank_records[bank_idx]) > 0:
                out_messages.append(MsgEnvelope(self.client_id, BankRecord.MESSAGE_TYPE, 
                                                BankRecord.serialize_batch(bank_records[bank_idx])).serialize())
            else:
                out_messages.append(None)
        return out_messages
    
    def prepare_tran_batch(self, data: list[tuple]):
        transactions = []
        for item in data:
            timestamp, from_bank, from_account, to_bank, to_account, amount, currency, format = item
            transactions.append(Transaction(timestamp, from_bank, from_account, to_bank, to_account, currency, format, amount))
        
        message = MsgEnvelope(self.client_id, Transaction.MESSAGE_TYPE, Transaction.serialize_batch(transactions))
        return message.serialize()
    
    def serialize_eof_message(self, data):
        message = MsgEnvelope(self.client_id, MsgType.END_OF_RECORDS, b"")
        return message.serialize()
    
    def deserialize_result_message(self, raw_message):
        message = MsgEnvelope.deserialize(raw_message)
        if message.client_id == self.client_id:
            return message
        return None
    