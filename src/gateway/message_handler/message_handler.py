from common.protocol.internal import MsgEnvelope, MsgType
from common.protocol.internal_messages import BankRecord, Transaction

class MessageHandler:

    def __init__(self, client_id):
        self.client_id = client_id

    def serialize_account_message(self, data):
        [bank_name, bank_id, _, _, _] = data
        bank = BankRecord(bank_id, bank_name)
        message = MsgEnvelope(self.client_id, BankRecord.MESSAGE_TYPE, bank.serialize())
        return message.serialize()
    
    def serialize_data_message(self, data):
        [timestamp, from_bank, from_account, to_bank, to_account, amount, currency, format] = data
        transaction = Transaction(timestamp, from_bank, from_account, to_bank, to_account, currency, format, amount)
        message = MsgEnvelope(self.client_id, Transaction.MESSAGE_TYPE, transaction.serialize())
        return message.serialize()

    def serialize_eof_message(self, data):
        message = MsgEnvelope(self.client_id, MsgType.END_OF_RECORDS, b"")
        return message.serialize()
    
    def deserialize_result_message(self, raw_message):
        message = MsgEnvelope.deserialize(raw_message)
        if message.client_id == self.client_id:
            return message
        return None
    