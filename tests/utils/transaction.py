class Transaction:
    def __init__(self, timestamp: int, from_bank: str, from_account: str, 
                 to_bank: str, to_account: str, currency: str, 
                 payment_format: str, amount: float):
        self.timestamp = timestamp
        self.from_bank_id = from_bank
        self.from_account = from_account
        self.to_bank_id = to_bank
        self.to_account = to_account
        self.currency = currency
        self.payment_format = payment_format
        self.amount = amount
