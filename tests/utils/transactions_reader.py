import csv
from datetime import datetime, UTC
from tests.utils.transaction import Transaction


class TransactionsReader: 
    def __init__(self, input_file):
        self._csv_file = open(input_file, newline="\n")
        self._csv_reader = csv.reader(self._csv_file, delimiter=",", quotechar='"')
        next(self._csv_reader)  # Skip header row

    def next_transaction(self):
        try:
            row = next(self._csv_reader)
            [timestamp, from_bank_id, from_account, to_bank_id, to_account, _, _, amount, currency, payment_format, _] = row
            timestamp_dt = datetime.strptime(timestamp, "%Y/%m/%d %H:%M").replace(tzinfo=UTC).timestamp()
            return Transaction(
                int(timestamp_dt),
                int(from_bank_id),
                from_account,
                int(to_bank_id),
                to_account,
                currency,
                payment_format,
                float(amount)
            )
        except StopIteration:
            return None

    def close(self):
        self._csv_file.close()
