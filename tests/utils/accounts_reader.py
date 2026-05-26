import csv


class AccountsReader:
    def __init__(self, accounts_file_name):
        self._csv_file = open(accounts_file_name, newline="\n")
        self._csv_reader = csv.reader(self._csv_file, delimiter=",", quotechar='"')
        next(self._csv_reader)  # Skip header row

    def next_account(self):
        try:
            row = next(self._csv_reader)
            [bank_name, bank_id, _, _, _] = row
            return (int(bank_id), bank_name)
        except StopIteration:
            return None
        except Exception as e:
            raise Exception(f"Error parsing account: {e}")
    
    def close(self):
        self._csv_file.close()
