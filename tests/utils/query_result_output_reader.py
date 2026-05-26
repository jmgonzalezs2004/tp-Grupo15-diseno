import csv


class QueryResultOutputReader:
    def __init__(self, output_file, query_number):
        self._query_number = query_number
        self._csv_file = open(output_file, newline="\n")
        self._csv_reader = csv.reader(self._csv_file, delimiter=",", quotechar='"')
        next(self._csv_reader)  # Skip header row

    def next_output(self):
        try:
            row = next(self._csv_reader)
            
            parse_query_output_method = getattr(self, f"_parse_q{self._query_number}_output")
            query_result = parse_query_output_method(row)
            
            return query_result
        except StopIteration:
            return None
        except Exception as e:
            raise Exception(f"Error parsing output for query {self._query_number}: {e}")

    def _parse_q1_output(self, row):
        [from_bank_id, from_account, to_bank_id, to_account, amount] = row
        return [int(from_bank_id), from_account, int(to_bank_id), to_account, float(amount)]
    
    # TODO: Row should be [from_bank_name, account, from_bank_id, amount]
    def _parse_q2_output(self, row):
        [from_bank_name, account, amount] = row
        return [from_bank_name, account, float(amount)]

    def _parse_q3_output(self, row):
        [from_bank_id, from_account, payment_format, amount] = row
        return [int(from_bank_id), from_account, payment_format, float(amount)]

    def _parse_q4_output(self, row):
        [from_bank_id, from_account] = row
        return [int(from_bank_id), from_account]
    
    def _parse_q5_output(self, row):
        [count] = row
        return [int(count)]

    def close(self):
        self._csv_file.close()
