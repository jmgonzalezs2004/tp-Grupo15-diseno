import logging
from datetime import datetime, UTC
from tests.utils.transactions_reader import TransactionsReader
from tests.utils.usd_converter import USDConverter
from tests.utils.accounts_reader import AccountsReader


QUERY_AMOUNT = 5


class ExpectedQueryResultsGenerator:
    def __init__(self, client_id, accounts_file_name, input_file_name):
        self._client_id = client_id
        self._accounts_file_name = accounts_file_name
        self._input_file_name = input_file_name
        self._usd_converter = USDConverter()
        self._bank_names = {}
        self._expected_output_file_headers = {
            1: ["From Bank", "From Account", "To Bank", "To Account", "Amount"],
            2: ["Bank ID", "Account", "Bank Name", "Amount"],
            3: ["Bank", "Account", "Payment Format", "Amount"],
            4: ["Bank", "Account"],
            5: ["Count"]
        }

    def generate_expected_query_results(self):
        """
        Generates sorted expected query results for all queries and stores them in output 
        files with the format: 
        ./expected_output/expected_output_{self._client_id}_{query_number}.csv
        """
        
        try:
            self._store_bank_names()
            for query_number in range(1, QUERY_AMOUNT + 1):
                logging.info(f"Generating expected query {query_number} results...")
                _generate_query_result = getattr(self, f"_generate_q{query_number}_result")
                expected_query_results = _generate_query_result()
                self._store_expected_query_results(expected_query_results, query_number)
        except Exception as e:
            return f"{str(e)}"

        return None

    def _generate_q1_result(self):
        tran_reader = None
        try:
            tran_reader = TransactionsReader(self._input_file_name)
            input_q1_results = []
            for transaction_item in iter(tran_reader.next_transaction, None):
                # ---- QUERY 1 PROCESSING ----
                # Filter non US Dollar transactions
                if transaction_item.currency != "US Dollar":
                    continue
                # Filter transactions with amount >= 50 USD
                if transaction_item.amount >= 50:
                    continue
                
                # ---- STORE QUERY 1 RESULT ----
                input_q1_results.append([
                    transaction_item.from_bank_id,
                    transaction_item.from_account,
                    transaction_item.to_bank_id,
                    transaction_item.to_account,
                    transaction_item.amount
                ])
            
            # ---- SORT QUERY 1 RESULTS ----
            input_q1_results.sort()
            
            return input_q1_results
        except Exception as e:
            raise Exception("Couldn't build input file query 1 result. " + str(e))
        finally:
            if tran_reader:
                tran_reader.close()

    def _generate_q2_result(self):
        tran_reader = None
        try:
            tran_reader = TransactionsReader(self._input_file_name)
            max_tran_per_bank = {}
            for transaction_item in iter(tran_reader.next_transaction, None):
                # ---- QUERY 2 PROCESSING ----
                # Filter non US Dollar transactions
                if transaction_item.currency != "US Dollar":
                    continue

                # Update max transaction per bank
                if transaction_item.from_bank_id not in max_tran_per_bank:
                    max_tran_per_bank[transaction_item.from_bank_id] = (transaction_item.from_account, transaction_item.amount)
                elif (transaction_item.amount > max_tran_per_bank[transaction_item.from_bank_id][1] or 
                      (transaction_item.amount == max_tran_per_bank[transaction_item.from_bank_id][1] and 
                      transaction_item.from_account < max_tran_per_bank[transaction_item.from_bank_id][0])):
                    max_tran_per_bank[transaction_item.from_bank_id] = (transaction_item.from_account, transaction_item.amount)

            input_q2_results = []
            for bank_id, (account, amount) in max_tran_per_bank.items():
                # ---- STORE QUERY 2 RESULT ----
                if bank_id not in self._bank_names:
                    continue
                input_q2_results.append([
                    bank_id,
                    account, 
                    self._bank_names[bank_id],
                    amount
                ])

            # ---- SORT QUERY 2 RESULTS ----
            input_q2_results.sort()

            return input_q2_results
        except Exception as e:
            raise Exception("Couldn't build input file query 2 result. " + str(e))
        finally:
            if tran_reader:
                tran_reader.close()

    def _generate_q3_result(self):
        tran_reader = None
        try:
            tran_reader = TransactionsReader(self._input_file_name)

            from_preceding_dt = int(datetime(2022, 9, 1, tzinfo=UTC).timestamp())
            to_preceding_dt = int(datetime(2022, 9, 5, 23, 59, 59, tzinfo=UTC).timestamp())
            from_subsequent_dt = int(datetime(2022, 9, 6, tzinfo=UTC).timestamp())
            to_subsequent_dt = int(datetime(2022, 9, 15, 23, 59, 59, tzinfo=UTC).timestamp())

            sum_count_per_payment_format = {}
            transactions_subsequent = []
            for transaction_item in iter(tran_reader.next_transaction, None):
                # ---- QUERY 3 PROCESSING ----
                # Filter non US Dollar transactions
                if transaction_item.currency != "US Dollar":
                    continue
                
                # For transactions between 2022-09-01 and 2022-09-05, calculate sum and count per payment format
                if from_preceding_dt <= transaction_item.timestamp <= to_preceding_dt:
                    if transaction_item.payment_format not in sum_count_per_payment_format:
                        sum_count_per_payment_format[transaction_item.payment_format] = (0, 0)
                    sum, count = sum_count_per_payment_format[transaction_item.payment_format]
                    sum_count_per_payment_format[transaction_item.payment_format] = (sum + transaction_item.amount, count + 1)
                # For transactions between 2022-09-06 and 2022-09-15, store them for later processing
                elif from_subsequent_dt <= transaction_item.timestamp <= to_subsequent_dt:
                    transactions_subsequent.append(transaction_item)

            # Calculate average per payment format for preceding transactions
            average_per_payment_format = {}
            for payment_format, (sum, count) in sum_count_per_payment_format.items():
                average_per_payment_format[payment_format] = sum / count
            
            input_q3_results = []
            for transaction_item in transactions_subsequent:
                if transaction_item.payment_format not in average_per_payment_format:
                    continue

                # Filter subsequent transactions with amount >= 1% of the average amount 
                # per payment format of preceding transactions
                if transaction_item.amount < average_per_payment_format[transaction_item.payment_format] / 100:
                    # ---- STORE QUERY 3 RESULT ----
                    input_q3_results.append([
                        transaction_item.from_bank_id,
                        transaction_item.from_account,
                        transaction_item.payment_format,
                        transaction_item.amount
                    ])

            # ---- SORT QUERY 3 RESULTS ----
            input_q3_results.sort()

            return input_q3_results
        except Exception as e:
            raise Exception("Couldn't build input file query 3 result. " + str(e))
        finally:
            if tran_reader:
                tran_reader.close()

    def _generate_q4_result(self):
        tran_reader = None
        try:
            tran_reader = TransactionsReader(self._input_file_name)

            from_dt = int(datetime(2022, 9, 1, tzinfo=UTC).timestamp())
            to_dt = int(datetime(2022, 9, 5, 23, 59, 59, tzinfo=UTC).timestamp())

            transactions = []
            for transaction_item in iter(tran_reader.next_transaction, None):
                # ---- QUERY 4 PROCESSING ----
                # Filter non US Dollar transactions
                if transaction_item.currency != "US Dollar":
                    continue
                # Filter transactions that are not between 2022-09-01 and 2022-09-05
                if not (from_dt <= transaction_item.timestamp <= to_dt):
                    continue
                
                transactions.append([
                    transaction_item.from_bank_id,
                    transaction_item.from_account,
                    transaction_item.to_bank_id,
                    transaction_item.to_account,
                ])

            # Build outgoing transactions dictionary
            outgoing = {}
            for from_bank_id, from_acc, to_bank_id, to_acc in transactions:
                if (from_bank_id, from_acc) not in outgoing:
                    outgoing[(from_bank_id, from_acc)] = []
                outgoing[(from_bank_id, from_acc)].append(
                    (to_bank_id, to_acc)
                )

            # Count distinct Scatter-Gather occurrences
            scatter_gather_count = {}
            for from_bank_id, from_acc, mid_bank_id, mid_acc in transactions:
                if (mid_bank_id, mid_acc) not in outgoing:
                    continue
                for to_bank_id, to_acc in outgoing[(mid_bank_id, mid_acc)]:
                    # Ignore A -> B -> A patterns
                    if (from_bank_id, from_acc) == (to_bank_id, to_acc):
                        continue

                    key = (from_bank_id, from_acc, to_bank_id, to_acc)
                    if key not in scatter_gather_count:
                        scatter_gather_count[key] = set()
                    scatter_gather_count[key].add((mid_bank_id, mid_acc))

            # Identify laundering accounts as those with more than 5 Scatter-Gather occurrences
            laundering_accounts = set()
            for (from_bank_id, from_acc, to_bank_id, to_acc), intermediates in scatter_gather_count.items():
                if len(intermediates) > 5:
                    laundering_accounts.add((from_bank_id, from_acc))
                    laundering_accounts.add((to_bank_id, to_acc))

            input_q4_results = []
            for bank_id, acc in laundering_accounts:
                # ---- STORE QUERY 4 RESULT ----
                input_q4_results.append([
                    bank_id,
                    acc
                ])

            # ---- SORT QUERY 4 RESULTS ----
            input_q4_results.sort()

            return input_q4_results
        except Exception as e:
            raise Exception("Couldn't build input file query 4 result. " + str(e))
        finally:
            if tran_reader:
                tran_reader.close()

    def _generate_q5_result(self):
        tran_reader = None
        try:
            tran_reader = TransactionsReader(self._input_file_name)

            from_dt = int(datetime(2022, 9, 1, tzinfo=UTC).timestamp())
            to_dt = int(datetime(2022, 9, 5, 23, 59, 59, tzinfo=UTC).timestamp())
            
            count = 0
            for transaction_item in iter(tran_reader.next_transaction, None):
                # ---- QUERY 5 PROCESSING ----
                # Filter transactions that are not between 2022-09-01 and 2022-09-05
                if not (from_dt <= transaction_item.timestamp <= to_dt):
                    continue
                # Filter transactions with payment format different than 'Wire' or 'ACH'
                if transaction_item.payment_format != "Wire" and transaction_item.payment_format != "ACH":
                    continue
                # Filter transactions with amount >= 1 USD
                amount_usd = self._usd_converter.convert_to_usd(
                    transaction_item.timestamp,
                    transaction_item.currency, 
                    transaction_item.amount
                )
                if amount_usd is None or amount_usd >= 1:
                    continue

                count += 1

            return [[count]]
        except Exception as e:
            raise Exception("Couldn't build input file query 5 result. " + str(e))
        finally:
            if tran_reader:
                tran_reader.close()

    def _store_bank_names(self):
        accounts_reader = None
        try:
            accounts_reader = AccountsReader(self._accounts_file_name)
            self._bank_names = {}
            for (bank_id, bank_name) in iter(accounts_reader.next_account, None):
                self._bank_names[bank_id] = bank_name
        except Exception as e:
            raise Exception("Couldn't read accounts file. " + str(e))
        finally:
            if accounts_reader:
                accounts_reader.close()

    def _store_expected_query_results(self, expected_query_results, query_number):
        logging.info(f"Storing expected query {query_number} results...")
        output_file_name = f"./expected_output/expected_{self._client_id}_{query_number}.csv"
        try:
            with open(output_file_name, "w") as output_file:
                output_file.write(",".join(self._expected_output_file_headers.get(query_number, [])) + "\n")
                for result in expected_query_results:
                    output_file.write(",".join(map(str, result)) + "\n")
        except Exception as e:
            raise Exception(f"Couldn't store expected query {query_number} results. " + str(e))
