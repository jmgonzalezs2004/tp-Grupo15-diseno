import logging
from tests.utils.query_results_reader import QueryResultsReader


QUERY_AMOUNT = 5


class QueryResultsVerifier:
    def __init__(self, client_id, output_file_prefix_name):
        self._client_id = client_id
        self._output_file_prefix_name = output_file_prefix_name

    def verify_query_results(self):
        try:
            for q in range(1, QUERY_AMOUNT + 1):
                logging.info(f"Verifying query {q} results...")
                self._compare_query_results(q)
        except Exception as e:
            return str(e)

        return None

    def _compare_query_results(self, query_number):
        expected_query_results = self._read_query_results(f"./expected_output/expected_{self._client_id}_{query_number}.csv", query_number)
        output_query_results = self._read_query_results(f"{self._output_file_prefix_name}{query_number}.csv", query_number)

        if len(expected_query_results) != len(output_query_results):
            logging.error(f"Q{query_number}: Number of results differ\n"
                          f"Expected: {len(expected_query_results)}\n"
                          f"Actual:   {len(output_query_results)}")
            return

        output_query_results.sort()
        if expected_query_results != output_query_results:
            for i, (expected, actual) in enumerate(zip(expected_query_results, output_query_results)):
                if expected != actual:
                    logging.error(f"Q{query_number}: Difference at row {i}\n"
                                  f"Expected: {expected}\n"
                                  f"Actual:   {actual}")
                    return

    def _read_query_results(self, file, query_number):
        query_result_reader = None
        try:
            query_result_reader = QueryResultsReader(file, query_number)
            query_results = []
            for output_item in iter(query_result_reader.next_output, None):
                query_results.append(output_item)
            return query_results
        except Exception as e:
            raise Exception(f"Error reading query results from file {file}: {e}")
        finally:
            if query_result_reader:
                query_result_reader.close()
