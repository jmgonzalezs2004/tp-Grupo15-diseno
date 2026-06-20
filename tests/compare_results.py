import os
import yaml
import logging
import subprocess
import threading

from tests.utils.query_results_verifier import QueryResultsVerifier
from tests.utils.chaos_monkey import ChaosMonkey

DOCKER_FILE_PATH = "./docker-compose.yaml"


class ClientValidationError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


def await_client_containers(client_services_name):
    result = subprocess.run(
        ["docker", "container", "wait"] + client_services_name, capture_output=True
    )

    zero_exit_code_count = 0
    for char in result.stdout.decode("utf-8"):
        if char == "0":
            zero_exit_code_count += 1

    if zero_exit_code_count != len(client_services_name):
        raise ClientValidationError("One or more clients exited with an error code")


def find_environment_variable(environment_variables, target_environment_variable):
    for environment_variable in environment_variables:
        [name, value] = environment_variable.split("=")
        if name == target_environment_variable:
            return value
    return None


def verify_client_output(client_service):
    client_name = client_service["container_name"]
    logging.info(client_name)
    environment = client_service["environment"]
    client_id = find_environment_variable(environment, "ID")
    output_file_prefix = "." + find_environment_variable(environment, "OUTPUT_FILE_PREFIX")

    if not client_id or not output_file_prefix:
        raise ClientValidationError("Bad file environment variable config")

    verifier = QueryResultsVerifier(client_id, output_file_prefix)
    err = verifier.verify_query_results()
    if err:
        raise ClientValidationError(f"Query results verification failed: {err}")

    logging.info("OK")


def main():
    logging.basicConfig(level=logging.INFO)

    try:
        with open(DOCKER_FILE_PATH, "r") as docker_compose_file:
            parsed_docker_compose_file = yaml.safe_load(docker_compose_file)
            services = parsed_docker_compose_file["services"]
            client_services_name = list(
                filter(
                    lambda service_key: "client"
                    in services[service_key]["build"]["dockerfile"],
                    services.keys(),
                )
            )

            chaos_monkey_enabled = os.getenv("CHAOS_MONKEY") == "1"
            thread = None
            chaos_monkey = None
            if chaos_monkey_enabled:
                logging.info("Chaos Monkey mode enabled.")
                chaos_monkey = ChaosMonkey(services)
                thread = threading.Thread(target=chaos_monkey.run)
                thread.start()

            logging.info("Awaiting client containers to exit...")
            await_client_containers(client_services_name)

            if chaos_monkey_enabled:
                logging.info("Stopping Chaos Monkey...")
                chaos_monkey.stop()
                thread.join()

            logging.info("Validating clients...")
            for client_service_name in client_services_name:
                client_service = services[client_service_name]
                verify_client_output(client_service)
            logging.info("All query results match the expected output")
    except ClientValidationError as e:
        logging.error(e.message)
        return 1
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    main()
