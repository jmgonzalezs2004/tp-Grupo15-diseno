import yaml
import logging

from tests.utils.expected_query_results_generator import ExpectedQueryResultsGenerator

DOCKER_FILE_PATH = "./docker-compose.yaml"

class ExpectedQueryResultGenerationError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

def find_environment_variable(environment_variables, target_environment_variable):
    for environment_variable in environment_variables:
        [name, value] = environment_variable.split("=")
        if name == target_environment_variable:
            return value
    return None

def generate_expected_query_results(client_service):
    client_name = client_service["container_name"]
    logging.info(f"Generating expected query results for {client_name}...")
    environment = client_service["environment"]
    client_id = find_environment_variable(environment, "ID")
    accounts_file = "." + find_environment_variable(environment, "ACCOUNTS_FILE")
    input_file = "." + find_environment_variable(environment, "INPUT_FILE")

    if not accounts_file or not input_file:
        raise ExpectedQueryResultGenerationError("Bad file environment variable config")

    generator = ExpectedQueryResultsGenerator(client_id, accounts_file, input_file)
    err = generator.generate_expected_query_results()
    if err:
        raise ExpectedQueryResultGenerationError(f"Expected query results generation failed: {err}")
    
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
            
            logging.info("Generating expected query results per client...")
            for client_service_name in client_services_name:
                client_service = services[client_service_name]
                generate_expected_query_results(client_service)
            logging.info("Expected query results generated successfully for all clients")
    except ExpectedQueryResultGenerationError as e:
        logging.error(e.message)
        return 1
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    main()
