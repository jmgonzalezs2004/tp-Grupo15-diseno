import logging
import random
import subprocess
import threading

INTERVAL_BETWEEN_FAILURES = 20  # seconds


class ChaosMonkey:
    def __init__(self, services):
        self._services = services
        self._stop_event = threading.Event()

    def run(self):
        target_services = self._filter_target_services()
        if not target_services:
            logging.warning("No target services found for Chaos Monkey.")
            return

        while not self._stop_event.wait(INTERVAL_BETWEEN_FAILURES):
            running_target_services = target_services & self._get_total_running_services()
            if not running_target_services:
                logging.info("No running target services found.")
                continue
            service = random.choice(list(running_target_services))
            self._kill_service(service)

    def _filter_target_services(self):
        target_services = set()
        excluded_services_prefix = {"client", "gateway", "rabbitmq"}
        for service_name in self._services.keys():
            if not any(service_name.startswith(prefix) for prefix in excluded_services_prefix):
                target_services.add(service_name)
        return target_services

    def _get_total_running_services(self):
        try:
            result = subprocess.run([
                    "docker",
                    "compose",
                    "ps",
                    "--services",
                    "--filter",
                    "status=running"],
                capture_output=True,
                text=True,
                check=True,
            )
            running_services = result.stdout.splitlines()
            return set(running_services)
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to get running services: {e}")
            return set()
        except FileNotFoundError:
            logging.error("Docker command not found")
            return set()

    def _kill_service(self, service_name):
        try:
            subprocess.run(
                ["docker", "compose", "kill", service_name],
                check=True,
            )
            logging.info(f"Killed service: {service_name}")
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to kill service {service_name}: {e}")
        except FileNotFoundError:
            logging.error("Docker command not found")

    def stop(self):
        self._stop_event.set()
