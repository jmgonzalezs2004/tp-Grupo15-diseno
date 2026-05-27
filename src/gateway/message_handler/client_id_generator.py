import threading

class ClientIdGenerator:
    '''Thread-safe auto-incremental client id generator'''

    def __init__(self):
        self.current = 1
        self.lock = threading.Lock()

    def generate(self) -> int:
        with self.lock:
            value = self.current
            self.current += 1
            return value