import multiprocessing

class ClientIdGenerator:
    '''Thread-safe auto-incremental client id generator'''

    def __init__(self):
        self.current = multiprocessing.Value("i", 1)
        self.lock = multiprocessing.Lock()

    def generate(self):
        with self.lock:
            value = self.current.value
            self.current.value += 1
            return value