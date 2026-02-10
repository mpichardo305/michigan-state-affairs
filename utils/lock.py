import fcntl
import time
from datetime import datetime
from pathlib import Path


class FileLock:
    """File-based lock to prevent concurrent runs"""

    def __init__(self, lock_path, logger, timeout=60):
        self.lock_file = Path(lock_path)
        self.logger = logger
        self.timeout = timeout
        self.lock_fd = None

    def acquire(self):
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        self.lock_fd = open(self.lock_file, 'w')

        start_time = time.time()
        while True:
            try:
                fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.lock_fd.write(str(datetime.now().isoformat()))
                self.lock_fd.flush()
                return True
            except IOError:
                if time.time() - start_time > self.timeout:
                    self.logger.error("Could not acquire lock - another instance running?")
                    return False
                time.sleep(1)

    def release(self):
        if self.lock_fd:
            fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)
            self.lock_fd.close()
            self.lock_fd = None
            try:
                self.lock_file.unlink()
            except FileNotFoundError:
                pass

    def __enter__(self):
        if not self.acquire():
            raise RuntimeError("Could not acquire lock")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False
