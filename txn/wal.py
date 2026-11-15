"""
Write-ahead log: one JSON record per line, fsync'd on append. This is
what gives the transaction subsystem the Durability in ACID, and is
what a crashed coordinator/participant replays on restart to recover
its in-doubt transactions (Milestone 5: "distributed transaction
recovery").
"""
import json
import os
import threading


class WAL:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        open(path, "a").close()

    def append(self, record: dict):
        with self._lock:
            with open(self.path, "a") as f:
                f.write(json.dumps(record) + "\n")
                f.flush()
                os.fsync(f.fileno())

    def read_all(self):
        if not os.path.exists(self.path):
            return []
        with open(self.path) as f:
            return [json.loads(line) for line in f if line.strip()]

    def truncate(self):
        with self._lock:
            open(self.path, "w").close()
