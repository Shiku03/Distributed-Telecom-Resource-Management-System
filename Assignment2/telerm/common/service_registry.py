"""Thread-safe logical service-name registry for TeleRM.

The registry is owned by the global manager. It stores logical names
separately from physical site endpoints so that migration and failover can
change an endpoint without changing the client-facing name.

Naming convention: ``service://telerm/{service}/{instance_id}``.

Every record carries a monotonically increasing ``generation``. A relocation
(migration or replica promotion) installs a strictly larger generation, and
sites reject calls that carry an older one, so a client holding a stale
endpoint can never be served by the wrong copy.

Record states (a name is resolvable only in RUNNING or RESIZING):
ALLOCATING, RUNNING, RESIZING, MIGRATING, UNAVAILABLE, DRAINING.
"""
from __future__ import annotations

import threading
import time

from common.lifecycle import RESOLVABLE


class NameConflict(ValueError):
    pass


class StaleGeneration(ValueError):
    pass


class ServiceRegistry:
    def __init__(self):
        self._lock = threading.RLock()
        self._by_name = {}
        self._by_instance = {}

    # ----------------------------------------------------------- naming
    @staticmethod
    def default_name(service: str, instance_id: str) -> str:
        return f"service://telerm/{service}/{instance_id}"

    @staticmethod
    def validate_name(name: str) -> str:
        if not isinstance(name, str):
            raise ValueError("logical service name must be a string")
        name = name.strip()
        if not name or len(name) > 200 or not name.startswith("service://"):
            raise ValueError("logical name must be a non-empty service:// URI (max 200 chars)")
        if any(ch.isspace() for ch in name):
            raise ValueError("logical service name cannot contain whitespace")
        return name

    # --------------------------------------------------------- mutation
    def register(self, name: str, instance_id: str, service: str,
                 site_id: str, host: str, port: int, state: str = "RUNNING",
                 generation: int = 1) -> dict:
        name = self.validate_name(name)
        if not instance_id or not site_id or not host or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("invalid service endpoint or instance identity")
        record = {
            "name": name, "instance_id": str(instance_id), "service": service,
            "site_id": site_id, "host": host, "port": port,
            "state": state, "generation": int(generation), "updated_at": time.time(),
        }
        with self._lock:
            existing = self._by_name.get(name)
            if existing and existing["instance_id"] != str(instance_id):
                raise NameConflict(f"logical name already registered: {name}")
            self._by_name[name] = record
            self._by_instance[str(instance_id)] = name
            return dict(record)

    def restore(self, records) -> int:
        """Load records persisted by a previous manager run. Every restored
        record is forced to UNAVAILABLE: a record in the database proves the
        service existed, not that it is running; it becomes resolvable only
        after the hosting site's inventory has been reconciled."""
        n = 0
        with self._lock:
            for r in records:
                rec = {
                    "name": r.get("name") or r["logical_name"], "instance_id": str(r["instance_id"]),
                    "service": r["service"], "site_id": r["site_id"],
                    "host": r["host"], "port": int(r["port"]),
                    "state": "UNAVAILABLE", "generation": int(r.get("generation", 1)),
                    "updated_at": time.time(),
                }
                self._by_name[rec["name"]] = rec
                self._by_instance[rec["instance_id"]] = rec["name"]
                n += 1
        return n

    def update_location(self, instance_id: str, site_id: str, host: str, port: int,
                        state: str = "RUNNING", generation: int | None = None) -> bool:
        with self._lock:
            name = self._by_instance.get(str(instance_id))
            if not name:
                return False
            rec = self._by_name[name]
            if generation is not None:
                if int(generation) < rec["generation"]:
                    raise StaleGeneration(
                        f"generation {generation} older than registered {rec['generation']}")
                rec["generation"] = int(generation)
            rec.update(site_id=site_id, host=host, port=port, state=state, updated_at=time.time())
            return True

    def set_state(self, instance_id: str, state: str) -> bool:
        with self._lock:
            name = self._by_instance.get(str(instance_id))
            if not name:
                return False
            self._by_name[name]["state"] = state
            self._by_name[name]["updated_at"] = time.time()
            return True

    def unregister_instance(self, instance_id: str) -> bool:
        with self._lock:
            name = self._by_instance.pop(str(instance_id), None)
            if name is None:
                return False
            self._by_name.pop(name, None)
            return True

    def unregister_name(self, name: str) -> bool:
        with self._lock:
            rec = self._by_name.pop(name, None)
            if rec is None:
                return False
            self._by_instance.pop(rec["instance_id"], None)
            return True

    # ----------------------------------------------------------- queries
    def resolve(self, name: str):
        """Endpoint record if the name is registered *and* resolvable."""
        name = self.validate_name(name)
        with self._lock:
            record = self._by_name.get(name)
            if not record or record["state"] not in RESOLVABLE:
                return None
            return dict(record)

    def resolve_detail(self, name: str):
        """(record, reason). ``reason`` is None on success, else one of
        ``not_found``, ``migrating`` (transient, retry) or ``unavailable``."""
        name = self.validate_name(name)
        with self._lock:
            record = self._by_name.get(name)
            if record is None:
                return None, "not_found"
            if record["state"] in RESOLVABLE:
                return dict(record), None
            if record["state"] == "MIGRATING":
                return None, "migrating"
            return None, "unavailable"

    def lookup(self, name: str):
        """Registered record regardless of state (administrative view)."""
        with self._lock:
            rec = self._by_name.get(self.validate_name(name))
            return dict(rec) if rec else None

    def name_of(self, instance_id: str):
        with self._lock:
            return self._by_instance.get(str(instance_id))

    def by_service(self, service: str, only_resolvable: bool = True):
        with self._lock:
            return [dict(r) for r in self._by_name.values()
                    if r["service"] == service and (not only_resolvable or r["state"] in RESOLVABLE)]

    def snapshot(self):
        with self._lock:
            return [dict(v) for v in self._by_name.values()]
