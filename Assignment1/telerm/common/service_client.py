"""Transparent logical-service client.

A client names a service only by its logical identity
(``service://telerm/UPF/S00012``).  The client library

* resolves the name through the manager (access + location transparency);
* sends the call to whichever site currently hosts the service, stamping it
  with the endpoint's generation so a site can refuse a stale or fenced copy
  (migration + replication transparency: clients never address a replica);
* on any relocation symptom -- ``not_here``, ``stale_generation``,
  ``not_running``, ``site_fenced``, a refused/reset connection, or a name that
  is momentarily ``migrating`` / ``unavailable`` -- drops its cached endpoint,
  re-resolves and retries with back-off until ``deadline_s`` (failure
  transparency); only then does it raise ``ServiceUnavailable``, which is the
  *reported*, consistent form of unavailability.
"""
from __future__ import annotations

import time

from common import protocol


class ServiceUnavailable(RuntimeError):
    pass


RELOCATION_ERRORS = ("not_here", "stale_generation", "site_fenced", "wrong_service")


class ServiceClient:
    def __init__(self, manager_host, manager_port, timeout=2.0, deadline_s=15.0, backoff_s=0.15):
        self.manager_host, self.manager_port = manager_host, manager_port
        self.timeout, self.deadline_s, self.backoff_s = timeout, deadline_s, backoff_s
        self.cache = {}
        self.stats = {"resolutions": 0, "resolve_failures": 0, "calls": 0, "retries": 0,
                      "relocations_observed": 0, "failures": 0}
        self.resolve_latency_ms = []     # client-observed round trip to the manager
        self.call_latency_ms = []
        self._last_site = {}

    # ------------------------------------------------------------ resolve
    def resolve(self, logical_name, refresh=False):
        """Endpoint dict {host, port, site_id, instance_id, generation}. Raises
        ServiceUnavailable(reason) if the name cannot currently be resolved."""
        if not refresh and logical_name in self.cache:
            return dict(self.cache[logical_name])
        t0 = time.perf_counter()
        try:
            r = protocol.request(self.manager_host, self.manager_port,
                                 {"type": "RESOLVE", "name": logical_name}, timeout=self.timeout)
        except (ConnectionError, OSError, TimeoutError) as exc:
            self.stats["resolve_failures"] += 1
            raise ServiceUnavailable(f"manager_unreachable: {exc}") from exc
        self.resolve_latency_ms.append((time.perf_counter() - t0) * 1000)
        self.stats["resolutions"] += 1
        if r.get("type") != "RESOLVED":
            self.stats["resolve_failures"] += 1
            raise ServiceUnavailable(r.get("reason") or r.get("error") or "resolution_failed")
        ep = r["endpoint"]
        prev = self._last_site.get(logical_name)
        if prev is not None and prev != (ep["site_id"], ep["generation"]):
            self.stats["relocations_observed"] += 1
        self._last_site[logical_name] = (ep["site_id"], ep["generation"])
        self.cache[logical_name] = ep
        return dict(ep)

    def lookup(self, service):
        """Logical names of every resolvable instance of a service type."""
        r = protocol.request(self.manager_host, self.manager_port,
                             {"type": "LOOKUP", "service": service}, timeout=self.timeout)
        return r.get("instances", [])

    # --------------------------------------------------------------- call
    def call(self, logical_name, payload=None, deadline_s=None):
        """Invoke the service; returns the SERVICE_REPLY dict (incl. ``site_id``/``generation``
        of whichever copy answered)."""
        deadline = time.time() + (self.deadline_s if deadline_s is None else deadline_s)
        attempt, last = 0, "no_attempt"
        t0 = time.perf_counter()
        while True:
            try:
                ep = self.resolve(logical_name, refresh=attempt > 0)
                reply = protocol.request(ep["host"], int(ep["port"]), {
                    "type": "SERVICE_CALL", "sid": ep["instance_id"], "generation": ep["generation"],
                    "logical_name": logical_name, "payload": payload}, timeout=self.timeout)
                if reply.get("type") == "SERVICE_REPLY":
                    self.stats["calls"] += 1
                    self.call_latency_ms.append((time.perf_counter() - t0) * 1000)
                    return reply
                last = reply.get("error", "service_error")
            except ServiceUnavailable as exc:
                last = str(exc)
                if last == "not_found":
                    self.stats["failures"] += 1
                    raise                                  # terminated: retrying cannot help
            except (ConnectionError, OSError, TimeoutError) as exc:
                last = f"endpoint_unreachable: {exc}"
            self.cache.pop(logical_name, None)
            attempt += 1
            self.stats["retries"] += 1
            if time.time() >= deadline:
                self.stats["failures"] += 1
                raise ServiceUnavailable(f"{logical_name}: {last} (after {attempt} attempts)")
            time.sleep(self.backoff_s)

    request = call          # name used by the Milestone 9 scaffolding

    def invalidate(self, logical_name):
        self.cache.pop(logical_name, None)
