#!/usr/bin/env python3
"""
TeleRM Global Resource Manager (design doc section 3, "Global Resource
Manager" box: Admission control, Placement policy, Allocation table,
Topology + link reservations, Site registry + failure detector,
Utilisation sampler) -- extended for Milestones 9 and 10 with the control
plane: durable naming, process lifecycle, transparent migration, active-standby
replication, failure detection with fencing, and reconciliation.

Locking rule: ``self.lock`` guards the in-memory state and is *never* held
across a network call.  Admission reserves resources under the lock, releases
it for the ALLOCATE round trip, and finalises (or rolls back) under the lock
again; the reservation ledger makes that interleaving safe.

Run standalone:
    python3 manager.py --config config.json --results-dir results/run1
"""
import argparse
import json
import math
import os
import sys
import threading
import time

from common import protocol, util, placement
from common.counters import Counters
from common.topology import Topology
from common.service_registry import ServiceRegistry, NameConflict
from common.lifecycle import (event as lifecycle_event, transition as lifecycle_transition,
                              InvalidTransition, RESOLVABLE)
from common.durable_store import DurableStore
from common.ledger import ResourceLedger, ReservationError
from common.failure_recovery import FailureConfig, FailureDetector, plan_reconciliation
from common.control_plane import ControlPlane
from common import reporting

REQUIRED = {
    "REGISTER": ("site_id", "host", "port", "tier", "capacity"),
    "HEARTBEAT": ("site_id",),
    "REQUEST": ("service", "ingress", "demand", "max_latency_ms", "holding_s"),
    "RESIZE": ("sid", "factor"),
    "INSTANCE_EXIT": ("sid",),
    "RESOLVE": ("name",),
    "MIGRATE": ("target_site",),
    "REPLICATE": ("target_site",),
    "REPLICA_REMOVE": ("replica_id",),
    "REPLICA_RECONFIGURE": ("replica_id", "target_site"),
    "ABANDON_SITE": ("site_id",),
    "SITE_RECOVER": ("site_id",),
}
CONTROL_MESSAGES = {"MIGRATE", "REPLICATE", "REPLICA_REMOVE", "REPLICA_RECONFIGURE", "FAILOVER",
                    "ABANDON_SITE", "SITE_RECOVER", "CONTROL_STATUS"}


class SiteUnreachable(ConnectionError):
    pass


class Manager:
    def __init__(self, config: dict, results_dir: str, policy: str = None, db_path: str = None):
        self.config = config
        self.results_dir = results_dir
        self.policy = policy or config.get("placement_policy", "edge_first")
        if self.policy not in placement.POLICIES:
            raise ValueError(f"unknown placement policy {self.policy!r}")
        self.fcfg = FailureConfig.from_config(config)
        self.detector = FailureDetector(self.fcfg)
        self.heartbeat_interval = self.fcfg.heartbeat_interval_s
        self.failure_timeout_hb = config.get("failure_timeout_heartbeats", 3)
        self.fairness_threshold = config.get("fairness_saturation_threshold", 0.9)

        self.topology = Topology(config["links"])
        self.counters = Counters()

        # --- protected state; the lock is never held across an RPC ---
        self.lock = threading.RLock()
        self.sites = {}             # site_id -> record (capacity, allocated, status, ...)
        self.allocation_table = {}  # sid -> instance record
        self.replicas = {}          # replica_id -> replica record
        self.registry = ServiceRegistry()
        self.service_registry = self.registry   # name used by Milestone 9 phase 1
        os.makedirs(results_dir, exist_ok=True)
        self.store = DurableStore(db_path or config.get("control_db")
                                  or os.path.join(results_dir, "telerm_control.sqlite3"))
        self.ledger = ResourceLedger(self.topology, self.sites, self.store)
        self.lifecycle_events = []
        self.failure_events = []
        self._next_sid = 1
        self.started_at = time.time()

        # metrics
        self.offered = {"_all": 0}
        self.blocked = {"_all": 0}
        self.blocked_reasons = {"_all": {}}
        self.admitted = {"_all": 0}
        self.decision_times_ms = []
        self.resolution_ms = []
        self.resolve_outcomes = {"ok": 0, "migrating": 0, "unavailable": 0, "not_found": 0}
        self.completed_instances = []
        self.heartbeat_log = []
        self.utilisation_samples = []

        self._stop = threading.Event()
        self._srv = None
        self.cp = ControlPlane(self)
        self.restore_summary = self.cp.restore()

    # ---------------------------------------------------------- lifecycle
    def start(self):
        host, port = self.config["manager"]["host"], self.config["manager"]["port"]
        self._srv = protocol.serve_forever(host, port, self._dispatch, counters=self.counters)
        threading.Thread(target=self._sampler_loop, daemon=True, name="sampler").start()
        threading.Thread(target=self._failure_detector_loop, daemon=True, name="detector").start()
        self.cp.start()
        print(f"[manager] listening on {host}:{port} (policy={self.policy}, "
              f"restored={self.restore_summary})", flush=True)

    def stop(self):
        self._stop.set()
        self.cp.stop()
        protocol.close_server(self._srv)
        self.store.flush()

    # ------------------------------------------------------------ helpers
    def emit(self, event_type, category, sid=None, name=None, site=None, op=None, buffered=False, **payload):
        self.store.event(event_type, sid, name, site, payload, category=category, operation_id=op,
                         buffered=buffered)

    def set_state(self, inst, new_state, event_name=None, **details):
        """Validated lifecycle transition + durable lifecycle event (lock held)."""
        old = inst["state"]
        inst["state"] = lifecycle_transition(old, new_state)
        op = details.pop("operation_id", inst.get("op"))
        if "generation" in details:           # a *target* generation, not the record's current one
            details["to_generation"] = details.pop("generation")
        ev = lifecycle_event(inst["sid"], event_name or f"STATE_{new_state}", new_state,
                             logical_name=inst["logical_name"], site=inst["site"], operation_id=op,
                             generation=inst["generation"], previous_state=old, **details)
        self.lifecycle_events.append(ev)
        self.store.event(ev["event"], inst["sid"], inst["logical_name"], inst["site"], ev,
                         category="LIFECYCLE", operation_id=op, ts=ev["ts"])

    def reg_state(self, inst, state, since=None):
        """Set the registry state and keep per-instance availability accounting.
        ``since`` backdates the start of an outage (a failed site is unreachable from its
        last heartbeat, not from the moment it was suspected).
        Returns the length (s) of an outage that this call ended, else None."""
        self.registry.set_state(inst["sid"], state)
        now = time.time()
        if state not in RESOLVABLE:
            if inst["unavailable_since"] is None:
                inst["unavailable_since"] = since or now
            return None
        if inst["unavailable_since"] is not None:
            dur = now - inst["unavailable_since"]
            inst["unavailable_total_s"] += dur
            inst["unavailable_since"] = None
            return dur
        return None

    def persist(self, inst):
        """Durably record the process and its endpoint in one transaction (lock held)."""
        rec = self.registry.lookup(inst["logical_name"])
        self.store.save_instance({
            "instance_id": inst["sid"], "logical_name": inst["logical_name"], "service": inst["service"],
            "site_id": inst["site"], "state": inst["state"], "demand": inst["demand"],
            "holding_s": inst["holding_s"], "created_at": inst["admitted_at"], "generation": inst["generation"],
            "metadata": {"ingress": inst["ingress"], "path": inst["path"],
                         "path_latency_ms": inst["path_latency_ms"], "max_latency_ms": inst["max_latency_ms"],
                         "res_node": inst["res_node"], "res_path": inst["res_path"],
                         "unavailable_total_s": inst["unavailable_total_s"],
                         "migration_downtime_s": inst.get("migration_downtime_s", 0.0),
                         "replica_seq": inst.get("replica_seq", 0), "migrations": inst.get("migrations", 0),
                         "failovers": inst.get("failovers", 0)}},
            None if rec is None else {
                "logical_name": rec["name"], "service": rec["service"], "instance_id": rec["instance_id"],
                "site_id": rec["site_id"], "host": rec["host"], "port": rec["port"], "state": rec["state"],
                "generation": rec["generation"]})

    def find_instance(self, ref):
        """Instance record by sid or logical name (lock held)."""
        if ref is None:
            return None
        inst = self.allocation_table.get(str(ref))
        if inst is not None or not str(ref).startswith("service://"):
            return inst
        try:
            rec = self.registry.lookup(ref)
        except ValueError:
            return None
        return self.allocation_table.get(rec["instance_id"]) if rec else None

    def rpc(self, site_id, msg, timeout=5.0, allow_inactive=False):
        with self.lock:
            site = self.sites.get(site_id)
            if site is None:
                raise SiteUnreachable(f"unknown site {site_id}")
            if site["status"] != "ACTIVE" and not allow_inactive:
                raise SiteUnreachable(f"site {site_id} is {site['status']}")
            host, port = site["host"], site["port"]
        try:
            return protocol.request(host, port, msg, counters=self.counters, timeout=timeout)
        except (OSError, TimeoutError, ConnectionError) as exc:
            raise SiteUnreachable(f"{site_id}: {exc}") from exc

    def queue_replica_removal(self, site, rid):
        self.cp.enqueue("REPLICA_REMOVE", (site, rid))

    def _count_block(self, service, reason):
        self.blocked["_all"] += 1
        self.blocked[service] = self.blocked.get(service, 0) + 1
        sr = self.blocked_reasons.setdefault(service, {})
        sr[reason] = sr.get(reason, 0) + 1
        self.blocked_reasons["_all"][reason] = self.blocked_reasons["_all"].get(reason, 0) + 1

    # ---------------------------------------------------------- dispatch
    def _dispatch(self, msg: dict) -> dict:
        t = msg.get("type")
        if not isinstance(t, str):
            return {"type": "ERROR", "error": "missing message type"}
        missing = [f for f in REQUIRED.get(t, ()) if f not in msg]
        if t in ("RESOLVE", "LOOKUP") and "name" not in msg and "service" not in msg:
            missing = ["name"]
        if t in ("MIGRATE", "REPLICATE", "FAILOVER") and not (msg.get("sid") or msg.get("name")):
            missing = missing + ["sid|name"]
        if t == "TERMINATE" and not (msg.get("sid") or msg.get("name")):
            missing = ["sid|name"]
        if missing:
            return {"type": "ERROR", "error": f"missing_field:{','.join(missing)}"}
        try:
            if t == "REGISTER":
                return self._on_register(msg)
            if t == "HEARTBEAT":
                return self._on_heartbeat(msg)
            if t == "REQUEST":
                return self._on_request(msg)
            if t == "RESIZE":
                return self._on_resize(msg)
            if t == "INSTANCE_EXIT":
                return self._on_instance_exit(msg)
            if t in ("RESOLVE", "LOOKUP"):
                return self._on_resolve(msg)
            if t == "TERMINATE":
                return self._on_terminate(msg)
            if t in CONTROL_MESSAGES:
                return self.cp.handle(msg)
            if t == "STATUS":
                return self._status()
            if t == "METRICS":
                return {"type": "METRICS_OK", "metrics": reporting.compute_metrics(self)}
            if t == "WRITE_REPORT":
                self._write_report()
                return {"type": "WRITE_REPORT_OK", "results_dir": self.results_dir}
            if t == "SHUTDOWN":
                threading.Thread(target=self._delayed_shutdown, daemon=True).start()
                return {"type": "SHUTDOWN_OK"}
        except InvalidTransition as exc:
            return {"type": "ERROR", "error": f"invalid_lifecycle_transition: {exc}"}
        except (KeyError, TypeError, ValueError) as exc:
            return {"type": "ERROR", "error": f"invalid_message: {exc!r}"}
        except Exception as e:  # pragma: no cover - defensive
            return {"type": "ERROR", "error": str(e)}
        return {"type": "ERROR", "error": f"unknown message type {t!r}"}

    def _delayed_shutdown(self):
        time.sleep(0.2)  # let the SHUTDOWN_OK reply flush first
        self._write_report()
        self.stop()

    # ---------------------------------------------------------- REGISTER
    def _on_register(self, msg):
        sid = msg["site_id"]
        with self.lock:
            site = self.sites.get(sid)
            known = site is not None
            if site is None:
                site = {"site_id": sid, "allocated": self.ledger.alloc_for(sid), "status": "ACTIVE",
                        "boot_id": None, "last_fairness": None, "last_slot_util": None,
                        "last_inventory": None}
                self.sites[sid] = site
            site.update(host=msg["host"], port=msg["port"], tier=msg["tier"],
                        capacity=dict(msg["capacity"]), workers=msg.get("workers", 1))
            self.store.upsert_site({**site, "boot_id": msg.get("boot_id")})
            self.emit("SITE_REREGISTERED" if known else "SITE_REGISTERED", "SITE", None, None, sid, None,
                      tier=msg["tier"], host=msg["host"], port=msg["port"], previous_status=site["status"],
                      boot_id=msg.get("boot_id"))
            directives = self._site_contact(site, msg.get("boot_id"), msg.get("instances"), "register")
        print(f"[manager] REGISTER {sid} ({msg['tier']}) at {msg['host']}:{msg['port']}"
              f"{' (re-registration)' if known else ''}", flush=True)
        return {"type": "REGISTER_OK", "site_id": sid, **directives}

    # ----------------------------------------------- site contact / reconcile
    def _site_contact(self, site, boot_id, inventory, source):
        """Common path of REGISTER and HEARTBEAT (lock held). Updates the health state,
        reconciles the reported inventory and returns directives for the site."""
        sid = site["site_id"]
        now = time.time()
        prev = site["status"]
        restarted = bool(boot_id) and site.get("boot_id") not in (None, boot_id)
        if boot_id:
            site["boot_id"] = boot_id
        site["last_heartbeat"] = now
        site["last_inventory"] = inventory
        directives = {}
        recovering = prev in ("FAILED", "UNKNOWN", "RECOVERING")
        if (prev in ("FAILED", "RECOVERING") and not self.fcfg.auto_recover
                and not site.get("recovery_approved")):
            # controlled recovery: the site is talking again but an operator must approve
            # reconciliation (SITE_RECOVER). It is not acknowledged, so its lease stays expired.
            if prev == "FAILED":
                site["status"] = "RECOVERING"
                self._site_event(sid, "SITE_RECOVERY_AWAITING_APPROVAL", prev=prev)
            return {"hold": True}
        if prev == "FAILED":
            self._site_event(sid, "SITE_RECOVERY_STARTED", prev=prev, restarted=restarted)
            site["status"] = "RECOVERING"
        if inventory is not None:
            records_here = {s: i for s, i in self.allocation_table.items() if i["site"] == sid}
            reps_here = {r: v for r, v in self.replicas.items() if v["site"] == sid}
            plan = plan_reconciliation(sid, records_here, self.allocation_table, reps_here, inventory,
                                       restarted=restarted, missing_threshold=self.fcfg.missing_threshold)
            self.cp.apply_plan(sid, plan, restarted)
            if plan["release"]:
                directives["release"] = sorted(set(plan["release"]))
            if plan["resume"]:
                directives["resume"] = plan["resume"]
            if recovering or restarted or plan["release"] or plan["lost"]:
                self.emit("SITE_RECONCILED", "RECOVERY", None, None, sid, None, source=source,
                          restarted=restarted, restored=plan["restore"], released=plan["release"],
                          lost=plan["lost"], missing=plan["missing"], replicas_present=plan["replica_present"],
                          previous_status=prev)
        if site["status"] != "ACTIVE":
            site["status"] = "ACTIVE"
            site.pop("recovery_approved", None)
            if prev == "SUSPECT":
                self._site_event(sid, "SITE_HEARTBEAT_RECOVERED", prev=prev,
                                 reconciled=inventory is not None)
                print(f"[manager] {sid} heartbeat restored; instance list reconciled", flush=True)
            else:
                self._site_event(sid, "SITE_RECOVERED" if recovering else "SITE_ACTIVE", prev=prev,
                                 restarted=restarted)
                print(f"[manager] {sid} -> ACTIVE (was {prev})", flush=True)
            self.store.mark_site(sid, "ACTIVE", 0)
        return directives

    def _site_event(self, site_id, event, **extra):
        ev = {"ts": time.time(), "site_id": site_id, "event": event, **extra}
        self.failure_events.append(ev)
        self.emit(event, "FAILURE", None, None, site_id, None, **extra)

    # --------------------------------------------------------- HEARTBEAT
    def _on_heartbeat(self, msg):
        with self.lock:
            sid = msg["site_id"]
            site = self.sites.get(sid)
            if site is None:
                return {"type": "ERROR", "error": "unregistered site"}
            inventory = msg.get("instances")
            directives = self._site_contact(site, msg.get("boot_id"), inventory, "heartbeat")
            site["last_fairness"] = msg.get("fairness_j")
            site["last_slot_util"] = msg.get("slot_utilisation")
            self.heartbeat_log.append({
                "ts": msg.get("ts", time.time()), "site_id": sid,
                "slot_utilisation": msg.get("slot_utilisation"), "jobs_interval": msg.get("jobs_interval"),
                "fairness_j": msg.get("fairness_j"), "instances_running": msg.get("instances_running"),
                "site_status": site["status"]})
            self.store.site_heartbeat(sid, site["status"], {"instances": msg.get("instances_running")})
            self.emit("HEARTBEAT", "HEARTBEAT", None, None, sid, None, buffered=True,
                      instances=msg.get("instances_running"), slot_utilisation=msg.get("slot_utilisation"))
        return {"type": "HEARTBEAT_OK", **directives}

    # ----------------------------------------------------------- REQUEST
    def _on_request(self, msg):
        t0 = time.perf_counter()
        service, ingress = msg["service"], msg["ingress"]
        demand, max_latency, holding_s = msg["demand"], msg["max_latency_ms"], msg["holding_s"]
        requested_name = msg.get("logical_name")
        req_id = msg.get("req_id")
        try:
            if not isinstance(demand, dict):
                raise ValueError("demand must be an object")
            demand = {k: float(demand.get(k, 0)) for k in ("cpu", "mem", "bw")}
            if any(not math.isfinite(v) or v < 0 for v in demand.values()) or demand["cpu"] <= 0:
                raise ValueError("demand values must be finite, non-negative, cpu > 0")
            holding_s = float(holding_s)
            max_latency = float(max_latency)
            if not math.isfinite(holding_s) or holding_s <= 0:
                raise ValueError("holding_s must be positive")
            if "services" in self.config and service not in self.config["services"]:
                raise ValueError(f"unknown service {service!r}")
            if ingress not in self.topology.adj and ingress not in self.sites:
                raise ValueError(f"unknown ingress {ingress!r}")
        except (TypeError, ValueError) as exc:
            return {"type": "ERROR", "req_id": req_id, "error": f"invalid_request: {exc}"}

        with self.lock:
            self.offered["_all"] += 1
            self.offered[service] = self.offered.get(service, 0) + 1
            if requested_name:
                try:
                    requested_name = self.registry.validate_name(requested_name)
                except ValueError as exc:
                    return {"type": "ERROR", "req_id": req_id, "error": str(exc)}
                if self.registry.lookup(requested_name):
                    return {"type": "ERROR", "req_id": req_id, "error": "logical_name_in_use"}

            candidates, fail_reasons = placement.evaluate_sites(
                self.sites, self.topology, ingress, demand, max_latency)
            if not candidates:
                reason = placement.pick_reason(fail_reasons)
                self._count_block(service, reason)
                dt = (time.perf_counter() - t0) * 1000
                self.decision_times_ms.append(dt)
                return {"type": "BLOCKED", "req_id": req_id, "reason": reason, "decision_ms": round(dt, 3)}

            chosen = placement.choose(self.policy, candidates)
            sid = chosen["site_id"]
            site = self.sites[sid]
            path = chosen["path"]
            new_sid = util.format_sid(self._next_sid)
            self._next_sid += 1
            logical_name = requested_name or ServiceRegistry.default_name(service, new_sid)
            try:   # reserve node vector + path bandwidth *before* contacting the site
                res_node = self.ledger.reserve_node(new_sid, sid, demand, purpose="PRIMARY")
                res_path = self.ledger.reserve_path(new_sid, path, demand["bw"], purpose="INGRESS")
                self.registry.register(logical_name, new_sid, service, sid, site["host"], site["port"],
                                       state="ALLOCATING", generation=1)
            except (ReservationError, NameConflict, ValueError) as exc:
                self.ledger.release_owner(new_sid)
                self.registry.unregister_instance(new_sid)
                reason = getattr(exc, "reason", "logical_name_in_use" if isinstance(exc, NameConflict) else "error")
                self._count_block(service, reason)
                return {"type": "BLOCKED" if hasattr(exc, "reason") else "ERROR", "req_id": req_id,
                        "reason": reason, "error": str(exc)}
            inst = {
                "sid": new_sid, "service": service, "site": sid, "ingress": ingress, "demand": dict(demand),
                "path": path, "path_latency_ms": chosen["path_latency_ms"], "max_latency_ms": max_latency,
                "holding_s": holding_s, "admitted_at": time.time(), "state": "CREATED",
                "logical_name": logical_name, "generation": 1, "op": None, "res_node": res_node,
                "res_path": res_path, "unavailable_since": time.time(), "unavailable_total_s": 0.0,
                "migration_downtime_s": 0.0, "replica_seq": 0, "missing_count": 0, "failed_at": None,
                "migrations": 0, "failovers": 0}
            self.allocation_table[new_sid] = inst
            self.set_state(inst, "ALLOCATING", "INSTANCE_ALLOCATING", service=service)
            host, port = site["host"], site["port"]

        try:                                           # ---- RPC outside the lock
            reply = protocol.request(host, port, {
                "type": "ALLOCATE", "sid": new_sid, "service": service, "demand": demand,
                "holding_s": holding_s, "logical_name": logical_name, "generation": 1},
                counters=self.counters)
            ok = reply.get("type") == "ALLOCATE_OK"
        except (ConnectionError, OSError, TimeoutError):
            ok = False

        with self.lock:
            if not ok or self.allocation_table.get(new_sid) is not inst:
                # roll back the reservation -- design doc 5.1
                self.ledger.release_owner(new_sid)
                self.registry.unregister_instance(new_sid)
                self.allocation_table.pop(new_sid, None)
                if inst["state"] == "ALLOCATING":
                    self.set_state(inst, "FAILED", "ALLOCATION_FAILED")
                    self.set_state(inst, "TERMINATED", "ALLOCATION_ROLLED_BACK")
                self.store.save_instance({
                    "instance_id": new_sid, "logical_name": logical_name, "service": service, "site_id": sid,
                    "state": "TERMINATED", "demand": demand, "holding_s": holding_s,
                    "created_at": inst["admitted_at"], "generation": 1})
                self._count_block(service, "site_unreachable")
                dt = (time.perf_counter() - t0) * 1000
                self.decision_times_ms.append(dt)
                return {"type": "BLOCKED", "req_id": req_id, "reason": "site_unreachable",
                        "decision_ms": round(dt, 3)}
            self.set_state(inst, "RUNNING", "INSTANCE_ADMITTED", service=service)
            self.reg_state(inst, "RUNNING")
            self.persist(inst)
            self.emit("SERVICE_REGISTERED", "NAMING", new_sid, logical_name, sid, None, generation=1,
                      endpoint=f"{host}:{port}")
            self.admitted["_all"] += 1
            self.admitted[service] = self.admitted.get(service, 0) + 1
            dt = (time.perf_counter() - t0) * 1000
            self.decision_times_ms.append(dt)
            return {"type": "ACCEPTED", "req_id": req_id, "sid": new_sid, "site": sid,
                    "logical_name": logical_name, "generation": 1, "path": path,
                    "path_latency_ms": chosen["path_latency_ms"], "decision_ms": round(dt, 3)}

    # ------------------------------------------------------- NAME RESOLUTION
    def _on_resolve(self, msg):
        started = time.perf_counter()
        if msg.get("type") == "LOOKUP" and "name" not in msg:       # list by service type
            recs = self.registry.by_service(msg["service"])
            return {"type": "LOOKUP_OK", "service": msg["service"], "instances": [
                {k: r[k] for k in ("name", "instance_id", "site_id", "host", "port", "generation")}
                for r in recs]}
        name = msg.get("name")
        try:
            record, reason = self.registry.resolve_detail(name)
        except ValueError as exc:
            return {"type": "RESOLVE_FAIL", "error": str(exc), "reason": "invalid_name"}
        ms = (time.perf_counter() - started) * 1000
        self.resolution_ms.append(ms)
        outcome = "ok" if record else reason
        self.resolve_outcomes[outcome] = self.resolve_outcomes.get(outcome, 0) + 1
        self.emit("NAME_RESOLVED" if record else "NAME_RESOLUTION_FAILED", "RESOLUTION", None, name,
                  record["site_id"] if record else None, None, buffered=True, outcome=outcome,
                  resolution_ms=round(ms, 4), generation=record["generation"] if record else None)
        if record is None:
            return {"type": "RESOLVE_FAIL", "name": name, "reason": reason, "retryable": reason == "migrating",
                    "resolution_ms": round(ms, 3)}
        endpoint = {"host": record["host"], "port": record["port"], "site_id": record["site_id"],
                    "instance_id": record["instance_id"], "generation": record["generation"]}
        return {"type": "RESOLVED", **record, "endpoint": endpoint, "resolution_ms": round(ms, 3)}

    # ------------------------------------------------------------ RESIZE
    def _on_resize(self, msg):
        sid = msg["sid"]
        req_id = msg.get("req_id")
        with self.lock:
            inst = self.find_instance(sid)
            if inst is None or inst["state"] != "RUNNING" or inst["op"]:
                return {"type": "RESIZE_FAIL", "req_id": req_id, "reason": "unknown_or_terminated"}
            sid = inst["sid"]
            factor = float(msg["factor"])
            if not math.isfinite(factor) or factor <= 0:
                return {"type": "RESIZE_FAIL", "req_id": req_id, "reason": "invalid_factor"}
            old_cpu = inst["demand"]["cpu"]
            new_cpu = max(1, round(old_cpu * factor))
            reps = list(self.cp.replication.replicas_of(sid))
            adjusted = []          # (res_id, previous_demand) for rollback
            try:                   # reserve growth on the primary AND on every standby, atomically
                for res_id in [inst["res_node"]] + [r["res_node"] for r in reps]:
                    prev = dict(self.ledger.get(res_id)["demand"])
                    self.ledger.adjust_node(res_id, {"cpu": new_cpu})
                    adjusted.append((res_id, prev))
            except ReservationError:
                for res_id, prev in adjusted:
                    self.ledger.adjust_node(res_id, prev)
                return {"type": "RESIZE_FAIL", "req_id": req_id, "reason": "node_capacity"}
            inst["op"] = f"resize-{sid}-{int(time.time() * 1000)}"
            self.set_state(inst, "RESIZING", "RESIZE_STARTED", cpu=new_cpu, previous_cpu=old_cpu)
            site_id = inst["site"]
            rep_targets = [(r["replica_id"], r["site"]) for r in reps if r["state"] == "STANDBY"]
        try:
            reply = self.rpc(site_id, {"type": "RESIZE", "sid": sid, "cpu": new_cpu})
            ok = reply.get("type") == "RESIZE_OK"
        except SiteUnreachable:
            ok = False
        bad_reps = []
        if ok:
            for rid, rsite in rep_targets:
                try:
                    r = self.rpc(rsite, {"type": "RESIZE", "sid": rid, "cpu": new_cpu})
                    if r.get("type") != "RESIZE_OK":
                        bad_reps.append(rid)
                except SiteUnreachable:
                    bad_reps.append(rid)
        with self.lock:
            inst["op"] = None
            if not ok:
                for res_id, prev in adjusted:
                    self.ledger.adjust_node(res_id, prev)
                if inst["state"] == "RESIZING":
                    self.set_state(inst, "RUNNING", "RESIZE_FAILED")
                return {"type": "RESIZE_FAIL", "req_id": req_id, "reason": "site_unreachable"}
            for rid in bad_reps:
                if rid in self.replicas:
                    self.replicas[rid]["state"] = "UNAVAILABLE"      # out of date: not promotable
            inst["demand"]["cpu"] = new_cpu
            for r in reps:
                r["demand"]["cpu"] = new_cpu
            if inst["state"] == "RESIZING":
                self.set_state(inst, "RUNNING", "RESIZE_COMPLETED", cpu=new_cpu)
            self.persist(inst)
            return {"type": "RESIZE_OK", "req_id": req_id, "sid": sid, "cpu": new_cpu}

    # ------------------------------------------------------- INSTANCE_EXIT
    def _on_instance_exit(self, msg):
        with self.lock:
            sid = msg["sid"]
            inst = self.allocation_table.get(sid)
            if inst is None:
                return {"type": "INSTANCE_EXIT_OK"}
            from_site, gen = msg.get("site_id"), msg.get("generation")
            if (from_site and from_site != inst["site"]) or (gen is not None and int(gen) != inst["generation"]):
                self.emit("STALE_EXIT_IGNORED", "LIFECYCLE", sid, inst["logical_name"], from_site, None,
                          generation=gen, current_generation=inst["generation"], current_site=inst["site"])
                return {"type": "INSTANCE_EXIT_OK"}
            self.finalize_termination(inst, summary=msg.get("summary"), reason="exit")
        return {"type": "INSTANCE_EXIT_OK"}

    def finalize_termination(self, inst, summary=None, reason="exit", lost=False):
        """Release every reservation, retire the name, drop the standbys (lock held)."""
        sid = inst["sid"]
        if sid not in self.allocation_table:
            return
        self.ledger.release_owner(sid)               # node + path + any operation-held reservations
        for rep in list(self.cp.replication.replicas_of(sid)):
            self.queue_replica_removal(rep["site"], rep["replica_id"])
            self.cp.replication.drop_replica(rep["replica_id"], f"primary_terminated:{reason}")
        if inst["state"] not in ("DRAINING", "FAILED", "RECOVERING", "TERMINATED"):
            self.set_state(inst, "DRAINING", "INSTANCE_DRAINING", reason=reason)
        self.set_state(inst, "TERMINATED", "INSTANCE_TERMINATED", reason=reason, lost=lost)
        end = time.time()
        if not lost:
            inst["unavailable_since"] = None         # an orderly end is not an outage
        lifetime = end - inst["admitted_at"]
        unavail = inst["unavailable_total_s"] + ((end - inst["unavailable_since"])
                                                 if inst["unavailable_since"] is not None else 0.0)
        name = inst["logical_name"]
        self.registry.unregister_instance(sid)
        self.store.save_instance({
            "instance_id": sid, "logical_name": name, "service": inst["service"], "site_id": inst["site"],
            "state": "TERMINATED", "demand": inst["demand"], "holding_s": inst["holding_s"],
            "created_at": inst["admitted_at"], "generation": inst["generation"],
            "metadata": {"reason": reason, "lost": lost}})
        self.store.delete_service(name)
        summary = dict(summary or {})
        summary.update({"sid": sid, "service": inst["service"], "site": inst["site"],
                        "logical_name": name, "generation": inst["generation"], "reason": reason,
                        "lost": lost, "lifetime_s": round(lifetime, 4), "unavailable_s": round(unavail, 4),
                        "migrations": inst.get("migrations", 0), "failovers": inst.get("failovers", 0),
                        "migration_downtime_s": round(inst.get("migration_downtime_s", 0.0), 4)})
        self.completed_instances.append(summary)
        del self.allocation_table[sid]

    # ----------------------------------------------------------- TERMINATE
    def _on_terminate(self, msg):
        with self.lock:
            inst = self.find_instance(msg.get("sid") or msg.get("name"))
            if inst is None:
                return {"type": "TERMINATE_FAIL", "reason": "unknown_service"}
            if inst["op"]:
                return {"type": "TERMINATE_FAIL", "reason": "operation_in_progress", "op": inst["op"]}
            sid, site_id = inst["sid"], inst["site"]
            if inst["state"] in ("RUNNING", "RESIZING", "STANDBY"):
                self.set_state(inst, "DRAINING", "TERMINATE_REQUESTED")
            self.reg_state(inst, "DRAINING")
        forced = False
        try:
            self.rpc(site_id, {"type": "RELEASE", "sid": sid}, timeout=5.0)
        except SiteUnreachable:
            forced = True
        deadline = time.time() + 3.0
        while not forced and time.time() < deadline:   # the site reports INSTANCE_EXIT when drained
            with self.lock:
                if sid not in self.allocation_table:
                    return {"type": "TERMINATE_OK", "sid": sid, "forced": False}
            time.sleep(0.05)
        with self.lock:
            inst = self.allocation_table.get(sid)
            if inst is not None:
                self.finalize_termination(inst, reason="forced_termination" if forced else "drain_timeout")
        return {"type": "TERMINATE_OK", "sid": sid, "forced": True}

    # ------------------------------------------------------------ STATUS
    def _status(self):
        with self.lock:
            return {
                "type": "STATUS_OK",
                "policy": self.policy,
                "sites": {sid: {
                    "tier": s["tier"], "status": s["status"], "capacity": s["capacity"],
                    "allocated": dict(s["allocated"]), "last_fairness": s["last_fairness"],
                    "last_slot_util": s["last_slot_util"],
                } for sid, s in self.sites.items()},
                "running_instances": len(self.allocation_table),
                "replicas": len(self.replicas),
                "service_registry": self.registry.snapshot(),
                "lifecycle_events": len(self.lifecycle_events),
                "offered": dict(self.offered),
                "blocked": dict(self.blocked),
                "admitted": dict(self.admitted),
                "link_utilisation": self.topology.link_utilisation(),
                "ledger_problems": self.ledger.verify(),
                "counters": self.counters.snapshot(),
            }

    # -------------------------------------------------------- background
    def _sampler_loop(self):
        while not self._stop.is_set():
            time.sleep(self.heartbeat_interval)
            with self.lock:
                standby = {"cpu": 0.0, "mem": 0.0, "bw": 0.0}
                for r in self.replicas.values():
                    for k in standby:
                        standby[k] += r["demand"].get(k, 0)
                self.utilisation_samples.append({
                    "ts": time.time(),
                    "sites": {sid: {
                        "cpu_util": s["allocated"]["cpu"] / s["capacity"]["cpu"] if s["capacity"]["cpu"] else 0,
                        "mem_util": s["allocated"]["mem"] / s["capacity"]["mem"] if s["capacity"]["mem"] else 0,
                        "bw_util": s["allocated"]["bw"] / s["capacity"]["bw"] if s["capacity"]["bw"] else 0,
                        "status": s["status"],
                    } for sid, s in self.sites.items()},
                    "links": self.topology.link_utilisation(),
                    "instances": len(self.allocation_table), "replicas": len(self.replicas),
                    "standby_reserved": standby,
                })
            self.store.flush()

    def _failure_detector_loop(self):
        tick = max(0.05, self.heartbeat_interval / 2)
        while not self._stop.is_set():
            time.sleep(tick)
            now = time.time()
            with self.lock:
                for sid, s in list(self.sites.items()):
                    age = now - s["last_heartbeat"]
                    target = self.detector.classify(age)
                    if target == "ACTIVE":
                        continue
                    if s["status"] in ("ACTIVE", "UNKNOWN", "RECOVERING"):
                        s["status"] = "SUSPECT"
                        self._site_event(sid, "SITE_SUSPECT", heartbeat_age_s=round(age, 3))
                        self.store.mark_site(sid, "SUSPECT")
                        self.cp.on_site_suspect(sid)
                        print(f"[manager] {sid} -> SUSPECT (no heartbeat for {age:.1f}s)", flush=True)
                    if target == "FAILED" and s["status"] == "SUSPECT":
                        s["status"] = "FAILED"
                        self._site_event(sid, "SITE_FAILED", heartbeat_age_s=round(age, 3))
                        self.store.mark_site(sid, "FAILED")
                        print(f"[manager] {sid} -> FAILED (no heartbeat for {age:.1f}s)", flush=True)
                        self.cp.on_site_failed(sid)
                self.cp.scan()

    # ------------------------------------------------------------ report
    def _write_report(self):
        reporting.write_reports(self)
        print(f"[manager] report written to {self.results_dir}", flush=True)


def main():
    ap = argparse.ArgumentParser(description="TeleRM Global Resource Manager")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--results-dir", default="results/latest")
    ap.add_argument("--policy", default=None, choices=placement.POLICIES)
    ap.add_argument("--db", default=None, help="SQLite control-plane database (default: <results-dir>/telerm_control.sqlite3)")
    args = ap.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    mgr = Manager(config, args.results_dir, policy=args.policy, db_path=args.db)
    mgr.start()
    try:
        while not mgr._stop.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        mgr._write_report()
        mgr.stop()
        sys.exit(0)


if __name__ == "__main__":
    main()
