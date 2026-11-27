"""Control plane: wires durable storage, migration, replication, failure
handling and reconciliation to the manager.

Threads owned here (fixed, never spawned per request):
  * ``control-worker``  executes queued recovery tasks (failover, replica
    clean-up, resuming a migration after a manager restart) one at a time;
  * ``replication-sync`` periodic standby synchronisation.
The manager's own sampler and failure-detector threads call into
``scan``/``on_site_*``; request-driven operations (MIGRATE, REPLICATE ...) run
on the connection thread that received them.

Everything that touches shared state takes ``manager.lock``; every RPC runs
with the lock released.
"""
from __future__ import annotations

import queue
import threading
import time

from common.migration import MigrationCoordinator, MigrationError
from common.replication import ReplicationManager, ReplicationError


class ControlPlane:
    def __init__(self, manager):
        self.m = manager
        self.migration = MigrationCoordinator(manager)
        self.replication = ReplicationManager(manager)
        self.tasks = queue.Queue()
        self.pending = set()
        self.recovering_ops = []
        self._threads = []
        self._stop = threading.Event()

    # ------------------------------------------------------------ threads
    def start(self):
        for target, name in ((self._worker_loop, "control-worker"), (self._sync_loop, "replication-sync")):
            t = threading.Thread(target=target, daemon=True, name=name)
            self._threads.append(t)
            t.start()

    def stop(self):
        self._stop.set()

    def enqueue(self, kind, arg):
        key = (kind, str(arg))
        with self.m.lock:
            if key in self.pending:
                return False
            self.pending.add(key)
        self.tasks.put((kind, arg))
        return True

    def kick(self):
        self.enqueue("SCAN", "now")

    def _worker_loop(self):
        while not self._stop.is_set():
            try:
                kind, arg = self.tasks.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                if kind == "FAILOVER":
                    try:
                        self.replication.failover(arg, reason="site_failure")
                    except ReplicationError as exc:
                        if exc.reason not in ("no_eligible_replica", "primary_healthy"):
                            self.m.emit("FAILOVER_ERROR", "RECOVERY", arg, None, None, None, reason=str(exc))
                elif kind == "REPLICA_REMOVE":
                    site, rid = arg
                    try:
                        self.m.rpc(site, {"type": "REPLICA_REMOVE", "replica_id": rid}, timeout=2.0,
                                   allow_inactive=True)
                    except (ConnectionError, OSError, TimeoutError):
                        pass                     # unreachable: its next reconcile releases the orphan
                elif kind == "RESUME_MIGRATION":
                    if self.migration.resume_pending(arg):
                        self.recovering_ops = [o for o in self.recovering_ops
                                               if o["operation_id"] != arg["operation_id"]]
                elif kind == "SCAN":
                    self.scan()
            except Exception as exc:  # noqa: BLE001
                print(f"[control-plane] task {kind} failed: {exc!r}", flush=True)
            finally:
                with self.m.lock:
                    key_arg = arg["operation_id"] if kind == "RESUME_MIGRATION" else arg
                    self.pending.discard((kind, str(key_arg)))

    def _sync_loop(self):
        while not self._stop.is_set():
            time.sleep(self.replication.sync_interval_s)
            try:
                self.replication.sync_tick()
            except Exception as exc:  # noqa: BLE001
                print(f"[control-plane] sync tick failed: {exc!r}", flush=True)

    # ------------------------------------------------------------- restore
    def restore(self):
        """Rebuild control-plane state from SQLite after a manager restart.
        Nothing is restored as available: every service is RECOVERING /
        UNAVAILABLE until its site's inventory has been reconciled."""
        m = self.m
        now = time.time()
        with m.lock:
            for row in m.store.list_sites():
                m.sites[row["site_id"]] = {
                    "site_id": row["site_id"], "host": row["host"], "port": row["port"], "tier": row["tier"],
                    "capacity": dict(row["capacity"]), "allocated": m.ledger.alloc_for(row["site_id"]),
                    "workers": row["workers"], "status": "UNKNOWN", "last_heartbeat": now,
                    "boot_id": row["boot_id"], "last_fairness": None, "last_slot_util": None,
                    "last_inventory": None}
            m.ledger.restore(m.store.held_reservations())
            max_sid = 0
            for p in m.store.list_processes():
                try:
                    max_sid = max(max_sid, int(p["instance_id"].lstrip("S")))
                except ValueError:
                    pass
                if p["state"] == "TERMINATED":
                    continue
                md = p.get("metadata") or {}
                m.allocation_table[p["instance_id"]] = {
                    "sid": p["instance_id"], "service": p["service"], "site": p["site_id"],
                    "ingress": md.get("ingress"), "demand": p["demand"], "path": md.get("path") or [],
                    "path_latency_ms": md.get("path_latency_ms", 0), "max_latency_ms": md.get("max_latency_ms"),
                    "holding_s": p["holding_s"], "admitted_at": p["created_at"], "state": "RECOVERING",
                    "logical_name": p["logical_name"], "generation": p["generation"], "op": None,
                    "res_node": md.get("res_node"), "res_path": md.get("res_path"),
                    "unavailable_since": now, "unavailable_total_s": md.get("unavailable_total_s", 0.0),
                    "migration_downtime_s": md.get("migration_downtime_s", 0.0),
                    "replica_seq": md.get("replica_seq", 0), "missing_count": 0, "failed_at": None,
                    "migrations": md.get("migrations", 0), "failovers": md.get("failovers", 0)}
            m._next_sid = max_sid + 1
            m.registry.restore([s for s in m.store.list_services() if s["instance_id"] in m.allocation_table])
            for r in m.store.list_replicas():
                md = r.get("metadata") or {}
                m.replicas[r["replica_id"]] = {
                    "replica_id": r["replica_id"], "primary_sid": r["primary_id"], "site": r["site_id"],
                    "service": (m.allocation_table.get(r["primary_id"]) or {}).get("service"),
                    "demand": r["demand"], "state": "UNAVAILABLE", "generation": r["generation"],
                    "res_node": md.get("res_node"), "res_sync": md.get("res_sync"),
                    "sync_path": md.get("sync_path"), "created_at": r["created_at"],
                    "last_sync_ts": r["last_sync"], "sync_seq": r["sync_seq"], "sync_msgs": 0, "sync_bytes": 0,
                    "sync_failures": 0, "missing_count": 0, "mode": md.get("mode", "STANDBY"), "op": None}
            restored_ops = 0
            for op in m.store.incomplete_operations():
                restored_ops += 1
                oid, sid, typ = op["operation_id"], op["instance_id"], op["operation_type"]
                if typ == "MIGRATION" and op["state"] == "COMMIT_DECIDED" and sid in m.allocation_table:
                    m.allocation_table[sid]["op"] = oid          # roll forward once the destination is back
                    self.recovering_ops.append(op)
                    continue
                m.ledger.release_op(oid)                          # everything else rolls back
                m.store.update_operation(oid, "ABORTED_ON_RESTART")
                if typ == "REPLICATION":
                    rid = (op["payload"] or {}).get("replica_id")
                    if rid in m.replicas and m.replicas[rid]["state"] == "CREATING":
                        self.replication.drop_replica(rid, "manager_restart_during_creation")
            for rid in [r for r, v in m.replicas.items() if v["state"] == "CREATING"]:
                self.replication.drop_replica(rid, "manager_restart_during_creation")
            m.emit("MANAGER_RESTORED", "RECOVERY", None, None, None, None,
                   processes=len(m.allocation_table), replicas=len(m.replicas), sites=len(m.sites),
                   incomplete_operations=restored_ops, services_unavailable_until_reconciled=True)
            return {"processes": len(m.allocation_table), "replicas": len(m.replicas)}

    # ------------------------------------------------- failure transitions
    def _fail_instance(self, inst, reason):
        m = self.m
        if inst["state"] != "FAILED":
            m.set_state(inst, "FAILED", "INSTANCE_FAILED", reason=reason)
            inst["failed_at"] = time.time()
        m.reg_state(inst, "UNAVAILABLE")
        m.persist(inst)

    def on_site_suspect(self, site_id):
        """Lock held. SUSPECT: may be only slow -> hide services, tear nothing down."""
        m = self.m
        for sid, inst in m.allocation_table.items():
            if inst["site"] == site_id and not inst["op"] and inst["state"] in ("RUNNING", "RESIZING"):
                m.reg_state(inst, "UNAVAILABLE", since=m.sites[site_id]["last_heartbeat"])
                m.emit("HOST_SITE_SUSPECT", "FAILURE", sid, inst["logical_name"], site_id, None)
                m.lifecycle_events.append({"ts": time.time(), "instance_id": sid, "event": "HOST_SITE_SUSPECT",
                                           "state": inst["state"], "site": site_id})

    def on_site_failed(self, site_id):
        """Lock held. FAILED is confirmed: fail hosted services, mark replicas, start failover."""
        m = self.m
        for rep in m.replicas.values():
            if rep["site"] == site_id and rep["state"] == "STANDBY":
                rep["state"] = "UNAVAILABLE"
                m.emit("REPLICA_UNAVAILABLE", "FAILURE", rep["primary_sid"], None, site_id, None,
                       replica_id=rep["replica_id"])
        self.scan()

    def scan(self):
        """Idempotent failure scan: fail instances on FAILED sites, trigger failover, expire TTLs."""
        m = self.m
        now = time.time()
        with m.lock:
            for op in list(self.recovering_ops):
                dst = m.sites.get(op["target_site"])
                inst = m.allocation_table.get(op["instance_id"])
                if dst is not None and dst["status"] == "ACTIVE":
                    self._enqueue_resume(op)
                elif dst is not None and dst["status"] == "FAILED":
                    if inst is not None:
                        inst["op"] = None
                    m.store.update_operation(op["operation_id"], "FAILED", {"reason": "destination_failed"})
                    self.recovering_ops.remove(op)
            for sid, inst in list(m.allocation_table.items()):
                if inst["op"] or inst["state"] in ("ALLOCATING", "DRAINING"):
                    continue
                site = m.sites.get(inst["site"])
                site_failed = site is not None and site["status"] == "FAILED"
                if site_failed and inst["state"] != "FAILED":
                    self._fail_instance(inst, "host_site_failed")
                if inst["state"] != "FAILED":
                    continue
                if site is not None and site["status"] == "ACTIVE":
                    continue                                   # site is back: reconciliation decides
                if self.replication.has_candidate(inst):
                    self.enqueue("FAILOVER", sid)
                elif site_failed and now - (inst["failed_at"] or now) > m.fcfg.failed_instance_ttl_s:
                    self.declare_lost(inst, "failed_instance_ttl_expired")
            for rep in m.replicas.values():
                s = m.sites.get(rep["site"])
                if s is not None and s["status"] == "FAILED" and rep["state"] == "STANDBY":
                    rep["state"] = "UNAVAILABLE"

    def _enqueue_resume(self, op):
        key = ("RESUME_MIGRATION", op["operation_id"])
        if key in self.pending:
            return
        self.pending.add(key)
        self.tasks.put(("RESUME_MIGRATION", op))

    # ---------------------------------------------------------- lost / loss
    def declare_lost(self, inst, reason):
        """Lock held. The instance's state is gone. Fail over if a replica can take over,
        otherwise terminate the record and free every reservation."""
        m = self.m
        if inst["op"]:
            return
        self._fail_instance(inst, reason)
        if self.replication.has_candidate(inst):
            m.emit("INSTANCE_LOST_FAILOVER_PENDING", "FAILURE", inst["sid"], inst["logical_name"],
                   inst["site"], None, reason=reason)
            self.enqueue("FAILOVER", inst["sid"])
            return
        m.emit("INSTANCE_LOST", "FAILURE", inst["sid"], inst["logical_name"], inst["site"], None, reason=reason)
        m.finalize_termination(inst, reason=f"lost:{reason}", lost=True)

    # ------------------------------------------------------ reconciliation
    def apply_plan(self, site_id, plan, restarted):
        """Lock held. Apply the bookkeeping part of a reconciliation plan."""
        m = self.m
        for sid in plan["restore"]:
            inst = m.allocation_table[sid]
            inst["missing_count"] = 0
            changed = inst["state"] in ("FAILED", "RECOVERING")
            if inst["state"] == "FAILED":
                m.set_state(inst, "RECOVERING", "INSTANCE_RECOVERING")
            if inst["state"] == "RECOVERING":
                m.set_state(inst, "RUNNING", "INSTANCE_RECONCILED")
            if inst["state"] in ("RUNNING", "RESIZING") and m.registry.lookup(inst["logical_name"])["state"] \
                    not in ("RUNNING", "RESIZING"):
                outage = m.reg_state(inst, "RUNNING")
                m.persist(inst)
                if changed or outage:
                    m.emit("SERVICE_RECOVERED", "RECOVERY", sid, inst["logical_name"], site_id, None,
                           via="site_reconciliation", recovery_time_s=round(outage or 0.0, 4))
        for sid in plan["resume"]:
            inst = m.allocation_table[sid]
            m.set_state(inst, "RUNNING", "MIGRATION_ROLLED_BACK_ON_RESTART")
            m.reg_state(inst, "RUNNING")
            m.persist(inst)
        for sid in plan["missing"]:
            inst = m.allocation_table[sid]
            inst["missing_count"] = inst.get("missing_count", 0) + 1
            if inst["state"] in ("RUNNING", "RESIZING"):
                m.reg_state(inst, "UNAVAILABLE")
        for sid in plan["lost"]:
            inst = m.allocation_table.get(sid)
            if inst is not None:
                self.declare_lost(inst, "site_restarted_without_instance" if restarted
                                  else "instance_missing_from_inventory")
        for rid in plan["replica_present"]:
            rep = m.replicas[rid]
            rep["missing_count"] = 0
            if rep["state"] == "UNAVAILABLE":
                rep["state"] = "STANDBY"
                self.replication._persist(rep)
                m.emit("REPLICA_RECONCILED", "RECOVERY", rep["primary_sid"], None, site_id, None, replica_id=rid)
        for rid in plan["replica_missing"]:
            m.replicas[rid]["missing_count"] = m.replicas[rid].get("missing_count", 0) + 1
            if m.replicas[rid]["state"] == "STANDBY":
                m.replicas[rid]["state"] = "UNAVAILABLE"
        for rid in plan["replica_lost"]:
            if rid in m.replicas:
                m.emit("REPLICA_LOST", "FAILURE", m.replicas[rid]["primary_sid"], None, site_id, None,
                       replica_id=rid)
                self.replication.drop_replica(rid, "replica_missing_from_inventory")

    # ------------------------------------------------------- admin messages
    def handle(self, msg):
        t = msg["type"]
        ref = msg.get("sid") or msg.get("name")
        try:
            if t == "MIGRATE":
                return {"type": "MIGRATE_OK", **self.migration.migrate(ref, msg["target_site"],
                                                                         msg.get("reason", "requested"))}
            if t == "REPLICATE":
                return {"type": "REPLICATE_OK", **self.replication.create_replica(
                    ref, msg["target_site"], msg.get("mode", "STANDBY"))}
            if t == "REPLICA_REMOVE":
                return {"type": "REPLICA_REMOVE_OK", **self.replication.remove_replica(msg["replica_id"])}
            if t == "REPLICA_RECONFIGURE":
                return {"type": "REPLICA_RECONFIGURE_OK",
                        **self.replication.reconfigure(msg["replica_id"], msg["target_site"])}
            if t == "FAILOVER":
                return {"type": "FAILOVER_OK", **self.replication.failover(
                    ref, reason=msg.get("reason", "requested"), replica_id=msg.get("replica_id"),
                    force=bool(msg.get("force")))}
        except (MigrationError, ReplicationError) as exc:
            return {"type": t.split("_")[0] + "_FAIL" if t != "REPLICA_REMOVE" else "REPLICA_FAIL",
                    "reason": exc.reason, "detail": str(exc)}
        if t == "ABANDON_SITE":
            return self._abandon_site(msg["site_id"])
        if t == "SITE_RECOVER":
            with self.m.lock:
                site = self.m.sites.get(msg["site_id"])
                if site is None:
                    return {"type": "SITE_RECOVER_FAIL", "reason": "unknown_site"}
                site["recovery_approved"] = bool(msg.get("approve", True))
            return {"type": "SITE_RECOVER_OK", "site_id": msg["site_id"],
                    "note": "reconciliation runs on the site's next heartbeat"}
        if t == "CONTROL_STATUS":
            return self.status()
        return {"type": "ERROR", "error": f"unhandled control message {t}"}

    def _abandon_site(self, site_id):
        """Controlled recovery: operator declares a FAILED site's state gone."""
        m = self.m
        with m.lock:
            site = m.sites.get(site_id)
            if site is None or site["status"] != "FAILED":
                return {"type": "ABANDON_SITE_FAIL", "reason": "site_not_failed"}
            n = 0
            for inst in list(m.allocation_table.values()):
                if inst["site"] == site_id and not inst["op"]:
                    self.declare_lost(inst, "site_abandoned_by_operator")
                    n += 1
            for rid in [r for r, v in m.replicas.items() if v["site"] == site_id]:
                self.replication.drop_replica(rid, "site_abandoned_by_operator")
            m.emit("SITE_ABANDONED", "RECOVERY", None, None, site_id, None, instances=n)
        return {"type": "ABANDON_SITE_OK", "site_id": site_id, "instances_handled": n}

    def status(self):
        m = self.m
        with m.lock:
            return {
                "type": "CONTROL_STATUS_OK",
                "sites": {s: {"status": v["status"], "boot_id": v.get("boot_id")} for s, v in m.sites.items()},
                "services": [{"sid": i["sid"], "name": i["logical_name"], "site": i["site"],
                              "state": i["state"], "generation": i["generation"], "op": i["op"]}
                             for i in m.allocation_table.values()],
                "replicas": [{k: r[k] for k in ("replica_id", "primary_sid", "site", "state", "sync_seq",
                                                "last_sync_ts", "sync_bytes")} for r in m.replicas.values()],
                "ledger_problems": m.ledger.verify(),
                "migration": dict(self.migration.stats), "replication": dict(self.replication.stats),
            }
