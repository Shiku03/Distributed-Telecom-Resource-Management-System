"""Manager-side transparent service migration (transactional, fenced).

Protocol (every step is recorded in the ``operations`` table):

  PREPARING            validate; reserve destination node resources and the
                       new ingress path under the operation id; mark the
                       service MIGRATING (briefly unresolvable).
  SOURCE_FENCED        source froze the instance (no new jobs / service
                       calls), in-flight jobs drained, snapshot taken.
  DESTINATION_STAGED   destination holds the snapshot, fenced (invisible to
                       the scheduler and to service calls).
  COMMIT_DECIDED       *the commit point*: in one lock hold and one SQLite
                       transaction the manager switches the allocation record,
                       the logical endpoint (+ new generation), the resource
                       ledger and the process row to the destination.
  COMMITTED            destination activated, source released.

Failure handling
  before COMMIT_DECIDED  roll back: thaw the source, discard the staged copy,
                         release every reservation held under the operation id.
  after  COMMIT_DECIDED  roll forward only: retry destination activation; if
                         the destination cannot activate it is treated as a
                         failed site hosting the service (replica promotion or
                         controlled loss), never as "move back".  The source
                         stays fenced, so two copies never run.

Duplicate execution is impossible because the source is frozen before the
destination is staged and the destination is activated only after the commit
decision; stale endpoints are rejected by generation on the data path.
"""
from __future__ import annotations

import time
import uuid

from common.ledger import ReservationError


class MigrationError(RuntimeError):
    def __init__(self, reason, detail=""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason


class MigrationCoordinator:
    def __init__(self, manager):
        self.m = manager
        mc = manager.config.get("migration", {}) or {}
        self.quiesce_timeout_s = float(mc.get("quiesce_timeout_s", 2.0))
        self.rpc_timeout_s = float(mc.get("rpc_timeout_s", 5.0))
        self.commit_retries = int(mc.get("commit_retries", 3))
        self.stats = {"started": 0, "committed": 0, "aborted": 0, "failed_forward": 0}

    # ---------------------------------------------------------------- API
    def migrate(self, ref, target_site, reason="requested"):
        m = self.m
        t0 = time.time()
        # ---------------------------------------------------- 1. PREPARING
        with m.lock:
            inst = m.find_instance(ref)
            if inst is None:
                raise MigrationError("unknown_service", str(ref))
            sid = inst["sid"]
            if inst["op"]:
                raise MigrationError("operation_in_progress", inst["op"])
            if inst["state"] != "RUNNING":
                raise MigrationError("service_not_running", inst["state"])
            src = inst["site"]
            if src == target_site:
                raise MigrationError("same_site")
            ssite, tsite = m.sites.get(src), m.sites.get(target_site)
            if ssite is None or ssite["status"] != "ACTIVE":
                raise MigrationError("source_not_active", src)
            if tsite is None or tsite["status"] != "ACTIVE":
                raise MigrationError("destination_not_active", target_site)
            if any(r["site"] == target_site and r["primary_sid"] == sid for r in m.replicas.values()):
                raise MigrationError("destination_hosts_replica", target_site)
            demand = dict(inst["demand"])
            spath, slat = m.topology.shortest_path(inst["ingress"], target_site)
            if spath is None or (inst["max_latency_ms"] is not None and slat > inst["max_latency_ms"]):
                raise MigrationError("latency_constraint", f"{slat}ms > {inst['max_latency_ms']}ms")
            new_path, new_lat = m.topology.best_feasible_path(
                inst["ingress"], target_site, demand["bw"], inst["max_latency_ms"])
            if new_path is None:
                raise MigrationError("link_bandwidth", f"no feasible path {inst['ingress']}->{target_site}")
            op_id = "mig-" + uuid.uuid4().hex[:12]
            new_gen = inst["generation"] + 1
            try:
                m.ledger.reserve_node(sid, target_site, demand, op_id=op_id, purpose="MIGRATION_TARGET")
                m.ledger.reserve_path(sid, new_path, demand["bw"], op_id=op_id, purpose="MIGRATION_PATH")
            except ReservationError as exc:
                m.ledger.release_op(op_id)
                raise MigrationError(exc.reason, str(exc))
            inst["op"] = op_id
            m.set_state(inst, "MIGRATING", "MIGRATION_STARTED", operation_id=op_id,
                        source=src, destination=target_site, new_generation=new_gen, reason=reason)
            m.reg_state(inst, "MIGRATING")
            m.store.begin_operation({
                "operation_id": op_id, "operation_type": "MIGRATION", "state": "PREPARING",
                "instance_id": sid, "source_site": src, "target_site": target_site,
                "payload": {"logical_name": inst["logical_name"], "old_generation": inst["generation"],
                            "new_generation": new_gen, "old_path": inst["path"], "new_path": new_path,
                            "demand": demand, "started_at": t0, "reason": reason}})
            m.emit("MIGRATION_STARTED", "MIGRATION", sid, inst["logical_name"], src, op_id,
                   source=src, destination=target_site, path=new_path, generation=new_gen)
            self.stats["started"] += 1
            old_gen = inst["generation"]
            frozen_at = time.time()
            name = inst["logical_name"]

        # -------------------------------------- 2. freeze, snapshot, stage
        stage = "PREPARE"
        try:
            prep = m.rpc(src, {"type": "MIGRATION_PREPARE", "sid": sid, "operation_id": op_id,
                               "generation": old_gen, "quiesce_timeout_s": self.quiesce_timeout_s},
                         timeout=self.rpc_timeout_s + self.quiesce_timeout_s)
            if prep.get("type") != "MIGRATION_SNAPSHOT":
                raise MigrationError("source_prepare_failed", str(prep.get("error")))
            m.store.update_operation(op_id, "SOURCE_FENCED")
            stage = "IMPORT"
            imp = m.rpc(target_site, {"type": "MIGRATION_IMPORT", "sid": sid, "operation_id": op_id,
                                      "snapshot": prep["snapshot"], "generation": new_gen},
                        timeout=self.rpc_timeout_s)
            if imp.get("type") != "MIGRATION_IMPORT_OK":
                raise MigrationError("destination_import_failed", str(imp.get("error")))
            m.store.update_operation(op_id, "DESTINATION_STAGED")
        except (MigrationError, ConnectionError, OSError, TimeoutError) as exc:
            return self._rollback(sid, op_id, src, target_site, str(exc), stage, t0)

        # ------------------------------------------- 3. the commit decision
        with m.lock:
            inst = m.allocation_table.get(sid)
            problem = None
            if inst is None or inst["op"] != op_id:
                problem = "service_terminated_during_migration"
            elif m.sites[target_site]["status"] != "ACTIVE":
                problem = "destination_lost_before_commit"
            elif m.sites[src]["status"] != "ACTIVE":
                problem = "source_lost_before_commit"
            if problem is None:
                tsite = m.sites[target_site]
                old_node = inst["res_node"]
                old_path_res = inst["res_path"]
                new_node = m.ledger.held(owner=sid, op_id=op_id, kind="NODE")[0]["res_id"]
                new_path_res = m.ledger.held(owner=sid, op_id=op_id, kind="PATH")[0]["res_id"]
                with m.store.transaction():
                    # one lock hold + one SQLite transaction: ledger, record, endpoint, op state
                    m.ledger.rebind(new_node, purpose="PRIMARY", op_id=None)
                    m.ledger.rebind(new_path_res, purpose="INGRESS", op_id=None)
                    m.ledger.release(old_node)
                    m.ledger.release(old_path_res)
                    inst["res_node"], inst["res_path"] = new_node, new_path_res
                    inst["site"], inst["path"] = target_site, new_path
                    inst["path_latency_ms"] = self._latency(new_path)
                    inst["generation"] = new_gen
                    m.registry.update_location(sid, target_site, tsite["host"], tsite["port"],
                                               state="MIGRATING", generation=new_gen)
                    m.persist(inst)
                    m.store.update_operation(op_id, "COMMIT_DECIDED", {"committed_at": time.time()})
                m.emit("MIGRATION_COMMIT_DECIDED", "MIGRATION", sid, name, target_site, op_id,
                       generation=new_gen, source=src)
        if problem:
            return self._rollback(sid, op_id, src, target_site, problem, "COMMIT", t0)

        # ------------------------------ 4. roll forward: activate, release
        activated = False
        last_err = None
        for attempt in range(self.commit_retries):
            try:
                r = m.rpc(target_site, {"type": "MIGRATION_COMMIT", "sid": sid, "operation_id": op_id,
                                        "generation": new_gen}, timeout=self.rpc_timeout_s)
                if r.get("type") == "MIGRATION_COMMIT_OK":
                    activated = True
                    break
                last_err = str(r.get("error"))
            except (ConnectionError, OSError, TimeoutError) as exc:
                last_err = str(exc)
            time.sleep(0.1 * (attempt + 1))
        if not activated:
            return self._fail_forward(sid, op_id, src, target_site, last_err, t0)

        with m.lock:
            inst = m.allocation_table.get(sid)
            downtime = None
            if inst is not None and inst["op"] == op_id:
                inst["op"] = None
                m.set_state(inst, "RUNNING", "MIGRATION_ACTIVATED", operation_id=op_id,
                            destination=target_site, generation=new_gen)
                downtime = m.reg_state(inst, "RUNNING")
                inst["migration_downtime_s"] = inst.get("migration_downtime_s", 0.0) + (downtime or 0.0)
                inst["migrations"] = inst.get("migrations", 0) + 1
                m.persist(inst)
        source_released = True
        try:
            rr = m.rpc(src, {"type": "MIGRATION_SOURCE_RELEASE", "sid": sid, "operation_id": op_id,
                             "generation": new_gen}, timeout=self.rpc_timeout_s, allow_inactive=True)
            source_released = rr.get("type") == "MIGRATION_SOURCE_RELEASE_OK"
        except (ConnectionError, OSError, TimeoutError):
            source_released = False      # fenced source is cleaned up by reconciliation
        end = time.time()
        duration = end - t0
        m.store.update_operation(op_id, "COMMITTED", {
            "duration_s": duration, "downtime_s": downtime, "source_released": source_released,
            "frozen_at": frozen_at, "completed_at": end})
        m.emit("MIGRATION_COMMITTED", "MIGRATION", sid, name, target_site, op_id,
               source=src, generation=new_gen, duration_s=round(duration, 4),
               downtime_s=None if downtime is None else round(downtime, 4), source_released=source_released)
        if not source_released:
            m.emit("MIGRATION_SOURCE_RELEASE_DEFERRED", "MIGRATION", sid, name, src, op_id)
        self.stats["committed"] += 1
        m.cp.replication.rebalance_sync_paths(sid)
        return {"operation_id": op_id, "sid": sid, "logical_name": name, "source": src,
                "target": target_site, "generation": new_gen, "duration_s": duration,
                "downtime_s": downtime, "source_released": source_released, "state": "COMMITTED"}

    # ------------------------------------------------------------ helpers
    def _latency(self, path):
        total = 0.0
        for a, b in zip(path, path[1:]):
            total += next(w for n, w in self.m.topology.adj[a] if n == b)
        return total

    def _rollback(self, sid, op_id, src, dst, reason, stage, t0):
        """Pre-commit failure: restore the pre-migration state."""
        m = self.m
        for site, typ in ((dst, "MIGRATION_ABORT"), (src, "MIGRATION_ABORT")):
            try:
                m.rpc(site, {"type": typ, "sid": sid, "operation_id": op_id}, timeout=2.0, allow_inactive=True)
            except (ConnectionError, OSError, TimeoutError):
                pass
        with m.lock:
            m.ledger.release_op(op_id)
            inst = m.allocation_table.get(sid)
            if inst is not None and inst["op"] == op_id:
                inst["op"] = None
                if inst["state"] == "MIGRATING":
                    m.set_state(inst, "RUNNING", "MIGRATION_ROLLED_BACK", operation_id=op_id, reason=reason)
                    site_ok = m.sites.get(inst["site"], {}).get("status") == "ACTIVE"
                    if site_ok:
                        m.reg_state(inst, "RUNNING")
                m.persist(inst)
            m.store.update_operation(op_id, "ABORTED", {"reason": reason, "stage": stage,
                                                         "duration_s": time.time() - t0})
            m.emit("MIGRATION_ABORTED", "MIGRATION", sid, inst["logical_name"] if inst else None, src, op_id,
                   reason=reason, stage=stage, destination=dst)
        self.stats["aborted"] += 1
        raise MigrationError("aborted", f"{stage}: {reason}")

    def _fail_forward(self, sid, op_id, src, dst, err, t0):
        """The commit was decided but the destination could not activate."""
        m = self.m
        with m.lock:
            inst = m.allocation_table.get(sid)
            if inst is not None and inst["op"] == op_id:
                inst["op"] = None
                m.set_state(inst, "FAILED", "MIGRATION_ACTIVATION_FAILED", operation_id=op_id, error=err)
                m.reg_state(inst, "UNAVAILABLE")
                m.persist(inst)
            m.store.update_operation(op_id, "FAILED", {"reason": f"activation_failed: {err}",
                                                        "duration_s": time.time() - t0})
            m.emit("MIGRATION_ACTIVATION_FAILED", "MIGRATION", sid, inst["logical_name"] if inst else None,
                   dst, op_id, error=err)
        try:
            m.rpc(src, {"type": "MIGRATION_SOURCE_RELEASE", "sid": sid, "operation_id": op_id,
                        "generation": 0}, timeout=2.0, allow_inactive=True)
        except (ConnectionError, OSError, TimeoutError):
            pass
        self.stats["failed_forward"] += 1
        m.cp.kick()          # let the failure scanner decide: replica promotion or controlled loss
        raise MigrationError("activation_failed", str(err))

    def resume_pending(self, op):
        """Manager restarted after COMMIT_DECIDED: finish activating the destination."""
        m = self.m
        sid, op_id, dst = op["instance_id"], op["operation_id"], op["target_site"]
        gen = op["payload"].get("new_generation")
        try:
            r = m.rpc(dst, {"type": "MIGRATION_COMMIT", "sid": sid, "operation_id": op_id, "generation": gen},
                      timeout=self.rpc_timeout_s)
        except (ConnectionError, OSError, TimeoutError):
            return False
        ok = r.get("type") == "MIGRATION_COMMIT_OK"
        with m.lock:
            inst = m.allocation_table.get(sid)
            if ok and inst is not None:
                inst["op"] = None
                if inst["state"] in ("RECOVERING", "MIGRATING", "FAILED"):
                    if inst["state"] == "FAILED":
                        m.set_state(inst, "RECOVERING", "MIGRATION_RESUMED")
                    elif inst["state"] == "MIGRATING":
                        m.set_state(inst, "RUNNING", "MIGRATION_RESUMED")
                    if inst["state"] == "RECOVERING":
                        m.set_state(inst, "RUNNING", "MIGRATION_RESUMED")
                m.reg_state(inst, "RUNNING")
                m.persist(inst)
            if ok or r.get("error") in ("staged_snapshot_missing",):
                m.store.update_operation(op_id, "COMMITTED" if ok else "FAILED",
                                         {"recovered_after_restart": True})
                if not ok and inst is not None:
                    inst["op"] = None
                    m.cp.declare_lost(inst, "migration_destination_lost_state")
        if ok:
            try:
                m.rpc(op["source_site"], {"type": "MIGRATION_SOURCE_RELEASE", "sid": sid, "operation_id": op_id,
                                          "generation": gen}, timeout=2.0, allow_inactive=True)
            except (ConnectionError, OSError, TimeoutError):
                pass
        return ok or r.get("error") in ("staged_snapshot_missing",)
