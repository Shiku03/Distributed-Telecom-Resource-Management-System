"""Active-standby replication and fenced failover (manager side).

Replication vs migration
  * migration relocates the *active* service (one copy, new location);
  * replication keeps an *additional* STANDBY copy on another site.  A standby
    reserves the primary's full resource vector on its own site (so promotion
    can never over-allocate), reserves a configurable fraction of the
    service's bandwidth on the primary->replica path for state
    synchronisation, receives periodic snapshots, is never scheduled and never
    answers service calls.

Failover (promotion) order, which is what prevents split brain:
  1. choose a healthy, bandwidth-feasible replica; record the operation;
  2. fence the old primary: an explicit FENCE RPC, or -- if its site is
     unreachable -- the site's own manager-lease expiry, which is guaranteed to
     have happened before the site was declared FAILED (see failure_recovery);
  3. promote the replica at the *new generation* (the standby takes over the
     primary's instance id, so identity and logical name are unchanged);
  4. in one lock hold / SQLite transaction switch allocation record, ledger,
     logical endpoint and replica table;
  5. a primary that later returns is told to release its stale copy.
"""
from __future__ import annotations

import json
import time
import uuid

from common.ledger import ReservationError


class ReplicationError(RuntimeError):
    def __init__(self, reason, detail=""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason


class ReplicationManager:
    def __init__(self, manager):
        self.m = manager
        rc = manager.config.get("replication", {}) or {}
        self.sync_interval_s = float(rc.get("sync_interval_s", manager.config.get("heartbeat_interval_s", 1.0)))
        self.sync_bw_fraction = float(rc.get("sync_bw_fraction", 0.1))
        self.max_replicas = int(rc.get("max_replicas", 2))
        self.rpc_timeout_s = float(rc.get("rpc_timeout_s", 3.0))
        self.stats = {"sync_ok": 0, "sync_fail": 0, "sync_bytes": 0, "sync_ms_total": 0.0,
                      "failovers_started": 0, "failovers_ok": 0, "failovers_failed": 0}
        self._last_no_candidate = {}

    # --------------------------------------------------------- persistence
    def _persist(self, rep):
        self.m.store.upsert_replica({
            "replica_id": rep["replica_id"], "primary_id": rep["primary_sid"], "site_id": rep["site"],
            "state": rep["state"], "generation": rep.get("generation", 1), "sync_seq": rep["sync_seq"],
            "last_sync": rep["last_sync_ts"], "demand": rep["demand"], "created_at": rep["created_at"],
            "metadata": {"res_node": rep["res_node"], "res_sync": rep["res_sync"],
                         "sync_path": rep["sync_path"], "mode": rep.get("mode", "STANDBY")}})

    def replicas_of(self, sid):
        return [r for r in self.m.replicas.values() if r["primary_sid"] == sid]

    def _sync_bw(self, demand):
        return round(demand.get("bw", 0) * self.sync_bw_fraction, 6)

    # -------------------------------------------------------------- create
    def create_replica(self, ref, target_site, mode="STANDBY"):
        m = self.m
        t0 = time.time()
        with m.lock:
            inst = m.find_instance(ref)
            if inst is None:
                raise ReplicationError("unknown_service", str(ref))
            sid = inst["sid"]
            if inst["op"]:
                raise ReplicationError("operation_in_progress", inst["op"])
            if inst["state"] != "RUNNING":
                raise ReplicationError("service_not_running", inst["state"])
            psite, tsite = inst["site"], m.sites.get(target_site)
            if target_site == psite:
                raise ReplicationError("replica_must_be_on_different_site")
            if tsite is None or tsite["status"] != "ACTIVE":
                raise ReplicationError("target_not_active", target_site)
            existing = self.replicas_of(sid)
            if any(r["site"] == target_site for r in existing):
                raise ReplicationError("replica_already_on_site", target_site)
            if len(existing) >= self.max_replicas:
                raise ReplicationError("max_replicas_reached", str(self.max_replicas))
            demand = dict(inst["demand"])
            bw = self._sync_bw(demand)
            sync_path = None
            if bw > 0:
                sync_path, _ = m.topology.best_feasible_path(psite, target_site, bw)
                if sync_path is None:
                    raise ReplicationError("link_bandwidth", f"no path {psite}->{target_site} for sync")
            inst["replica_seq"] = inst.get("replica_seq", 0) + 1
            rid = f"{sid}-R{inst['replica_seq']}"
            op_id = "rep-" + uuid.uuid4().hex[:12]
            try:
                res_node = m.ledger.reserve_node(rid, target_site, demand, op_id=op_id, purpose="REPLICA")
                res_sync = (m.ledger.reserve_path(rid, sync_path, bw, op_id=op_id, purpose="SYNC")
                            if sync_path else None)
            except ReservationError as exc:
                m.ledger.release_op(op_id)
                raise ReplicationError(exc.reason, str(exc))
            rep = {"replica_id": rid, "primary_sid": sid, "site": target_site, "service": inst["service"],
                   "demand": demand, "state": "CREATING", "generation": inst["generation"],
                   "res_node": res_node, "res_sync": res_sync, "sync_path": sync_path,
                   "created_at": t0, "last_sync_ts": None, "sync_seq": 0, "sync_msgs": 0, "sync_bytes": 0,
                   "sync_failures": 0, "missing_count": 0, "mode": mode, "op": op_id}
            m.replicas[rid] = rep
            m.store.begin_operation({
                "operation_id": op_id, "operation_type": "REPLICATION", "state": "PREPARING",
                "instance_id": sid, "source_site": psite, "target_site": target_site,
                "payload": {"replica_id": rid, "mode": mode, "demand": demand, "sync_path": sync_path}})
            self._persist(rep)
            m.emit("REPLICA_CREATING", "REPLICATION", sid, inst["logical_name"], target_site, op_id,
                   replica_id=rid, primary_site=psite)
            name, pgen = inst["logical_name"], inst["generation"]

        try:
            snap = m.rpc(psite, {"type": "REPLICA_SNAPSHOT", "sid": sid}, timeout=self.rpc_timeout_s)
            if snap.get("type") != "REPLICA_SNAPSHOT_OK":
                raise ReplicationError("primary_snapshot_failed", str(snap.get("error")))
            r = m.rpc(target_site, {"type": "REPLICA_CREATE", "replica_id": rid, "primary_sid": sid,
                                    "snapshot": snap["snapshot"]}, timeout=self.rpc_timeout_s)
            if r.get("type") != "REPLICA_CREATE_OK":
                raise ReplicationError("replica_site_rejected", str(r.get("error")))
        except (ReplicationError, ConnectionError, OSError, TimeoutError) as exc:
            with m.lock:
                m.ledger.release_op(op_id)
                m.replicas.pop(rid, None)
                m.store.delete_replica(rid)
                m.store.update_operation(op_id, "ABORTED", {"reason": str(exc)})
                m.emit("REPLICA_CREATE_FAILED", "REPLICATION", sid, name, target_site, op_id,
                       replica_id=rid, reason=str(exc))
            try:
                m.rpc(target_site, {"type": "REPLICA_REMOVE", "replica_id": rid}, timeout=2.0, allow_inactive=True)
            except (ConnectionError, OSError, TimeoutError):
                pass
            raise ReplicationError("replica_creation_failed", str(exc))

        with m.lock:
            cur = m.allocation_table.get(sid)
            if cur is None or cur["site"] != psite or cur["generation"] != pgen or rid not in m.replicas:
                m.ledger.release_op(op_id)
                m.replicas.pop(rid, None)
                m.store.delete_replica(rid)
                m.store.update_operation(op_id, "ABORTED", {"reason": "primary_changed_during_replication"})
                m.queue_replica_removal(target_site, rid)
                raise ReplicationError("primary_changed_during_replication")
            m.ledger.rebind(res_node, op_id=None)
            if res_sync:
                m.ledger.rebind(res_sync, op_id=None)
            rep["state"], rep["op"] = "STANDBY", None
            rep["last_sync_ts"], rep["sync_seq"] = time.time(), 1
            self._persist(rep)
            dur = time.time() - t0
            m.store.update_operation(op_id, "COMMITTED", {"replica_id": rid, "duration_s": dur})
            m.emit("REPLICA_CREATED", "REPLICATION", sid, name, target_site, op_id,
                   replica_id=rid, primary_site=psite, duration_s=round(dur, 4), mode=mode)
        return {"replica_id": rid, "operation_id": op_id, "site": target_site, "state": "STANDBY",
                "primary": sid, "logical_name": name}

    # -------------------------------------------------------------- remove
    def remove_replica(self, rid, reason="requested"):
        m = self.m
        with m.lock:
            rep = m.replicas.get(rid)
            if rep is None:
                raise ReplicationError("unknown_replica", rid)
            if rep["state"] == "PROMOTING":
                raise ReplicationError("replica_promoting", rid)
            rep["state"] = "REMOVING"
            site = rep["site"]
        try:
            m.rpc(site, {"type": "REPLICA_REMOVE", "replica_id": rid}, timeout=2.0, allow_inactive=True)
        except (ConnectionError, OSError, TimeoutError):
            m.queue_replica_removal(site, rid)      # site unreachable: its next reconcile releases it
        with m.lock:
            self.drop_replica(rid, reason)
        return {"replica_id": rid, "removed": True}

    def drop_replica(self, rid, reason):
        """Forget a replica and free everything it reserved (lock held)."""
        m = self.m
        rep = m.replicas.pop(rid, None)
        if rep is None:
            return
        m.ledger.release_owner(rid)
        m.store.delete_replica(rid)
        inst = m.allocation_table.get(rep["primary_sid"])
        m.emit("REPLICA_REMOVED", "REPLICATION", rep["primary_sid"], inst["logical_name"] if inst else None,
               rep["site"], None, replica_id=rid, reason=reason)

    def reconfigure(self, rid, target_site):
        """Move a standby to another site: create the new one first, then remove the old."""
        with self.m.lock:
            rep = self.m.replicas.get(rid)
            if rep is None:
                raise ReplicationError("unknown_replica", rid)
            sid = rep["primary_sid"]
        new = self.create_replica(sid, target_site)
        self.remove_replica(rid, reason="reconfigured")
        return {"old_replica": rid, "new_replica": new["replica_id"], "site": target_site}

    # ---------------------------------------------------------------- sync
    def sync_tick(self):
        """One synchronisation round (called by the single sync thread; RPCs run outside the lock)."""
        m = self.m
        with m.lock:
            work = {}
            for rep in m.replicas.values():
                inst = m.allocation_table.get(rep["primary_sid"])
                if (rep["state"] == "STANDBY" and inst is not None and inst["state"] == "RUNNING"
                        and not inst["op"]
                        and m.sites.get(inst["site"], {}).get("status") == "ACTIVE"
                        and m.sites.get(rep["site"], {}).get("status") == "ACTIVE"):
                    work.setdefault(inst["sid"], (inst["site"], []))[1].append(
                        (rep["replica_id"], rep["site"], rep["sync_seq"] + 1))
        for sid, (psite, targets) in work.items():
            t0 = time.perf_counter()
            try:
                snap = m.rpc(psite, {"type": "REPLICA_SNAPSHOT", "sid": sid}, timeout=self.rpc_timeout_s)
                if snap.get("type") != "REPLICA_SNAPSHOT_OK":
                    continue
            except (ConnectionError, OSError, TimeoutError):
                continue
            nbytes = len(json.dumps(snap["snapshot"]))
            for rid, rsite, seq in targets:
                ok = False
                try:
                    r = m.rpc(rsite, {"type": "REPLICA_SYNC", "replica_id": rid, "seq": seq,
                                      "snapshot": snap["snapshot"]}, timeout=self.rpc_timeout_s)
                    ok = r.get("type") == "REPLICA_SYNC_OK"
                except (ConnectionError, OSError, TimeoutError):
                    ok = False
                ms = (time.perf_counter() - t0) * 1000
                with m.lock:
                    rep = m.replicas.get(rid)
                    if rep is None:
                        continue
                    if ok:
                        rep["last_sync_ts"], rep["sync_seq"] = time.time(), seq
                        rep["generation"] = snap["snapshot"]["generation"]
                        rep["sync_msgs"] += 2
                        rep["sync_bytes"] += nbytes
                        rep["sync_failures"] = 0
                        self.stats["sync_ok"] += 1
                        self.stats["sync_bytes"] += nbytes
                        self.stats["sync_ms_total"] += ms
                        m.emit("REPLICA_SYNCED", "REPLICATION", sid, None, rep["site"], None, buffered=True,
                               replica_id=rid, seq=seq, bytes=nbytes, duration_ms=round(ms, 3))
                    else:
                        rep["sync_failures"] += 1
                        self.stats["sync_fail"] += 1
                        if rep["sync_failures"] in (1, 5):
                            m.emit("REPLICA_SYNC_FAILED", "REPLICATION", sid, None, rep["site"], None,
                                   replica_id=rid, consecutive=rep["sync_failures"])

    def rebalance_sync_paths(self, sid):
        """After a primary moves, re-reserve each replica's sync bandwidth on the new path."""
        m = self.m
        with m.lock:
            inst = m.allocation_table.get(sid)
            if inst is None:
                return
            for rep in self.replicas_of(sid):
                if rep["res_sync"]:
                    m.ledger.release(rep["res_sync"])
                    rep["res_sync"], rep["sync_path"] = None, None
                bw = self._sync_bw(rep["demand"])
                if bw > 0:
                    path, _ = m.topology.best_feasible_path(inst["site"], rep["site"], bw)
                    if path is None:
                        rep["state"] = "UNAVAILABLE"
                        m.emit("REPLICA_SYNC_PATH_LOST", "REPLICATION", sid, inst["logical_name"], rep["site"],
                               None, replica_id=rep["replica_id"])
                    else:
                        rep["res_sync"] = m.ledger.reserve_path(rep["replica_id"], path, bw, purpose="SYNC")
                        rep["sync_path"] = path
                self._persist(rep)

    # ------------------------------------------------------------ failover
    def eligible(self, inst):
        """Replicas of ``inst`` that could be promoted right now (lock held)."""
        out = []
        for rep in self.replicas_of(inst["sid"]):
            site = self.m.sites.get(rep["site"])
            if rep["state"] == "STANDBY" and site is not None and site["status"] == "ACTIVE":
                out.append(rep)
        return out

    def has_candidate(self, inst):
        return bool(self.eligible(inst))

    def failover(self, ref, reason="site_failure", replica_id=None, force=False):
        m = self.m
        t_start = time.time()
        tried = set()
        while True:
            with m.lock:
                inst = m.find_instance(ref)
                if inst is None:
                    raise ReplicationError("unknown_service", str(ref))
                sid = inst["sid"]
                if inst["op"]:
                    raise ReplicationError("operation_in_progress", inst["op"])
                old_site = inst["site"]
                osite = m.sites.get(old_site)
                healthy_primary = (inst["state"] == "RUNNING" and osite is not None and osite["status"] == "ACTIVE")
                if healthy_primary and not force:
                    raise ReplicationError("primary_healthy", "use force to fence and fail over")
                cands = []
                for rep in self.eligible(inst):
                    if replica_id and rep["replica_id"] != replica_id:
                        continue
                    if rep["replica_id"] in tried:
                        continue
                    path, lat = m.topology.best_feasible_path(inst["ingress"], rep["site"],
                                                              inst["demand"]["bw"], inst["max_latency_ms"])
                    if path is None:
                        continue
                    cands.append((rep["last_sync_ts"] or 0, -lat, rep, path, lat))
                if not cands:
                    key = (sid, "no_candidate")
                    if time.time() - self._last_no_candidate.get(key, 0) > 10:
                        self._last_no_candidate[key] = time.time()
                        m.emit("FAILOVER_NO_CANDIDATE", "REPLICATION", sid, inst["logical_name"], old_site, None,
                               reason=reason)
                    self.stats["failovers_failed"] += 1 if tried else 0
                    raise ReplicationError("no_eligible_replica")
                cands.sort(key=lambda c: (c[0], c[1]), reverse=True)     # freshest sync, then lowest latency
                _, _, rep, new_path, new_lat = cands[0]
                rid, rsite = rep["replica_id"], rep["site"]
                tried.add(rid)
                op_id = "fo-" + uuid.uuid4().hex[:12]
                new_gen = inst["generation"] + 1
                try:
                    res_ingress = m.ledger.reserve_path(sid, new_path, inst["demand"]["bw"], op_id=op_id,
                                                        purpose="FAILOVER_PATH")
                except ReservationError:
                    continue
                inst["op"], rep["state"] = op_id, "PROMOTING"
                if inst["state"] == "RUNNING":
                    m.set_state(inst, "FAILED", "PRIMARY_FORCED_OUT", operation_id=op_id, reason=reason)
                    m.reg_state(inst, "UNAVAILABLE")
                m.store.begin_operation({
                    "operation_id": op_id, "operation_type": "FAILOVER", "state": "PREPARING", "instance_id": sid,
                    "source_site": old_site, "target_site": rsite,
                    "payload": {"replica_id": rid, "reason": reason, "new_generation": new_gen,
                                "new_path": new_path, "force": force, "started_at": t_start}})
                m.emit("FAILOVER_STARTED", "REPLICATION", sid, inst["logical_name"], rsite, op_id,
                       replica_id=rid, failed_site=old_site, reason=reason, generation=new_gen)
                self.stats["failovers_started"] += 1
                name = inst["logical_name"]
                site_status = osite["status"] if osite else "UNKNOWN"
                unavailable_since = inst.get("unavailable_since") or t_start

            # ---- fence the old primary BEFORE promoting anyone
            fence_method, fenced = None, False
            try:
                fr = m.rpc(old_site, {"type": "FENCE", "sid": sid, "generation": new_gen, "operation_id": op_id},
                           timeout=m.fcfg.fence_rpc_timeout_s, allow_inactive=True)
                fenced = fr.get("type") == "FENCE_OK"
                fence_method = "rpc"
            except (ConnectionError, OSError, TimeoutError):
                if site_status == "FAILED":
                    fenced, fence_method = True, "lease_expiry"   # site cannot run: lease expired before FAILED
            if not fenced:
                self._abort_failover(sid, op_id, rid, "fence_failed", old_site)
                raise ReplicationError("fence_failed", old_site)
            m.store.update_operation(op_id, "PRIMARY_FENCED", {"fence_method": fence_method})
            m.emit("PRIMARY_FENCED", "REPLICATION", sid, name, old_site, op_id, method=fence_method)

            # ---- promote
            try:
                pr = m.rpc(rsite, {"type": "REPLICA_PROMOTE", "replica_id": rid, "sid": sid,
                                   "generation": new_gen, "operation_id": op_id}, timeout=self.rpc_timeout_s)
                promoted = pr.get("type") == "REPLICA_PROMOTE_OK"
                perr = pr.get("error")
            except (ConnectionError, OSError, TimeoutError) as exc:
                promoted, perr = False, str(exc)
            if not promoted:
                self._abort_failover(sid, op_id, rid, f"promotion_failed:{perr}", rsite,
                                     drop_replica=(perr in ("not_found",) or str(perr).startswith("not_standby")))
                if replica_id:
                    raise ReplicationError("promotion_failed", str(perr))
                continue                                    # try the next candidate

            # ---- commit (one lock hold, one SQLite transaction)
            with m.lock:
                inst = m.allocation_table.get(sid)
                rep = m.replicas.get(rid)
                if inst is None or inst["op"] != op_id or rep is None:
                    m.ledger.release_op(op_id)
                    raise ReplicationError("primary_terminated_during_failover")
                rsite_rec = m.sites[rsite]
                with m.store.transaction():
                    m.ledger.release(inst["res_node"])
                    m.ledger.release(inst["res_path"])
                    if rep["res_sync"]:
                        m.ledger.release(rep["res_sync"])
                    m.ledger.rebind(rep["res_node"], owner=sid, purpose="PRIMARY", op_id=None)
                    m.ledger.rebind(res_ingress, purpose="INGRESS", op_id=None)
                    inst["res_node"], inst["res_path"] = rep["res_node"], res_ingress
                    inst["site"], inst["path"], inst["path_latency_ms"] = rsite, new_path, new_lat
                    inst["generation"] = new_gen
                    del m.replicas[rid]
                    m.store.delete_replica(rid)
                    inst["op"] = None
                    if inst["state"] == "FAILED":
                        m.set_state(inst, "RECOVERING", "FAILOVER_PROMOTED", operation_id=op_id, replica_id=rid)
                    m.set_state(inst, "RUNNING", "FAILOVER_COMPLETED", operation_id=op_id, replica_id=rid)
                    m.registry.update_location(sid, rsite, rsite_rec["host"], rsite_rec["port"],
                                               state="RUNNING", generation=new_gen)
                    outage = m.reg_state(inst, "RUNNING")
                    inst["failovers"] = inst.get("failovers", 0) + 1
                    m.persist(inst)
                    now = time.time()
                    m.store.update_operation(op_id, "COMMITTED", {
                        "duration_s": now - t_start, "recovery_time_s": now - unavailable_since,
                        "outage_s": outage, "fence_method": fence_method, "completed_at": now})
                m.emit("FAILOVER_COMPLETED", "REPLICATION", sid, name, rsite, op_id,
                       replica_id=rid, failed_site=old_site, generation=new_gen, fence_method=fence_method,
                       duration_s=round(now - t_start, 4), recovery_time_s=round(now - unavailable_since, 4))
                m.emit("SERVICE_RECOVERED", "RECOVERY", sid, name, rsite, op_id, via="replica_promotion",
                       recovery_time_s=round(now - unavailable_since, 4), failed_site=old_site)
                self.stats["failovers_ok"] += 1
            self.rebalance_sync_paths(sid)
            return {"operation_id": op_id, "sid": sid, "logical_name": name, "promoted_replica": rid,
                    "new_site": rsite, "old_site": old_site, "generation": new_gen,
                    "fence_method": fence_method, "recovery_time_s": now - unavailable_since}

    def _abort_failover(self, sid, op_id, rid, reason, site, drop_replica=False):
        m = self.m
        with m.lock:
            m.ledger.release_op(op_id)
            inst = m.allocation_table.get(sid)
            if inst is not None and inst["op"] == op_id:
                inst["op"] = None
                # a forced failover that could not fence a healthy primary must leave it serving
                if (reason.startswith("fence_failed") and inst["state"] == "FAILED"
                        and m.sites.get(inst["site"], {}).get("status") == "ACTIVE"):
                    m.set_state(inst, "RECOVERING", "FAILOVER_ABORTED", operation_id=op_id)
                    m.set_state(inst, "RUNNING", "FAILOVER_ABORTED", operation_id=op_id)
                    m.reg_state(inst, "RUNNING")
            rep = m.replicas.get(rid)
            if rep is not None:
                if drop_replica:
                    self.drop_replica(rid, reason)
                else:
                    rep["state"] = "STANDBY"
            m.store.update_operation(op_id, "ABORTED", {"reason": reason})
            m.emit("FAILOVER_ATTEMPT_FAILED", "REPLICATION", sid, inst["logical_name"] if inst else None, site,
                   op_id, replica_id=rid, reason=reason)
