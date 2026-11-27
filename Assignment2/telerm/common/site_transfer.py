"""Site-side migration, replica and fencing handlers.

These operate on a ``Site`` (see site_node.py) and move only the *simulation
state* of an instance (identity, demand, remaining holding time, stride
scheduler state, counters, generation), not arbitrary process memory.

Migration, source side
    MIGRATION_PREPARE  freeze RUNNING -> MIGRATING (no new jobs, no service
                       calls), wait for in-flight jobs to finish, return a
                       snapshot.  The frozen instance is the source *fence*.
    MIGRATION_SOURCE_RELEASE  discard the frozen instance after the manager
                       has committed the move.
    MIGRATION_ABORT    thaw MIGRATING -> RUNNING if the move is abandoned.
Migration, destination side
    MIGRATION_IMPORT   stage the snapshot.  A staged instance is invisible to
                       the scheduler and to service calls: it is the
                       destination *fence* until commit.
    MIGRATION_COMMIT   activate the staged instance (idempotent).
    MIGRATION_ABORT    discard the staged instance.
Replicas
    REPLICA_SNAPSHOT   non-fencing read of a primary's recovery state
    REPLICA_CREATE / REPLICA_SYNC / REPLICA_PROMOTE / REPLICA_REMOVE
    FENCE              remove an instance and refuse every older generation
"""
from __future__ import annotations

import time

from common.instance import Instance
from common.lifecycle import transition as lifecycle_transition

STATE_FIELDS = ("tickets", "pass_offset", "jobs_done", "busy_s", "latency_sum_ms",
                "jobs_since_hb", "calls_served")


def _used(site, include_staged=True):
    used = {"cpu": 0.0, "mem": 0.0, "bw": 0.0}
    for i in site.instances.values():
        if i.state not in ("TERMINATED", "FAILED"):
            for k in used:
                used[k] += i.demand.get(k, 0)
    if include_staged:
        for st in site.staged.values():
            for k in used:
                used[k] += st["inst"].demand.get(k, 0)
    return used


def _fits(site, demand):
    used = _used(site)
    return all(used[k] + demand[k] <= site.capacity[k] + 1e-9 for k in used)


def _min_pass(site):
    vals = [i.pass_value for i in site.instances.values() if i.state in ("RUNNING", "MIGRATING")]
    return min(vals) if vals else 0.0


def snapshot_locked(site, inst):
    """Recovery/migration state of ``inst`` (caller holds site.cv).
    ``pass_offset`` is the instance's stride-pass relative to the site's
    minimum, so fairness position survives a move between sites whose
    absolute pass counters differ."""
    return {
        "sid": inst.sid, "service": inst.service, "logical_name": inst.logical_name,
        "demand": dict(inst.demand), "generation": inst.generation,
        "holding_remaining_s": inst.holding_remaining(),
        "tickets": inst.tickets, "stride": inst.stride, "pass_value": inst.pass_value,
        "pass_offset": inst.pass_value - _min_pass(site),
        "jobs_done": inst.jobs_done, "busy_s": inst.busy_s,
        "latency_sum_ms": inst.latency_sum_ms, "jobs_since_hb": inst.jobs_since_hb,
        "calls_served": inst.calls_served,
    }


def _valid_snapshot(snap):
    demand = {k: float(snap["demand"][k]) for k in ("cpu", "mem", "bw")}
    holding = float(snap["holding_remaining_s"])
    if holding <= 0 or demand["cpu"] <= 0 or any(v < 0 for v in demand.values()):
        raise ValueError("invalid snapshot values")
    return demand, holding


def _apply_state(inst, snap):
    inst.set_tickets(float(snap["tickets"]))
    inst.jobs_done = int(snap["jobs_done"])
    inst.busy_s = float(snap["busy_s"])
    inst.latency_sum_ms = float(snap["latency_sum_ms"])
    inst.jobs_since_hb = int(snap.get("jobs_since_hb", 0))
    inst.calls_served = int(snap.get("calls_served", 0))


# ------------------------------------------------------------ source side
def migration_prepare(site, msg):
    sid, op = str(msg["sid"]), msg["operation_id"]
    quiesce = float(msg.get("quiesce_timeout_s", 2.0))
    with site.cv:
        inst = site.instances.get(sid)
        if inst is None:
            return {"type": "MIGRATION_ERROR", "error": "instance_not_found"}
        if inst.state != "RUNNING" or inst.role != "PRIMARY":
            return {"type": "MIGRATION_ERROR", "error": f"instance_not_running:{inst.state}"}
        if "generation" in msg and int(msg["generation"]) != inst.generation:
            return {"type": "MIGRATION_ERROR", "error": "stale_generation"}
        inst.state = lifecycle_transition(inst.state, "MIGRATING")     # fence: scheduler & calls skip it
        inst.migration_op = op
        site._event(sid, "MIGRATION_SOURCE_FENCED", inst.state, operation_id=op)
        deadline = time.monotonic() + quiesce
        while inst.in_flight > 0:                                      # scheduler quiescence
            left = deadline - time.monotonic()
            if left <= 0:
                inst.state = lifecycle_transition(inst.state, "RUNNING")
                inst.migration_op = None
                site._event(sid, "MIGRATION_QUIESCE_TIMEOUT", inst.state, operation_id=op)
                site.cv.notify_all()
                return {"type": "MIGRATION_ERROR", "error": "quiesce_timeout"}
            site.cv.wait(timeout=left)
        snap = snapshot_locked(site, inst)
        site._event(sid, "MIGRATION_SNAPSHOT_TAKEN", inst.state, operation_id=op)
        return {"type": "MIGRATION_SNAPSHOT", "snapshot": snap, "sid": sid}


def migration_source_release(site, msg):
    sid, op = str(msg["sid"]), msg["operation_id"]
    with site.cv:
        inst = site.instances.get(sid)
        if inst is None:
            site.fence_floor[sid] = max(site.fence_floor.get(sid, 0), int(msg.get("generation", 0)))
            return {"type": "MIGRATION_SOURCE_RELEASE_OK", "sid": sid, "already_gone": True}
        if inst.state != "MIGRATING" or inst.migration_op != op:
            return {"type": "MIGRATION_ERROR", "error": "source_not_fenced"}
        inst.state = lifecycle_transition(inst.state, "DRAINING")
        inst.state = lifecycle_transition(inst.state, "TERMINATED")
        site.instances.pop(sid, None)
        site.fence_floor[sid] = max(site.fence_floor.get(sid, 0), int(msg.get("generation", 0)))
        site._event(sid, "MIGRATION_SOURCE_RELEASED", "TERMINATED", operation_id=op,
                    generation=msg.get("generation"))
        site.cv.notify_all()
    return {"type": "MIGRATION_SOURCE_RELEASE_OK", "sid": sid}


def migration_abort(site, msg):
    sid, op = str(msg["sid"]), msg["operation_id"]
    with site.cv:
        inst = site.instances.get(sid)
        if inst is not None and inst.state == "MIGRATING" and inst.migration_op == op:
            inst.state = lifecycle_transition(inst.state, "RUNNING")
            inst.migration_op = None
            site._event(sid, "MIGRATION_ABORTED", "RUNNING", operation_id=op)
            site.cv.notify_all()
        site.staged.pop((op, sid), None)
    return {"type": "MIGRATION_ABORT_OK", "sid": sid}


def resume_fenced(site, sid):
    """Manager directive after a manager restart rolled back a pre-commit migration."""
    with site.cv:
        inst = site.instances.get(sid)
        if inst is not None and inst.state == "MIGRATING":
            inst.state = lifecycle_transition(inst.state, "RUNNING")
            inst.migration_op = None
            site._event(sid, "MIGRATION_RESUMED_BY_MANAGER", "RUNNING")
            site.cv.notify_all()


# ------------------------------------------------------- destination side
def migration_import(site, msg):
    sid, op = str(msg["sid"]), msg["operation_id"]
    gen = int(msg.get("generation", 1))
    try:
        snap = msg["snapshot"]
        demand, holding = _valid_snapshot(snap)
    except (KeyError, TypeError, ValueError) as exc:
        return {"type": "MIGRATION_ERROR", "error": f"invalid_snapshot:{exc}"}
    with site.cv:
        if gen <= site.fence_floor.get(sid, 0):
            return {"type": "MIGRATION_ERROR", "error": "fenced_generation"}
        if sid in site.instances:
            return {"type": "MIGRATION_ERROR", "error": "sid_already_present"}
        if (op, sid) in site.staged:
            return {"type": "MIGRATION_IMPORT_OK", "sid": sid, "duplicate": True}
        if not _fits(site, demand):
            return {"type": "MIGRATION_ERROR", "error": "insufficient_capacity"}
        inst = Instance(sid, snap["service"], demand, holding, logical_name=snap.get("logical_name"),
                        generation=gen)
        _apply_state(inst, snap)
        inst.state = "MIGRATING"                       # fenced: not in site.instances, never scheduled
        site.staged[(op, sid)] = {"inst": inst, "snapshot": snap, "staged_at": time.monotonic()}
        site._event(sid, "MIGRATION_DESTINATION_STAGED", "MIGRATING", operation_id=op, generation=gen)
    return {"type": "MIGRATION_IMPORT_OK", "sid": sid}


def migration_commit(site, msg):
    sid, op = str(msg["sid"]), msg["operation_id"]
    gen = int(msg.get("generation", 1))
    with site.cv:
        live = site.instances.get(sid)
        if live is not None:                           # idempotent retry
            if live.generation == gen and live.state == "RUNNING":
                return {"type": "MIGRATION_COMMIT_OK", "sid": sid, "duplicate": True}
            return {"type": "MIGRATION_ERROR", "error": "sid_already_present"}
        st = site.staged.pop((op, sid), None)
        if st is None:
            return {"type": "MIGRATION_ERROR", "error": "staged_snapshot_missing"}
        inst, snap = st["inst"], st["snapshot"]
        inst.generation = gen
        inst.logical_name = snap.get("logical_name")
        inst.expires_at = time.monotonic() + float(snap["holding_remaining_s"])   # timer was frozen
        inst.pass_value = _min_pass(site) + float(snap["pass_offset"])             # keep relative fairness
        inst.state = "RUNNING"
        site.instances[sid] = inst
        site._event(sid, "MIGRATION_DESTINATION_ACTIVATED", "RUNNING", operation_id=op, generation=gen)
        site.cv.notify_all()
    return {"type": "MIGRATION_COMMIT_OK", "sid": sid, "generation": gen}


# ------------------------------------------------------------ replication
def replica_snapshot(site, msg):
    sid = str(msg["sid"])
    with site.cv:
        inst = site.instances.get(sid)
        if inst is None or inst.role != "PRIMARY" or inst.state not in ("RUNNING", "RESIZING"):
            return {"type": "REPLICA_SNAPSHOT_FAIL", "error": "primary_not_running"}
        return {"type": "REPLICA_SNAPSHOT_OK", "snapshot": snapshot_locked(site, inst)}


def replica_create(site, msg):
    rid, primary = str(msg["replica_id"]), str(msg["primary_sid"])
    try:
        snap = msg["snapshot"]
        demand, holding = _valid_snapshot(snap)
    except (KeyError, TypeError, ValueError) as exc:
        return {"type": "REPLICA_CREATE_FAIL", "error": f"invalid_snapshot:{exc}"}
    with site.cv:
        if rid in site.instances:
            return {"type": "REPLICA_CREATE_OK", "replica_id": rid, "duplicate": True}
        if not _fits(site, demand):
            return {"type": "REPLICA_CREATE_FAIL", "error": "insufficient_capacity"}
        inst = Instance(rid, snap["service"], demand, holding, logical_name=snap.get("logical_name"),
                        generation=int(snap.get("generation", 1)), role="REPLICA", replica_of=primary)
        _apply_state(inst, snap)
        inst.state = "STANDBY"                         # never scheduled, never serves calls
        inst.standby_remaining_s = holding
        inst.sync_mono = time.monotonic()
        inst.sync_seq = 1
        site.instances[rid] = inst
        site._event(rid, "REPLICA_CREATED", "STANDBY", replica_of=primary)
    return {"type": "REPLICA_CREATE_OK", "replica_id": rid}


def replica_sync(site, msg):
    rid = str(msg["replica_id"])
    snap = msg["snapshot"]
    with site.cv:
        inst = site.instances.get(rid)
        if inst is None or inst.state != "STANDBY":
            return {"type": "REPLICA_SYNC_FAIL", "error": "replica_not_standby"}
        if int(msg.get("seq", 0)) <= inst.sync_seq:
            return {"type": "REPLICA_SYNC_OK", "replica_id": rid, "seq": inst.sync_seq, "stale": True}
        _apply_state(inst, snap)
        inst.generation = int(snap.get("generation", inst.generation))
        inst.standby_remaining_s = float(snap["holding_remaining_s"])
        inst.sync_mono = time.monotonic()
        inst.sync_seq = int(msg["seq"])
        inst.pass_value = float(snap.get("pass_offset", 0.0))      # stored as offset until promotion
    return {"type": "REPLICA_SYNC_OK", "replica_id": rid, "seq": inst.sync_seq}


def replica_promote(site, msg):
    rid, sid = str(msg["replica_id"]), str(msg["sid"])
    gen = int(msg["generation"])
    with site.cv:
        inst = site.instances.get(rid)
        if inst is None:
            return {"type": "REPLICA_PROMOTE_FAIL", "error": "not_found"}
        if inst.state != "STANDBY":
            return {"type": "REPLICA_PROMOTE_FAIL", "error": f"not_standby:{inst.state}"}
        if gen <= site.fence_floor.get(sid, 0):
            return {"type": "REPLICA_PROMOTE_FAIL", "error": "fenced_generation"}
        if sid in site.instances:
            return {"type": "REPLICA_PROMOTE_FAIL", "error": "sid_already_present"}
        remaining = inst.holding_remaining()
        if remaining <= 0:
            return {"type": "REPLICA_PROMOTE_FAIL", "error": "holding_time_elapsed"}
        del site.instances[rid]
        inst.sid = sid                                   # the service keeps its identity
        inst.role, inst.replica_of = "PRIMARY", None
        inst.generation = gen
        inst.expires_at = time.monotonic() + remaining
        inst.pass_value = _min_pass(site) + inst.pass_value   # offset -> absolute
        inst.state = lifecycle_transition(inst.state, "RUNNING")
        site.instances[sid] = inst
        site._event(sid, "REPLICA_PROMOTED", "RUNNING", replica_id=rid, generation=gen,
                    operation_id=msg.get("operation_id"))
        site.cv.notify_all()
    return {"type": "REPLICA_PROMOTE_OK", "sid": sid, "generation": gen, "remaining_s": remaining}


def replica_remove(site, msg):
    rid = str(msg["replica_id"])
    with site.cv:
        inst = site.instances.get(rid)
        if inst is not None and inst.role == "REPLICA":
            inst.state = "TERMINATED"
            del site.instances[rid]
            site._event(rid, "REPLICA_REMOVED", "TERMINATED")
    return {"type": "REPLICA_REMOVE_OK", "replica_id": rid}


# ---------------------------------------------------------------- fencing
def fence(site, msg):
    """Remove ``sid`` (if present) and refuse every generation <= the given
    one from now on.  Used to fence a primary before its replica is promoted."""
    sid = str(msg["sid"])
    gen = int(msg["generation"])
    with site.cv:
        existed = False
        inst = site.instances.get(sid)
        if inst is not None and inst.generation < gen:
            existed = True
            inst.state = "TERMINATED"
            del site.instances[sid]
            site._event(sid, "INSTANCE_FENCED", "TERMINATED", operation_id=msg.get("operation_id"),
                        generation=gen)
        site.fence_floor[sid] = max(site.fence_floor.get(sid, 0), gen - 1)
        for key in [k for k in site.staged if k[1] == sid]:
            site.staged.pop(key, None)
        site.cv.notify_all()
    return {"type": "FENCE_OK", "sid": sid, "existed": existed}
