"""Failure detection thresholds and inventory reconciliation.

Site health states: ACTIVE -> SUSPECT -> FAILED -> RECOVERING -> ACTIVE.

* SUSPECT (no heartbeat for ``suspect_after_s``): the site may only be slow.
  Its services are made unresolvable, but nothing is torn down and a single
  heartbeat returns the site to ACTIVE after its inventory is checked.
* FAILED (no heartbeat for ``failed_after_s``): the failure is confirmed.
  Replicas are promoted, allocation to the site stays blocked.
* RECOVERING: a FAILED site is talking again. It is not ACTIVE until its
  reported inventory has been reconciled against the manager's records.

Split-brain safety rests on one inequality: a site fences itself (stops
scheduling and refuses service calls) once it has gone ``lease_s`` without a
manager acknowledgement, and the manager promotes a replica only after
``failed_after_s``.  ``FailureConfig`` refuses configurations where
``failed_after_s`` does not exceed ``lease_s`` by at least one heartbeat.

``plan_reconciliation`` is a pure function (no I/O, no locks) so the decision
logic can be tested exhaustively; the manager applies the plan.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class FailureConfig:
    heartbeat_interval_s: float = 1.0
    suspect_after_s: float = 3.0
    failed_after_s: float = 6.0
    lease_s: float = 3.0
    failed_instance_ttl_s: float = 120.0
    auto_recover: bool = True
    missing_threshold: int = 3          # heartbeats an instance may be absent before it is declared lost
    fence_rpc_timeout_s: float = 1.0

    @classmethod
    def from_config(cls, config: dict) -> "FailureConfig":
        hb = float(config.get("heartbeat_interval_s", 1.0))
        fd = config.get("failure_detector", {}) or {}
        suspect_n = fd.get("suspect_after_heartbeats", config.get("failure_timeout_heartbeats", 3))
        failed_n = fd.get("failed_after_heartbeats", 2 * suspect_n)
        lease_n = fd.get("site_lease_heartbeats", suspect_n)
        fc = cls(
            heartbeat_interval_s=hb,
            suspect_after_s=hb * suspect_n,
            failed_after_s=hb * failed_n,
            lease_s=hb * lease_n,
            failed_instance_ttl_s=float(fd.get("failed_instance_ttl_s", 120.0)),
            auto_recover=bool(fd.get("auto_recover", True)),
            missing_threshold=int(fd.get("missing_threshold_heartbeats", 3)),
            fence_rpc_timeout_s=float(fd.get("fence_rpc_timeout_s", 1.0)),
        )
        fc.validate()
        return fc

    def validate(self):
        if self.failed_after_s <= self.suspect_after_s:
            raise ValueError("failed_after must exceed suspect_after")
        if self.failed_after_s < self.lease_s + self.heartbeat_interval_s - 1e-9:
            raise ValueError(
                "failure_detector: failed_after must exceed the site lease by at least one "
                "heartbeat interval, otherwise a partitioned primary could still be running "
                "when its replica is promoted")


class FailureDetector:
    def __init__(self, cfg: FailureConfig):
        self.cfg = cfg

    def classify(self, age_s: float) -> str:
        """Health implied by heartbeat age alone: ACTIVE, SUSPECT or FAILED."""
        if age_s <= self.cfg.suspect_after_s:
            return "ACTIVE"
        if age_s <= self.cfg.failed_after_s:
            return "SUSPECT"
        return "FAILED"


def plan_reconciliation(site_id, records_here, all_records, replicas_here, inventory,
                        restarted=False, missing_threshold=3):
    """Compare what the manager believes a site hosts with what the site
    reports.  Returns a dict of sid/replica-id lists:

    restore         primaries confirmed RUNNING at the expected generation
    resume          fenced leftovers of an aborted migration (site should resume)
    release         instances the site must discard (stale, orphaned or superseded)
    missing         owned primaries absent from the report
    lost            subset of ``missing`` (plus generation-mismatched) now declared lost
    replica_present replicas confirmed present
    replica_missing replicas absent from the report
    replica_lost    subset of replica_missing now declared lost
    """
    plan = {k: [] for k in ("restore", "resume", "release", "missing", "lost",
                            "replica_present", "replica_missing", "replica_lost")}
    reported = {str(i["sid"]): i for i in inventory}

    for sid, rec in records_here.items():
        if rec.get("op") or rec["state"] in ("ALLOCATING", "DRAINING"):
            continue                                  # an in-flight operation owns this instance
        i = reported.get(sid)
        if i is not None and i.get("role", "PRIMARY") == "PRIMARY":
            if int(i.get("generation", 1)) == int(rec["generation"]):
                if i["state"] == "RUNNING":
                    plan["restore"].append(sid)
                elif i["state"] == "MIGRATING" and rec["state"] == "RECOVERING":
                    plan["resume"].append(sid)
                else:
                    plan["missing"].append(sid)       # present but not serving (e.g. draining)
            else:
                plan["release"].append(sid)           # wrong generation: not a copy we may keep
                plan["lost"].append(sid)
        else:
            plan["missing"].append(sid)
            if restarted or rec.get("missing_count", 0) + 1 >= missing_threshold:
                plan["lost"].append(sid)

    for sid, i in reported.items():
        if i.get("role", "PRIMARY") == "REPLICA":
            if sid in replicas_here:
                plan["replica_present"].append(sid)
            else:
                plan["release"].append(sid)           # a replica the manager does not know
        elif sid not in records_here:
            other = all_records.get(sid)
            if other is not None and other.get("op"):
                continue                              # mid-operation elsewhere; revisit next heartbeat
            plan["release"].append(sid)               # stale primary (superseded, terminated, unknown)

    for rid, rep in replicas_here.items():
        if rep["state"] in ("CREATING", "PROMOTING", "REMOVING"):
            continue
        if rid not in reported:
            plan["replica_missing"].append(rid)
            if restarted or rep.get("missing_count", 0) + 1 >= missing_threshold:
                plan["replica_lost"].append(rid)
    return plan
