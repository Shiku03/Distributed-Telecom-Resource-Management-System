"""Performance metrics and result files.

Units: timestamps are epoch seconds (UTC); ``*_s`` durations are seconds,
``*_ms`` are milliseconds; utilisations, rates and ratios are fractions in
[0, 1]; bandwidth is in the topology's bandwidth units (Mb/s).

SQLite is the durable source of control-plane events; the JSON files below are
views of it (plus the manager's in-memory samples) and ``analytics/`` holds
flat JSONL/CSV exports for Spark and Power BI.
"""
from __future__ import annotations

import csv
import json
import os
import time


def _pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    k = min(len(xs) - 1, max(0, int(round(p / 100.0 * (len(xs) - 1)))))
    return xs[k]


def _stats(xs):
    if not xs:
        return {"count": 0, "mean": None, "p95": None, "max": None}
    return {"count": len(xs), "mean": sum(xs) / len(xs), "p95": _pct(xs, 95), "max": max(xs)}


def compute_metrics(m):
    now = time.time()
    with m.lock:
        uptime = max(1e-9, now - m.started_at)
        offered, admitted, blocked = m.offered["_all"], m.admitted["_all"], m.blocked["_all"]
        # --- availability: 1 - unavailable/lifetime over completed and live instances
        life = sum(c.get("lifetime_s", 0.0) for c in m.completed_instances)
        unav = sum(c.get("unavailable_s", 0.0) for c in m.completed_instances)
        for i in m.allocation_table.values():
            life += now - i["admitted_at"]
            unav += i["unavailable_total_s"] + (now - i["unavailable_since"] if i["unavailable_since"] else 0.0)
        mig_down = [c["migration_downtime_s"] for c in m.completed_instances if c.get("migrations")]
        # --- utilisation
        sites = {}
        for s in m.utilisation_samples:
            for sid, u in s["sites"].items():
                d = sites.setdefault(sid, {"cpu": [], "mem": [], "bw": []})
                d["cpu"].append(u["cpu_util"]); d["mem"].append(u["mem_util"]); d["bw"].append(u["bw_util"])
        links = {}
        for s in m.utilisation_samples:
            for name, u in s["links"].items():
                links.setdefault(name, []).append(u["utilisation"])
        ratios = []
        for smp in m.utilisation_samples:
            tot = sum(u["cpu_util"] * m.sites[sid]["capacity"]["cpu"] for sid, u in smp["sites"].items()
                      if sid in m.sites)
            if tot > 0:
                ratios.append(smp["standby_reserved"]["cpu"] / tot)
        held = m.ledger.held(kind="NODE")
        prim = {k: sum(r["demand"][k] for r in held if r["purpose"] == "PRIMARY") for k in ("cpu", "mem", "bw")}
        stby = {k: sum(r["demand"][k] for r in held if r["purpose"] == "REPLICA") for k in ("cpu", "mem", "bw")}
        sync_bw = sum(r["demand"]["bw"] for r in m.ledger.held(kind="PATH", purpose="SYNC"))
        rs = dict(m.cp.replication.stats)
        jobs = sum(c.get("jobs_done", 0) for c in m.completed_instances)
        calls = sum(c.get("calls_served", 0) for c in m.completed_instances)
        mig_stats, rep_stats = dict(m.cp.migration.stats), dict(rs)
        resolve = list(m.resolution_ms)
        outcomes = dict(m.resolve_outcomes)
        decision = list(m.decision_times_ms)
        ledger_problems = m.ledger.verify()
    ops = m.store.operations()
    migs = [o for o in ops if o["operation_type"] == "MIGRATION"]
    fos = [o for o in ops if o["operation_type"] == "FAILOVER"]
    reps = [o for o in ops if o["operation_type"] == "REPLICATION"]
    done = lambda os_, st="COMMITTED": [o for o in os_ if o["state"] == st]
    fin = lambda os_: [o for o in os_ if o["state"] in ("COMMITTED", "ABORTED", "FAILED", "ABORTED_ON_RESTART")]
    mig_ok = done(migs)
    fo_ok = done(fos)
    syncs = [e["payload"] for e in m.store.all_events("REPLICATION", ["REPLICA_SYNCED"])]
    rs["sync_ok"] = max(rs["sync_ok"], len(syncs))
    rs["sync_bytes"] = max(rs["sync_bytes"], sum(p.get("bytes", 0) for p in syncs))
    rs["sync_ms_total"] = max(rs["sync_ms_total"], sum(p.get("duration_ms", 0.0) for p in syncs))
    detect = [e["payload"].get("heartbeat_age_s") for e in m.store.all_events("FAILURE", ["SITE_FAILED"])
              if e["payload"].get("heartbeat_age_s") is not None]
    return {
        "generated_at": now, "uptime_s": uptime,
        "resource_utilisation": {
            "per_site_mean_peak": {sid: {r: {"mean": sum(v) / len(v), "peak": max(v)} for r, v in d.items() if v}
                                   for sid, d in sites.items()},
            "samples": len(m.utilisation_samples)},
        "allocation": {"offered": offered, "admitted": admitted, "blocked": blocked,
                       "success_rate": admitted / offered if offered else None,
                       "decision_time_ms": _stats(decision)},
        "service_availability": {"availability": 1 - unav / life if life > 0 else None,
                                 "service_seconds": life, "unavailable_seconds": unav},
        "migration": {"started": len(migs), "committed": len(mig_ok), "aborted_or_failed": len(fin(migs)) - len(mig_ok),
                      "success_rate": len(mig_ok) / len(fin(migs)) if fin(migs) else None,
                      "duration_s": _stats([o["payload"].get("duration_s") for o in mig_ok
                                            if o["payload"].get("duration_s") is not None]),
                      "downtime_s": _stats([o["payload"].get("downtime_s") for o in mig_ok
                                            if o["payload"].get("downtime_s") is not None])},
        "failover": {"started": len(fos), "committed": len(fo_ok),
                     "success_rate": len(fo_ok) / len(fin(fos)) if fin(fos) else None,
                     "recovery_time_s": _stats([o["payload"].get("recovery_time_s") for o in fo_ok
                                                if o["payload"].get("recovery_time_s") is not None]),
                     "failover_duration_s": _stats([o["payload"].get("duration_s") for o in fo_ok
                                                    if o["payload"].get("duration_s") is not None]),
                     "failure_detection_s": _stats(detect)},
        "replication_overhead": {
            "replicas_created": len(done(reps)), "standby_reserved": stby, "primary_reserved": prim,
            "standby_to_primary_cpu_ratio": stby["cpu"] / prim["cpu"] if prim["cpu"] else None,
            "standby_share_of_allocated_cpu_time_avg": sum(ratios) / len(ratios) if ratios else None,
            "sync_bandwidth_reserved": sync_bw, "sync_rounds_ok": rs["sync_ok"], "sync_rounds_failed": rs["sync_fail"],
            "sync_bytes": rs["sync_bytes"],
            "sync_ms_mean": rs["sync_ms_total"] / rs["sync_ok"] if rs["sync_ok"] else None},
        "service_resolution": {"manager_side_ms": _stats(resolve), "outcomes": outcomes},
        "network_bandwidth_utilisation": {name: {"mean": sum(v) / len(v), "peak": max(v)}
                                          for name, v in links.items() if v},
        "failed_requests": {"blocked": blocked, "reasons": dict(m.blocked_reasons.get("_all", {})),
                            "failed_resolutions": sum(v for k, v in outcomes.items() if k != "ok")},
        "throughput": {"admissions_per_s": admitted / uptime, "completions_per_s": len(m.completed_instances) / uptime,
                       "jobs_per_s": jobs / uptime, "client_calls_served": calls},
        "migration_downtime_per_migrated_instance_s": _stats(mig_down),
        "coordinator_counters": {"migration": mig_stats, "replication": rep_stats},
        "notes": {"in_memory_counters": "offered/admitted/blocked, decision time, resolution latency and "
                  "availability accumulators are per manager process; migration, failover and replication "
                  "figures are read from SQLite and span manager restarts"},
        "ledger_consistent": not ledger_problems, "ledger_problems": ledger_problems,
    }


def _dump(path, obj):
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, default=str)


def write_reports(m):
    d = m.results_dir
    os.makedirs(d, exist_ok=True)
    m.store.flush()
    metrics = compute_metrics(m)
    with m.lock:
        report = {
            "policy": m.policy,
            "offered": dict(m.offered), "blocked": dict(m.blocked), "admitted": dict(m.admitted),
            "blocking_probability": {k: (m.blocked.get(k, 0) / v if v else 0.0) for k, v in m.offered.items()},
            "blocked_reasons": m.blocked_reasons,
            "decision_time_ms": {
                "mean": sum(m.decision_times_ms) / len(m.decision_times_ms) if m.decision_times_ms else None,
                "max": max(m.decision_times_ms) if m.decision_times_ms else None,
                "count": len(m.decision_times_ms)},
            "link_utilisation_final": m.topology.link_utilisation(),
            "counters": m.counters.snapshot(),
            "service_registry": m.registry.snapshot(),
            "lifecycle_event_count": len(m.lifecycle_events),
            "failure_events": list(m.failure_events),
            "sites_final": {sid: {"capacity": s["capacity"], "allocated": dict(s["allocated"]), "status": s["status"]}
                            for sid, s in m.sites.items()},
            "replicas_final": [{k: r[k] for k in ("replica_id", "primary_sid", "site", "state")}
                               for r in m.replicas.values()],
            "metrics": metrics,
        }
        completed, hb, util = list(m.completed_instances), list(m.heartbeat_log), list(m.utilisation_samples)
        registry = m.registry.snapshot()
    _dump(os.path.join(d, "manager_report.json"), report)
    _dump(os.path.join(d, "performance_metrics.json"), metrics)
    _dump(os.path.join(d, "instances_log.json"), completed)
    _dump(os.path.join(d, "heartbeat_log.json"), hb)
    _dump(os.path.join(d, "utilisation_timeseries.json"), util)
    _dump(os.path.join(d, "service_registry_log.json"), registry)

    st = m.store
    ev = lambda *a, **k: [dict(e["payload"], ts=e["ts"], event=e["event_type"], instance_id=e["instance_id"],
                               logical_name=e["logical_name"], site_id=e["site_id"], operation_id=e["operation_id"])
                          for e in st.all_events(*a, **k)]
    lifecycle = [e["payload"] if "timestamp" in e["payload"] else
                 dict(e["payload"], ts=e["ts"], event=e["event_type"], instance_id=e["instance_id"])
                 for e in st.all_events("LIFECYCLE")]
    _dump(os.path.join(d, "process_lifecycle_log.json"), lifecycle)
    ops = st.operations()
    _dump(os.path.join(d, "migration_log.json"),
          {"operations": [o for o in ops if o["operation_type"] == "MIGRATION"], "events": ev("MIGRATION")})
    _dump(os.path.join(d, "replication_log.json"),
          {"operations": [o for o in ops if o["operation_type"] in ("REPLICATION", "FAILOVER")],
           "events": ev("REPLICATION")})
    _dump(os.path.join(d, "failure_events.json"),
          [dict(e["payload"], ts=e["ts"], site_id=e["site_id"], event=e["event_type"],
                instance_id=e["instance_id"]) for e in st.all_events("FAILURE")])
    _dump(os.path.join(d, "recovery_log.json"),
          {"events": ev("RECOVERY"), "failover_operations": [o for o in ops if o["operation_type"] == "FAILOVER"]})
    _dump(os.path.join(d, "name_resolution_log.json"), ev("RESOLUTION"))
    export_analytics(st, os.path.join(d, "analytics"))


def export_analytics(store, out_dir):
    """Flat exports for Spark / Power BI: events.jsonl, events.csv, operations.csv."""
    os.makedirs(out_dir, exist_ok=True)
    rows = store.all_events()
    with open(os.path.join(out_dir, "events.jsonl"), "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")
    if rows:
        with open(os.path.join(out_dir, "events.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            for r in rows:
                w.writerow({k: (json.dumps(v, default=str) if isinstance(v, (dict, list)) else v)
                            for k, v in r.items()})
    ops = store.operations()
    if ops:
        with open(os.path.join(out_dir, "operations.csv"), "w", newline="", encoding="utf-8") as f:
            cols = ["operation_id", "operation_type", "state", "source_site", "target_site", "instance_id",
                    "started_at", "updated_at", "duration_s", "downtime_s", "recovery_time_s", "payload"]
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for o in ops:
                p = o["payload"] if isinstance(o["payload"], dict) else {}
                w.writerow({**{c: o.get(c) for c in cols[:9]}, "duration_s": p.get("duration_s"),
                            "downtime_s": p.get("downtime_s"), "recovery_time_s": p.get("recovery_time_s"),
                            "payload": json.dumps(p, default=str)})
    return len(rows)
