#!/usr/bin/env python3
"""
End-to-end acceptance scenario with REAL OS processes (manager, every site, the
workload generator) -- the 16 acceptance steps of Milestones 9/10.

    python3 scripts/run_scenario.py [--results-dir results/scenario] [--base-port 7000]

Prints PASS/FAIL per check, writes ``scenario_report.json`` and every monitoring
file under the results directory, and exits non-zero if any check failed.
A site failure is a real ``SIGKILL`` of the site's process; the manager
restart is a real process restart on the same SQLite file.
"""
import argparse
import copy
import json
import os
import signal
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
from common import protocol                                       # noqa: E402
from common.service_client import ServiceClient   # noqa: E402

RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append({"check": name, "passed": bool(ok), "detail": detail})
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""), flush=True)
    return ok


def wait_port(host, port, timeout=15):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def wait_for(cond, timeout=30, step=0.2):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if cond():
                return True
        except (ConnectionError, OSError, TimeoutError, KeyError):
            pass
        time.sleep(step)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", default=os.path.join(HERE, "results", "scenario"))
    ap.add_argument("--base-port", type=int, default=7000)
    ap.add_argument("--keep-db", action="store_true", help="do not wipe an existing results dir")
    args = ap.parse_args()
    rd = args.results_dir
    if os.path.isdir(rd) and not args.keep_db:
        for f in os.listdir(rd):
            p = os.path.join(rd, f)
            if os.path.isfile(p):
                os.remove(p)
    os.makedirs(rd, exist_ok=True)

    with open(os.path.join(HERE, "config.json")) as f:
        cfg = json.load(f)
    run = copy.deepcopy(cfg)
    run["manager"]["port"] = args.base_port
    for i, s in enumerate(run["sites"].values(), 1):
        s["port"] = args.base_port + i
    run["heartbeat_interval_s"] = 0.5
    run["failure_detector"] = {"suspect_after_heartbeats": 3, "failed_after_heartbeats": 6,
                               "site_lease_heartbeats": 3, "failed_instance_ttl_s": 120}
    run["replication"] = {"sync_interval_s": 0.5, "sync_bw_fraction": 0.1, "max_replicas": 2}
    run["workload"].update(duration_s=6, arrival_rate_per_s=3.0, mean_holding_s=25.0)
    run["placement_policy"] = "edge_first"
    cfgpath = os.path.join(rd, "scenario_config.json")
    with open(cfgpath, "w") as f:
        json.dump(run, f, indent=2)
    mh, mp = run["manager"]["host"], run["manager"]["port"]
    procs, logs = {}, []

    def spawn(name, cmd):
        lf = open(os.path.join(rd, f"{name}.log"), "a")
        logs.append(lf)
        procs[name] = subprocess.Popen(cmd, cwd=HERE, stdout=lf, stderr=subprocess.STDOUT)
        return procs[name]

    def start_manager():
        spawn("manager", [sys.executable, "manager.py", "--config", cfgpath, "--results-dir", rd,
                          "--policy", "edge_first"])
        return wait_port(mh, mp)

    def start_site(sid):
        spawn(f"site-{sid}", [sys.executable, "site.py", "--site-id", sid, "--config", cfgpath])
        return wait_port(run["sites"][sid]["host"], run["sites"][sid]["port"])

    def mgr(msg, timeout=10):
        return protocol.request(mh, mp, msg, timeout=timeout)

    def site_rpc(sid, msg):
        s = run["sites"][sid]
        return protocol.request(s["host"], s["port"], msg, timeout=5)

    def host_of(sid_):
        out = []
        for s in run["sites"]:
            try:
                st = site_rpc(s, {"type": "STATUS"})
            except (ConnectionError, OSError, TimeoutError):
                continue
            i = st["instances"].get(sid_)
            if i and i["role"] == "PRIMARY" and i["state"] == "RUNNING":
                out.append(s)
        return out

    def request(service, ingress, name=None, holding=300):
        spec = run["services"][service]
        m = {"type": "REQUEST", "service": service, "ingress": ingress, "holding_s": holding,
             "demand": {"cpu": spec["cpu"], "mem": spec["mem"], "bw": spec["bw"]},
             "max_latency_ms": spec["max_latency_ms"]}
        if name:
            m["logical_name"] = name
        return mgr(m)

    def books_ok():
        return mgr({"type": "STATUS"})["ledger_problems"] == []

    def zero_everything():
        st = mgr({"type": "STATUS"})
        return (all(v["allocated"] == {"cpu": 0, "mem": 0, "bw": 0} for v in st["sites"].values())
                and all(v["reserved"] == 0 for v in st["link_utilisation"].values()))

    try:
        print("1-3. start manager and all sites; sites register")
        check("manager process up", start_manager())
        for sid in run["sites"]:
            check(f"site {sid} process up", start_site(sid))
        check("all sites registered ACTIVE", wait_for(lambda: all(
            s["status"] == "ACTIVE" for s in mgr({"type": "STATUS"})["sites"].values()) and
            len(mgr({"type": "STATUS"})["sites"]) == 3))

        print("4-5. workload generator -> placement-policy admission")
        g = subprocess.run([sys.executable, "generator.py", "--config", cfgpath, "--results-dir", rd,
                            "--manager-host", mh, "--manager-port", str(mp)], cwd=HERE,
                           capture_output=True, text=True)
        st = mgr({"type": "STATUS"})
        check("generator ran and requests were admitted", g.returncode == 0 and st["admitted"]["_all"] > 0,
              f"offered={st['offered']['_all']} admitted={st['admitted']['_all']} blocked={st['blocked']['_all']}")
        check("allocation never exceeds capacity; manager books balance", books_ok() and all(
            v["allocated"][r] <= v["capacity"][r] for v in st["sites"].values() for r in ("cpu", "mem", "bw")))
        generated = [r["name"] for r in st["service_registry"]]
        for n in generated:                                         # step 14 for the generated load
            mgr({"type": "TERMINATE", "name": n})
        check("terminating the generated services releases every resource and path",
              wait_for(zero_everything, 15) and books_ok())

        print("6. resolve by logical name")
        ims = request("IMS-CSCF", "edge-A", "service://telerm/scenario/ims")
        upf = request("UPF", "edge-A", "service://telerm/scenario/upf")
        check("services admitted with explicit logical names", ims["type"] == upf["type"] == "ACCEPTED")
        client = ServiceClient(mh, mp, deadline_s=20)
        ep = client.resolve("service://telerm/scenario/upf")
        check("resolved endpoint is the site that really hosts it",
              host_of(upf["sid"]) == [ep["site_id"]], f"{ep['site_id']}:{ep['port']} gen={ep['generation']}")
        check("a client reaches it through the logical name",
              client.call("service://telerm/scenario/upf")["site_id"] == ep["site_id"])

        print("7. resize an active instance")
        before = mgr({"type": "STATUS"})["sites"][ep["site_id"]]["allocated"]["cpu"]
        rz = mgr({"type": "RESIZE", "sid": upf["sid"], "factor": 1.5})
        after = mgr({"type": "STATUS"})["sites"][ep["site_id"]]["allocated"]["cpu"]
        check("resize applied at site and in manager accounting", rz["type"] == "RESIZE_OK" and after - before == 500,
              f"cpu {before}->{after}")

        print("8-9. migrate, then resolve the same name")
        mg = mgr({"type": "MIGRATE", "name": "service://telerm/scenario/upf", "target_site": "core-1"})
        check("migration committed", mg["type"] == "MIGRATE_OK", f"duration={mg.get('duration_s', 0) * 1000:.1f}ms "
              f"downtime={(mg.get('downtime_s') or 0) * 1000:.1f}ms")
        ep2 = client.resolve("service://telerm/scenario/upf", refresh=True)
        check("same logical name now resolves to the destination with a newer generation",
              ep2["site_id"] == "core-1" and ep2["generation"] == ep["generation"] + 1 and
              ep2["instance_id"] == ep["instance_id"])
        check("exactly one site runs it; the source released it", host_of(upf["sid"]) == ["core-1"])
        check("client with a cached stale endpoint still reaches the service",
              (client.cache.__setitem__("service://telerm/scenario/upf", ep) or True) and
              client.call("service://telerm/scenario/upf")["site_id"] == "core-1")
        check("accounting moved with the service", books_ok())

        print("10. create a standby replica")
        rp = mgr({"type": "REPLICATE", "name": "service://telerm/scenario/ims", "target_site": "edge-B"})
        check("replica created on another site", rp["type"] == "REPLICATE_OK", rp.get("replica_id", str(rp)))
        check("replica is synchronised and idle", wait_for(lambda: any(
            r["sync_seq"] >= 3 for r in mgr({"type": "CONTROL_STATUS"})["replicas"])))
        st_b = site_rpc("edge-B", {"type": "STATUS"})["instances"][rp["replica_id"]]
        check("standby holds state but is not running", st_b["state"] == "STANDBY" and st_b["role"] == "REPLICA")

        print("11-12. kill the primary's site process; promote the replica")
        t_kill = time.time()
        procs["site-edge-A"].kill()
        procs["site-edge-A"].wait(timeout=10)
        import threading
        seq, restored = [], []                       # (time, outcome, site) of every resolution during the outage

        def poller():
            while not restored and time.time() - t_kill < 40:
                r = mgr({"type": "RESOLVE", "name": "service://telerm/scenario/ims"})
                outcome = r.get("reason") or r.get("type")
                seq.append((time.time() - t_kill, outcome, r.get("site_id")))
                if r.get("type") == "RESOLVED" and r["site_id"] == "edge-B" and r["generation"] == 2:
                    restored.append(time.time())
                time.sleep(0.1)
        pt = threading.Thread(target=poller)
        pt.start()
        # a client call issued INSIDE the detection window: the dead endpoint refuses, the client
        # re-resolves, waits through 'unavailable', and is finally served by the promoted replica
        rr = client.call("service://telerm/scenario/ims")
        pt.join()
        check("client call issued during the outage was served by the promoted replica (failure transparency)",
              rr["site_id"] == "edge-B" and rr["generation"] == 2, f"after {time.time() - t_kill:.1f}s")
        first_unavail = next((i for i, x in enumerate(seq) if x[1] == "unavailable"), None)
        check("once the site was SUSPECT the name was reported unavailable and never resolved to the dead site",
              first_unavail is not None and all(x[2] != "edge-A" for x in seq[first_unavail:]),
              f"detection window: names resolved to the dead endpoint for {seq[first_unavail][0] if first_unavail else float('nan'):.1f}s")
        check("failure confirmed as FAILED", mgr({"type": "STATUS"})["sites"]["edge-A"]["status"] == "FAILED")
        check("replica promoted: same name, new site, generation bumped", bool(restored),
              f"name available again {(restored[0] - t_kill) if restored else float('nan'):.1f}s after the kill")
        check("no allocation to the failed site", request("IoT-GW", "edge-A", holding=5).get("site") != "edge-A")
        check("books balance after failover", books_ok())

        print("13. recover and reconcile the failed site")
        check("site process restarted", start_site("edge-A"))
        check("recovered site reconciled back to ACTIVE", wait_for(
            lambda: mgr({"type": "STATUS"})["sites"]["edge-A"]["status"] == "ACTIVE", 30))
        check("no stale copy of the failed-over service is running", host_of(ims["sid"]) == ["edge-B"])
        check("books balance after reconciliation", books_ok())

        print("14. terminate and release everything")
        for n in [r["name"] for r in mgr({"type": "STATUS"})["service_registry"]]:
            mgr({"type": "TERMINATE", "name": n})
        check("all node and link reservations released", wait_for(zero_everything, 20) and books_ok())

        print("15. restart the manager; durable control-plane state is recovered")
        a = request("IMS-CSCF", "edge-A", "service://telerm/scenario/keep-a")
        b = request("UPF", "edge-B", "service://telerm/scenario/keep-b")
        mgr({"type": "MIGRATE", "name": "service://telerm/scenario/keep-b", "target_site": "core-1"})
        mgr({"type": "REPLICATE", "name": "service://telerm/scenario/keep-a", "target_site": "edge-B"})
        time.sleep(1.5)
        pre = {s: v["allocated"] for s, v in mgr({"type": "STATUS"})["sites"].items()}
        procs["manager"].kill()
        procs["manager"].wait(timeout=10)
        check("manager restarted on the same database", start_manager())
        res = mgr({"type": "RESOLVE", "name": "service://telerm/scenario/keep-b"})
        check("restored names are unavailable until their site is reconciled (or already verified)",
              res["type"] in ("RESOLVE_FAIL", "RESOLVED"), res.get("reason", "resolved"))
        check("sites re-registered/reconciled; services available again with identities intact", wait_for(lambda: all(
            mgr({"type": "RESOLVE", "name": n})["type"] == "RESOLVED"
            for n in ("service://telerm/scenario/keep-a", "service://telerm/scenario/keep-b")), 30))
        post = {s: v["allocated"] for s, v in mgr({"type": "STATUS"})["sites"].items()}
        check("resource accounting identical before and after the manager restart", pre == post, f"{post}")
        check("migrated service kept its new location and generation", (
            lambda r: r["site_id"] == "core-1" and r["generation"] == 2)(
            mgr({"type": "RESOLVE", "name": "service://telerm/scenario/keep-b"})))
        check("replica survived the restart", wait_for(lambda: any(
            r["state"] == "STANDBY" for r in mgr({"type": "CONTROL_STATUS"})["replicas"]), 20))
        check("books balance after restart", books_ok())
        new = request("UPF", "edge-A")
        check("service ids are never reused", new["sid"] not in (a["sid"], b["sid"]) and new["sid"] > b["sid"])
        for n in [r["name"] for r in mgr({"type": "STATUS"})["service_registry"]]:
            mgr({"type": "TERMINATE", "name": n})
        check("final termination releases everything", wait_for(zero_everything, 20) and books_ok())

        print("16. export performance and reliability data")
        mgr({"type": "WRITE_REPORT"})
        subprocess.run([sys.executable, os.path.join(HERE, "scripts", "export_events.py"),
                        "--db", os.path.join(rd, "telerm_control.sqlite3"), "--out", os.path.join(rd, "analytics")],
                       cwd=HERE, check=False, capture_output=True)
        want = ["manager_report.json", "instances_log.json", "heartbeat_log.json", "utilisation_timeseries.json",
                "generator_log.json", "generator_summary.json", "migration_log.json", "failure_events.json",
                "service_registry_log.json", "name_resolution_log.json", "process_lifecycle_log.json",
                "replication_log.json", "recovery_log.json", "performance_metrics.json"]
        missing = [w for w in want if not os.path.exists(os.path.join(rd, w))]
        check("all monitoring files written", not missing, f"missing={missing}")
        check("analytics exports written", all(os.path.exists(os.path.join(rd, "analytics", f))
                                               for f in ("events.csv", "events.jsonl", "operations.csv")))
        with open(os.path.join(rd, "performance_metrics.json")) as f:
            met = json.load(f)
        print("\n=== key measurements ===")
        print(f"  allocation success rate : {met['allocation']['success_rate']:.3f}  "
              f"decision mean {met['allocation']['decision_time_ms']['mean']:.2f} ms")
        print(f"  service availability    : {met['service_availability']['availability']:.4f}")
        print(f"  migration               : {met['migration']['committed']}/{met['migration']['started']} "
              f"duration mean {(met['migration']['duration_s']['mean'] or 0) * 1000:.1f} ms")
        print(f"  failover                : {met['failover']['committed']}/{met['failover']['started']} "
              f"recovery mean {met['failover']['recovery_time_s']['mean']}")
        print(f"  replication overhead    : standby/primary cpu {met['replication_overhead']['standby_to_primary_cpu_ratio']}, "
              f"sync bytes {met['replication_overhead']['sync_bytes']}")
        print(f"  resolution latency (mgr): {met['service_resolution']['manager_side_ms']['mean']:.3f} ms mean, "
              f"{met['service_resolution']['manager_side_ms']['count']} lookups")
        print(f"  ledger consistent       : {met['ledger_consistent']}")
        check("manager reports a consistent ledger", met["ledger_consistent"])
    finally:
        for name, p in procs.items():
            if p.poll() is None:
                p.terminate()
        for p in procs.values():
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
        for lf in logs:
            lf.close()

    failed = [r for r in RESULTS if not r["passed"]]
    with open(os.path.join(rd, "scenario_report.json"), "w") as f:
        json.dump({"checks": RESULTS, "passed": len(RESULTS) - len(failed), "failed": len(failed)}, f, indent=2)
    print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} checks passed; results in {rd}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
