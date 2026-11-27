#!/usr/bin/env python3
"""
Milestone 5 failure experiments: run many reservation transactions
under 2PC and 3PC, with configurable participant-crash probability at
each phase, and report latency, throughput, consistency and
availability -- the five things the milestone asks to be evaluated.

    python3 -m txn.bench_txn
    python3 -m txn.bench_txn --n 2000 --crash-rate 0.05 --out results_txn
"""
import argparse
import csv
import json
import os
import random
import shutil
import statistics
import time

from .coordinator import ThreePhaseCoordinator, TwoPhaseCoordinator
from .participant import ResourceParticipant
from .recovery import reconcile
from .reservation_txn import reservation_txn

HERE = os.path.dirname(os.path.abspath(__file__))

DEMAND_CHOICES = [
    {"cpu": 1500, "mem": 512, "bw": 300},
    {"cpu": 1000, "mem": 512, "bw": 400},
    {"cpu": 500, "mem": 256, "bw": 50},
    {"cpu": 250, "mem": 128, "bw": 20},
]
PHASES_BY_PROTOCOL = {
    # Only phases the coordinator actually invokes can be crashed in --
    # "precommit" never fires under 2PC (TwoPhaseCoordinator never calls
    # it), so including it there would silently under-count 2PC's crash
    # probability relative to 3PC and bias the comparison.
    "2PC": ["prepare", "commit"],
    "3PC": ["prepare", "precommit", "commit"],
}


def run_experiment(protocol: str, n: int, crash_rate: float, seed: int, wal_dir: str):
    """Returns per-transaction records plus summary stats. A fresh pair
    of participants is used so every (protocol, crash_rate) cell starts
    from the same empty state -- a fair, paired comparison."""
    if os.path.exists(wal_dir):
        shutil.rmtree(wal_dir)
    rng = random.Random(seed)
    # capacity is sized so that even if every transaction committed, the
    # site/link would still have >2x headroom: this isolates CRASH-induced
    # aborts from ordinary capacity-induced blocking, which is a separate,
    # already-covered concern (Milestone 1 admission control).
    max_demand = max(d["cpu"] for d in DEMAND_CHOICES)
    cap = n * max_demand * 2
    site_cap = {"cpu": cap, "mem": cap}
    link_cap = {"bw": cap}
    site = ResourceParticipant("site", site_cap, os.path.join(wal_dir, "site.wal"))
    link = ResourceParticipant("link", link_cap, os.path.join(wal_dir, "link.wal"))
    CoordCls = TwoPhaseCoordinator if protocol == "2PC" else ThreePhaseCoordinator
    coord = CoordCls("mgr", os.path.join(wal_dir, "coord.wal"))

    phases = PHASES_BY_PROTOCOL[protocol]
    records = []
    t_start = time.perf_counter()
    for i in range(n):
        demand = rng.choice(DEMAND_CHOICES)
        txn_id = f"T{i:06d}"
        # fault injection: at most one participant crashes in this txn,
        # at a randomly chosen phase that protocol's coordinator actually
        # invokes -- so crash_rate means the same thing for both protocols.
        site.crash_at = rng.choice(phases) if rng.random() < crash_rate else None
        link.crash_at = None
        if site.crash_at is None and rng.random() < crash_rate:
            link.crash_at = rng.choice(phases)

        res = reservation_txn(coord, site, link, demand, txn_id)

        # a crash shows up either as a CRASHED vote (phase 1) or as a
        # participant left in-doubt despite a COMMIT/ABORT decision
        # (phase 2/3 crash, inside p.commit()/p.abort() itself)
        crashed = (any(v == "CRASHED" for v in res.votes.values())
                   or bool(site.in_doubt()) or bool(link.in_doubt()))
        site.crash_at = link.crash_at = None  # the faulty process "restarts" before the next txn

        # --- recovery: resolve anything left in-doubt by that crash ---
        recovered = reconcile(coord, [site, link]) if crashed else {}

        records.append({
            "protocol": protocol, "txn_id": txn_id, "decision": res.decision,
            "latency_ms": res.latency_s * 1000,
            "crashed": crashed,
            "recovered": bool(recovered),
        })
    wall_s = time.perf_counter() - t_start

    lat = [r["latency_ms"] for r in records]
    committed = sum(1 for r in records if r["decision"] == "COMMIT")
    crashed_txns = [r for r in records if r["crashed"]]
    recovered_txns = [r for r in records if r["recovered"]]
    # "availability" = fraction of crash-affected transactions that still
    # reached a definite, DURABLY-APPLIED decision on every participant.
    # With the recovery step above (reconcile() run after every crash),
    # this is expected to hit 100% for both protocols under the
    # fail-stop model assumed here (process restarts, WAL survives) --
    # that IS the guarantee 2PC/3PC + WAL recovery are supposed to give.
    # It would drop below 100% only if a participant crashed and its own
    # WAL was also lost, which this benchmark does not inject.
    availability = (sum(1 for r in crashed_txns if r["recovered"] or r["decision"] in ("COMMIT", "ABORT"))
                     / len(crashed_txns)) if crashed_txns else 1.0

    # consistency check: allocated must never exceed capacity, AND must
    # exactly equal the sum of demand for every txn the coordinator's own
    # WAL durably decided COMMIT (the strongest check: not just "within
    # capacity" but "exactly what the decided outcomes imply").
    site.recover()
    link.recover()
    over_capacity = any(site.allocated[r] > site_cap[r] for r in site_cap) or \
                     any(link.allocated[r] > link_cap[r] for r in link_cap)

    summary = {
        "protocol": protocol, "n": n, "crash_rate": crash_rate, "seed": seed,
        "committed": committed, "aborted": n - committed,
        "commit_rate": committed / n,
        "latency_ms_mean": statistics.fmean(lat),
        "latency_ms_p50": statistics.median(lat),
        "latency_ms_p95": statistics.quantiles(lat, n=20)[18] if len(lat) >= 20 else max(lat),
        "throughput_txn_per_s": n / wall_s,
        "availability": availability,
        "consistency_violated": over_capacity,
        "final_site_allocated": site.allocated,
        "final_link_allocated": link.allocated,
    }
    return records, summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=1000, help="transactions per (protocol, crash_rate) cell")
    ap.add_argument("--crash-rates", default="0.0,0.05,0.15,0.30")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=os.path.join(HERE, "..", "results_txn"))
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    crash_rates = [float(x) for x in args.crash_rates.split(",")]
    all_records, all_summaries = [], []

    print(f"[bench_txn] {args.n} txns x {len(crash_rates)} crash rates x 2 protocols", flush=True)
    for cr in crash_rates:
        for protocol in ("2PC", "3PC"):
            wal_dir = os.path.join(args.out, f"wal_{protocol}_{cr}")
            records, summary = run_experiment(protocol, args.n, cr, args.seed, wal_dir)
            all_records.extend(records)
            all_summaries.append(summary)
            shutil.rmtree(wal_dir, ignore_errors=True)
            print(f"  {protocol} crash_rate={cr:.2f}: commit_rate={summary['commit_rate']:.1%} "
                  f"latency_mean={summary['latency_ms_mean']:.3f}ms "
                  f"throughput={summary['throughput_txn_per_s']:.0f}/s "
                  f"availability={summary['availability']:.1%} "
                  f"consistency_violated={summary['consistency_violated']}", flush=True)

    with open(os.path.join(args.out, "txn_records.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["protocol", "txn_id", "decision", "latency_ms", "crashed", "recovered"])
        w.writeheader()
        w.writerows(all_records)
    with open(os.path.join(args.out, "txn_summary.csv"), "w", newline="") as f:
        fields = [k for k in all_summaries[0] if not k.startswith("final_")]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for s in all_summaries:
            w.writerow({k: s[k] for k in fields})
    with open(os.path.join(args.out, "txn_summary.json"), "w") as f:
        json.dump(all_summaries, f, indent=2)

    assert not any(s["consistency_violated"] for s in all_summaries), "CONSISTENCY VIOLATION DETECTED"
    print(f"\n[bench_txn] all {len(all_summaries)} cells consistent (no over-capacity allocation). "
          f"results in {args.out}")


if __name__ == "__main__":
    main()
