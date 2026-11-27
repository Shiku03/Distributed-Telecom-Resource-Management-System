#!/usr/bin/env python3
"""
Milestone 6 deliverable: 'Concurrent distributed implementation' at
realistic scale (N processes, not just the 2-process AB-BA demo) plus
'Resource utilization analysis', comparing three strategies head to
head: naive (detection+recovery only), ordering-prevention, wound-wait
prevention.

Each simulated process repeatedly: picks 2-3 resources in a RANDOM
order (so cycles can genuinely form under "naive"), holds them for a
short time, releases, and repeats, for a fixed wall-clock duration.

    python3 -m concurrency.bench_concurrency
    python3 -m concurrency.bench_concurrency --processes 12 --resources 6 --duration 5
"""
import argparse
import csv
import json
import os
import random
import threading
import time

from .deadlock import DeadlockMonitor
from .lock_manager import DeadlockVictim, LockManager
from .prevention import WoundWaitManager, acquire_ordered

HERE = os.path.dirname(os.path.abspath(__file__))


def run_strategy(strategy: str, n_processes: int, n_resources: int, duration: float,
                  seed: int, hold_s: float = 0.01):
    """Returns a summary dict. `strategy` in {'naive', 'ordered', 'wound_wait'}."""
    resources = [f"R{i}" for i in range(n_resources)]
    lm = LockManager()
    ww = WoundWaitManager(lm) if strategy == "wound_wait" else None
    monitor = DeadlockMonitor(lm, poll_interval=0.01) if strategy == "naive" else None
    if monitor:
        monitor.start()

    stop_at = time.perf_counter() + duration
    completions = [0] * n_processes
    aborts = [0] * n_processes
    latencies = []
    lat_lock = threading.Lock()

    def worker(pid_idx):
        pid = f"P{pid_idx}"
        rng = random.Random(f"{seed}-{pid}")
        while time.perf_counter() < stop_at:
            k = rng.randint(2, min(3, n_resources))
            want = rng.sample(resources, k)
            t0 = time.perf_counter()
            try:
                if strategy == "ordered":
                    ok = acquire_ordered(lm, pid, want, timeout=2.0)
                    if not ok:
                        continue
                elif strategy == "wound_wait":
                    ww.timestamp_for(pid)
                    for r in want:  # requested order is deliberately NOT sorted
                        ww.acquire(pid, r, timeout=2.0)
                else:  # naive: no protection beyond detection+recovery
                    for r in want:
                        ok = lm.acquire(pid, r, timeout=2.0)
                        if not ok:
                            lm.release_all(pid)
                            raise TimeoutError
                time.sleep(hold_s)
                for r in want:
                    lm.release(pid, r)
                with lat_lock:
                    latencies.append(time.perf_counter() - t0)
                completions[pid_idx] += 1
            except DeadlockVictim:
                lm.release_all(pid)
                aborts[pid_idx] += 1
                # a real process backs off before retrying rather than
                # immediately re-contending for the same resource; without
                # this, wound-wait's "die and retry" path busy-spins.
                time.sleep(rng.uniform(0.001, 0.005))
            except TimeoutError:
                aborts[pid_idx] += 1

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_processes)]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=duration + 5)
    wall_s = time.perf_counter() - t_start
    if monitor:
        monitor.stop()

    util = lm.utilisation()  # resource -> seconds held
    total_completions = sum(completions)
    total_aborts = sum(aborts)
    summary = {
        "strategy": strategy, "processes": n_processes, "resources": n_resources,
        "duration_s": round(wall_s, 3),
        "completions": total_completions,
        "aborts": total_aborts,
        "throughput_per_s": round(total_completions / wall_s, 2),
        "abort_rate": round(total_aborts / (total_completions + total_aborts), 4) if (total_completions + total_aborts) else 0.0,
        "latency_ms_mean": round(1000 * sum(latencies) / len(latencies), 3) if latencies else None,
        "deadlocks_detected": len(monitor.events) if monitor else 0,
        "wounds_issued": len(ww.wounds) if ww else 0,
        "mean_resource_utilisation_pct": round(
            100 * sum(util.values()) / (n_resources * wall_s), 2) if wall_s > 0 else 0.0,
        "per_resource_utilisation_pct": {r: round(100 * v / wall_s, 2) for r, v in util.items()},
    }
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--processes", type=int, default=8)
    ap.add_argument("--resources", type=int, default=4)
    ap.add_argument("--duration", type=float, default=4.0, help="seconds per strategy")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=os.path.join(HERE, "..", "results_concurrency"))
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    print(f"[bench_concurrency] {args.processes} processes x {args.resources} resources, "
          f"{args.duration}s per strategy (fewer resources than processes -> real contention)", flush=True)

    summaries = []
    for strategy in ("naive", "ordered", "wound_wait"):
        s = run_strategy(strategy, args.processes, args.resources, args.duration, args.seed)
        summaries.append(s)
        print(f"  {strategy:10s} throughput={s['throughput_per_s']:>7.1f}/s  "
              f"completions={s['completions']:<6d} aborts={s['aborts']:<5d} "
              f"abort_rate={s['abort_rate']:.1%}  deadlocks_detected={s['deadlocks_detected']:<3d} "
              f"wounds={s['wounds_issued']:<4d} mean_util={s['mean_resource_utilisation_pct']:.1f}%", flush=True)

    with open(os.path.join(args.out, "concurrency_summary.json"), "w") as f:
        json.dump(summaries, f, indent=2)
    fields = [k for k in summaries[0] if k != "per_resource_utilisation_pct"]
    with open(os.path.join(args.out, "concurrency_summary.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for s in summaries:
            w.writerow({k: s[k] for k in fields})

    naive = next(s for s in summaries if s["strategy"] == "naive")
    assert naive["deadlocks_detected"] > 0, \
        "naive strategy produced zero detected deadlocks -- increase --processes or lower --resources"
    for s in summaries:
        if s["strategy"] != "naive":
            assert s["deadlocks_detected"] == 0, f"{s['strategy']} should structurally prevent deadlocks"
    print(f"\n[bench_concurrency] results in {args.out}")
    print(f"[bench_concurrency] sanity check passed: naive hit {naive['deadlocks_detected']} real "
          f"deadlocks; both prevention strategies hit zero.")


if __name__ == "__main__":
    main()
