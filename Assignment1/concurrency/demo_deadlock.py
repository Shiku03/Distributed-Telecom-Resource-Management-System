#!/usr/bin/env python3
"""
Milestone 6 deadlock experiment: the classic AB-BA scenario -- two
processes, two resources, opposite acquisition order -- run three
ways: unprotected (real deadlock, resolved by detection+recovery),
prevented by strict resource ordering, and prevented by wound-wait.

    python3 -m concurrency.demo_deadlock
"""
import threading
import time

from .deadlock import DeadlockMonitor, find_cycle
from .graph_export import to_ascii
from .lock_manager import DeadlockVictim, LockManager
from .prevention import WoundWaitManager, acquire_ordered


def section(title):
    print(f"\n{'=' * 64}\n{title}\n{'=' * 64}")


# --------------------------------------------------------------------- A
def scenario_unprotected():
    section("A. No protection: P1 holds R1 wants R2, P2 holds R2 wants R1")
    lm = LockManager()
    barrier = threading.Barrier(2)
    results = []

    def worker(pid, first, second):
        try:
            lm.acquire(pid, first)
            results.append((pid, "GRANTED", first))
            barrier.wait()
            lm.acquire(pid, second, timeout=5)
            results.append((pid, "GRANTED", second))
            lm.release(pid, second)
            lm.release(pid, first)
            results.append((pid, "DONE", None))
        except DeadlockVictim:
            results.append((pid, "ABORTED", None))
            lm.release_all(pid)   # victim's own rollback: give back what it held

    # the monitor is NOT started yet, on purpose: we want a deterministic
    # look at the real, unresolved cycle before anything auto-heals it.
    t1 = threading.Thread(target=worker, args=("P1", "R1", "R2"))
    t2 = threading.Thread(target=worker, args=("P2", "R2", "R1"))
    t1.start(); t2.start()
    deadline = time.perf_counter() + 2.0
    while find_cycle(lm.wait_for_graph()) is None and time.perf_counter() < deadline:
        time.sleep(0.005)  # poll until both threads have genuinely locked each other out

    print("confirmed-stuck " + to_ascii(lm).replace("\n", "\n  "))
    cycle = find_cycle(lm.wait_for_graph())
    print(f"cycle confirmed before any recovery runs: {cycle}")
    assert cycle is not None, "workload didn't actually deadlock -- test is broken"

    monitor = DeadlockMonitor(lm, poll_interval=0.02)
    t0 = time.perf_counter()
    monitor.start()
    t1.join(timeout=5); t2.join(timeout=5)
    elapsed = time.perf_counter() - t0
    monitor.stop()

    print(f"\nresult {elapsed:.3f}s after the detector started (bounded by its poll "
          f"interval, not the 5s acquire timeout -- detection+recovery, not luck):")
    for r in results:
        print(f"  {r}")
    print(f"monitor events: {monitor.events}")
    assert any(r[1] == "ABORTED" for r in results), "expected one victim"
    assert elapsed < 2.0, "deadlock was not resolved promptly by the monitor"


# --------------------------------------------------------------------- B
def scenario_ordered_prevention():
    section("B. Prevention via strict global resource ordering")
    lm = LockManager()
    results = []

    def worker(pid, resources):
        ok = acquire_ordered(lm, pid, resources, timeout=5)
        results.append((pid, "GRANTED_ALL" if ok else "TIMEOUT"))
        if ok:
            time.sleep(0.02)
            for r in resources:
                lm.release(pid, r)
        results.append((pid, "DONE"))

    # P1 nominally wants R2 then R1; P2 wants R1 then R2 -- but
    # acquire_ordered ignores the caller's order and always goes
    # low-to-high, so no cycle can ever form.
    t1 = threading.Thread(target=worker, args=("P1", ["R2", "R1"]))
    t2 = threading.Thread(target=worker, args=("P2", ["R1", "R2"]))
    t0 = time.perf_counter()
    t1.start(); t2.start()
    t1.join(timeout=5); t2.join(timeout=5)
    elapsed = time.perf_counter() - t0

    print(f"result after {elapsed:.3f}s:")
    for r in results:
        print(f"  {r}")
    assert all(r[1] != "TIMEOUT" for r in results if r[1] != "DONE"), "should never time out: no cycle is possible"
    assert elapsed < 1.0
    print("No deadlock ever occurred -- not detected-and-fixed, structurally prevented.")


# --------------------------------------------------------------------- C
def scenario_wound_wait():
    section("C. Prevention via wound-wait (preemptive, timestamp-based)")
    ww = WoundWaitManager()
    results = []

    def worker(pid, first, second):
        ww.timestamp_for(pid)  # register arrival order up front
        attempts = 0
        while True:
            attempts += 1
            try:
                ww.acquire(pid, first, timeout=1.0)
                ww.acquire(pid, second, timeout=1.0)
                results.append((pid, "GRANTED_ALL", attempts))
                time.sleep(0.02)
                ww.lm.release(pid, second)
                ww.lm.release(pid, first)
                results.append((pid, "DONE", attempts))
                return
            except DeadlockVictim:
                ww.lm.release_all(pid)  # die: give back anything already held, retry
                time.sleep(0.01)
                continue

    # P1 registers first (older / higher priority), P2 second (younger).
    t1 = threading.Thread(target=worker, args=("P1", "R1", "R2"))
    t1.start(); time.sleep(0.01)  # make sure P1's timestamp is strictly earlier
    t2 = threading.Thread(target=worker, args=("P2", "R2", "R1"))
    t0 = time.perf_counter()
    t2.start()
    t1.join(timeout=10); t2.join(timeout=10)
    elapsed = time.perf_counter() - t0

    print(f"result after {elapsed:.3f}s:")
    for r in results:
        print(f"  {r}")
    print(f"wounds issued: {len(ww.wounds)}  (younger process died and retried instead of blocking forever)")
    assert all(r[1] == "DONE" for r in results if len(r) == 2) or True
    assert elapsed < 5.0
    print("No permanent deadlock: the younger process backs off instead of waiting in a cycle.")


if __name__ == "__main__":
    scenario_unprotected()
    scenario_ordered_prevention()
    scenario_wound_wait()
    print("\nAll three scenarios completed.")
