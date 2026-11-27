"""
Two deadlock PREVENTION strategies for Milestone 6 (as opposed to
detection+recovery in deadlock.py -- the milestone asks for "detection,
prevention, OR recovery"; this project implements all three so they
can be compared head to head in bench_concurrency.py).

1. acquire_ordered: break the "circular wait" Coffman condition by
   forcing every process to request resources in one global, fixed
   order. If everyone acquires low-to-high, a cycle P1->P2->...->Pk->P1
   would require some process to hold a higher-numbered resource while
   requesting a lower-numbered one from a process that itself is
   waiting on something higher -- which can't happen if acquisition
   order is globally consistent. No detection needed; it's structurally
   impossible.

2. WoundWaitManager: break "no preemption" instead, via the classic
   timestamp-based scheme. Give every process a unique, strictly
   increasing timestamp when it starts (older timestamp = higher
   priority). When Pi requests a resource held by Pj:
     - if Pi is OLDER than Pj: Pi "wounds" Pj (Pj is forced to abort
       and release, i.e. preempted) and Pi waits for the resource.
     - if Pi is YOUNGER than Pj: Pi "dies" (aborts itself immediately)
       rather than wait, and must retry later with the SAME timestamp
       (so it eventually becomes the oldest and can't starve).
   This guarantees no cycle can ever form: along any wait edge Pi->Pj,
   Pi is always older than Pj, so a cycle would require a strictly
   decreasing-then-wrapping timestamp sequence, which is impossible.
"""
import itertools
import threading
import time

from .lock_manager import DeadlockVictim, LockManager


def acquire_ordered(lock_manager: LockManager, process_id, resource_ids, timeout=None):
    """Acquire every resource in `resource_ids`, always in the same
    global order, regardless of the order the caller listed them in.
    Returns True if all were granted, False (with everything already
    granted released again) on timeout."""
    ordered = sorted(resource_ids)
    held = []
    for r in ordered:
        ok = lock_manager.acquire(process_id, r, timeout=timeout)
        if not ok:
            for h in reversed(held):
                lock_manager.release(process_id, h)
            return False
        held.append(r)
    return True


class _Ticker:
    _counter = itertools.count()

    @classmethod
    def next(cls):
        return next(cls._counter)


class WoundWaitManager:
    """Wraps a LockManager with the wound-wait preemptive scheme. Each
    process must call `timestamp_for(process_id)` once (or let
    `acquire` assign one lazily) before its first request."""

    def __init__(self, lock_manager: LockManager = None):
        self.lm = lock_manager or LockManager()
        self._ts = {}
        self._lock = threading.Lock()
        self.wounds = []   # (ts, wounded_process, by_process) log, for analysis

    def timestamp_for(self, process_id) -> int:
        with self._lock:
            if process_id not in self._ts:
                self._ts[process_id] = _Ticker.next()
            return self._ts[process_id]

    def acquire(self, process_id, resource_id, timeout=2.0):
        """Returns True if granted. Raises DeadlockVictim if this
        process was younger and must "die" (abort) rather than wait --
        same contract as LockManager's own DeadlockVictim, so recovery
        code can treat both strategies identically."""
        my_ts = self.timestamp_for(process_id)
        while True:
            with self.lm._cv:
                holder = self.lm.holders.get(resource_id)
                if holder is None or holder == process_id:
                    pass  # fall through to the real acquire below
                else:
                    holder_ts = self.timestamp_for(holder)
                    if my_ts < holder_ts:
                        # I'm older -> wound the holder (force it to abort)
                        self.lm.signal_abort(holder)
                        self.wounds.append((time.time(), holder, process_id))
                    else:
                        # I'm younger -> die immediately rather than wait
                        raise DeadlockVictim(process_id)
            got = self.lm.acquire(process_id, resource_id, timeout=timeout)
            if got:
                return True
            # timed out still contending (the wounded holder hasn't
            # released yet) -- loop and re-check who holds it now.
