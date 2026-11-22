"""
Per-resource exclusive lock manager for Milestone 6. This is the thing
that makes race conditions and deadlocks *possible* in the first
place: multiple threads (standing in for Pi processes/services) can
request the same resource, and must wait if it's already held.

Every grant/release is tracked so a resource-allocation graph (Pi -> Rj
for a pending request, Rj -> Pi for a current allocation) can be built
at any moment -- that graph is what deadlock detection runs on.
"""
import collections
import threading
import time


class DeadlockVictim(Exception):
    """Raised inside a blocked acquire() when this process has been
    chosen as the victim to break a detected deadlock cycle. The
    caller is expected to treat this exactly like a transaction abort:
    release whatever it already holds, undo any tentative work, and
    (usually) retry from the start."""
    def __init__(self, process_id):
        super().__init__(f"{process_id} aborted to break a deadlock cycle")
        self.process_id = process_id


class LockManager:
    def __init__(self):
        self._cv = threading.Condition()
        self.holders = {}                               # resource -> process_id
        self.waiters = collections.defaultdict(list)     # resource -> [process_id, ...] FIFO
        self.held_by_process = collections.defaultdict(set)
        self.wait_since = {}                             # (process, resource) -> ts
        self.grant_log = []                               # (event, process, resource, ts) for analysis
        self.wait_time_total = collections.defaultdict(float)   # resource -> cumulative wait seconds
        self.hold_time_total = collections.defaultdict(float)   # resource -> cumulative held seconds
        self._hold_since = {}                             # resource -> ts last granted
        self._abort_flags = set()                         # process_ids signalled to abort

    # ------------------------------------------------------------- core
    def acquire(self, process_id, resource_id, timeout=None) -> bool:
        """Blocks (up to `timeout`) until resource_id is granted to
        process_id. Returns False on timeout (caller must not assume it
        holds the lock). Re-entrant for the same process. Raises
        DeadlockVictim if a DeadlockMonitor picks this process to break
        a cycle while it's waiting here -- that's the only way a
        thread blocked inside this call can be unblocked from outside."""
        deadline = None if timeout is None else time.time() + timeout
        with self._cv:
            if process_id in self._abort_flags:
                self._abort_flags.discard(process_id)
                raise DeadlockVictim(process_id)
            if self.holders.get(resource_id) == process_id:
                return True
            if resource_id not in self.holders:
                self._grant(process_id, resource_id)
                return True
            self.waiters[resource_id].append(process_id)
            self.wait_since[(process_id, resource_id)] = time.time()
            try:
                while self.holders.get(resource_id) != process_id:
                    if process_id in self._abort_flags:
                        self._abort_flags.discard(process_id)
                        raise DeadlockVictim(process_id)
                    remaining = None if deadline is None else deadline - time.time()
                    if remaining is not None and remaining <= 0:
                        return False
                    self._cv.wait(timeout=remaining)
                return True
            finally:
                if process_id in self.waiters[resource_id] and self.holders.get(resource_id) != process_id:
                    self.waiters[resource_id].remove(process_id)
                    self.wait_since.pop((process_id, resource_id), None)

    def signal_abort(self, process_id):
        """Wake up a process that is (or will be) blocked in acquire()
        and have its next wait-check raise DeadlockVictim."""
        with self._cv:
            self._abort_flags.add(process_id)
            self._cv.notify_all()

    def release(self, process_id, resource_id):
        with self._cv:
            if self.holders.get(resource_id) != process_id:
                return
            self._account_hold_time(resource_id)
            del self.holders[resource_id]
            self.held_by_process[process_id].discard(resource_id)
            self.grant_log.append(("RELEASE", process_id, resource_id, time.time()))
            if self.waiters.get(resource_id):
                nxt = self.waiters[resource_id].pop(0)
                self._grant(nxt, resource_id)
            self._cv.notify_all()

    def release_all(self, process_id):
        """Used by deadlock recovery: forcibly releases every resource
        a victim process holds, breaking any cycle it participates in."""
        for r in list(self.held_by_process[process_id]):
            self.release(process_id, r)

    def _grant(self, process_id, resource_id):
        self.holders[resource_id] = process_id
        self.held_by_process[process_id].add(resource_id)
        self._hold_since[resource_id] = time.time()
        self.grant_log.append(("GRANT", process_id, resource_id, time.time()))
        started = self.wait_since.pop((process_id, resource_id), None)
        if started is not None:
            self.wait_time_total[resource_id] += time.time() - started
        self._cv.notify_all()

    def _account_hold_time(self, resource_id):
        started = self._hold_since.pop(resource_id, None)
        if started is not None:
            self.hold_time_total[resource_id] += time.time() - started

    # ---------------------------------------------------------- graphs
    def wait_for_graph(self):
        """Pi -> Pj: process i is waiting on a resource held by process j.
        This is the graph deadlock DETECTION runs cycle-search on."""
        with self._cv:
            edges = set()
            for resource, waiters in self.waiters.items():
                holder = self.holders.get(resource)
                if holder is None:
                    continue
                for w in waiters:
                    if w != holder:
                        edges.add((w, holder))
            return edges

    def resource_allocation_graph(self):
        """Pi -> Rj (request/wait edge) and Rj -> Pi (allocation edge) --
        the full bipartite graph Milestone 6 asks for explicitly."""
        with self._cv:
            allocation = [(r, p) for r, p in self.holders.items()]           # Rj -> Pi
            request = [(p, r) for r, ws in self.waiters.items() for p in ws]  # Pi -> Rj
            return {"allocation": allocation, "request": request}

    def utilisation(self):
        """Fraction of elapsed time (since manager creation... really
        since first grant) each resource has spent held -- the
        'resource utilization analysis' deliverable."""
        with self._cv:
            now = time.time()
            out = {}
            for r, total in self.hold_time_total.items():
                extra = (now - self._hold_since[r]) if r in self._hold_since else 0.0
                out[r] = total + extra
            for r in self._hold_since:
                out.setdefault(r, 0.0)
            return out
