"""
Deadlock detection and recovery for Milestone 6.

Detection: standard cycle search over the wait-for graph (Pi -> Pj
whenever process i is blocked on a resource process j holds). A cycle
P1 -> P2 -> ... -> Pk -> P1 is, by definition, a circular-wait
deadlock (one of Coffman's four necessary conditions, alongside mutual
exclusion, hold-and-wait and no-preemption -- all of which LockManager
provides unless a prevention strategy is layered on top, see
prevention.py).

Recovery: once a cycle is found, pick a victim and force it to release
everything it holds (LockManager.release_all), which breaks the cycle
and lets the rest of the waiters proceed. The victim's own transaction
must then abort and retry -- that's the caller's job (see
demo_deadlock.py), this module only decides *who*.
"""
import collections
import threading
import time


def find_cycle(edges):
    """DFS cycle search over a directed graph given as an iterable of
    (u, v) edges. Returns the cycle as a list of process ids
    [p1, p2, ..., pk, p1], or None if the graph is acyclic."""
    graph = collections.defaultdict(list)
    for u, v in edges:
        graph[u].append(v)

    WHITE, GRAY, BLACK = 0, 1, 2
    color = collections.defaultdict(int)
    parent = {}

    def dfs(start):
        stack = [(start, iter(graph[start]))]
        color[start] = GRAY
        while stack:
            node, it = stack[-1]
            advanced = False
            for nxt in it:
                if color[nxt] == WHITE:
                    color[nxt] = GRAY
                    parent[nxt] = node
                    stack.append((nxt, iter(graph[nxt])))
                    advanced = True
                    break
                elif color[nxt] == GRAY:
                    cycle = [nxt]
                    cur = node
                    while cur != nxt:
                        cycle.append(cur)
                        cur = parent[cur]
                    cycle.append(nxt)
                    cycle.reverse()
                    return cycle
            if not advanced:
                color[node] = BLACK
                stack.pop()
        return None

    for node in list(graph.keys()):
        if color[node] == WHITE:
            result = dfs(node)
            if result:
                return result
    return None


def choose_victim(cycle, lock_manager, strategy="fewest_resources"):
    """Pick which process in the cycle to abort. `fewest_resources`
    minimises collateral damage (releases the process holding the
    least); `youngest` aborts whichever process most recently entered
    the wait (favours long-running transactions, a common DBMS
    heuristic to avoid starving the same victim repeatedly)."""
    if strategy == "fewest_resources":
        return min(cycle, key=lambda p: len(lock_manager.held_by_process.get(p, ())))
    if strategy == "youngest":
        def started(p):
            times = [t for (pp, _r), t in lock_manager.wait_since.items() if pp == p]
            return max(times) if times else 0.0
        return max(cycle, key=started)
    return cycle[0]


class DeadlockMonitor:
    """Background thread that periodically polls the lock manager's
    wait-for graph, and on finding a cycle, aborts a victim to recover.
    Call `events` to inspect what it has found/done (for the
    'deadlock experiment' deliverable)."""

    def __init__(self, lock_manager, poll_interval=0.02, victim_strategy="fewest_resources",
                 on_victim=None):
        self.lm = lock_manager
        self.poll_interval = poll_interval
        self.victim_strategy = victim_strategy
        self.on_victim = on_victim  # callback(victim_process_id) -- e.g. to raise in its thread
        self.events = []
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _loop(self):
        while not self._stop.is_set():
            time.sleep(self.poll_interval)
            cycle = find_cycle(self.lm.wait_for_graph())
            if cycle is None:
                continue
            participants = cycle[:-1]  # last element repeats the first (closes the cycle)
            victim = choose_victim(participants, self.lm, self.victim_strategy)
            self.events.append({"ts": time.time(), "cycle": participants, "victim": victim})
            # wake the victim's blocked acquire() so it raises DeadlockVictim;
            # the victim's own thread is responsible for releasing what it
            # already holds (its abort/rollback path -- see demo_deadlock.py),
            # exactly like any other transaction abort.
            self.lm.signal_abort(victim)
            if self.on_victim:
                self.on_victim(victim)
