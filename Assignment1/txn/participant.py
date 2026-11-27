"""
Transaction participants for Milestone 5.

A participant owns one slice of state (a site's node capacity, or a
link's bandwidth) and exposes the standard 2PC/3PC verbs:

    prepare(txn, demand)   -> "YES" | "NO"      (can-commit vote; reserves tentatively)
    precommit(txn)         -> "ACK"             (3PC only: promise to commit)
    commit(txn)                                  (make the reservation durable)
    abort(txn)                                    (release the tentative reservation)

Every state change is written to the participant's own WAL *before* it
takes effect in memory, so `recover()` can replay the log after a
simulated crash and reach exactly the state it would have had if it
had never crashed (Durability + Recovery).
"""
import random
import threading

from .wal import WAL


class ParticipantCrashed(Exception):
    """Raised by a participant when a fault-injection hook fires,
    simulating the process dying at that exact point in the protocol."""
    def __init__(self, name, phase):
        super().__init__(f"{name} crashed during {phase}")
        self.name = name
        self.phase = phase


class ResourceParticipant:
    """A participant that owns a capacity vector (e.g. one site's
    cpu/mem/bw, or one link's bandwidth) and votes on whether a demand
    fits, per Milestone-1's admission rule (A_j + d_r <= C_j)."""

    def __init__(self, name, capacity: dict, wal_path: str, crash_at: str = None, rng=None):
        self.name = name
        self.capacity = dict(capacity)
        self.allocated = {r: 0 for r in capacity}
        self.wal = WAL(wal_path)
        self.pending = {}          # txn_id -> demand, reserved during PREPARED/PRECOMMITTED
        self.precommitted = set()  # txn_id set, 3PC only
        self.committed = {}        # txn_id -> demand actually applied
        self.aborted = set()
        self.crash_at = crash_at   # one of: prepare, precommit, commit, None
        self.rng = rng or random
        self.lock = threading.Lock()
        self.recover()

    # ------------------------------------------------------------ recovery
    def recover(self):
        """Replay the WAL to reconstruct state after a (real or
        simulated) restart. Idempotent: safe to call multiple times."""
        self.allocated = {r: 0 for r in self.capacity}
        self.pending.clear()
        self.precommitted.clear()
        self.committed.clear()
        self.aborted.clear()
        for rec in self.wal.read_all():
            ev, txn = rec["event"], rec["txn_id"]
            if ev == "PREPARE_OK":
                self.pending[txn] = rec["demand"]
            elif ev == "PRECOMMIT":
                self.precommitted.add(txn)
            elif ev == "COMMIT":
                demand = self.pending.pop(txn, rec.get("demand"))
                self.precommitted.discard(txn)
                if txn not in self.committed and demand:
                    for r, v in demand.items():
                        self.allocated[r] = self.allocated.get(r, 0) + v
                    self.committed[txn] = demand
            elif ev == "ABORT":
                self.pending.pop(txn, None)
                self.precommitted.discard(txn)
                self.aborted.add(txn)

    def in_doubt(self):
        """Transactions this participant voted YES on (or precommitted)
        but never saw a final COMMIT/ABORT for -- exactly what a real
        restart would need to ask the coordinator / other participants
        about. Used to demonstrate 2PC's blocking window vs 3PC's."""
        return {t for t in self.pending if t not in self.committed and t not in self.aborted}

    # ------------------------------------------------------------- 2PC/3PC
    def _maybe_crash(self, phase):
        if self.crash_at == phase:
            raise ParticipantCrashed(self.name, phase)

    def prepare(self, txn_id, demand: dict) -> str:
        with self.lock:
            self._maybe_crash("prepare")
            fits = all(self.allocated.get(r, 0) + demand.get(r, 0) <= self.capacity[r] + 1e-9
                       for r in self.capacity)
            if not fits:
                self.wal.append({"event": "PREPARE_FAIL", "txn_id": txn_id})
                return "NO"
            self.pending[txn_id] = dict(demand)
            self.wal.append({"event": "PREPARE_OK", "txn_id": txn_id, "demand": demand})
            return "YES"

    def precommit(self, txn_id) -> str:
        with self.lock:
            self._maybe_crash("precommit")
            if txn_id not in self.pending:
                return "NACK"
            self.precommitted.add(txn_id)
            self.wal.append({"event": "PRECOMMIT", "txn_id": txn_id})
            return "ACK"

    def commit(self, txn_id):
        with self.lock:
            self._maybe_crash("commit")
            demand = self.pending.pop(txn_id, None)
            self.precommitted.discard(txn_id)
            if txn_id not in self.committed and demand:
                for r, v in demand.items():
                    self.allocated[r] = self.allocated.get(r, 0) + v
                self.committed[txn_id] = demand
            self.wal.append({"event": "COMMIT", "txn_id": txn_id, "demand": demand})

    def abort(self, txn_id):
        with self.lock:
            self.pending.pop(txn_id, None)
            self.precommitted.discard(txn_id)
            self.aborted.add(txn_id)
            self.wal.append({"event": "ABORT", "txn_id": txn_id})

    def autonomous_decide(self, txn_id):
        """3PC's key property: if THIS participant reached PRECOMMITTED
        for txn_id, every participant that voted must also have voted
        YES (that's the only way precommit could have been sent), so it
        is always safe to commit unilaterally without waiting for a
        possibly-crashed coordinator. A participant that only reached
        PREPARED (not precommitted) cannot safely do this -- that's the
        2PC blocking case -- so it returns None (must keep blocking)."""
        with self.lock:
            if txn_id in self.precommitted:
                return "COMMIT"
            return None

    def utilisation(self) -> dict:
        return {r: self.allocated.get(r, 0) / c if c else 0.0 for r, c in self.capacity.items()}
