"""
Transaction coordinators for Milestone 5: Two-Phase and Three-Phase
Commit, both modelled explicitly as

    Prepare -> Commit -> Completion

with every decision durably logged before it's acted on, so a crashed
coordinator can resume exactly where it left off.
"""
import time

from .participant import ParticipantCrashed
from .wal import WAL


class TxnResult:
    def __init__(self, txn_id, decision, votes, latency_s, phases, blocked_s=0.0):
        self.txn_id = txn_id
        self.decision = decision        # "COMMIT" or "ABORT"
        self.votes = votes
        self.latency_s = latency_s
        self.phases = phases            # ordered list of phase names actually executed
        self.blocked_s = blocked_s      # time spent unable to make progress (failure experiments)

    def __repr__(self):
        return f"TxnResult({self.txn_id}, {self.decision}, latency={self.latency_s*1000:.2f}ms)"


class TwoPhaseCoordinator:
    """Classic 2PC. Known weakness, reproduced on purpose for the
    failure experiments: if the coordinator crashes after collecting
    all YES votes but before every participant has committed, a
    participant that already voted YES MUST block -- it cannot safely
    decide on its own, because it can't tell whether the others voted
    YES too (ACID: this is what we're probing in the failure runs)."""

    def __init__(self, name, wal_path, participant_timeout=1.0):
        self.name = name
        self.wal = WAL(wal_path)
        self.timeout = participant_timeout

    def run(self, txn_id, ops):
        """ops: list of (participant, demand) pairs."""
        t0 = time.perf_counter()
        phases = []
        self.wal.append({"event": "BEGIN", "txn_id": txn_id, "protocol": "2PC"})

        # ---- Phase 1: Prepare (vote collection) ----
        phases.append("PREPARE")
        votes = {}
        for p, demand in ops:
            try:
                votes[p.name] = p.prepare(txn_id, demand)
            except ParticipantCrashed:
                votes[p.name] = "CRASHED"
        self.wal.append({"event": "VOTES", "txn_id": txn_id, "votes": votes})

        decision = "COMMIT" if all(v == "YES" for v in votes.values()) else "ABORT"
        self.wal.append({"event": "DECISION", "txn_id": txn_id, "decision": decision})

        # ---- Phase 2: Commit / Abort ----
        phases.append(decision)
        acked = {}
        for p, demand in ops:
            try:
                (p.commit if decision == "COMMIT" else p.abort)(txn_id)
                acked[p.name] = True
            except ParticipantCrashed:
                acked[p.name] = False

        # ---- Completion ----
        phases.append("COMPLETE")
        self.wal.append({"event": "COMPLETE", "txn_id": txn_id, "decision": decision, "acked": acked})

        return TxnResult(txn_id, decision, votes, time.perf_counter() - t0, phases)


class ThreePhaseCoordinator:
    """3PC adds CanCommit -> PreCommit -> DoCommit. The extra round
    trip buys a non-blocking property under fail-stop coordinator
    crashes: once every participant has ACKed PreCommit, each of them
    independently knows every OTHER participant also voted YES (that's
    the only way the coordinator would ever have sent PreCommit), so
    if the coordinator then dies, a precommitted participant can
    proceed to commit on its own (`autonomous_decide`) instead of
    blocking forever. This assumes a fail-stop model with no network
    partitions -- real 3PC's non-blocking guarantee is famously fragile
    once partitions are allowed, which is exactly why industrial
    systems mostly use 2PC + a separate consensus layer instead."""

    def __init__(self, name, wal_path, participant_timeout=1.0):
        self.name = name
        self.wal = WAL(wal_path)
        self.timeout = participant_timeout

    def run(self, txn_id, ops):
        t0 = time.perf_counter()
        phases = []
        self.wal.append({"event": "BEGIN", "txn_id": txn_id, "protocol": "3PC"})

        # ---- Phase 1: CanCommit (= Prepare) ----
        phases.append("CANCOMMIT")
        votes = {}
        for p, demand in ops:
            try:
                votes[p.name] = p.prepare(txn_id, demand)
            except ParticipantCrashed:
                votes[p.name] = "CRASHED"
        self.wal.append({"event": "VOTES", "txn_id": txn_id, "votes": votes})

        if not all(v == "YES" for v in votes.values()):
            decision = "ABORT"
            phases.append("ABORT")
            acked = {}
            for p, demand in ops:
                try:
                    p.abort(txn_id)
                    acked[p.name] = True
                except ParticipantCrashed:
                    acked[p.name] = False
            phases.append("COMPLETE")
            self.wal.append({"event": "COMPLETE", "txn_id": txn_id, "decision": decision, "acked": acked})
            return TxnResult(txn_id, decision, votes, time.perf_counter() - t0, phases)

        # ---- Phase 2: PreCommit ----
        phases.append("PRECOMMIT")
        precommit_acks = {}
        for p, demand in ops:
            try:
                precommit_acks[p.name] = p.precommit(txn_id)
            except ParticipantCrashed:
                precommit_acks[p.name] = "CRASHED"
        self.wal.append({"event": "PRECOMMIT_ACKS", "txn_id": txn_id, "acks": precommit_acks})

        decision = "COMMIT" if all(a == "ACK" for a in precommit_acks.values()) else "ABORT"
        self.wal.append({"event": "DECISION", "txn_id": txn_id, "decision": decision})

        # ---- Phase 3: DoCommit / Abort ----
        phases.append(decision)
        acked = {}
        for p, demand in ops:
            try:
                (p.commit if decision == "COMMIT" else p.abort)(txn_id)
                acked[p.name] = True
            except ParticipantCrashed:
                acked[p.name] = False

        phases.append("COMPLETE")
        self.wal.append({"event": "COMPLETE", "txn_id": txn_id, "decision": decision, "acked": acked})
        return TxnResult(txn_id, decision, votes, time.perf_counter() - t0, phases)
