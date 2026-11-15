"""
Distributed transaction recovery (Milestone 5 deliverable).

When a participant crashes between voting YES and applying the final
decision, it restarts with an "in-doubt" transaction: its own WAL says
PREPARE_OK (or PRECOMMIT) but never COMMIT/ABORT. The textbook
crash-recovery protocol (assumes fail-stop, not Byzantine, and that
the coordinator's own WAL survives) is: ask the coordinator what it
decided, and replay that decision locally. This is the "Completion"
step in Prepare -> Commit -> Completion when Completion didn't happen
the first time around.
"""


def recover_in_doubt(coordinator, participant, txn_id: str):
    """Looks up the coordinator's durable DECISION record for txn_id and
    replays it onto `participant`. Returns the decision applied, or
    None if even the coordinator has no record (genuinely unresolved --
    would require participant-to-participant consensus, which plain
    2PC cannot provide; 3PC's `autonomous_decide` is the one case that
    can resolve itself without the coordinator at all)."""
    for rec in reversed(coordinator.wal.read_all()):
        if rec.get("txn_id") == txn_id and rec.get("event") == "DECISION":
            decision = rec["decision"]
            (participant.commit if decision == "COMMIT" else participant.abort)(txn_id)
            return decision
    return None


def reconcile(coordinator, participants):
    """Call after a simulated restart: for every in-doubt transaction on
    every participant, try 3PC's autonomous decision first (no
    coordinator round trip needed), then fall back to asking the
    coordinator. Returns {(participant_name, txn_id): outcome}."""
    outcomes = {}
    for p in participants:
        p.recover()
        for txn_id in sorted(p.in_doubt()):
            decision = p.autonomous_decide(txn_id) if hasattr(p, "autonomous_decide") else None
            source = "autonomous"
            if decision is None:
                decision = recover_in_doubt(coordinator, p, txn_id)
                source = "coordinator"
            outcomes[(p.name, txn_id)] = (decision, source)
    return outcomes
