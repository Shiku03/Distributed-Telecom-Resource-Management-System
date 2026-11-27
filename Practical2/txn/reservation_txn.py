"""
Two concrete distributed transactions built on the 2PC/3PC machinery,
tied to TeleRM's own resource model (node vector + link bandwidth,
design doc §5.1):

- reservation_txn: admit one request by atomically reserving node
  capacity on a site participant AND bandwidth on a link participant.
  In the live system (manager.py) this is trivially atomic because one
  process holds both pieces of state under one lock; here they are
  modelled as two independent participants, which is what makes the
  atomicity a genuine distributed-transactions problem (both
  reservations must happen, or neither must).

- migration_txn: atomically move a running instance's reservation from
  one site participant to another (release on the source, reserve on
  the destination) -- commits only if the destination can actually fit
  it; otherwise the source is left untouched (design doc §5.1: "Moving
  an instance to another site to satisfy a resize is Week 9").
"""


def reservation_txn(coordinator, site_participant, link_participant, demand: dict, txn_id: str):
    node_demand = {r: demand[r] for r in ("cpu", "mem")}
    link_demand = {"bw": demand["bw"]}
    ops = [(site_participant, node_demand), (link_participant, link_demand)]
    return coordinator.run(txn_id, ops)


def migration_txn(coordinator, src_participant, dst_participant, demand: dict, txn_id: str):
    """The release side always votes YES -- giving back a reservation
    you already hold is never resource-constrained; the destination is
    the only side that can vote NO. If it does, the whole transaction
    aborts and the source is untouched."""
    release = ReleaseParticipant(src_participant)
    ops = [(dst_participant, demand), (release, demand)]
    return coordinator.run(txn_id, ops)


class ReleaseParticipant:
    """Adapts a ResourceParticipant to be the "give this reservation
    back" side of a transaction: prepare always succeeds and remembers
    the amount to release, commit actually frees the capacity (logged
    to the SAME underlying WAL, so recovery sees one consistent
    history for the site), abort is a no-op since nothing changed."""

    def __init__(self, site_participant):
        self.inner = site_participant
        self.name = site_participant.name + "-release"
        self._pending = {}

    def prepare(self, txn_id, demand):
        self._pending[txn_id] = dict(demand)
        return "YES"

    def precommit(self, txn_id):
        return "ACK"

    def commit(self, txn_id):
        demand = self._pending.pop(txn_id, {})
        with self.inner.lock:
            for r, v in demand.items():
                self.inner.allocated[r] = max(0.0, self.inner.allocated.get(r, 0) - v)
            self.inner.wal.append({"event": "RELEASE", "txn_id": txn_id, "demand": demand})

    def abort(self, txn_id):
        self._pending.pop(txn_id, None)
