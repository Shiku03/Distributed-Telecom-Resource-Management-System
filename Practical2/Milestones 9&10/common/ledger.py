"""Resource and network reservation ledger (manager side).

The ledger is the *only* code that changes a site's ``allocated`` vector or a
link's ``reserved`` bandwidth.  Every change is a named reservation owned by an
instance (or replica) id and, while an operation is in flight, tagged with that
operation's id.  That gives the properties the accounting needs:

* atomic reserve / rollback: ``reserve_*`` either fully succeeds or raises and
  changes nothing;
* ownership: ``release_owner`` and ``release_op`` free exactly what an
  instance or operation holds, so a failed migration cannot leak the
  destination reservation and a terminated instance cannot leave a path
  reservation behind;
* no double release: ``release`` is idempotent and returns False the second
  time, so a duplicated INSTANCE_EXIT cannot subtract resources twice;
* auditability: ``verify`` recomputes every site and link total from the held
  reservations and reports any difference from the live counters.

Callers hold the manager lock around compound operations; the ledger has its
own re-entrant lock so each individual call is safe regardless.
"""
from __future__ import annotations

import threading
import time

RES = ("cpu", "mem", "bw")


class ReservationError(RuntimeError):
    def __init__(self, reason, detail=""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason


class ResourceLedger:
    def __init__(self, topology, sites, store=None):
        self.topology = topology
        self.sites = sites            # manager.sites: site_id -> record (capacity, allocated, ...)
        self.store = store
        self._lock = threading.RLock()
        self._res = {}                # res_id -> reservation dict (HELD and RELEASED)
        self._alloc = {}              # site_id -> {"cpu","mem","bw"}  (live counters)
        self._seq = 0

    # ------------------------------------------------------------ helpers
    def alloc_for(self, site_id):
        """The live allocated-vector for a site. The manager aliases
        ``site["allocated"]`` to this dict so reads see every reservation."""
        with self._lock:
            return self._alloc.setdefault(site_id, {"cpu": 0, "mem": 0, "bw": 0})

    def _next_id(self, owner, kind):
        self._seq += 1
        return f"{owner}:{kind[0]}{self._seq}"

    def _persist(self, res):
        if self.store is not None:
            self.store.upsert_reservation(res)

    # ------------------------------------------------------------- reserve
    def reserve_node(self, owner, site_id, demand, op_id=None, purpose="PRIMARY"):
        with self._lock:
            site = self.sites.get(site_id)
            if site is None:
                raise ReservationError("unknown_site", site_id)
            alloc = self.alloc_for(site_id)
            for r in RES:
                if alloc[r] + demand.get(r, 0) > site["capacity"][r] + 1e-9:
                    raise ReservationError("node_capacity", f"{site_id}.{r}")
            res = {"res_id": self._next_id(owner, "NODE"), "owner": owner, "op_id": op_id, "kind": "NODE",
                   "purpose": purpose, "site_id": site_id, "path": None,
                   "demand": {r: demand.get(r, 0) for r in RES}, "state": "HELD",
                   "created_at": time.time(), "released_at": None}
            for r in RES:
                alloc[r] += res["demand"][r]
            self._res[res["res_id"]] = res
            self._persist(res)
            return res["res_id"]

    def reserve_path(self, owner, path, bw, op_id=None, purpose="INGRESS"):
        with self._lock:
            ok, blocking = self.topology.path_has_bandwidth(path, bw)
            if not ok:
                raise ReservationError("link_bandwidth", str(sorted(blocking)))
            res = {"res_id": self._next_id(owner, "PATH"), "owner": owner, "op_id": op_id, "kind": "PATH",
                   "purpose": purpose, "site_id": None, "path": list(path), "demand": {"bw": bw},
                   "state": "HELD", "created_at": time.time(), "released_at": None}
            self.topology.reserve_path(path, bw)
            self._res[res["res_id"]] = res
            self._persist(res)
            return res["res_id"]

    # ------------------------------------------------------------- release
    def release(self, res_id):
        """Release one reservation. Idempotent: returns False if it was
        unknown or already released (so a double release changes nothing)."""
        with self._lock:
            res = self._res.get(res_id)
            if res is None or res["state"] != "HELD":
                return False
            if res["kind"] == "NODE":
                alloc = self.alloc_for(res["site_id"])
                for r in RES:
                    alloc[r] = max(0, alloc[r] - res["demand"][r])
            else:
                self.topology.release_path(res["path"], res["demand"]["bw"])
            res["state"] = "RELEASED"
            res["released_at"] = time.time()
            self._persist(res)
            return True

    def release_owner(self, owner, purposes=None):
        with self._lock:
            ids = [i for i, r in self._res.items() if r["owner"] == owner and r["state"] == "HELD"
                   and (purposes is None or r["purpose"] in purposes)]
            return [i for i in ids if self.release(i)]

    def release_op(self, op_id):
        """Release everything still held under an operation id (rollback)."""
        with self._lock:
            ids = [i for i, r in self._res.items() if r["op_id"] == op_id and r["state"] == "HELD"]
            return [i for i in ids if self.release(i)]

    # ------------------------------------------------------------ mutation
    def adjust_node(self, res_id, new_demand):
        """Change a held node reservation (resize). Fails atomically if the
        site cannot accommodate the growth."""
        with self._lock:
            res = self._res.get(res_id)
            if res is None or res["state"] != "HELD" or res["kind"] != "NODE":
                raise ReservationError("unknown_reservation", res_id)
            site = self.sites[res["site_id"]]
            alloc = self.alloc_for(res["site_id"])
            new = {r: new_demand.get(r, res["demand"][r]) for r in RES}
            for r in RES:
                if alloc[r] - res["demand"][r] + new[r] > site["capacity"][r] + 1e-9:
                    raise ReservationError("node_capacity", f"{res['site_id']}.{r}")
            for r in RES:
                alloc[r] += new[r] - res["demand"][r]
            res["demand"] = new
            self._persist(res)

    def rebind(self, res_id, owner=None, purpose=None, op_id="__keep__"):
        """Transfer ownership of a held reservation (e.g. a replica's node
        reservation becomes the promoted primary's)."""
        with self._lock:
            res = self._res.get(res_id)
            if res is None or res["state"] != "HELD":
                raise ReservationError("unknown_reservation", res_id)
            if owner is not None:
                res["owner"] = owner
            if purpose is not None:
                res["purpose"] = purpose
            if op_id != "__keep__":
                res["op_id"] = op_id
            self._persist(res)

    # --------------------------------------------------------------- views
    def get(self, res_id):
        with self._lock:
            r = self._res.get(res_id)
            return dict(r) if r else None

    def held(self, owner=None, op_id=None, kind=None, purpose=None):
        with self._lock:
            return [dict(r) for r in self._res.values() if r["state"] == "HELD"
                    and (owner is None or r["owner"] == owner)
                    and (op_id is None or r["op_id"] == op_id)
                    and (kind is None or r["kind"] == kind)
                    and (purpose is None or r["purpose"] == purpose)]

    def find(self, owner, kind, purpose):
        h = self.held(owner=owner, kind=kind, purpose=purpose)
        return h[0]["res_id"] if h else None

    # ------------------------------------------------------------- restore
    def restore(self, records):
        """Rebuild counters from persisted HELD reservations after a manager
        restart (sites need not have re-registered yet)."""
        with self._lock:
            for r in records:
                res = dict(r)
                res["path"] = list(r["path"]) if r.get("path") else None
                self._res[res["res_id"]] = res
                if res["kind"] == "NODE":
                    alloc = self.alloc_for(res["site_id"])
                    for k in RES:
                        alloc[k] += res["demand"].get(k, 0)
                else:
                    self.topology.reserve_path(res["path"], res["demand"]["bw"])
                try:
                    self._seq = max(self._seq, int(res["res_id"].rsplit(":", 1)[1][1:]))
                except (ValueError, IndexError):
                    pass

    # --------------------------------------------------------------- audit
    def verify(self):
        """Recompute totals from HELD reservations; return a list of
        discrepancy strings (empty means the books balance)."""
        problems = []
        with self._lock:
            exp_node, exp_link = {}, {}
            for r in self._res.values():
                if r["state"] != "HELD":
                    continue
                if r["kind"] == "NODE":
                    t = exp_node.setdefault(r["site_id"], {"cpu": 0, "mem": 0, "bw": 0})
                    for k in RES:
                        t[k] += r["demand"][k]
                else:
                    for key in self.topology.path_edges(r["path"]):
                        exp_link[key] = exp_link.get(key, 0) + r["demand"]["bw"]
            for sid in set(exp_node) | set(self._alloc):
                have = self._alloc.get(sid, {"cpu": 0, "mem": 0, "bw": 0})
                want = exp_node.get(sid, {"cpu": 0, "mem": 0, "bw": 0})
                for k in RES:
                    if abs(have[k] - want[k]) > 1e-6:
                        problems.append(f"site {sid} {k}: counter={have[k]} reservations={want[k]}")
                site = self.sites.get(sid)
                if site is not None:
                    for k in RES:
                        if have[k] > site["capacity"][k] + 1e-6:
                            problems.append(f"site {sid} {k} over-allocated: {have[k]}>{site['capacity'][k]}")
            for key in set(exp_link) | set(self.topology.reserved):
                have = self.topology.reserved.get(key, 0.0)
                want = exp_link.get(key, 0)
                if abs(have - want) > 1e-6:
                    a, b = self.topology.canonical.get(key, tuple(key))
                    problems.append(f"link {a}<->{b}: counter={have} reservations={want}")
                if key in self.topology.capacity and have > self.topology.capacity[key] + 1e-6:
                    problems.append(f"link {sorted(key)} over-reserved")
        return problems
