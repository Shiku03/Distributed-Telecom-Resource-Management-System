"""Unit tests for the Milestone 9/10 building blocks (no sockets)."""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.durable_store import DurableStore
from common.failure_recovery import FailureConfig, FailureDetector, plan_reconciliation
from common.ledger import ReservationError, ResourceLedger
from common.lifecycle import InvalidTransition, transition
from common.service_registry import NameConflict, ServiceRegistry, StaleGeneration
from common.topology import Topology

LINKS = [{"a": "edge-A", "b": "core-1", "capacity": 1000, "latency_ms": 5},
         {"a": "edge-B", "b": "core-1", "capacity": 1000, "latency_ms": 6},
         {"a": "edge-A", "b": "edge-B", "capacity": 400, "latency_ms": 3}]
CAP = {"cpu": 4000, "mem": 4096, "bw": 2000}


def ledger():
    topo = Topology(LINKS)
    sites = {s: {"capacity": dict(CAP), "allocated": None} for s in ("edge-A", "edge-B", "core-1")}
    lg = ResourceLedger(topo, sites)
    for s in sites:
        sites[s]["allocated"] = lg.alloc_for(s)
    return lg, topo, sites


class TopologyPaths(unittest.TestCase):
    def test_saturated_shortest_path_does_not_block_alternative(self):
        t = Topology(LINKS)
        t.reserve_path(["edge-A", "edge-B"], 400)                       # direct 3 ms link is full
        self.assertEqual(t.shortest_path("edge-A", "edge-B")[0], ["edge-A", "edge-B"])
        path, lat = t.best_feasible_path("edge-A", "edge-B", 100)
        self.assertEqual(path, ["edge-A", "core-1", "edge-B"])
        self.assertEqual(lat, 11)

    def test_latency_bound_is_respected(self):
        t = Topology(LINKS)
        t.reserve_path(["edge-A", "edge-B"], 400)
        self.assertEqual(t.best_feasible_path("edge-A", "edge-B", 100, max_latency=10), (None, None))

    def test_loop_free(self):
        for path, _ in Topology(LINKS).simple_paths("edge-A", "core-1"):
            self.assertEqual(len(path), len(set(path)))


class LedgerTests(unittest.TestCase):
    def test_reserve_release_balances_and_double_release_is_noop(self):
        lg, topo, sites = ledger()
        n = lg.reserve_node("S1", "edge-A", {"cpu": 1000, "mem": 512, "bw": 400})
        p = lg.reserve_path("S1", ["edge-A", "edge-B"], 400)
        self.assertEqual(sites["edge-A"]["allocated"]["cpu"], 1000)
        self.assertEqual(lg.verify(), [])
        self.assertTrue(lg.release(n))
        self.assertFalse(lg.release(n))                                  # no double release
        self.assertEqual(sites["edge-A"]["allocated"]["cpu"], 0)
        self.assertTrue(lg.release(p))
        self.assertEqual(sum(topo.reserved.values()), 0)

    def test_failed_reservation_changes_nothing(self):
        lg, topo, sites = ledger()
        lg.reserve_node("S1", "edge-A", {"cpu": 3000, "mem": 1, "bw": 1})
        before = dict(sites["edge-A"]["allocated"])
        with self.assertRaises(ReservationError):
            lg.reserve_node("S2", "edge-A", {"cpu": 2000, "mem": 1, "bw": 1})
        self.assertEqual(sites["edge-A"]["allocated"], before)
        with self.assertRaises(ReservationError):
            lg.reserve_path("S2", ["edge-A", "edge-B"], 500)
        self.assertEqual(lg.verify(), [])

    def test_release_op_rolls_back_only_that_operation(self):
        lg, topo, sites = ledger()
        lg.reserve_node("S1", "edge-A", {"cpu": 500, "mem": 1, "bw": 1})
        lg.reserve_node("S1", "core-1", {"cpu": 500, "mem": 1, "bw": 1}, op_id="op1", purpose="MIGRATION_TARGET")
        lg.reserve_path("S1", ["edge-A", "core-1"], 50, op_id="op1")
        self.assertEqual(len(lg.release_op("op1")), 2)
        self.assertEqual(sites["edge-A"]["allocated"]["cpu"], 500)
        self.assertEqual(sites["core-1"]["allocated"]["cpu"], 0)
        self.assertEqual(lg.verify(), [])

    def test_adjust_and_rebind(self):
        lg, _, sites = ledger()
        r = lg.reserve_node("R1", "edge-B", {"cpu": 1000, "mem": 1, "bw": 1}, purpose="REPLICA")
        lg.adjust_node(r, {"cpu": 1500})
        self.assertEqual(sites["edge-B"]["allocated"]["cpu"], 1500)
        with self.assertRaises(ReservationError):
            lg.adjust_node(r, {"cpu": 9000})
        self.assertEqual(sites["edge-B"]["allocated"]["cpu"], 1500)
        lg.rebind(r, owner="S1", purpose="PRIMARY")
        self.assertEqual(lg.held(owner="S1")[0]["purpose"], "PRIMARY")
        self.assertEqual(lg.verify(), [])

    def test_verify_detects_tampering(self):
        lg, _, sites = ledger()
        lg.reserve_node("S1", "edge-A", {"cpu": 500, "mem": 1, "bw": 1})
        sites["edge-A"]["allocated"]["cpu"] += 100                       # simulated accounting bug
        self.assertTrue(lg.verify())


class RegistryTests(unittest.TestCase):
    def test_generation_is_monotonic(self):
        r = ServiceRegistry()
        r.register("service://telerm/UPF/S1", "S1", "UPF", "edge-A", "h", 1, generation=1)
        r.update_location("S1", "core-1", "h", 2, generation=2)
        with self.assertRaises(StaleGeneration):
            r.update_location("S1", "edge-A", "h", 1, generation=1)
        self.assertEqual(r.resolve("service://telerm/UPF/S1")["generation"], 2)

    def test_resolve_detail_reasons_and_resizing_is_resolvable(self):
        r = ServiceRegistry()
        n = "service://telerm/UPF/S1"
        r.register(n, "S1", "UPF", "edge-A", "h", 1)
        r.set_state("S1", "RESIZING")
        self.assertIsNotNone(r.resolve(n))
        r.set_state("S1", "MIGRATING")
        self.assertEqual(r.resolve_detail(n)[1], "migrating")
        r.set_state("S1", "UNAVAILABLE")
        self.assertEqual(r.resolve_detail(n)[1], "unavailable")
        self.assertEqual(r.resolve_detail("service://telerm/none/1")[1], "not_found")

    def test_restore_is_never_available_and_multiple_instances_coexist(self):
        r = ServiceRegistry()
        for i in (1, 2):
            r.register(f"service://telerm/UPF/S{i}", f"S{i}", "UPF", "edge-A", "h", 1)
        snap = [{**x, "name": x["name"]} for x in r.snapshot()]
        r2 = ServiceRegistry()
        r2.restore(snap)
        self.assertEqual(len(r2.by_service("UPF", only_resolvable=False)), 2)
        self.assertEqual(r2.by_service("UPF"), [])
        with self.assertRaises(NameConflict):
            r2.register("service://telerm/UPF/S1", "S9", "UPF", "edge-A", "h", 1)


class LifecycleTests(unittest.TestCase):
    def test_standby_promotion_and_invalid_paths(self):
        self.assertEqual(transition("STANDBY", "RUNNING"), "RUNNING")
        for a, b in (("TERMINATED", "RUNNING"), ("STANDBY", "RESIZING"), ("RUNNING", "STANDBY"),
                     ("FAILED", "RUNNING"), ("MIGRATING", "TERMINATED")):
            with self.assertRaises(InvalidTransition):
                transition(a, b)


class DurableStoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "t.db")

    def proc(self, sid, state="RUNNING"):
        return {"instance_id": sid, "logical_name": f"service://telerm/UPF/{sid}", "service": "UPF",
                "site_id": "edge-A", "state": state, "demand": {"cpu": 1}, "holding_s": 5}

    def test_survives_reopen_and_transaction_rolls_back(self):
        s = DurableStore(self.path)
        s.save_instance(self.proc("S1"), {"logical_name": "service://telerm/UPF/S1", "service": "UPF",
                                          "instance_id": "S1", "site_id": "edge-A", "host": "h", "port": 1,
                                          "state": "RUNNING", "generation": 3})
        with self.assertRaises(RuntimeError):
            with s.transaction():
                s.update_service_state("service://telerm/UPF/S1", "UNAVAILABLE")
                raise RuntimeError("boom")
        s.close()
        s2 = DurableStore(self.path)
        row = s2.get_service("service://telerm/UPF/S1")
        self.assertEqual((row["state"], row["generation"]), ("RUNNING", 3))

    def test_constraints(self):
        import sqlite3
        s = DurableStore(self.path)
        with self.assertRaises(sqlite3.IntegrityError):
            s.upsert_process(self.proc("S1", "BOGUS"))
        with self.assertRaises(sqlite3.IntegrityError):               # service row needs its process row
            s.upsert_service({"logical_name": "n", "service": "UPF", "instance_id": "NOPE", "site_id": "a",
                              "host": "h", "port": 1, "state": "RUNNING"})

    def test_operations_merge_payload_and_incomplete_listing(self):
        s = DurableStore(self.path)
        s.begin_operation({"operation_id": "o1", "operation_type": "MIGRATION", "state": "PREPARING",
                           "instance_id": "S1", "payload": {"a": 1}})
        s.update_operation("o1", "COMMIT_DECIDED", {"b": 2})
        op = s.operation("o1")
        self.assertEqual(op["payload"], {"a": 1, "b": 2})
        self.assertEqual([o["operation_id"] for o in s.incomplete_operations()], ["o1"])
        s.update_operation("o1", "COMMITTED")
        self.assertEqual(s.incomplete_operations(), [])


class FailureLogicTests(unittest.TestCase):
    def test_config_enforces_lease_inequality(self):
        FailureConfig.from_config({"heartbeat_interval_s": 1.0, "failure_timeout_heartbeats": 3})
        with self.assertRaises(ValueError):
            FailureConfig.from_config({"failure_detector": {"suspect_after_heartbeats": 2,
                                                            "failed_after_heartbeats": 3,
                                                            "site_lease_heartbeats": 3}})

    def test_classification_thresholds(self):
        d = FailureDetector(FailureConfig.from_config({"heartbeat_interval_s": 1.0}))
        self.assertEqual([d.classify(x) for x in (0.5, 3.0, 3.1, 6.0, 6.1)],
                         ["ACTIVE", "ACTIVE", "SUSPECT", "SUSPECT", "FAILED"])

    def test_plan_distinguishes_restore_stale_lost_and_in_flight(self):
        here = {"S1": {"sid": "S1", "state": "RUNNING", "generation": 1},
                "S2": {"sid": "S2", "state": "RUNNING", "generation": 1},
                "S3": {"sid": "S3", "state": "MIGRATING", "generation": 2, "op": "mig-1"}}
        other = {"S9": {"sid": "S9", "state": "RUNNING", "generation": 2, "site": "core-1"}}
        inv = [{"sid": "S1", "state": "RUNNING", "generation": 1},
               {"sid": "S9", "state": "RUNNING", "generation": 1}]       # superseded copy of S9
        p = plan_reconciliation("edge-A", here, {**here, **other}, {}, inv, restarted=False, missing_threshold=3)
        self.assertEqual(p["restore"], ["S1"])
        self.assertEqual(p["release"], ["S9"])
        self.assertEqual(p["missing"], ["S2"])
        self.assertEqual(p["lost"], [])                                  # one miss is not a loss
        p = plan_reconciliation("edge-A", here, {**here, **other}, {}, [], restarted=True)
        self.assertEqual(p["lost"], ["S1", "S2"])                        # restarted site lost its state
        self.assertNotIn("S3", p["lost"])                                # in-flight operation owns S3


if __name__ == "__main__":
    unittest.main()
