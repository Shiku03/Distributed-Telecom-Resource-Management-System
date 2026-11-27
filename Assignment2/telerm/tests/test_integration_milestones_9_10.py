"""End-to-end tests: a real Manager and real Sites over real TCP sockets.

Every test checks *state*: what the sites actually host, what the manager's
ledger/topology say, what the registry resolves to, what SQLite holds, and that
the books balance (ledger.verify() == []).
"""
import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import Cluster, make_config                       # noqa: E402
from common.service_client import ServiceClient, ServiceUnavailable   # noqa: E402


def hosting(cluster, sid, running_only=True):
    """Sites whose live inventory contains ``sid`` as a (running) primary."""
    out = []
    for site_id, s in cluster.sites.items():
        with s.cv:
            i = s.instances.get(sid)
            if i is not None and i.role == "PRIMARY" and (not running_only or i.state == "RUNNING"):
                out.append(site_id)
    return out


def alloc(cluster, site):
    return dict(cluster.mgr.sites[site]["allocated"])


def links(cluster):
    return {k: v["reserved"] for k, v in cluster.mgr.topology.link_utilisation().items()}


class Base(unittest.TestCase):
    cfg = None

    def setUp(self):
        self.c = Cluster(self.cfg() if callable(self.cfg) else None)

    def tearDown(self):
        self.c.close()

    def assertBooksBalance(self):
        self.assertEqual(self.c.ledger_ok(), [])


class TestNamingAndAdmission(Base):
    def test_resolution_matches_actual_host_and_names_are_stable_and_unique(self):
        a = self.c.request("UPF", "edge-A")
        b = self.c.request("UPF", "edge-A")
        self.assertEqual(a["type"], "ACCEPTED")
        self.assertNotEqual(a["logical_name"], b["logical_name"])            # same type, distinct identities
        self.assertEqual(a["logical_name"], f"service://telerm/UPF/{a['sid']}")
        r = self.c.rpc({"type": "RESOLVE", "name": a["logical_name"]})
        self.assertEqual(r["type"], "RESOLVED")
        self.assertEqual(hosting(self.c, a["sid"]), [r["site_id"]])          # endpoint == real host
        self.assertEqual((r["host"], r["port"]), (self.c.cfg["sites"][r["site_id"]]["host"],
                                                   self.c.cfg["sites"][r["site_id"]]["port"]))
        cl = ServiceClient(self.c.mhost, self.c.mport)
        self.assertEqual(sorted(i["name"] for i in cl.lookup("UPF")), sorted([a["logical_name"], b["logical_name"]]))
        self.assertBooksBalance()

    def test_custom_name_conflict_and_unknown_and_validation(self):
        n = "service://telerm/custom/one"
        self.assertEqual(self.c.request("IoT-GW", "edge-A", logical_name=n)["type"], "ACCEPTED")
        self.assertEqual(self.c.request("IoT-GW", "edge-A", logical_name=n)["error"], "logical_name_in_use")
        self.assertEqual(self.c.rpc({"type": "RESOLVE", "name": "service://telerm/none/9"})["reason"], "not_found")
        self.assertEqual(self.c.request("IoT-GW", "edge-A", logical_name="not-a-uri")["type"], "ERROR")
        self.assertIn("missing_field", self.c.rpc({"type": "REQUEST", "service": "UPF"})["error"])
        self.assertEqual(self.c.rpc({"type": "NOPE"})["type"], "ERROR")
        self.c.request("UPF", "edge-A")
        self.assertEqual(self.c.rpc({"type": "REQUEST", "service": "UPF", "ingress": "edge-A", "holding_s": -1,
                                     "demand": {"cpu": 1, "mem": 1, "bw": 1}, "max_latency_ms": 5})["type"], "ERROR")
        self.assertEqual(len([s for s in self.c.mgr.allocation_table]), 2)   # rejected requests left no residue
        self.assertBooksBalance()

    def test_site_inventory_equals_manager_table(self):
        sids = [self.c.request(s, "edge-A")["sid"] for s in ("UPF", "IMS-CSCF", "IoT-GW")]
        self.assertTrue(self.c.wait(lambda: all(len(hosting(self.c, s)) == 1 for s in sids)))
        for sid in sids:
            self.assertEqual(hosting(self.c, sid), [self.c.mgr.allocation_table[sid]["site"]])
        total = sum(alloc(self.c, s)["cpu"] for s in self.c.sites)
        self.assertEqual(total, 1000 + 500 + 250)

    def test_concurrent_admission_never_over_allocates(self):
        results = []

        def go():
            results.append(self.c.request("Transcoder", "edge-A", holding=60)["type"])
        ts = [threading.Thread(target=go) for _ in range(30)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        admitted = results.count("ACCEPTED")
        # Transcoder: 2000 cpu / 200 bw, all ingress at edge-A. cpu alone would allow 2+2+6=10, but the
        # edge-A<->edge-B link (400) carries 2 and edge-A<->core-1 (1000) carries 5: bandwidth caps it at 2+2+5=9.
        self.assertEqual(admitted, 9)
        self.assertTrue(all(v <= 1.0 + 1e-9 for v in
                            (u["utilisation"] for u in self.c.mgr.topology.link_utilisation().values())))
        for s, rec in self.c.mgr.sites.items():
            for r in ("cpu", "mem", "bw"):
                self.assertLessEqual(rec["allocated"][r], rec["capacity"][r])
        self.assertEqual(sum(alloc(self.c, s)["cpu"] for s in self.c.sites), 2000 * admitted)
        self.assertBooksBalance()


class TestResizeAndTermination(Base):
    def test_resize_updates_site_and_manager_and_rejects_overflow(self):
        r = self.c.request("UPF", "edge-A")
        sid, site = r["sid"], r["site"]
        self.assertEqual(self.c.rpc({"type": "RESIZE", "sid": sid, "factor": 2.0})["type"], "RESIZE_OK")
        self.assertEqual(alloc(self.c, site)["cpu"], 2000)
        self.assertEqual(self.c.sites[site].instances[sid].tickets, 2000)
        fail = self.c.rpc({"type": "RESIZE", "sid": sid, "factor": 10.0})                # 20000 > 4000
        self.assertEqual((fail["type"], fail["reason"]), ("RESIZE_FAIL", "node_capacity"))
        self.assertEqual(alloc(self.c, site)["cpu"], 2000)                                # unchanged
        self.assertEqual(self.c.mgr.allocation_table[sid]["state"], "RUNNING")
        self.assertBooksBalance()

    def test_terminate_releases_everything_including_replica_and_path(self):
        r = self.c.request("UPF", "edge-A")
        # UPF at edge-A (edge_first) reserves no link bandwidth; use a remote placement to hold a path
        r2 = self.c.request("IMS-CSCF", "edge-A")
        self.c.rpc({"type": "MIGRATE", "sid": r2["sid"], "target_site": "core-1"})
        rep = self.c.rpc({"type": "REPLICATE", "sid": r["sid"], "target_site": "edge-B"})
        self.assertEqual(rep["type"], "REPLICATE_OK")
        self.assertGreater(sum(links(self.c).values()), 0)
        for sid in (r["sid"], r2["sid"]):
            self.assertEqual(self.c.rpc({"type": "TERMINATE", "sid": sid})["type"], "TERMINATE_OK")
        self.assertEqual(self.c.mgr.allocation_table, {})
        self.assertEqual(self.c.mgr.replicas, {})
        self.assertEqual([alloc(self.c, s) for s in self.c.sites], [{"cpu": 0, "mem": 0, "bw": 0}] * 3)
        self.assertEqual(sum(links(self.c).values()), 0)
        self.assertEqual(self.c.mgr.ledger.held(), [])
        self.assertTrue(self.c.wait(lambda: all(not s.instances for s in self.c.sites.values())))
        self.assertEqual(self.c.rpc({"type": "RESOLVE", "name": r["logical_name"]})["reason"], "not_found")
        self.assertEqual(self.c.mgr.store.get_process(r["sid"])["state"], "TERMINATED")
        self.assertIsNone(self.c.mgr.store.get_service(r["logical_name"]))
        # a duplicated exit report must not subtract resources a second time
        self.c.rpc({"type": "INSTANCE_EXIT", "sid": r["sid"], "site_id": "edge-A", "generation": 1})
        self.assertBooksBalance()

    def test_holding_time_expiry_releases_accounting(self):
        r = self.c.request("IoT-GW", "edge-A", holding=0.6)
        self.assertTrue(self.c.wait(lambda: r["sid"] not in self.c.mgr.allocation_table, 5))
        self.assertEqual(alloc(self.c, r["site"]), {"cpu": 0, "mem": 0, "bw": 0})
        self.assertBooksBalance()


class TestMigration(Base):
    def test_migration_keeps_identity_moves_state_and_accounting(self):
        r = self.c.request("IMS-CSCF", "edge-A", holding=60)
        sid, name = r["sid"], r["logical_name"]
        src = r["site"]
        cl = ServiceClient(self.c.mhost, self.c.mport)
        for _ in range(3):
            cl.call(name)
        self.assertTrue(self.c.wait(lambda: self.c.sites[src].instances[sid].jobs_done > 5))
        with self.c.sites[src].cv:
            before = self.c.sites[src].instances[sid]
            jobs_before, calls_before = before.jobs_done, before.calls_served
            remaining_before = before.holding_remaining()
        m = self.c.rpc({"type": "MIGRATE", "name": name, "target_site": "core-1"})
        self.assertEqual(m["type"], "MIGRATE_OK", m)
        # identity & endpoint
        r2 = self.c.rpc({"type": "RESOLVE", "name": name})
        self.assertEqual((r2["site_id"], r2["instance_id"], r2["generation"]), ("core-1", sid, 2))
        self.assertEqual(hosting(self.c, sid, running_only=False), ["core-1"])     # exactly one copy exists
        # preserved simulation state
        with self.c.sites["core-1"].cv:
            after = self.c.sites["core-1"].instances[sid]
            self.assertEqual((after.service, after.logical_name, after.generation), ("IMS-CSCF", name, 2))
            self.assertGreaterEqual(after.jobs_done, jobs_before)
            self.assertEqual(after.calls_served, calls_before)
            self.assertEqual(after.demand["cpu"], 500)
            self.assertLessEqual(after.holding_remaining(), remaining_before + 0.01)
            self.assertGreater(after.holding_remaining(), remaining_before - 3.0)   # timer was paused, not reset
        # accounting
        self.assertEqual(alloc(self.c, src), {"cpu": 0, "mem": 0, "bw": 0})
        self.assertEqual(alloc(self.c, "core-1"), {"cpu": 500, "mem": 256, "bw": 50})
        self.assertEqual(links(self.c)["edge-A<->core-1"], 50)                      # ingress->core path now held
        self.assertBooksBalance()
        # durability
        self.assertEqual(self.c.mgr.store.get_service(name)["site_id"], "core-1")
        self.assertEqual(self.c.mgr.store.get_service(name)["generation"], 2)
        op = self.c.mgr.store.operation(m["operation_id"])
        self.assertEqual(op["state"], "COMMITTED")
        self.assertEqual(op["source_site"], src)
        # the instance keeps working and is still terminable at its new home
        self.assertGreater(cl.call(name)["calls_served"], calls_before)
        self.assertEqual(self.c.rpc({"type": "TERMINATE", "name": name})["type"], "TERMINATE_OK")
        self.assertEqual(links(self.c)["edge-A<->core-1"], 0)
        self.assertBooksBalance()

    def test_stale_endpoint_is_refused_and_client_follows_transparently(self):
        r = self.c.request("IMS-CSCF", "edge-A")
        name, sid = r["logical_name"], r["sid"]
        cl = ServiceClient(self.c.mhost, self.c.mport)
        old_ep = cl.resolve(name)
        self.assertEqual(old_ep["site_id"], "edge-A")
        self.c.rpc({"type": "MIGRATE", "name": name, "target_site": "edge-B"})
        from common import protocol
        # the old host no longer has it
        e = protocol.request(self.c.cfg["sites"]["edge-A"]["host"], self.c.cfg["sites"]["edge-A"]["port"],
                             {"type": "SERVICE_CALL", "sid": sid, "generation": 1})
        self.assertEqual(e["error"], "not_here")
        # the new host refuses the old generation
        e = protocol.request(self.c.cfg["sites"]["edge-B"]["host"], self.c.cfg["sites"]["edge-B"]["port"],
                             {"type": "SERVICE_CALL", "sid": sid, "generation": 1})
        self.assertEqual(e["error"], "stale_generation")
        # a client holding the old endpoint in cache is transparently redirected
        self.assertEqual(cl.call(name)["site_id"], "edge-B")
        self.assertEqual(cl.stats["relocations_observed"], 1)

    def test_rejections_leave_no_residue(self):
        du = self.c.request("vRAN-DU", "edge-A")
        before = (alloc(self.c, "core-1"), links(self.c))
        for ref, target, reason in ((du["sid"], "edge-A", "same_site"),
                                    (du["sid"], "core-1", "latency_constraint"),     # 2 ms bound
                                    (du["sid"], "nowhere", "destination_not_active"),
                                    ("S99999", "core-1", "unknown_service")):
            r = self.c.rpc({"type": "MIGRATE", "sid": ref, "target_site": target})
            self.assertEqual((r["type"], r["reason"]), ("MIGRATE_FAIL", reason))
        self.assertEqual((alloc(self.c, "core-1"), links(self.c)), before)
        self.assertEqual(self.c.mgr.allocation_table[du["sid"]]["state"], "RUNNING")
        self.assertBooksBalance()

    def test_insufficient_destination_capacity_and_bandwidth(self):
        # fill edge-B cpu (4000) with two vRAN-DUs (1500) + ... then try to migrate a UPF there
        for _ in range(2):
            self.c.request("vRAN-DU", "edge-B")
        self.c.request("Transcoder", "edge-B")                                    # cpu now 5000? -> goes elsewhere
        up = self.c.request("Transcoder", "edge-A")
        r = self.c.rpc({"type": "MIGRATE", "sid": up["sid"], "target_site": "edge-B"})
        self.assertEqual(r["type"], "MIGRATE_FAIL")
        self.assertIn(r["reason"], ("insufficient_capacity", "node_capacity"))
        self.assertBooksBalance()

    def test_failure_before_commit_rolls_back_completely(self):
        r = self.c.request("IMS-CSCF", "edge-A")
        sid, name = r["sid"], r["logical_name"]
        real_rpc = self.c.mgr.rpc

        def flaky(site, msg, **kw):
            if msg["type"] == "MIGRATION_IMPORT":
                raise ConnectionError("injected: destination unreachable during import")
            return real_rpc(site, msg, **kw)
        self.c.mgr.rpc = flaky
        m = self.c.rpc({"type": "MIGRATE", "name": name, "target_site": "core-1"})
        self.c.mgr.rpc = real_rpc
        self.assertEqual((m["type"], m["reason"]), ("MIGRATE_FAIL", "aborted"))
        self.assertEqual(self.c.rpc({"type": "RESOLVE", "name": name})["site_id"], "edge-A")   # still reachable
        self.assertEqual(hosting(self.c, sid), ["edge-A"])                                     # source thawed
        self.assertEqual(alloc(self.c, "core-1"), {"cpu": 0, "mem": 0, "bw": 0})               # reservation freed
        self.assertEqual(sum(links(self.c).values()), 0)
        self.assertEqual(self.c.mgr.allocation_table[sid]["generation"], 1)
        self.assertEqual(self.c.mgr.allocation_table[sid]["state"], "RUNNING")
        self.assertEqual(len(self.c.sites["core-1"].staged), 0)
        ops = [o for o in self.c.mgr.store.operations("MIGRATION")]
        self.assertEqual([o["state"] for o in ops], ["ABORTED"])
        self.assertBooksBalance()
        self.assertEqual(ServiceClient(self.c.mhost, self.c.mport).call(name)["site_id"], "edge-A")
        # and a later migration still works
        self.assertEqual(self.c.rpc({"type": "MIGRATE", "name": name, "target_site": "core-1"})["type"], "MIGRATE_OK")

    def test_failure_after_commit_decision_rolls_forward_never_duplicates(self):
        r = self.c.request("IMS-CSCF", "edge-A")
        sid, name = r["sid"], r["logical_name"]
        real_rpc = self.c.mgr.rpc

        def flaky(site, msg, **kw):
            if msg["type"] == "MIGRATION_COMMIT":
                raise ConnectionError("injected: destination vanished after commit decision")
            return real_rpc(site, msg, **kw)
        self.c.mgr.rpc = flaky
        m = self.c.rpc({"type": "MIGRATE", "name": name, "target_site": "core-1"})
        self.c.mgr.rpc = real_rpc
        self.assertEqual((m["type"], m["reason"]), ("MIGRATE_FAIL", "activation_failed"))
        # the endpoint never moves back and no site is running the service
        self.assertEqual(hosting(self.c, sid), [])
        self.assertEqual(self.c.rpc({"type": "RESOLVE", "name": name})["type"], "RESOLVE_FAIL")
        self.assertEqual(self.c.mgr.store.operation(m.get("operation_id") or
                                                    self.c.mgr.store.operations("MIGRATION")[0]["operation_id"])["state"],
                         "FAILED")
        # no replica -> the destination's missing instance is declared lost; everything is released
        self.assertTrue(self.c.wait(lambda: sid not in self.c.mgr.allocation_table, 10))
        self.assertEqual([alloc(self.c, s) for s in self.c.sites], [{"cpu": 0, "mem": 0, "bw": 0}] * 3)
        self.assertEqual(sum(links(self.c).values()), 0)
        self.assertBooksBalance()
        kinds = {e["event_type"] for e in self.c.mgr.store.all_events("FAILURE")}
        self.assertIn("INSTANCE_LOST", kinds)

    def test_in_flight_calls_survive_migration(self):
        r = self.c.request("IoT-GW", "edge-A")
        name = r["logical_name"]
        stop, errors, served = threading.Event(), [], []

        def hammer():
            cl = ServiceClient(self.c.mhost, self.c.mport)
            while not stop.is_set():
                try:
                    served.append(cl.call(name)["site_id"])
                except ServiceUnavailable as exc:
                    errors.append(str(exc))
        t = threading.Thread(target=hammer)
        t.start()
        time.sleep(0.3)
        for target in ("edge-B", "core-1", "edge-A"):
            self.assertEqual(self.c.rpc({"type": "MIGRATE", "name": name, "target_site": target})["type"], "MIGRATE_OK")
            time.sleep(0.2)
        stop.set()
        t.join()
        self.assertEqual(errors, [])                                   # clients never saw an outage
        self.assertEqual(set(served), {"edge-A", "edge-B", "core-1"})  # ... though the host changed under them
        self.assertEqual(self.c.mgr.allocation_table[r["sid"]]["generation"], 4)
        self.assertBooksBalance()


class TestReplicationAndFailover(Base):
    def test_standby_reserves_resources_syncs_and_never_executes(self):
        r = self.c.request("IMS-CSCF", "edge-A")
        sid, name = r["sid"], r["logical_name"]
        rep = self.c.rpc({"type": "REPLICATE", "name": name, "target_site": "edge-B"})
        rid = rep["replica_id"]
        self.assertEqual(rid, f"{sid}-R1")
        self.assertEqual(alloc(self.c, "edge-B")["cpu"], 500)                    # reserved on the standby site
        self.assertGreater(links(self.c)["edge-A<->edge-B"], 0)                  # sync bandwidth reserved
        self.assertEqual(hosting(self.c, sid), ["edge-A"])                       # the primary is unique
        standby = self.c.sites["edge-B"].instances[rid]
        self.assertEqual((standby.state, standby.role), ("STANDBY", "REPLICA"))
        time.sleep(1.0)
        self.assertEqual(self.c.sites["edge-B"].jobs_executed, 0)                # edge-B hosts only the standby: no job ran
        self.assertEqual(standby.in_flight, 0)
        from common import protocol
        e = protocol.request("127.0.0.1", self.c.cfg["sites"]["edge-B"]["port"],
                             {"type": "SERVICE_CALL", "sid": rid})
        self.assertTrue(e["error"].startswith("not_running"))
        # state synchronisation advances and tracks the primary
        seq = self.c.mgr.replicas[rid]["sync_seq"]
        self.assertTrue(self.c.wait(lambda: self.c.mgr.replicas[rid]["sync_seq"] > seq + 1, 5))
        self.assertEqual(self.c.rpc({"type": "RESIZE", "sid": sid, "factor": 2.0})["type"], "RESIZE_OK")
        self.assertEqual(alloc(self.c, "edge-B")["cpu"], 1000)                   # standby resized with its primary
        self.assertEqual(standby.tickets, 1000)
        self.assertBooksBalance()
        # rules
        self.assertEqual(self.c.rpc({"type": "REPLICATE", "name": name, "target_site": "edge-A"})["reason"],
                         "replica_must_be_on_different_site")
        self.assertEqual(self.c.rpc({"type": "REPLICATE", "name": name, "target_site": "edge-B"})["reason"],
                         "replica_already_on_site")
        # removal frees everything; reconfiguration moves a standby
        rc = self.c.rpc({"type": "REPLICA_RECONFIGURE", "replica_id": rid, "target_site": "core-1"})
        self.assertEqual(rc["type"], "REPLICA_RECONFIGURE_OK")
        self.assertEqual(alloc(self.c, "edge-B"), {"cpu": 0, "mem": 0, "bw": 0})
        self.assertEqual(alloc(self.c, "core-1")["cpu"], 1000)
        self.assertTrue(self.c.wait(lambda: rid not in self.c.sites["edge-B"].instances))
        self.assertEqual(self.c.rpc({"type": "REPLICA_REMOVE", "replica_id": rc["new_replica"]})["type"],
                         "REPLICA_REMOVE_OK")
        self.assertEqual(self.c.mgr.replicas, {})
        self.assertEqual(sum(links(self.c).values()), 0)
        self.assertBooksBalance()

    def test_migration_of_a_replicated_primary_rebalances_sync_path(self):
        r = self.c.request("IMS-CSCF", "edge-A")
        self.c.rpc({"type": "REPLICATE", "name": r["logical_name"], "target_site": "edge-B"})
        self.assertEqual(self.c.rpc({"type": "MIGRATE", "name": r["logical_name"], "target_site": "core-1"})["type"],
                         "MIGRATE_OK")
        rep = list(self.c.mgr.replicas.values())[0]
        self.assertEqual(rep["sync_path"][0], "core-1")                          # now syncs from the new primary site
        self.assertEqual(rep["state"], "STANDBY")
        self.assertBooksBalance()

    def test_site_crash_promotes_replica_with_fencing_and_single_execution(self):
        r = self.c.request("IMS-CSCF", "edge-A")
        sid, name = r["sid"], r["logical_name"]
        rep = self.c.rpc({"type": "REPLICATE", "name": name, "target_site": "edge-B"})
        self.assertTrue(self.c.wait(lambda: self.c.mgr.replicas[rep["replica_id"]]["sync_seq"] >= 3))
        cl = ServiceClient(self.c.mhost, self.c.mport)
        self.assertEqual(cl.call(name)["site_id"], "edge-A")
        self.c.crash_site("edge-A")
        self.assertTrue(self.c.wait(lambda: self.c.mgr.allocation_table[sid]["generation"] == 2
                                    and self.c.mgr.allocation_table[sid]["state"] == "RUNNING", 20))
        reply = cl.call(name)                                                   # same name, new location
        self.assertEqual((reply["site_id"], reply["generation"]), ("edge-B", 2))
        self.assertEqual(hosting(self.c, sid), ["edge-B"])
        self.assertEqual(self.c.mgr.sites["edge-A"]["status"], "FAILED")
        self.assertEqual(self.c.mgr.replicas, {})                               # the standby became the primary
        # accounting moved with it: nothing left at the failed site, primary reservation at the new site
        self.assertEqual(alloc(self.c, "edge-A"), {"cpu": 0, "mem": 0, "bw": 0})
        self.assertEqual(alloc(self.c, "edge-B")["cpu"], 500)
        self.assertBooksBalance()
        ops = self.c.mgr.store.operations("FAILOVER")
        self.assertEqual([o["state"] for o in ops], ["COMMITTED"])
        self.assertEqual(ops[0]["payload"]["fence_method"], "lease_expiry")     # site was unreachable
        events = [e["event_type"] for e in self.c.mgr.store.all_events("REPLICATION")]
        self.assertLess(events.index("PRIMARY_FENCED"), events.index("FAILOVER_COMPLETED"))
        # no allocation to a failed site
        blocked = self.c.request("IMS-CSCF", "edge-A", holding=5)
        self.assertNotEqual(blocked.get("site"), "edge-A")
        # the recovered site is reconciled; stale state is not resurrected
        self.c.start_site("edge-A")
        self.assertTrue(self.c.wait(lambda: self.c.mgr.sites["edge-A"]["status"] == "ACTIVE", 10))
        self.assertEqual(hosting(self.c, sid), ["edge-B"])
        self.assertBooksBalance()

    def test_partitioned_primary_fences_itself_then_stale_copy_is_released(self):
        r = self.c.request("IMS-CSCF", "edge-A")
        sid, name = r["sid"], r["logical_name"]
        rep = self.c.rpc({"type": "REPLICATE", "name": name, "target_site": "edge-B"})
        self.assertTrue(self.c.wait(lambda: self.c.mgr.replicas[rep["replica_id"]]["sync_seq"] >= 2))
        edge_a = self.c.sites["edge-A"]
        edge_a._on_fault({"mode": "partition", "duration_s": 3.5})
        concurrent = []
        deadline = time.time() + 12
        done = False
        while time.time() < deadline and not done:
            both = [site for site in ("edge-A", "edge-B")
                    if (lambda s: (s.instances.get(sid) is not None and s.instances[sid].state == "RUNNING"
                                   and s.instances[sid].role == "PRIMARY" and not s.fenced))(self.c.sites[site])]
            if len(both) > 1:
                concurrent.append(both)                                          # split brain would show up here
            done = self.c.mgr.allocation_table[sid]["generation"] == 2 and self.c.mgr.allocation_table[sid]["state"] == "RUNNING"
            time.sleep(0.02)
        self.assertTrue(done, "failover did not complete")
        self.assertEqual(concurrent, [])
        self.assertTrue(edge_a.fenced)                                          # it fenced itself via its lease
        # the partition drops the manager's FENCE connection, so safety rested on the site's own lease
        self.assertEqual(self.c.mgr.store.operations("FAILOVER")[0]["payload"]["fence_method"], "lease_expiry")
        # partition heals -> manager reconciles -> the stale primary is told to release
        self.assertTrue(self.c.wait(lambda: sid not in edge_a.instances, 15))
        self.assertTrue(self.c.wait(lambda: not edge_a.fenced and self.c.mgr.sites["edge-A"]["status"] == "ACTIVE", 10))
        self.assertEqual(hosting(self.c, sid), ["edge-B"])
        self.assertEqual(ServiceClient(self.c.mhost, self.c.mport).call(name)["site_id"], "edge-B")
        self.assertBooksBalance()

    def test_forced_failover_fences_a_healthy_primary_via_rpc(self):
        r = self.c.request("IMS-CSCF", "edge-A")
        sid, name = r["sid"], r["logical_name"]
        self.c.rpc({"type": "REPLICATE", "name": name, "target_site": "edge-B"})
        self.assertEqual(self.c.rpc({"type": "FAILOVER", "name": name})["reason"], "primary_healthy")
        fo = self.c.rpc({"type": "FAILOVER", "name": name, "force": True})
        self.assertEqual(fo["type"], "FAILOVER_OK", fo)
        self.assertEqual(fo["fence_method"], "rpc")
        self.assertEqual(hosting(self.c, sid, running_only=False), ["edge-B"])    # old copy was removed, not just stopped
        self.assertBooksBalance()

    def test_failed_promotion_tries_next_replica_and_reports_when_none(self):
        r = self.c.request("IoT-GW", "edge-A")
        name = r["logical_name"]
        self.c.rpc({"type": "REPLICATE", "name": name, "target_site": "edge-B"})
        self.c.rpc({"type": "REPLICATE", "name": name, "target_site": "core-1"})
        real = self.c.mgr.rpc

        refused = []

        def flaky(site, msg, **kw):                                              # first candidate refuses promotion
            if msg["type"] == "REPLICA_PROMOTE" and not refused:
                refused.append(site)
                raise ConnectionError("injected")
            return real(site, msg, **kw)
        self.c.mgr.rpc = flaky
        fo = self.c.rpc({"type": "FAILOVER", "name": name, "force": True})
        self.c.mgr.rpc = real
        self.assertEqual(fo["type"], "FAILOVER_OK")
        self.assertNotEqual(fo["new_site"], refused[0])                          # fell through to the other replica
        failed = [e for e in self.c.mgr.store.all_events("REPLICATION") if e["event_type"] == "FAILOVER_ATTEMPT_FAILED"]
        self.assertEqual(len(failed), 1)
        self.assertBooksBalance()
        # with no replica left a forced failover is reported, not faked
        r2 = self.c.request("IoT-GW", "edge-A")
        self.assertEqual(self.c.rpc({"type": "FAILOVER", "name": r2["logical_name"], "force": True})["reason"],
                         "no_eligible_replica")


class TestFailureDetection(Base):
    @staticmethod
    def cfg():
        c = make_config()
        c["failure_detector"] = {"suspect_after_heartbeats": 3, "failed_after_heartbeats": 12,
                                 "site_lease_heartbeats": 3, "missing_threshold_heartbeats": 3}
        return c

    def test_temporary_delay_is_suspect_not_failed_and_recovers_without_teardown(self):
        r = self.c.request("IMS-CSCF", "edge-A")
        sid, name = r["sid"], r["logical_name"]
        site = self.c.sites["edge-A"]
        site._on_fault({"mode": "partition", "duration_s": 1.3})
        self.assertTrue(self.c.wait(lambda: self.c.mgr.sites["edge-A"]["status"] == "SUSPECT", 5))
        self.assertEqual(self.c.rpc({"type": "RESOLVE", "name": name})["reason"], "unavailable")   # hidden, not destroyed
        self.assertEqual(self.c.mgr.allocation_table[sid]["state"], "RUNNING")
        self.assertTrue(self.c.wait(lambda: self.c.mgr.sites["edge-A"]["status"] == "ACTIVE", 8))
        self.assertEqual(self.c.rpc({"type": "RESOLVE", "name": name})["type"], "RESOLVED")        # restored after verification
        self.assertEqual(self.c.mgr.allocation_table[sid]["generation"], 1)                       # nothing was failed over
        self.assertEqual(self.c.mgr.store.operations("FAILOVER"), [])
        kinds = [e["event_type"] for e in self.c.mgr.store.all_events("FAILURE")]
        self.assertIn("SITE_SUSPECT", kinds)
        self.assertNotIn("SITE_FAILED", kinds)
        self.assertBooksBalance()

    def test_state_machine_and_unreplicated_loss_is_reported_consistently(self):
        r = self.c.request("IMS-CSCF", "edge-A")
        sid, name = r["sid"], r["logical_name"]
        self.c.crash_site("edge-A")
        self.assertTrue(self.c.wait(lambda: self.c.mgr.sites["edge-A"]["status"] == "FAILED", 15))
        self.assertEqual(self.c.mgr.allocation_table[sid]["state"], "FAILED")
        self.assertEqual(self.c.rpc({"type": "RESOLVE", "name": name})["type"], "RESOLVE_FAIL")
        with self.assertRaises(ServiceUnavailable):
            ServiceClient(self.c.mhost, self.c.mport).call(name, deadline_s=1.0)
        self.assertEqual(alloc(self.c, "edge-A")["cpu"], 500)                    # held: the site may come back intact
        self.c.start_site("edge-A")                                              # restarted => state lost
        self.assertTrue(self.c.wait(lambda: sid not in self.c.mgr.allocation_table, 10))
        self.assertEqual(alloc(self.c, "edge-A"), {"cpu": 0, "mem": 0, "bw": 0})
        self.assertEqual(self.c.mgr.sites["edge-A"]["status"], "ACTIVE")
        self.assertBooksBalance()
        seq = [e["event_type"] for e in self.c.mgr.store.all_events("FAILURE")
               if e["event_type"].startswith("SITE_") and e["site_id"] == "edge-A"]
        for a, b in (("SITE_SUSPECT", "SITE_FAILED"), ("SITE_FAILED", "SITE_RECOVERY_STARTED"),
                     ("SITE_RECOVERY_STARTED", "SITE_RECOVERED")):
            self.assertLess(seq.index(a), seq.index(b), seq)
        self.assertIn("INSTANCE_LOST", [e["event_type"] for e in self.c.mgr.store.all_events("FAILURE")])

    def test_controlled_recovery_requires_operator_approval(self):
        self.c.close()
        cfg = self.cfg()
        cfg["failure_detector"]["auto_recover"] = False
        self.c = Cluster(cfg)
        r = self.c.request("IMS-CSCF", "edge-A")
        self.c.sites["edge-A"]._on_fault({"mode": "partition", "duration_s": 3.0})
        self.assertTrue(self.c.wait(lambda: self.c.mgr.sites["edge-A"]["status"] == "FAILED", 15) or
                        self.c.mgr.sites["edge-A"]["status"] in ("FAILED", "RECOVERING"))
        self.assertTrue(self.c.wait(lambda: self.c.mgr.sites["edge-A"]["status"] == "RECOVERING", 15))
        time.sleep(0.8)
        self.assertEqual(self.c.mgr.sites["edge-A"]["status"], "RECOVERING")      # not trusted yet
        self.assertEqual(self.c.rpc({"type": "RESOLVE", "name": r["logical_name"]})["type"], "RESOLVE_FAIL")
        self.assertTrue(self.c.sites["edge-A"].fenced)                            # still fenced: no ack was given
        self.assertEqual(self.c.rpc({"type": "SITE_RECOVER", "site_id": "edge-A", "approve": True})["type"],
                         "SITE_RECOVER_OK")
        self.assertTrue(self.c.wait(lambda: self.c.mgr.sites["edge-A"]["status"] == "ACTIVE", 10))
        self.assertEqual(self.c.rpc({"type": "RESOLVE", "name": r["logical_name"]})["type"], "RESOLVED")
        self.assertFalse(self.c.sites["edge-A"].fenced)
        self.assertBooksBalance()

    def test_abandon_site_releases_everything(self):
        self.c.request("IMS-CSCF", "edge-A")
        self.c.crash_site("edge-A")
        self.assertEqual(self.c.rpc({"type": "ABANDON_SITE", "site_id": "edge-A"})["type"], "ABANDON_SITE_FAIL")
        self.assertTrue(self.c.wait(lambda: self.c.mgr.sites["edge-A"]["status"] == "FAILED", 15))
        self.assertEqual(self.c.rpc({"type": "ABANDON_SITE", "site_id": "edge-A"})["type"], "ABANDON_SITE_OK")
        self.assertEqual(self.c.mgr.allocation_table, {})
        self.assertEqual(alloc(self.c, "edge-A"), {"cpu": 0, "mem": 0, "bw": 0})
        self.assertBooksBalance()


class TestManagerRestart(Base):
    def test_control_plane_state_survives_and_nothing_is_falsely_available(self):
        a = self.c.request("IMS-CSCF", "edge-A")
        b = self.c.request("IoT-GW", "edge-B")
        self.c.rpc({"type": "MIGRATE", "name": b["logical_name"], "target_site": "core-1"})
        self.c.rpc({"type": "REPLICATE", "name": a["logical_name"], "target_site": "edge-B"})
        self.assertTrue(self.c.wait(lambda: list(self.c.mgr.replicas.values())[0]["sync_seq"] >= 2))
        snapshot = {s: alloc(self.c, s) for s in self.c.sites}
        link_snapshot = links(self.c)
        mgr = self.c.restart_manager()
        # restored immediately, but not available until the sites have been reconciled
        self.assertEqual(set(mgr.allocation_table), {a["sid"], b["sid"]})
        self.assertEqual(mgr.registry.lookup(a["logical_name"])["generation"], 1)
        self.assertEqual(mgr.registry.lookup(b["logical_name"])["generation"], 2)
        self.assertEqual(mgr.registry.lookup(b["logical_name"])["site_id"], "core-1")
        self.assertIsNone(mgr.registry.resolve(a["logical_name"]))
        self.assertEqual(mgr.allocation_table[a["sid"]]["state"], "RECOVERING")
        self.assertEqual({s: dict(v["allocated"]) for s, v in mgr.sites.items()}, snapshot)   # ledger restored
        self.assertEqual(links(self.c), link_snapshot)
        # after the next heartbeats the sites are reconciled and the services come back as they were
        self.assertTrue(self.c.wait(lambda: all(i["state"] == "RUNNING" for i in mgr.allocation_table.values())
                                    and all(s["status"] == "ACTIVE" for s in mgr.sites.values()), 10))
        for x in (a, b):
            self.assertEqual(self.c.rpc({"type": "RESOLVE", "name": x["logical_name"]})["type"], "RESOLVED")
        self.assertEqual(ServiceClient(self.c.mhost, self.c.mport).call(b["logical_name"])["site_id"], "core-1")
        self.assertTrue(self.c.wait(lambda: all(r["state"] == "STANDBY" for r in mgr.replicas.values()), 10))
        self.assertEqual({s: alloc(self.c, s) for s in self.c.sites}, snapshot)
        self.assertEqual(mgr.ledger.verify(), [])
        # identities are never reused
        n = self.c.request("UPF", "edge-A")
        self.assertNotIn(n["sid"], (a["sid"], b["sid"]))
        self.assertGreater(int(n["sid"][1:]), int(b["sid"][1:]))

    def test_manager_restart_with_a_dead_site_fails_over_after_detection(self):
        a = self.c.request("IMS-CSCF", "edge-A")
        self.c.rpc({"type": "REPLICATE", "name": a["logical_name"], "target_site": "edge-B"})
        self.assertTrue(self.c.wait(lambda: list(self.c.mgr.replicas.values())[0]["sync_seq"] >= 2))
        self.c.crash_site("edge-A")                                               # site dies while manager is down
        mgr = self.c.restart_manager()
        self.assertTrue(self.c.wait(lambda: mgr.allocation_table[a["sid"]]["site"] == "edge-B"
                                    and mgr.allocation_table[a["sid"]]["state"] == "RUNNING", 25))
        self.assertEqual(self.c.rpc({"type": "RESOLVE", "name": a["logical_name"]})["site_id"], "edge-B")
        self.assertEqual(mgr.ledger.verify(), [])

    def test_commit_decided_migration_is_rolled_forward_after_restart(self):
        r = self.c.request("IMS-CSCF", "edge-A")
        sid, name = r["sid"], r["logical_name"]
        real = self.c.mgr.rpc

        class ProcessDied(BaseException):          # not caught by any `except Exception`: no cleanup runs
            pass

        def die_at_activation(site, msg, **kw):
            if msg["type"] == "MIGRATION_COMMIT":
                raise ProcessDied()
            return real(site, msg, **kw)
        self.c.mgr.rpc = die_at_activation
        old_mgr = self.c.mgr
        try:
            self.c.rpc({"type": "MIGRATE", "name": name, "target_site": "core-1"}, timeout=3)
        except (ConnectionError, OSError):
            pass                                    # the connection thread died without a reply
        op = old_mgr.store.operations("MIGRATION")[0]
        self.assertEqual(op["state"], "COMMIT_DECIDED")                      # the crash left exactly this behind
        self.assertEqual(old_mgr.store.get_service(name)["site_id"], "core-1")   # ... with the endpoint already moved
        self.assertEqual(old_mgr.store.get_service(name)["generation"], 2)
        self.assertEqual(self.c.sites["edge-A"].instances[sid].state, "MIGRATING")   # source still fenced
        self.assertEqual(len(self.c.sites["core-1"].staged), 1)                      # destination still staged
        self.assertEqual(hosting(self.c, sid), [])                                  # nobody is executing it
        mgr = self.c.restart_manager()
        self.assertEqual(mgr.allocation_table[sid]["op"], op["operation_id"])          # restored as an unfinished operation
        self.assertIsNone(mgr.registry.resolve(name))                                  # and not falsely available
        self.assertTrue(self.c.wait(lambda: mgr.allocation_table[sid]["state"] == "RUNNING"
                                    and mgr.allocation_table[sid]["op"] is None, 20))
        self.assertEqual(mgr.store.operation(op["operation_id"])["state"], "COMMITTED")
        self.assertEqual(mgr.store.operation(op["operation_id"])["payload"].get("recovered_after_restart"), True)
        self.assertTrue(self.c.wait(lambda: sid not in self.c.sites["edge-A"].instances, 10))   # fenced source released
        self.assertEqual(hosting(self.c, sid, running_only=False), ["core-1"])
        self.assertEqual(ServiceClient(self.c.mhost, self.c.mport).call(name)["site_id"], "core-1")
        self.assertEqual({s: alloc(self.c, s) for s in ("edge-A", "core-1")},
                         {"edge-A": {"cpu": 0, "mem": 0, "bw": 0}, "core-1": {"cpu": 500, "mem": 256, "bw": 50}})
        self.assertEqual(mgr.ledger.verify(), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
