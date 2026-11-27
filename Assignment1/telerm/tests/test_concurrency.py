"""
Unit tests for Milestone 6 (concurrency / deadlock management).

Run with:
    python3 -m unittest discover -s tests -v
"""
import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from concurrency.deadlock import DeadlockMonitor, choose_victim, find_cycle
from concurrency.lock_manager import DeadlockVictim, LockManager
from concurrency.prevention import WoundWaitManager, acquire_ordered


class TestCycleDetection(unittest.TestCase):
    def test_no_cycle(self):
        self.assertIsNone(find_cycle([("P1", "P2"), ("P2", "P3")]))

    def test_simple_two_cycle(self):
        cycle = find_cycle([("P1", "P2"), ("P2", "P1")])
        self.assertIsNotNone(cycle)
        self.assertEqual(set(cycle), {"P1", "P2"})

    def test_longer_cycle_detected(self):
        # P4 -> P1 -> P2 -> P3 -> P1: P4 merely feeds into the cycle, it is
        # not part of it, so it must NOT appear in the reported cycle.
        cycle = find_cycle([("P1", "P2"), ("P2", "P3"), ("P3", "P1"), ("P4", "P1")])
        self.assertIsNotNone(cycle)
        self.assertEqual(cycle[0], cycle[-1], "a cycle must close on itself")
        self.assertEqual(set(cycle), {"P1", "P2", "P3"})  # the repeated endpoint dedupes away
        self.assertNotIn("P4", cycle)

    def test_empty_graph(self):
        self.assertIsNone(find_cycle([]))


class TestLockManagerBasics(unittest.TestCase):
    def test_grant_and_release(self):
        lm = LockManager()
        self.assertTrue(lm.acquire("P1", "R1"))
        self.assertEqual(lm.holders["R1"], "P1")
        lm.release("P1", "R1")
        self.assertNotIn("R1", lm.holders)

    def test_reentrant_same_process(self):
        lm = LockManager()
        self.assertTrue(lm.acquire("P1", "R1"))
        self.assertTrue(lm.acquire("P1", "R1"))  # re-entrant, doesn't deadlock itself

    def test_second_process_blocks_until_release(self):
        lm = LockManager()
        lm.acquire("P1", "R1")
        got = []

        def waiter():
            got.append(lm.acquire("P2", "R1", timeout=2))

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.05)
        self.assertEqual(lm.waiters["R1"], ["P2"])
        lm.release("P1", "R1")
        t.join(timeout=2)
        self.assertEqual(got, [True])
        self.assertEqual(lm.holders["R1"], "P2")

    def test_timeout_returns_false_not_exception(self):
        lm = LockManager()
        lm.acquire("P1", "R1")
        self.assertFalse(lm.acquire("P2", "R1", timeout=0.05))

    def test_resource_allocation_graph_shape(self):
        lm = LockManager()
        lm.acquire("P1", "R1")
        got = []
        t = threading.Thread(target=lambda: got.append(lm.acquire("P2", "R1", timeout=2)))
        t.start()
        time.sleep(0.05)
        g = lm.resource_allocation_graph()
        self.assertIn(("R1", "P1"), g["allocation"])
        self.assertIn(("P2", "R1"), g["request"])
        lm.release("P1", "R1")
        t.join(timeout=2)


class TestDeadlockDetectionAndRecovery(unittest.TestCase):
    def test_ab_ba_deadlock_is_detected_and_resolved(self):
        lm = LockManager()
        results = []

        def worker(pid, first, second):
            try:
                lm.acquire(pid, first)
                time.sleep(0.05)  # widen the window so both sides truly hold-and-wait
                lm.acquire(pid, second, timeout=5)
                results.append((pid, "GRANTED"))
                lm.release(pid, second)
                lm.release(pid, first)
            except DeadlockVictim:
                results.append((pid, "ABORTED"))
                lm.release_all(pid)

        t1 = threading.Thread(target=worker, args=("P1", "R1", "R2"))
        t2 = threading.Thread(target=worker, args=("P2", "R2", "R1"))
        t1.start(); t2.start()

        monitor = DeadlockMonitor(lm, poll_interval=0.01)
        monitor.start()
        t1.join(timeout=5)
        t2.join(timeout=5)
        monitor.stop()

        self.assertGreaterEqual(len(monitor.events), 1, "monitor should have found the cycle")
        outcomes = {r[1] for r in results}
        self.assertIn("ABORTED", outcomes, "exactly one side must have been the victim")
        self.assertIn("GRANTED", outcomes, "the other side must have completed")
        # no resource should still be held after both threads finish
        self.assertEqual(lm.holders, {})

    def test_choose_victim_fewest_resources(self):
        lm = LockManager()
        lm.acquire("P1", "R1")
        lm.acquire("P1", "R2")
        lm.acquire("P2", "R3")
        victim = choose_victim(["P1", "P2"], lm, strategy="fewest_resources")
        self.assertEqual(victim, "P2")  # P2 holds fewer resources -> cheaper to abort


class TestPreventionOrdering(unittest.TestCase):
    def test_acquire_ordered_never_deadlocks(self):
        lm = LockManager()
        results = []

        def worker(pid, resources):
            ok = acquire_ordered(lm, pid, resources, timeout=2)
            results.append((pid, ok))
            if ok:
                time.sleep(0.02)
                for r in resources:
                    lm.release(pid, r)

        t1 = threading.Thread(target=worker, args=("P1", ["R2", "R1"]))
        t2 = threading.Thread(target=worker, args=("P2", ["R1", "R2"]))
        t1.start(); t2.start()
        t1.join(timeout=3); t2.join(timeout=3)

        self.assertTrue(all(ok for _, ok in results), "ordered acquisition must never time out here")


class TestWoundWait(unittest.TestCase):
    def test_older_process_wounds_younger_holder(self):
        lm = LockManager()
        ww = WoundWaitManager(lm)
        ww.timestamp_for("OLD")   # registers first -> older/higher priority
        ww.timestamp_for("YOUNG")
        ww.acquire("YOUNG", "R1", timeout=1)  # YOUNG grabs it first

        # OLD requests R1, held by YOUNG: OLD is older -> wounds YOUNG
        got = []

        def older_requester():
            got.append(ww.acquire("OLD", "R1", timeout=2))

        t = threading.Thread(target=older_requester)
        t.start()
        time.sleep(0.05)
        self.assertEqual(len(ww.wounds), 1)
        self.assertEqual(ww.wounds[0][1], "YOUNG")  # (ts, wounded, by)
        lm.release_all("YOUNG")  # YOUNG's own rollback after being wounded
        t.join(timeout=2)
        self.assertEqual(got, [True])

    def test_younger_process_dies_instead_of_waiting(self):
        lm = LockManager()
        ww = WoundWaitManager(lm)
        ww.timestamp_for("OLD")
        ww.timestamp_for("YOUNG")
        ww.acquire("OLD", "R1", timeout=1)  # OLD holds it

        with self.assertRaises(DeadlockVictim):
            ww.acquire("YOUNG", "R1", timeout=1)  # YOUNG is younger -> dies, doesn't block


if __name__ == "__main__":
    unittest.main()
