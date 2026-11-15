import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from txn.coordinator import TwoPhaseCoordinator, ThreePhaseCoordinator
from txn.participant import ResourceParticipant
from txn.recovery import reconcile, recover_in_doubt
from txn.reservation_txn import reservation_txn, migration_txn


class TxnTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def wal(self, name):
        return os.path.join(self.tmp, name + ".wal")


class TestTwoPhaseCommit(TxnTestBase):
    def test_commits_when_everyone_fits(self):
        site = ResourceParticipant("site", {"cpu": 1000, "mem": 1000}, self.wal("site"))
        link = ResourceParticipant("link", {"bw": 1000}, self.wal("link"))
        coord = TwoPhaseCoordinator("mgr", self.wal("coord"))
        res = reservation_txn(coord, site, link, {"cpu": 100, "mem": 100, "bw": 100}, "T1")
        self.assertEqual(res.decision, "COMMIT")
        self.assertEqual(res.phases, ["PREPARE", "COMMIT", "COMPLETE"])
        self.assertEqual(site.allocated["cpu"], 100)
        self.assertEqual(link.allocated["bw"], 100)

    def test_aborts_atomically_when_one_participant_cant_fit(self):
        # site has room, link does NOT -- the whole transaction must abort,
        # and the site must NOT end up with a dangling reservation.
        site = ResourceParticipant("site", {"cpu": 1000, "mem": 1000}, self.wal("site"))
        link = ResourceParticipant("link", {"bw": 50}, self.wal("link"))
        coord = TwoPhaseCoordinator("mgr", self.wal("coord"))
        res = reservation_txn(coord, site, link, {"cpu": 100, "mem": 100, "bw": 100}, "T1")
        self.assertEqual(res.decision, "ABORT")
        self.assertEqual(site.allocated["cpu"], 0)   # no partial/dangling reservation
        self.assertEqual(link.allocated["bw"], 0)

    def test_recovery_after_commit_reflects_same_state(self):
        site = ResourceParticipant("site", {"cpu": 1000, "mem": 1000}, self.wal("site"))
        link = ResourceParticipant("link", {"bw": 1000}, self.wal("link"))
        coord = TwoPhaseCoordinator("mgr", self.wal("coord"))
        reservation_txn(coord, site, link, {"cpu": 300, "mem": 100, "bw": 50}, "T1")
        # simulate a crash+restart: rebuild the participant from its WAL only
        site2 = ResourceParticipant("site", {"cpu": 1000, "mem": 1000}, self.wal("site"))
        self.assertEqual(site2.allocated["cpu"], 300)
        self.assertEqual(site2.in_doubt(), set())

    def test_in_doubt_after_prepare_without_completion(self):
        """If a participant votes YES and the WAL stops right there (no
        COMMIT/ABORT record -- modelling a coordinator or participant
        crash before Phase 2 completes), recovery must be able to see
        that this transaction is unresolved. This is the condition
        2PC cannot safely resolve locally (it must block / query the
        coordinator); 3PC's precommit step exists to avoid it."""
        site = ResourceParticipant("site", {"cpu": 1000, "mem": 1000}, self.wal("site"))
        site.prepare("T1", {"cpu": 100, "mem": 0})
        self.assertEqual(site.in_doubt(), {"T1"})
        site2 = ResourceParticipant("site", {"cpu": 1000, "mem": 1000}, self.wal("site"))
        self.assertEqual(site2.in_doubt(), {"T1"})
        self.assertIsNone(site2.autonomous_decide("T1"))  # must block: not precommitted


class TestThreePhaseCommit(TxnTestBase):
    def test_commits_with_precommit_phase(self):
        site = ResourceParticipant("site", {"cpu": 1000, "mem": 1000}, self.wal("site"))
        link = ResourceParticipant("link", {"bw": 1000}, self.wal("link"))
        coord = ThreePhaseCoordinator("mgr", self.wal("coord"))
        res = reservation_txn(coord, site, link, {"cpu": 100, "mem": 100, "bw": 100}, "T1")
        self.assertEqual(res.decision, "COMMIT")
        self.assertEqual(res.phases, ["CANCOMMIT", "PRECOMMIT", "COMMIT", "COMPLETE"])

    def test_autonomous_decide_after_precommit(self):
        """The whole point of 3PC: once a participant has been told to
        PRECOMMIT, it knows the coordinator must have seen all-YES votes
        (that's the only way precommit is ever sent), so even if the
        coordinator now vanishes, it can safely decide COMMIT on its
        own instead of blocking forever like the 2PC case above."""
        site = ResourceParticipant("site", {"cpu": 1000, "mem": 1000}, self.wal("site"))
        site.prepare("T1", {"cpu": 100, "mem": 0})
        site.precommit("T1")
        # coordinator "crashes" here -- never calls commit(). Participant
        # restarts and inspects its own WAL:
        site2 = ResourceParticipant("site", {"cpu": 1000, "mem": 1000}, self.wal("site"))
        self.assertEqual(site2.autonomous_decide("T1"), "COMMIT")

    def test_aborts_when_one_participant_fails_cancommit(self):
        site = ResourceParticipant("site", {"cpu": 1000, "mem": 1000}, self.wal("site"))
        link = ResourceParticipant("link", {"bw": 10}, self.wal("link"))
        coord = ThreePhaseCoordinator("mgr", self.wal("coord"))
        res = reservation_txn(coord, site, link, {"cpu": 100, "mem": 100, "bw": 100}, "T1")
        self.assertEqual(res.decision, "ABORT")
        self.assertEqual(res.phases, ["CANCOMMIT", "ABORT", "COMPLETE"])
        self.assertEqual(site.allocated["cpu"], 0)


class TestMigration(TxnTestBase):
    def test_migration_moves_reservation_atomically(self):
        src = ResourceParticipant("edge-A", {"cpu": 1000, "mem": 1000}, self.wal("a"))
        dst = ResourceParticipant("edge-B", {"cpu": 1000, "mem": 1000}, self.wal("b"))
        src.prepare("T0", {"cpu": 400, "mem": 0})
        src.commit("T0")
        self.assertEqual(src.allocated["cpu"], 400)

        coord = TwoPhaseCoordinator("mgr", self.wal("coord"))
        res = migration_txn(coord, src, dst, {"cpu": 400, "mem": 0}, "T1")
        self.assertEqual(res.decision, "COMMIT")
        self.assertEqual(src.allocated["cpu"], 0)
        self.assertEqual(dst.allocated["cpu"], 400)

    def test_migration_aborts_and_leaves_source_untouched_if_dest_full(self):
        src = ResourceParticipant("edge-A", {"cpu": 1000, "mem": 1000}, self.wal("a"))
        dst = ResourceParticipant("edge-B", {"cpu": 500, "mem": 1000}, self.wal("b"))
        src.prepare("T0", {"cpu": 400, "mem": 0})
        src.commit("T0")
        dst.prepare("other", {"cpu": 400, "mem": 0})
        dst.commit("other")  # dst now has only 100 free cpu

        coord = TwoPhaseCoordinator("mgr", self.wal("coord"))
        res = migration_txn(coord, src, dst, {"cpu": 400, "mem": 0}, "T1")
        self.assertEqual(res.decision, "ABORT")
        self.assertEqual(src.allocated["cpu"], 400)   # untouched -- still running at the source
        self.assertEqual(dst.allocated["cpu"], 400)   # unchanged (only "other")


class TestCrashInjection(TxnTestBase):
    def test_participant_crash_during_prepare_forces_abort(self):
        site = ResourceParticipant("site", {"cpu": 1000, "mem": 1000}, self.wal("site"), crash_at="prepare")
        link = ResourceParticipant("link", {"bw": 1000}, self.wal("link"))
        coord = TwoPhaseCoordinator("mgr", self.wal("coord"))
        res = reservation_txn(coord, site, link, {"cpu": 100, "mem": 100, "bw": 100}, "T1")
        self.assertEqual(res.decision, "ABORT")
        self.assertEqual(res.votes["site"], "CRASHED")
        self.assertEqual(link.allocated["bw"], 0)  # link's own abort still ran


class TestRecovery(TxnTestBase):
    def test_participant_crash_during_commit_is_resolved_by_recovery(self):
        site = ResourceParticipant("site", {"cpu": 1000, "mem": 1000}, self.wal("site"))
        link = ResourceParticipant("link", {"bw": 1000}, self.wal("link"), crash_at="commit")
        coord = TwoPhaseCoordinator("mgr", self.wal("coord"))
        res = reservation_txn(coord, site, link, {"cpu": 100, "mem": 100, "bw": 50}, "T1")

        self.assertEqual(res.decision, "COMMIT")           # coordinator's decision
        self.assertEqual(link.allocated["bw"], 0)           # but link crashed before applying it
        self.assertEqual(link.in_doubt(), {"T1"})

        link.crash_at = None  # process restarts
        outcomes = reconcile(coord, [site, link])
        self.assertEqual(outcomes[("link", "T1")][0], "COMMIT")
        self.assertEqual(link.allocated["bw"], 50)           # now matches the decision
        self.assertEqual(link.in_doubt(), set())

    def test_recover_in_doubt_returns_none_if_coordinator_has_no_record_either(self):
        site = ResourceParticipant("site", {"cpu": 1000, "mem": 1000}, self.wal("site"))
        coord = TwoPhaseCoordinator("mgr", self.wal("coord"))  # never ran any transaction
        site.prepare("ghost", {"cpu": 10, "mem": 0})
        self.assertIsNone(recover_in_doubt(coord, site, "ghost"))

    def test_3pc_autonomous_recovery_needs_no_coordinator_at_all(self):
        site = ResourceParticipant("site", {"cpu": 1000, "mem": 1000}, self.wal("site"))
        link = ResourceParticipant("link", {"bw": 1000}, self.wal("link"))
        coord = ThreePhaseCoordinator("mgr", self.wal("coord"), participant_timeout=0.01)
        # crash the coordinator's own WAL path to prove recovery doesn't need it:
        # simplest proxy -- just don't consult it, call reconcile only with the
        # participants and check autonomous_decide alone resolves it.
        site.prepare("TX", {"cpu": 50, "mem": 0})
        site.precommit("TX")
        site2 = ResourceParticipant("site", {"cpu": 1000, "mem": 1000}, self.wal("site"))
        self.assertEqual(site2.autonomous_decide("TX"), "COMMIT")


if __name__ == "__main__":
    unittest.main()
