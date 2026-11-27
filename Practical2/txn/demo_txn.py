#!/usr/bin/env python3
"""
Milestone 5 walkthrough: prints the Prepare -> Commit -> Completion
trail for a normal transaction, then stages the one failure scenario
numbers alone can't show -- what happens when the COORDINATOR itself
crashes after a participant has voted YES, for 2PC vs 3PC.

    python3 -m txn.demo_txn
"""
import os
import shutil
import tempfile

from .coordinator import ThreePhaseCoordinator, TwoPhaseCoordinator
from .participant import ResourceParticipant
from .recovery import recover_in_doubt
from .reservation_txn import migration_txn, reservation_txn


def section(title):
    print(f"\n{'=' * 60}\n{title}\n{'=' * 60}")


def main():
    tmp = tempfile.mkdtemp(prefix="telerm_txn_demo_")

    # ---------------------------------------------------------------- 1
    section("1. A normal reservation transaction (2PC), full trail")
    site = ResourceParticipant("edge-A", {"cpu": 4000, "mem": 4096}, os.path.join(tmp, "site.wal"))
    link = ResourceParticipant("edge-A<->core-1", {"bw": 1000}, os.path.join(tmp, "link.wal"))
    coord = TwoPhaseCoordinator("manager", os.path.join(tmp, "coord.wal"))
    res = reservation_txn(coord, site, link, {"cpu": 1000, "mem": 512, "bw": 400}, "T001")
    print(f"phases executed : {' -> '.join(res.phases)}")
    print(f"votes           : {res.votes}")
    print(f"decision        : {res.decision}")
    print(f"latency         : {res.latency_s * 1000:.3f} ms")
    print(f"site allocated  : {site.allocated}")
    print(f"link allocated  : {link.allocated}")
    print("coordinator WAL :")
    for rec in coord.wal.read_all():
        print(f"    {rec}")

    # ---------------------------------------------------------------- 2
    section("2. A transaction that must ABORT (link has no room) -- atomicity check")
    link2 = ResourceParticipant("edge-A<->edge-B", {"bw": 50}, os.path.join(tmp, "link2.wal"))
    site2 = ResourceParticipant("edge-B", {"cpu": 4000, "mem": 4096}, os.path.join(tmp, "site2.wal"))
    res2 = reservation_txn(coord, site2, link2, {"cpu": 500, "mem": 256, "bw": 400}, "T002")
    print(f"phases executed : {' -> '.join(res2.phases)}")
    print(f"decision        : {res2.decision}")
    print(f"site2 allocated : {site2.allocated}  <- must be all zero: no dangling reservation")

    # ---------------------------------------------------------------- 3
    section("3. Migration transaction: move a running instance edge-A -> edge-B")
    site_b = ResourceParticipant("edge-B-mig", {"cpu": 4000, "mem": 4096}, os.path.join(tmp, "siteb_mig.wal"))
    # pretend an instance is already running on `site`, reserved outside a txn:
    site.prepare("precondition", {"cpu": 1500, "mem": 0})
    site.commit("precondition")
    print(f"before migration: edge-A={site.allocated}, edge-B={site_b.allocated}")
    mres = migration_txn(coord, site, site_b, {"cpu": 1500, "mem": 0}, "T003")
    print(f"decision        : {mres.decision}")
    print(f"after migration : edge-A={site.allocated}, edge-B={site_b.allocated}")

    # ---------------------------------------------------------------- 4
    section("4. The real 2PC-vs-3PC difference: COORDINATOR crashes mid-protocol")
    print("""
Scenario: a participant has voted YES (durably logged PREPARE_OK), and
then the COORDINATOR process dies before it can tell anyone the final
decision. The participant restarts. What can it safely do?
""")

    print("--- 2PC ---")
    s2pc = ResourceParticipant("site", {"cpu": 4000, "mem": 4096}, os.path.join(tmp, "s2pc.wal"))
    s2pc.prepare("TX", {"cpu": 500, "mem": 0})   # voted YES; coordinator now "dies"
    s2pc_restarted = ResourceParticipant("site", {"cpu": 4000, "mem": 4096}, os.path.join(tmp, "s2pc.wal"))
    print(f"in-doubt after restart : {s2pc_restarted.in_doubt()}")
    decision = s2pc_restarted.autonomous_decide("TX")
    print(f"can it decide alone?   : {decision!r}  -> MUST BLOCK until the coordinator "
          f"comes back (or a human/operator intervenes) -- classic 2PC blocking.")

    print("\n--- 3PC ---")
    s3pc = ResourceParticipant("site", {"cpu": 4000, "mem": 4096}, os.path.join(tmp, "s3pc.wal"))
    s3pc.prepare("TX", {"cpu": 500, "mem": 0})
    s3pc.precommit("TX")                          # coordinator sent PreCommit, then "dies"
    s3pc_restarted = ResourceParticipant("site", {"cpu": 4000, "mem": 4096}, os.path.join(tmp, "s3pc.wal"))
    print(f"in-doubt after restart : {s3pc_restarted.in_doubt()}")
    decision = s3pc_restarted.autonomous_decide("TX")
    print(f"can it decide alone?   : {decision!r}  -> YES: reaching PRECOMMIT proves every "
          f"participant voted YES, so committing alone is always safe here.")
    s3pc_restarted.commit("TX")
    print(f"site allocated after self-recovery: {s3pc_restarted.allocated}")

    # ---------------------------------------------------------------- 5
    section("5. Participant-side recovery via the coordinator's WAL (the 2PC fallback)")
    print("Once the coordinator DOES come back, recover_in_doubt() replays its logged "
          "decision onto whichever participant was stuck:")
    coord2 = TwoPhaseCoordinator("manager2", os.path.join(tmp, "coord2.wal"))
    link3 = ResourceParticipant("linkX", {"bw": 1000}, os.path.join(tmp, "linkX.wal"), crash_at="commit")
    site3 = ResourceParticipant("siteX", {"cpu": 4000, "mem": 4096}, os.path.join(tmp, "siteX.wal"))
    res3 = reservation_txn(coord2, site3, link3, {"cpu": 100, "mem": 0, "bw": 50}, "T004")
    print(f"coordinator decided : {res3.decision}")
    print(f"link3 in-doubt?      : {link3.in_doubt()}  (crashed inside commit() before logging it)")
    link3.crash_at = None  # link process restarts
    link3_restarted = ResourceParticipant("linkX", {"bw": 1000}, os.path.join(tmp, "linkX.wal"))
    applied = recover_in_doubt(coord2, link3_restarted, "T004")
    print(f"recovered decision   : {applied}")
    print(f"link allocated now   : {link3_restarted.allocated}  <- matches the coordinator's decision")

    shutil.rmtree(tmp, ignore_errors=True)
    print("\nDone.")


if __name__ == "__main__":
    main()
