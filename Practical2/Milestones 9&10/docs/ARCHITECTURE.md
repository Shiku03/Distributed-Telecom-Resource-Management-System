# TeleRM Milestones 9 & 10 — Architecture

Transparency and reliability (M9), naming and distributed process management (M10).
Everything below is implemented in the running manager/site processes; section 9 lists what is *not*.

## 1. Components

```
                    clients ──RESOLVE/LOOKUP──▶  MANAGER (authoritative)
   ServiceClient ◀─ endpoint+generation ─────   ├ ServiceRegistry   logical name → endpoint, generation
        │                                       ├ ResourceLedger    every node/path reservation, by owner & op id
        │ SERVICE_CALL(sid, generation)         ├ ControlPlane      worker + sync threads, restore, scan, reconcile
        ▼                                       │   ├ MigrationCoordinator   (common/migration.py)
   SITE (own OS process)                        │   └ ReplicationManager     (common/replication.py)
    ├ stride scheduler, worker pool             ├ FailureDetector   ACTIVE→SUSPECT→FAILED→RECOVERING→ACTIVE
    ├ instances: PRIMARY / STANDBY(REPLICA)     └ DurableStore      SQLite (WAL): the durable control plane
    ├ staged (destination fence), fence_floor
    └ manager lease (self-fencing)
```

| Module | Responsibility |
|---|---|
| `manager.py` | dispatch + validation, admission, resize, termination, resolution, site contact/reconciliation, detector & sampler threads |
| `common/control_plane.py` | restore after restart, failure scan, apply reconciliation plan, loss handling, admin messages, 2 fixed threads |
| `common/migration.py` | transactional, fenced migration; roll-back before / roll-forward after the commit point |
| `common/replication.py` | standby creation/removal/reconfiguration, periodic sync, fenced failover |
| `common/ledger.py` | the only code that changes `site["allocated"]` or link `reserved`; ownership, idempotent release, `verify()` |
| `common/durable_store.py` | SQLite schema (sites, site_health, services, processes, replicas, reservations, operations, events) |
| `common/service_registry.py` | names, generations, resolvable states, restore-as-unavailable |
| `common/failure_recovery.py` | thresholds, lease inequality check, pure `plan_reconciliation()` |
| `common/site_node.py` / `site_transfer.py` | site process; migration/replica/fence handlers; lease; fault injector |
| `common/instance.py` | `Instance` (moved out of `site.py`, see §8) |
| `common/service_client.py` | resolve/call/retry client |
| `common/reporting.py` | metrics + all result files |

## 2. Naming and resolution

* Name: `service://telerm/{service}/{instance_id}` (or a caller-supplied `service://…` name, rejected if in use).
  The name is bound to the instance id for life and **never changes on migration or failover** — a promoted replica
  takes over the primary's instance id.
* Every record has a `generation`. Migration and promotion install `generation+1`; `update_location` refuses to go backwards.
* A name resolves only in `RUNNING`/`RESIZING`. `MIGRATING` replies `RESOLVE_FAIL reason=migrating retryable=true`;
  `SUSPECT`/`FAILED`/restored/draining names reply `unavailable`; terminated names `not_found`.
* Data path: `SERVICE_CALL{sid, generation}` is answered only by a site that hosts the *running, current-generation, non-fenced*
  primary. Errors: `not_here`, `stale_generation`, `not_running:<state>` (standby, frozen, draining), `site_fenced`.
  `ServiceClient.call` turns these (and connection failures) into: drop cache → re-resolve → back off → retry, until a deadline.
* Persistence: `services` table written in the same SQLite transaction as the `processes` row. After a manager restart every
  record is restored **UNAVAILABLE**; it becomes resolvable only when the hosting site's inventory verifies it.

## 3. Process lifecycle

`CREATE→LOCATE→SCHEDULE` = admission (`CREATED`, placement via `common/placement.py`); `ALLOCATE`=`ALLOCATING`; `RUN`=`RUNNING`;
`RESIZE`=`RESIZING`; `MIGRATE`=`MIGRATING`; `DRAIN`=`DRAINING`; `TERMINATE`=`TERMINATED`; plus `FAILED`, `RECOVERING`, and `STANDBY`
(a passive replica). `common/lifecycle.transition()` rejects illegal moves (e.g. `TERMINATED→RUNNING`, `STANDBY→RESIZING`).
Every change goes through `Manager.set_state`, which writes a durable event with timestamp, instance id, logical name, site,
operation id and generation.

## 4. Resource and network accounting

All reservations are ledger entries `{res_id, owner, op_id, kind NODE|PATH, purpose, demand, HELD|RELEASED}`:

* admission reserves node + path **under the manager lock, then releases the lock for the ALLOCATE RPC**, then finalises or rolls back;
* `release()` is idempotent (a duplicate `INSTANCE_EXIT` cannot double-release); `release_owner` / `release_op` free exactly what an
  instance / operation holds (so an aborted migration cannot leak the destination reservation);
* resize reserves growth first on the primary **and every standby**, rolls everything back if any RPC fails;
* migration/failover use make-before-break: the destination reservation exists before the commit point, the source one is released in the commit transaction;
* `ledger.verify()` recomputes every site and link total from HELD reservations; the tests assert `== []` after every scenario and `STATUS` exposes it;
* path selection: `Topology.best_feasible_path` = lowest-latency path with the bandwidth and within the latency bound — a saturated shortest path no longer hides a feasible alternative.

## 5. Migration (transparent, transactional)

```
PREPARING → SOURCE_FENCED → DESTINATION_STAGED → COMMIT_DECIDED → COMMITTED        (operations table)
              │ source frozen,     │ destination holds snapshot,   │ ONE lock hold + ONE SQLite txn:
              │ in-flight jobs     │ invisible to scheduler        │ ledger switch, allocation record, endpoint
              │ drained, snapshot  │ and to service calls          │ + generation+1, process row, op state
```
Validation: source/destination ACTIVE, different sites, destination has no replica of the same primary, latency bound holds,
destination capacity, a bandwidth-feasible new path. Preserved state: instance id, service, logical name, demand, **remaining holding time** (timer is
frozen during the move), scheduler tickets/stride/pass (rebased relative to the destination's minimum pass), counters, calls served, generation (+1).

* failure **before** `COMMIT_DECIDED`: thaw the source, discard the staged copy, release everything held under the op id → `ABORTED`;
* failure **after**: roll forward only (retry activation; if impossible the service is `FAILED` on a failed destination and recovery decides: replica promotion or controlled loss). The source stays fenced → two copies never run;
* manager crash: pre-commit ops are rolled back on restart (fenced source is told to `resume`); `COMMIT_DECIDED` ops are rolled forward when the destination re-registers.

## 6. Replication and failover

A standby reserves the primary's full vector on its own site (so promotion cannot over-allocate), reserves `sync_bw_fraction × bw` on the primary→replica
path, is synchronised every `sync_interval_s` (snapshot pulled from the primary, pushed to the replica, sequence-numbered), is never scheduled and refuses service calls.
Replication = extra standby copy; migration = relocate the active copy.

Failover: choose the freshest bandwidth-feasible replica → record op → **fence the old primary** (`FENCE` RPC removes it and raises the site's `fence_floor`;
if the site is unreachable, the guarantee is its lease) → `REPLICA_PROMOTE` at `generation+1` → one transaction switching ledger/record/endpoint/replica table →
failed attempts fall through to the next replica; with none the service stays `FAILED/unavailable` and is reported as such.
A primary that returns later is told to release its stale copy (reconciliation) and can never serve (generation).

**Split-brain argument.** Each site fences itself (stops scheduling, refuses calls, defers expiry) after `site_lease_heartbeats` without a manager acknowledgement.
The manager promotes only after `failed_after_heartbeats`. `FailureConfig` refuses configurations where `failed_after ≥ lease + 1 heartbeat` does not hold.
A fenced site resumes only after a heartbeat reply carrying the manager's reconciliation verdict. (Test: a partitioned primary is sampled every 20 ms during failover; two live primaries never coexist.)

## 7. Failure detection and recovery

`ACTIVE → SUSPECT` (age > suspect): services hidden, nothing torn down; one heartbeat returns it to ACTIVE after inventory verification (no false-positive teardown).
`SUSPECT → FAILED` (age > failed): hosted services `FAILED`, standbys `UNAVAILABLE`, allocation blocked, failover queued.
`FAILED → RECOVERING → ACTIVE` when the site talks again **and** its inventory has been reconciled (`plan_reconciliation`): confirmed instances restored, stale/orphan copies released,
missing instances declared lost after `missing_threshold` heartbeats (immediately if the site's `boot_id` changed — it restarted and lost its state), lost instances fail over or are terminated with all reservations freed.
`failure_detector.auto_recover=false` = **controlled recovery**: the site stays `RECOVERING` (and fenced) until `SITE_RECOVER`; `ABANDON_SITE` declares a failed site's state gone; `failed_instance_ttl_s` bounds how long an unreplicated service waits for its site.

## 8. Corrections to the baseline (all tested)

1. `latency_slack` (the `config.json` default) was rejected by `manager.py --policy` and silently ran `edge_first`; the manager now uses `common/placement.py`, the same code as the simulator.
2. Re-registering a known site used to reset its `allocated` vector while allocations stayed → accounting corrupted; now ledger-owned and reconciled.
3. `Instance` cannot live in `site.py` (the stdlib has a `site` module, so `from site import Instance` is impossible); moved to `common/instance.py`, `Site` to `common/site_node.py`; `site.py` stays the executable.
4. Admission no longer performs a network call under the global lock (reserve → RPC → finalise).
5. Names are resolvable while `RESIZING`; `MIGRATING` is reported as retryable.
6. Sockets: closing a listening socket does not wake a thread blocked in `accept()`; `protocol.close_server` shuts down first, so a stopped site really stops answering and a restarted manager can rebind.
7. Scaffold defects: `MigrationCoordinator.migrate` signature mismatch, `ServiceClient` expecting a key the manager never sent, migration that moved instances without touching manager accounting.

## 9. Limitations (read before relying on this)

* **One manager.** It is the authority and a single point of failure; restart recovery is implemented, manager replication is not.
* **Detection window.** Until `suspect_after` elapses, a name still resolves to a just-crashed site (measured ≈1.2–1.5 s with 0.5 s heartbeats ×3). Clients recover through the retry path; the manager cannot know sooner.
* The lease argument assumes local monotonic clocks run at roughly the same rate; it does not need synchronised clocks.
* **Simulated state.** Migration/replication move simulation state (identity, demand, timers, scheduler counters); there is no real packet/process state. Migration state transfer does not consume modelled link bandwidth (only the new ingress path reservation does). `SERVICE_CALL` is a counted echo, not a workload.
* No automatic re-replication after a promotion; replicas must be re-created with `REPLICATE`. A standby holds the full resource vector (no "warm/cold" tiers).
* An unreplicated service on a failed site waits for the site (up to `failed_instance_ttl_s`) rather than being rescheduled elsewhere (its state would be lost anyway).
* Counters for offered/admitted/blocked, decision time, resolution latency and the availability accumulators are **per manager process** (reset on restart); migration/failover/replication metrics come from SQLite and span restarts.
* SQLite runs WAL + `synchronous=NORMAL`: consistent across a process crash; the last transaction could be lost on sudden power loss.
* Verified on one host (loopback, separate OS processes). Not tested across hosts, with message loss/reordering beyond connection resets, or at scale.
* Not automatically tested: a manager restart in which the migration destination is also down (the code path exists: the op is marked FAILED and the service handled by the failure scan).

## 10. Measurement definitions (`performance_metrics.json`)

Timestamps: epoch seconds UTC. `*_s` seconds, `*_ms` milliseconds, utilisation/rates/ratios fractions in [0,1], bandwidth in topology units.

| Metric | Definition |
|---|---|
| allocation success rate | admitted / offered (this manager process) |
| allocation decision time | REQUEST receipt → reply, incl. the ALLOCATE round trip |
| service availability | 1 − Σ unavailable time / Σ lifetime over all instances; unavailable = not resolvable (SUSPECT/FAILED/MIGRATING/restored); outage start is backdated to the failed site's last heartbeat |
| migration duration / downtime | request → COMMITTED / source frozen → destination active (name unresolvable) |
| migration success rate | COMMITTED / (COMMITTED+ABORTED+FAILED) |
| failure detection time | last heartbeat → FAILED declaration |
| recovery time | outage start (last heartbeat) → name resolvable again; failover duration = promotion op only |
| failover success rate | committed / finished failover operations |
| replication overhead | standby-reserved share of allocated CPU (time-averaged), sync bandwidth reserved, sync bytes / rounds / mean sync ms (from SQLite) |
| resolution latency | manager-side compute time per RESOLVE; clients also record round-trip (`ServiceClient.resolve_latency_ms`) |
| bandwidth utilisation | per-link mean/peak of reserved/capacity from the sampler |
| failed requests | blocked requests by reason + failed resolutions |
| throughput | admissions/s, completions/s, jobs/s over manager uptime |
