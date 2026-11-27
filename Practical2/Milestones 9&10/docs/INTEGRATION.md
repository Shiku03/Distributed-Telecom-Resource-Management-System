# Running, testing and operating TeleRM (Milestones 9 & 10)

Python 3.9+ (tested on 3.12), standard library only.

## Run
```bash
python3 run_demo.py --policy edge_first                 # original pipeline, now with the control plane
python3 scripts/run_scenario.py --results-dir results/scenario   # 16-step acceptance scenario, real processes (~1 min)
python3 -m unittest discover -s tests                            # 89 tests, ~40 s
```
Manual:
```bash
python3 manager.py --config config.json --results-dir results/m1 [--policy latency_slack] [--db path.sqlite3]
python3 site.py --site-id edge-A --config config.json     # one per site
python3 generator.py --config config.json --results-dir results/m1
python3 scripts/export_events.py --db results/m1/telerm_control.sqlite3 --out results/m1/analytics
```
Restarting the manager with the same `--results-dir`/`--db` restores the control plane; sites re-register on their next heartbeat.

## Configuration (all optional, defaults shown in `config.json`)
`failure_detector.{suspect_after_heartbeats, failed_after_heartbeats, site_lease_heartbeats, missing_threshold_heartbeats, failed_instance_ttl_s, auto_recover, fence_rpc_timeout_s}`,
`migration.{quiesce_timeout_s, rpc_timeout_s, commit_retries}`, `replication.{sync_interval_s, sync_bw_fraction, max_replicas}`, `control_db`.
Invariant enforced at startup: `failed_after ≥ site_lease + 1 heartbeat`.

## Messages (TCP, 4-byte length + JSON, `corr_id` echoed)
Client/admin → manager: `REQUEST` (optional `logical_name`), `RESOLVE{name}`, `LOOKUP{service}`, `RESIZE{sid|name,factor}`, `TERMINATE{sid|name}`,
`MIGRATE{sid|name,target_site}`, `REPLICATE{sid|name,target_site}`, `REPLICA_REMOVE{replica_id}`, `REPLICA_RECONFIGURE{replica_id,target_site}`,
`FAILOVER{sid|name,[replica_id],[force]}`, `SITE_RECOVER{site_id}`, `ABANDON_SITE{site_id}`, `STATUS`, `CONTROL_STATUS`, `METRICS`, `WRITE_REPORT`, `SHUTDOWN`.
Site → manager: `REGISTER` (with `boot_id`, inventory), `HEARTBEAT` (with inventory; reply may carry `release`/`resume`/`hold`), `INSTANCE_EXIT`.
Manager → site: `ALLOCATE`, `RELEASE`, `RESIZE`, `MIGRATION_{PREPARE,IMPORT,COMMIT,SOURCE_RELEASE,ABORT}`, `REPLICA_{SNAPSHOT,CREATE,SYNC,PROMOTE,REMOVE}`, `FENCE`, `INVENTORY`, `STATUS`.
Client → site: `SERVICE_CALL{sid,generation}`. Fault injection → site: `FAULT_INJECT{mode: partition|heal|crash}`.
Errors are `{"type":"ERROR","error":…}` (missing field, malformed frame, invalid lifecycle transition…); failures of control operations are `*_FAIL` with a `reason`.

```python
from common.service_client import ServiceClient
c = ServiceClient("127.0.0.1", 5000)
c.call("service://telerm/UPF/S00007")       # survives migration, failover; raises ServiceUnavailable after the deadline
```

## Output files (`<results-dir>/`)
`manager_report.json`, `performance_metrics.json`, `instances_log.json`, `heartbeat_log.json`, `utilisation_timeseries.json`, `generator_log.json`, `generator_summary.json`,
`migration_log.json`, `failure_events.json`, `service_registry_log.json`, `name_resolution_log.json`, `process_lifecycle_log.json`, `replication_log.json`, `recovery_log.json`,
`telerm_control.sqlite3` (durable source), `analytics/{events.jsonl,events.csv,operations.csv}` (Spark / Power BI / MATLAB-readable).
Event `category` ∈ LIFECYCLE, NAMING, MIGRATION, REPLICATION, FAILURE, RECOVERY, RESOLUTION, SITE, HEARTBEAT.

## What the tests establish
* `tests/test_unit_milestones_9_10.py` (18): ledger atomicity/idempotence/audit, feasible-path selection, registry generations, lifecycle, SQLite persistence & constraints, detector thresholds, reconciliation planner.
* `tests/test_integration_milestones_9_10.py` (27, real sockets): naming vs actual host, concurrent admission never over-allocates, resize with standbys, termination frees everything, migration state/accounting/stale-endpoint rejection,
  rollback before commit, roll-forward after commit, calls surviving three migrations, standby never runs jobs, failover with fencing, partition + self-fencing + stale release, forced failover, failed promotion fall-through,
  SUSPECT without teardown, FAILED/recovery ordering, controlled recovery, abandon site, manager restart (idle, with dead site, crash at the commit point).
* `scripts/run_scenario.py` (43 checks, real OS processes incl. `SIGKILL` of a site and of the manager).
* The original 44 baseline tests pass unchanged.
