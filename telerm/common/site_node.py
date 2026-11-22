"""TeleRM site node (design doc 3.1/4.4): registers with the manager, then
runs a fixed worker pool driven by stride scheduling. Every running
instance is backlogged (always has jobs waiting); the scheduler hands
each free worker to the eligible instance with the smallest pass value,
then advances that instance's pass by its stride = 10^6 / tickets.

Workers are implemented as threads within this one OS process (each
site *is* its own OS process -- that's what satisfies "every node is a
separate OS process with its own TCP port" in the design doc). We
simulate a "job" as a short unit of work of configurable duration
rather than real traffic processing, since S1 has no real payload.

Milestone 9/10 additions
------------------------
* generation-checked SERVICE_CALL: the data path a client uses after it has
  resolved a logical name; stale, fenced, standby or frozen instances refuse;
* migration / replica / fence handlers (common/site_transfer.py);
* a manager *lease*: a site that has not had a heartbeat acknowledged for
  ``lease_s`` fences itself (stops scheduling and refuses service calls) until
  the manager has reconciled it.  This is what makes promoting a replica
  safe while the old primary's site is only partitioned, not dead;
* the heartbeat carries the full instance inventory so the manager can
  reconcile, and re-registers automatically after a manager restart;
* a fault injector (``FAULT_INJECT``) for failure experiments.

The class lives here (not in site.py) because site.py is the executable and
shares its name with a standard-library module.
"""
import argparse
import hashlib
import json
import math
import os
import random
import threading
import time
import uuid

from common import protocol, util, site_transfer
from common.counters import Counters
from common.failure_recovery import FailureConfig
from common.instance import Instance
from common.lifecycle import event as lifecycle_event, transition as lifecycle_transition

__all__ = ["Site", "Instance", "main"]


class Site:
    def __init__(self, site_id, config, manager_host, manager_port, allow_crash=False):
        self.site_id = site_id
        self.config = config
        self.manager_host = manager_host
        self.manager_port = manager_port
        scfg = config["sites"][site_id]
        self.host = scfg["host"]
        self.port = scfg["port"]
        self.tier = scfg["tier"]
        self.capacity = {"cpu": scfg["cpu"], "mem": scfg["mem"], "bw": scfg["bw"]}
        self.n_workers = scfg["workers"]
        self.job_service_ms = config.get("job_service_ms", 6)
        self.heartbeat_interval = config.get("heartbeat_interval_s", 1.0)
        self.fairness_threshold = config.get("fairness_saturation_threshold", 0.9)
        self.failure_cfg = FailureConfig.from_config(config)
        self.lease_s = self.failure_cfg.lease_s
        self.allow_crash = allow_crash
        self.boot_id = uuid.uuid4().hex[:10]

        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.instances = {}   # sid -> Instance (primaries and standby replicas)
        self.staged = {}      # (operation_id, sid) -> staged migration import (destination fence)
        self.fence_floor = {}  # sid -> highest generation this site refuses
        self.lifecycle_events = []
        self._threads = []
        self.rng = random.Random(int.from_bytes(hashlib.sha256(site_id.encode("utf-8")).digest()[:4], "big"))
        self.counters = Counters()

        self._stop = threading.Event()
        self._srv = None
        self.busy_workers = 0
        self.jobs_executed = 0      # jobs this site's workers actually ran (not synchronised state)
        self.busy_time_since_hb = 0.0
        self._last_hb_ts = time.time()
        # manager lease / fault injection
        self.last_ack_mono = time.monotonic()
        self.fenced = False
        self.partitioned_until = 0.0

    # ---------------------------------------------------------- helpers
    def _event(self, sid, name, state, **details):
        """Record a lifecycle event (caller normally holds self.cv)."""
        self.lifecycle_events.append(lifecycle_event(sid, name, state, site=self.site_id, **details))
        if len(self.lifecycle_events) > 5000:
            del self.lifecycle_events[:1000]

    def _inventory_locked(self):
        return [{"sid": sid, "state": i.state, "generation": i.generation, "role": i.role,
                 "service": i.service} for sid, i in self.instances.items()]

    # ---------------------------------------------------------- lifecycle
    def start(self, register_retries=20):
        self._srv = protocol.serve_forever(self.host, self.port, self._dispatch, counters=self.counters)
        last = None
        for _ in range(register_retries):
            try:
                self._register()
                last = None
                break
            except (ConnectionError, OSError, TimeoutError) as exc:
                last = exc
                time.sleep(0.5)
        if last is not None:
            raise RuntimeError(f"could not register with manager: {last}")
        for i in range(self.n_workers):
            self._start_thread(self._worker_loop, args=(i,), name=f"{self.site_id}-worker-{i}")
        self._start_thread(self._reaper_loop, name=f"{self.site_id}-reaper")
        self._start_thread(self._heartbeat_loop, name=f"{self.site_id}-heartbeat")
        print(f"[{self.site_id}] listening on {self.host}:{self.port}, "
              f"{self.n_workers} workers, registered with manager", flush=True)

    def _start_thread(self, target, args=(), name=None):
        thread = threading.Thread(target=target, args=args, daemon=True, name=name)
        self._threads.append(thread)
        thread.start()
        return thread

    def stop(self):
        self._stop.set()
        with self.cv:
            self.cv.notify_all()
        protocol.close_server(self._srv)
        current = threading.current_thread()
        for thread in self._threads:
            if thread is not current:
                thread.join(timeout=2.0)

    crash = stop   # an in-process "crash": stop everything, tell nobody

    def _register(self):
        with self.cv:
            inventory = self._inventory_locked()
        reply = protocol.request(self.manager_host, self.manager_port, {
            "type": "REGISTER", "site_id": self.site_id, "host": self.host, "port": self.port,
            "tier": self.tier, "capacity": self.capacity, "workers": self.n_workers,
            "boot_id": self.boot_id, "instances": inventory,
        }, counters=self.counters)
        if reply.get("type") != "REGISTER_OK":
            raise RuntimeError(f"registration failed: {reply}")
        self._apply_directives(reply)
        self._ack()

    def _ack(self):
        with self.cv:
            self.last_ack_mono = time.monotonic()
            if self.fenced:
                self.fenced = False
                self._event("-", "SITE_LEASE_RENEWED", "RUNNING")
                self.cv.notify_all()

    def _apply_directives(self, reply):
        """Act on the manager's reconciliation verdict (HEARTBEAT_OK / REGISTER_OK)."""
        for sid in reply.get("release", []) or []:
            self._release_silent(str(sid), reason="manager_reconcile")
        for sid in reply.get("resume", []) or []:
            site_transfer.resume_fenced(self, str(sid))

    # ---------------------------------------------------------- dispatch
    def _dispatch(self, msg: dict) -> dict:
        t = msg.get("type")
        if time.monotonic() < self.partitioned_until and t != "FAULT_INJECT":
            raise ConnectionError("partitioned")          # drop the connection: no reply
        try:
            if t == "ALLOCATE":
                return self._on_allocate(msg)
            if t == "RELEASE":
                return self._on_release(msg)
            if t == "RESIZE":
                return self._on_resize(msg)
            if t == "SERVICE_CALL":
                return self._on_service_call(msg)
            if t == "INVENTORY":
                with self.cv:
                    return {"type": "INVENTORY_OK", "site_id": self.site_id, "boot_id": self.boot_id,
                            "instances": self._inventory_locked(), "fenced": self.fenced}
            if t == "STATUS":
                return self._status()
            if t == "MIGRATION_PREPARE":
                return site_transfer.migration_prepare(self, msg)
            if t == "MIGRATION_IMPORT":
                return site_transfer.migration_import(self, msg)
            if t == "MIGRATION_COMMIT":
                return site_transfer.migration_commit(self, msg)
            if t == "MIGRATION_SOURCE_RELEASE":
                return site_transfer.migration_source_release(self, msg)
            if t == "MIGRATION_ABORT":
                return site_transfer.migration_abort(self, msg)
            if t == "REPLICA_SNAPSHOT":
                return site_transfer.replica_snapshot(self, msg)
            if t == "REPLICA_CREATE":
                return site_transfer.replica_create(self, msg)
            if t == "REPLICA_SYNC":
                return site_transfer.replica_sync(self, msg)
            if t == "REPLICA_PROMOTE":
                return site_transfer.replica_promote(self, msg)
            if t == "REPLICA_REMOVE":
                return site_transfer.replica_remove(self, msg)
            if t == "FENCE":
                return site_transfer.fence(self, msg)
            if t == "FAULT_INJECT":
                return self._on_fault(msg)
            if t == "SHUTDOWN":
                threading.Thread(target=self._delayed_shutdown, daemon=True).start()
                return {"type": "SHUTDOWN_OK"}
        except KeyError as exc:
            return {"type": "ERROR", "error": f"missing_field:{exc.args[0]}"}
        except (TypeError, ValueError) as exc:
            return {"type": "ERROR", "error": f"invalid_message:{exc}"}
        return {"type": "ERROR", "error": f"unknown message type {t!r}"}

    def _delayed_shutdown(self):
        time.sleep(0.2)
        self.stop()

    # ------------------------------------------------------ fault injection
    def _on_fault(self, msg):
        mode = msg.get("mode")
        if mode == "partition":
            # Cut the site off from the manager in both directions for duration_s:
            # inbound connections are dropped, heartbeats are not sent.
            self.partitioned_until = time.monotonic() + float(msg.get("duration_s", 10))
            with self.cv:
                self._event("-", "FAULT_PARTITION_STARTED", "RUNNING", duration_s=msg.get("duration_s"))
            return {"type": "FAULT_OK", "mode": mode}
        if mode == "heal":
            self.partitioned_until = 0.0
            return {"type": "FAULT_OK", "mode": mode}
        if mode == "crash":
            if not self.allow_crash:
                return {"type": "ERROR", "error": "crash_not_permitted"}
            threading.Thread(target=lambda: (time.sleep(0.05), os._exit(137)), daemon=True).start()
            return {"type": "FAULT_OK", "mode": mode}
        return {"type": "ERROR", "error": f"unknown fault mode {mode!r}"}

    # ---------------------------------------------------------- ALLOCATE
    def _on_allocate(self, msg):
        sid = str(msg["sid"])
        demand = msg["demand"]
        holding_s = float(msg["holding_s"])
        if not isinstance(demand, dict):
            return {"type": "ALLOCATE_FAIL", "sid": sid, "error": "invalid_demand"}
        try:
            normalized_demand = {key: float(demand.get(key, 0)) for key in ("cpu", "mem", "bw")}
        except (TypeError, ValueError):
            return {"type": "ALLOCATE_FAIL", "sid": sid, "error": "invalid_demand"}
        cpu = normalized_demand["cpu"]
        if (not sid or any(not math.isfinite(value) or value < 0 for value in normalized_demand.values())
                or cpu <= 0):
            return {"type": "ALLOCATE_FAIL", "sid": sid, "error": "invalid_demand"}
        demand = normalized_demand
        if not math.isfinite(holding_s) or holding_s <= 0:
            return {"type": "ALLOCATE_FAIL", "sid": sid, "error": "invalid_holding_time"}
        generation = int(msg.get("generation", 1))
        inst = Instance(sid, msg["service"], demand, holding_s,
                        logical_name=msg.get("logical_name"), generation=generation)
        with self.cv:
            if sid in self.instances:
                return {"type": "ALLOCATE_OK", "sid": sid, "duplicate": True}
            if generation <= self.fence_floor.get(sid, 0):
                return {"type": "ALLOCATE_FAIL", "sid": sid, "error": "fenced_generation"}
            primaries = [i.pass_value for i in self.instances.values() if i.role == "PRIMARY"]
            inst.pass_value = min(primaries) if primaries else 0.0
            self.instances[sid] = inst
            self._event(sid, "INSTANCE_STARTED", "RUNNING", service=inst.service,
                        logical_name=inst.logical_name, generation=generation)
            self.cv.notify_all()
        return {"type": "ALLOCATE_OK", "sid": sid}

    # ----------------------------------------------------------- RELEASE
    def _release_silent(self, sid, reason):
        """Discard an instance without telling the manager (stale/orphan/replica)."""
        with self.cv:
            inst = self.instances.get(sid)
            if inst is None:
                return False
            if inst.state not in ("DRAINING", "TERMINATED"):
                try:
                    inst.state = lifecycle_transition(inst.state, "DRAINING")
                except ValueError:
                    pass
            inst.state = "TERMINATED"
            del self.instances[sid]
            self._event(sid, "INSTANCE_RELEASED_SILENTLY", "TERMINATED", reason=reason,
                        generation=inst.generation)
            self.cv.notify_all()
            return True

    def _on_release(self, msg):
        sid = str(msg["sid"])
        finished = None
        with self.cv:
            inst = self.instances.get(sid)
            silent = inst is not None and (inst.role == "REPLICA" or msg.get("silent"))
        if silent:
            self._release_silent(sid, reason="release_request")
            return {"type": "RELEASE_OK", "sid": sid, "state": "TERMINATED"}
        with self.cv:
            inst = self.instances.get(sid)
            if inst is not None and inst.state == "RUNNING":
                inst.state = lifecycle_transition(inst.state, "DRAINING")
                self._event(sid, "DRAINING_STARTED", inst.state)
            if inst is not None and inst.state == "DRAINING" and inst.in_flight == 0:
                finished = self.instances.pop(sid)
                finished.state = lifecycle_transition(finished.state, "TERMINATED")
                self._event(sid, "INSTANCE_TERMINATED", finished.state)
            self.cv.notify_all()
        if finished is not None:
            self._report_exit(finished)
        return {"type": "RELEASE_OK", "sid": sid,
                "state": "TERMINATED" if finished else (inst.state if inst else "NOT_FOUND")}

    # ------------------------------------------------------------ RESIZE
    def _on_resize(self, msg):
        sid = msg["sid"]
        with self.cv:
            inst = self.instances.get(sid)
            if inst is None:
                return {"type": "RESIZE_FAIL", "sid": sid, "error": "not_found"}
            new_cpu = float(msg["cpu"])
            if not math.isfinite(new_cpu) or new_cpu <= 0 or inst.state not in ("RUNNING", "STANDBY"):
                return {"type": "RESIZE_FAIL", "sid": sid, "error": "invalid_state_or_cpu"}
            if inst.state == "STANDBY":
                inst.set_tickets(new_cpu)       # replicas track the primary's size but never run
                return {"type": "RESIZE_OK", "sid": sid, "cpu": new_cpu}
            inst.state = lifecycle_transition(inst.state, "RESIZING")
            self._event(sid, "RESIZE_STARTED", inst.state, cpu=new_cpu)
            inst.set_tickets(new_cpu)
            inst.state = lifecycle_transition(inst.state, "RUNNING")
            self._event(sid, "RESIZE_COMPLETED", inst.state, cpu=new_cpu)
            self.cv.notify_all()
            # pass_value is left untouched on purpose: resizing changes the
            # *rate* an instance earns share at from now on, without
            # resetting the fairness bookkeeping it has already accrued.
        return {"type": "RESIZE_OK", "sid": sid, "cpu": new_cpu}

    # ------------------------------------------------------ SERVICE_CALL
    def _on_service_call(self, msg):
        """Data-path call addressed to a logical service. Refused unless this
        site currently hosts the *running, current-generation* instance."""
        sid = str(msg.get("sid", ""))
        with self.cv:
            inst = self.instances.get(sid)
            if inst is None:
                return {"type": "SERVICE_ERROR", "error": "not_here", "site_id": self.site_id}
            if self.fenced:
                return {"type": "SERVICE_ERROR", "error": "site_fenced", "site_id": self.site_id}
            if inst.role != "PRIMARY" or inst.state not in ("RUNNING", "RESIZING"):
                return {"type": "SERVICE_ERROR", "error": f"not_running:{inst.state}", "site_id": self.site_id}
            if "generation" in msg and int(msg["generation"]) != inst.generation:
                return {"type": "SERVICE_ERROR", "error": "stale_generation", "site_id": self.site_id,
                        "generation": inst.generation}
            if msg.get("logical_name") and inst.logical_name and msg["logical_name"] != inst.logical_name:
                return {"type": "SERVICE_ERROR", "error": "wrong_service", "site_id": self.site_id}
            inst.calls_served += 1
            return {"type": "SERVICE_REPLY", "sid": sid, "site_id": self.site_id,
                    "generation": inst.generation, "calls_served": inst.calls_served,
                    "service": inst.service, "echo": msg.get("payload")}

    # ------------------------------------------------------------ STATUS
    def _status(self):
        with self.lock:
            return {
                "type": "STATUS_OK", "site_id": self.site_id, "boot_id": self.boot_id, "fenced": self.fenced,
                "instances": {sid: {
                    "service": i.service, "tickets": i.tickets, "jobs_done": i.jobs_done,
                    "busy_s": round(i.busy_s, 3), "expires_in_s": round(i.holding_remaining(), 1),
                    "state": i.state, "in_flight": i.in_flight, "role": i.role,
                    "generation": i.generation, "calls_served": i.calls_served,
                    "logical_name": i.logical_name,
                } for sid, i in self.instances.items()},
                "staged": [list(k) for k in self.staged],
                "counters": self.counters.snapshot(),
            }

    # ------------------------------------------------------ stride pick
    def _pick_next_locked(self):
        """Caller holds self.lock. Returns the eligible instance with the
        smallest pass value, or None. Standby, frozen (MIGRATING), draining
        instances -- and everything while the site is lease-fenced -- are
        never eligible, so a replica or a fenced primary executes no jobs."""
        if self.fenced:
            return None
        best = None
        for inst in self.instances.values():
            if inst.state != "RUNNING" or inst.in_flight >= inst.max_concurrent:
                continue
            if best is None or inst.pass_value < best.pass_value:
                best = inst
        return best

    # ------------------------------------------------------- worker loop
    def _worker_loop(self, worker_idx):
        while not self._stop.is_set():
            with self.cv:
                inst = self._pick_next_locked()
                while inst is None and not self._stop.is_set():
                    self.cv.wait(timeout=0.5)
                    inst = self._pick_next_locked()
                if self._stop.is_set():
                    return
                inst.in_flight += 1
                inst.pass_value += inst.stride  # charge the scheduling turn at dispatch
                self.busy_workers += 1

            job_start = time.perf_counter()
            # simulate the job: exponential service time around job_service_ms
            duration = self.rng.expovariate(1.0 / (self.job_service_ms / 1000.0))
            duration = min(duration, self.job_service_ms * 5 / 1000.0)  # cap the long tail
            time.sleep(duration)
            elapsed_ms = (time.perf_counter() - job_start) * 1000

            with self.cv:
                inst.in_flight -= 1
                self.jobs_executed += 1
                inst.jobs_done += 1
                inst.jobs_since_hb += 1
                inst.busy_s += duration
                inst.latency_sum_ms += elapsed_ms
                self.busy_workers -= 1
                self.busy_time_since_hb += duration
                finished = None
                if inst.state == "DRAINING" and inst.in_flight == 0 and self.instances.get(inst.sid) is inst:
                    finished = self.instances.pop(inst.sid, None)
                    if finished is not None:
                        finished.state = lifecycle_transition(finished.state, "TERMINATED")
                        self._event(inst.sid, "INSTANCE_TERMINATED", finished.state)
                self.cv.notify_all()
            if finished is not None:
                self._report_exit(finished)

    # -------------------------------------------------------- reaper
    def _reaper_loop(self):
        """Terminates instances whose holding time has elapsed and reports
        INSTANCE_EXIT to the manager (design doc 4.2 life cycle). Only
        RUNNING instances expire: a standby never does (its primary's exit
        removes it), a frozen migrating instance's timer is paused, and
        nothing expires while the site is fenced."""
        while not self._stop.is_set():
            time.sleep(0.1)
            now = time.monotonic()
            expired = []
            with self.cv:
                if self.fenced:
                    continue
                for sid, inst in list(self.instances.items()):
                    if inst.state == "RUNNING" and now >= inst.expires_at:
                        inst.state = lifecycle_transition(inst.state, "DRAINING")
                        self._event(sid, "HOLDING_TIME_EXPIRED", inst.state)
                    if inst.state == "DRAINING" and inst.in_flight == 0:
                        inst.state = lifecycle_transition(inst.state, "TERMINATED")
                        expired.append(inst)
                        del self.instances[sid]
                        self._event(sid, "INSTANCE_TERMINATED", inst.state)
                if expired:
                    self.cv.notify_all()
            for inst in expired:
                self._report_exit(inst)

    def _report_exit(self, inst: Instance):
        message = {
            "type": "INSTANCE_EXIT", "sid": inst.sid, "site_id": self.site_id,
            "generation": inst.generation,
            "summary": {
                "jobs_done": inst.jobs_done,
                "busy_s": round(inst.busy_s, 3),
                "latency_sum_ms": round(inst.latency_sum_ms, 3),
                "holding_s": inst.holding_s,
                "calls_served": inst.calls_served,
            },
        }
        for attempt in range(3):
            try:
                reply = protocol.request(self.manager_host, self.manager_port, message,
                                         counters=self.counters, timeout=3.0)
                if reply.get("type") == "INSTANCE_EXIT_OK":
                    return True
                raise RuntimeError(f"unexpected manager reply: {reply}")
            except (ConnectionError, OSError, TimeoutError, RuntimeError) as e:
                if attempt == 2:
                    print(f"[{self.site_id}] failed to report INSTANCE_EXIT for {inst.sid}: {e}", flush=True)
                else:
                    time.sleep(0.25 * (attempt + 1))
        return False

    # ------------------------------------------------------- heartbeat
    def _heartbeat_loop(self):
        while not self._stop.is_set():
            time.sleep(self.heartbeat_interval)
            now = time.time()
            partitioned = time.monotonic() < self.partitioned_until
            with self.cv:
                # manager lease: no acknowledgement for lease_s -> fence ourselves
                if (not self.fenced and time.monotonic() - self.last_ack_mono > self.lease_s):
                    self.fenced = True
                    self._event("-", "SITE_FENCED_LEASE_EXPIRED", "RUNNING",
                                silent_s=round(time.monotonic() - self.last_ack_mono, 2))
                elapsed = now - self._last_hb_ts
                self._last_hb_ts = now
                slot_capacity_s = self.n_workers * elapsed
                slot_util = (self.busy_time_since_hb / slot_capacity_s) if slot_capacity_s > 0 else 0.0
                jobs_interval = sum(i.jobs_since_hb for i in self.instances.values())

                fairness_j = None
                if slot_util > self.fairness_threshold and self.instances:
                    xs = [i.jobs_since_hb / i.tickets for i in self.instances.values()
                          if i.tickets > 0 and i.state == "RUNNING"]
                    fairness_j = util.jains_fairness_index(xs)

                instances_running = len(self.instances)
                instance_ids = [sid for sid, inst in self.instances.items()
                                if inst.state == "RUNNING" and inst.role == "PRIMARY"]
                inventory = self._inventory_locked()
                for i in self.instances.values():
                    i.jobs_since_hb = 0
                self.busy_time_since_hb = 0.0
            if partitioned:
                continue
            try:
                reply = protocol.request(self.manager_host, self.manager_port, {
                    "type": "HEARTBEAT", "site_id": self.site_id, "ts": now,
                    "slot_utilisation": round(slot_util, 4),
                    "jobs_interval": jobs_interval,
                    "fairness_j": round(fairness_j, 4) if fairness_j is not None else None,
                    "instances_running": instances_running,
                    "instance_ids": instance_ids,
                    "instances": inventory,
                    "boot_id": self.boot_id,
                }, counters=self.counters, timeout=2.0)
            except (ConnectionError, OSError, TimeoutError):
                continue  # manager may be briefly unreachable; the lease clock keeps running
            if reply.get("type") == "HEARTBEAT_OK":
                self._apply_directives(reply)   # verdict first ...
                if not reply.get("hold"):
                    self._ack()                 # ... then the lease may be renewed
            elif "unregistered" in str(reply.get("error", "")):
                try:                            # manager restarted and lost us: register again
                    self._register()
                except (ConnectionError, OSError, TimeoutError, RuntimeError):
                    pass


def main():
    ap = argparse.ArgumentParser(description="TeleRM site node")
    ap.add_argument("--site-id", required=True)
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--manager-host", default=None)
    ap.add_argument("--manager-port", type=int, default=None)
    args = ap.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    mhost = args.manager_host or config["manager"]["host"]
    mport = args.manager_port or config["manager"]["port"]

    site = Site(args.site_id, config, mhost, mport, allow_crash=True)
    site.start()
    try:
        while not site._stop.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        site.stop()
