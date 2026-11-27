"""A service instance as hosted by a site (shared by site and migration code).

Kept in its own module because ``site.py`` is both the site's executable
(``python3 site.py``) and the name of a standard-library module: importing
``Instance`` from ``site`` is therefore impossible.
"""
import time

from common import util


class Instance:
    def __init__(self, sid, service, demand, holding_s, logical_name=None, generation=1,
                 role="PRIMARY", replica_of=None):
        self.sid = sid
        self.service = service
        self.logical_name = logical_name
        self.generation = int(generation)
        self.role = role                     # PRIMARY or REPLICA
        self.replica_of = replica_of
        self.demand = dict(demand)
        self.tickets = demand["cpu"]
        self.stride = 1_000_000 / self.tickets
        self.pass_value = 0.0
        self.max_concurrent = util.ceil_div(self.tickets, 1000)
        self.in_flight = 0
        self.created_at = time.time()
        self.created_mono = time.monotonic()
        self.expires_at = self.created_mono + holding_s
        self.state = "RUNNING"
        self.holding_s = holding_s
        self.jobs_done = 0
        self.busy_s = 0.0
        self.latency_sum_ms = 0.0
        self.calls_served = 0                # client SERVICE_CALLs answered
        # counters since the last heartbeat, for jobs_interval / fairness
        self.jobs_since_hb = 0
        # migration fence: operation that froze this instance
        self.migration_op = None
        # standby bookkeeping (replicas)
        self.sync_seq = 0
        self.sync_mono = None
        self.standby_remaining_s = holding_s

    def holding_remaining(self, now_mono=None):
        now_mono = time.monotonic() if now_mono is None else now_mono
        if self.state == "STANDBY":
            if self.sync_mono is None:
                return self.standby_remaining_s
            return max(0.0, self.standby_remaining_s - (now_mono - self.sync_mono))
        return max(0.0, self.expires_at - now_mono)

    def set_tickets(self, cpu):
        self.tickets = cpu
        self.demand["cpu"] = cpu
        self.stride = 1_000_000 / cpu
        self.max_concurrent = util.ceil_div(cpu, 1000)
