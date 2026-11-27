"""SQLite-backed durable control-plane state for TeleRM.

Tables
------
sites        registered sites (endpoint, capacity) so a restarted manager
             knows which sites to expect
site_health  last observed health state per site
services     logical name -> physical endpoint (+ generation)
processes    one row per service instance, kept after termination as history
replicas     standby replicas and their primary
reservations ownership ledger of node and path reservations
operations   migration / replication / failover transactions
events       append-only structured event log (source for exports)

A single connection guarded by a lock serves the whole process.  WAL journal
mode with ``synchronous=NORMAL`` keeps the database consistent across a
process crash; only the very last transaction could be lost on sudden *power*
loss.  Multi-row updates that must be atomic (an endpoint update together with
the process row and the operation record) use ``transaction()``.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path

PROCESS_STATES = ("CREATED", "ALLOCATING", "RUNNING", "RESIZING", "MIGRATING", "STANDBY",
                  "DRAINING", "FAILED", "RECOVERING", "TERMINATED")
TERMINAL_OP_STATES = ("COMMITTED", "ABORTED", "FAILED", "ABORTED_ON_RESTART", "COMPLETED")

SCHEMA = """
CREATE TABLE IF NOT EXISTS sites(
  site_id TEXT PRIMARY KEY, tier TEXT NOT NULL, host TEXT NOT NULL, port INTEGER NOT NULL,
  capacity TEXT NOT NULL, workers INTEGER NOT NULL DEFAULT 1, boot_id TEXT,
  registered_at REAL NOT NULL, updated_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS site_health(
  site_id TEXT PRIMARY KEY, state TEXT NOT NULL
    CHECK(state IN ('ACTIVE','SUSPECT','FAILED','RECOVERING','UNKNOWN')),
  last_heartbeat REAL NOT NULL, missed INTEGER NOT NULL DEFAULT 0,
  updated_at REAL NOT NULL, metadata TEXT NOT NULL DEFAULT '{}');
CREATE TABLE IF NOT EXISTS processes(
  instance_id TEXT PRIMARY KEY, logical_name TEXT NOT NULL,
  service TEXT NOT NULL, site_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('CREATED','ALLOCATING','RUNNING','RESIZING','MIGRATING',
       'STANDBY','DRAINING','FAILED','RECOVERING','TERMINATED')),
  demand TEXT NOT NULL, holding_s REAL NOT NULL,
  created_at REAL NOT NULL, updated_at REAL NOT NULL,
  generation INTEGER NOT NULL DEFAULT 1 CHECK(generation >= 1),
  parent_id TEXT, metadata TEXT NOT NULL DEFAULT '{}');
CREATE TABLE IF NOT EXISTS services(
  logical_name TEXT PRIMARY KEY, service TEXT NOT NULL,
  instance_id TEXT NOT NULL UNIQUE REFERENCES processes(instance_id),
  site_id TEXT NOT NULL, host TEXT NOT NULL, port INTEGER NOT NULL,
  state TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 1 CHECK(generation >= 1),
  updated_at REAL NOT NULL, metadata TEXT NOT NULL DEFAULT '{}');
CREATE TABLE IF NOT EXISTS replicas(
  replica_id TEXT PRIMARY KEY,
  primary_id TEXT NOT NULL REFERENCES processes(instance_id),
  site_id TEXT NOT NULL, state TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 1,
  sync_seq INTEGER NOT NULL DEFAULT 0, last_sync REAL,
  demand TEXT NOT NULL, created_at REAL NOT NULL, updated_at REAL NOT NULL,
  metadata TEXT NOT NULL DEFAULT '{}');
CREATE TABLE IF NOT EXISTS reservations(
  res_id TEXT PRIMARY KEY, owner TEXT NOT NULL, op_id TEXT,
  kind TEXT NOT NULL CHECK(kind IN ('NODE','PATH')), purpose TEXT NOT NULL,
  site_id TEXT, path TEXT, demand TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('HELD','RELEASED')),
  created_at REAL NOT NULL, released_at REAL);
CREATE TABLE IF NOT EXISTS operations(
  operation_id TEXT PRIMARY KEY, operation_type TEXT NOT NULL,
  state TEXT NOT NULL, source_site TEXT, target_site TEXT,
  instance_id TEXT NOT NULL, started_at REAL NOT NULL,
  updated_at REAL NOT NULL, payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events(
  event_id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL,
  category TEXT NOT NULL DEFAULT 'LIFECYCLE',
  event_type TEXT NOT NULL, instance_id TEXT, logical_name TEXT,
  site_id TEXT, operation_id TEXT, payload TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_process_site ON processes(site_id);
CREATE INDEX IF NOT EXISTS idx_replica_primary ON replicas(primary_id);
CREATE INDEX IF NOT EXISTS idx_res_owner ON reservations(owner, state);
CREATE INDEX IF NOT EXISTS idx_res_op ON reservations(op_id, state);
CREATE INDEX IF NOT EXISTS idx_events_instance ON events(instance_id);
CREATE INDEX IF NOT EXISTS idx_events_cat ON events(category, event_id);
CREATE INDEX IF NOT EXISTS idx_ops_state ON operations(state);
"""


class DurableStore:
    def __init__(self, path="results/telerm_control.sqlite3"):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self._db = sqlite3.connect(self.path, timeout=15, check_same_thread=False,
                                   isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.executescript(SCHEMA)
        self._in_tx = 0
        self._ebuf = []

    # --------------------------------------------------------- plumbing
    @contextmanager
    def transaction(self):
        """Atomic group of writes. Re-entrant: nested use joins the outer one."""
        with self.lock:
            outer = self._in_tx == 0
            if outer:
                self._db.execute("BEGIN IMMEDIATE")
            self._in_tx += 1
            try:
                yield self
            except BaseException:
                self._in_tx -= 1
                if outer:
                    self._db.execute("ROLLBACK")
                raise
            else:
                self._in_tx -= 1
                if outer:
                    self._db.execute("COMMIT")

    def _x(self, sql, args=()):
        with self.lock:
            if self._in_tx:
                return self._db.execute(sql, args)
            self._db.execute("BEGIN IMMEDIATE")
            try:
                cur = self._db.execute(sql, args)
                self._db.execute("COMMIT")
                return cur
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def _q(self, sql, args=()):
        with self.lock:
            return [self._dict(r) for r in self._db.execute(sql, args).fetchall()]

    def close(self):
        with self.lock:
            self.flush()
            try:
                self._db.close()
            except sqlite3.Error:
                pass

    # ------------------------------------------------------------ events
    def event(self, event_type, instance_id=None, logical_name=None, site_id=None, payload=None,
              category="LIFECYCLE", operation_id=None, buffered=False, ts=None):
        row = (ts or time.time(), category, event_type, instance_id, logical_name, site_id,
               operation_id, json.dumps(payload or {}, sort_keys=True, default=str))
        with self.lock:
            if buffered:
                self._ebuf.append(row)
                if len(self._ebuf) >= 200:
                    self.flush()
                return
            self.flush()
            self._x("INSERT INTO events(ts,category,event_type,instance_id,logical_name,site_id,"
                    "operation_id,payload) VALUES(?,?,?,?,?,?,?,?)", row)

    def flush(self):
        with self.lock:
            if not self._ebuf:
                return
            rows, self._ebuf = self._ebuf, []
            if self._in_tx:
                self._db.executemany(
                    "INSERT INTO events(ts,category,event_type,instance_id,logical_name,site_id,"
                    "operation_id,payload) VALUES(?,?,?,?,?,?,?,?)", rows)
                return
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self._db.executemany(
                    "INSERT INTO events(ts,category,event_type,instance_id,logical_name,site_id,"
                    "operation_id,payload) VALUES(?,?,?,?,?,?,?,?)", rows)
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def events(self, limit=1000):
        """Most recent events, newest first."""
        self.flush()
        return self._q("SELECT * FROM events ORDER BY event_id DESC LIMIT ?", (int(limit),))

    def all_events(self, category=None, event_types=None, since_id=0):
        self.flush()
        sql, args = "SELECT * FROM events WHERE event_id>?", [int(since_id)]
        if category:
            sql += " AND category=?"; args.append(category)
        if event_types:
            sql += " AND event_type IN (%s)" % ",".join("?" * len(event_types)); args.extend(event_types)
        return self._q(sql + " ORDER BY event_id", args)

    # ------------------------------------------------------------- sites
    def upsert_site(self, s):
        now = time.time()
        self._x("""INSERT INTO sites(site_id,tier,host,port,capacity,workers,boot_id,registered_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(site_id) DO UPDATE SET tier=excluded.tier,
                   host=excluded.host,port=excluded.port,capacity=excluded.capacity,
                   workers=excluded.workers,boot_id=excluded.boot_id,updated_at=excluded.updated_at""",
                (s["site_id"], s["tier"], s["host"], int(s["port"]), json.dumps(s["capacity"]),
                 int(s.get("workers", 1)), s.get("boot_id"), now, now))

    def list_sites(self):
        return self._q("SELECT * FROM sites ORDER BY site_id")

    def site_heartbeat(self, site_id, state="ACTIVE", metadata=None, missed=0):
        now = time.time()
        self._x("""INSERT INTO site_health VALUES(?,?,?,?,?,?)
                   ON CONFLICT(site_id) DO UPDATE SET state=excluded.state,last_heartbeat=excluded.last_heartbeat,
                   missed=excluded.missed,updated_at=excluded.updated_at,metadata=excluded.metadata""",
                (site_id, state, now, missed, now, json.dumps(metadata or {})))

    def mark_site(self, site_id, state, missed=None):
        now = time.time()
        with self.lock:
            if not self._q("SELECT 1 FROM site_health WHERE site_id=?", (site_id,)):
                self._x("INSERT INTO site_health VALUES(?,?,?,?,?,?)",
                        (site_id, state, now, missed or 0, now, "{}"))
            elif missed is None:
                self._x("UPDATE site_health SET state=?,updated_at=? WHERE site_id=?", (state, now, site_id))
            else:
                self._x("UPDATE site_health SET state=?,missed=?,updated_at=? WHERE site_id=?",
                        (state, missed, now, site_id))

    def site_health(self):
        return self._q("SELECT * FROM site_health ORDER BY site_id")

    # --------------------------------------------------------- processes
    def upsert_process(self, p):
        now = time.time()
        self._x("""INSERT INTO processes(instance_id,logical_name,service,site_id,state,demand,holding_s,
                   created_at,updated_at,generation,parent_id,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(instance_id) DO UPDATE SET site_id=excluded.site_id,state=excluded.state,
                   demand=excluded.demand,updated_at=excluded.updated_at,generation=excluded.generation,
                   metadata=excluded.metadata""",
                (p["instance_id"], p["logical_name"], p["service"], p["site_id"], p["state"],
                 json.dumps(p.get("demand", {})), float(p.get("holding_s", 0)), p.get("created_at", now), now,
                 int(p.get("generation", 1)), p.get("parent_id"), json.dumps(p.get("metadata", {}), default=str)))

    def get_process(self, instance_id):
        r = self._q("SELECT * FROM processes WHERE instance_id=?", (instance_id,))
        return r[0] if r else None

    def list_processes(self, active_only=False):
        sql = "SELECT * FROM processes"
        if active_only:
            sql += " WHERE state!='TERMINATED'"
        return self._q(sql + " ORDER BY instance_id")

    # ---------------------------------------------------------- services
    def upsert_service(self, s):
        now = time.time()
        self._x("""INSERT INTO services(logical_name,service,instance_id,site_id,host,port,state,generation,
                   updated_at,metadata) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(logical_name) DO UPDATE SET
                   service=excluded.service,instance_id=excluded.instance_id,site_id=excluded.site_id,
                   host=excluded.host,port=excluded.port,state=excluded.state,generation=excluded.generation,
                   updated_at=excluded.updated_at,metadata=excluded.metadata""",
                (s["logical_name"], s["service"], s["instance_id"], s["site_id"], s["host"], int(s["port"]),
                 s["state"], int(s.get("generation", 1)), now, json.dumps(s.get("metadata", {}))))

    def get_service(self, name):
        r = self._q("SELECT * FROM services WHERE logical_name=?", (name,))
        return r[0] if r else None

    def list_services(self, state=None):
        if state:
            return self._q("SELECT * FROM services WHERE state=? ORDER BY logical_name", (state,))
        return self._q("SELECT * FROM services ORDER BY logical_name")

    def update_service_state(self, name, state):
        return self._x("UPDATE services SET state=?,updated_at=? WHERE logical_name=?",
                       (state, time.time(), name)).rowcount > 0

    def delete_service(self, name):
        self._x("DELETE FROM services WHERE logical_name=?", (name,))

    def save_instance(self, process, service=None):
        """Atomically persist the process row and (optionally) its service row."""
        with self.transaction():
            self.upsert_process(process)
            if service is not None:
                self.upsert_service(service)

    # ---------------------------------------------------------- replicas
    def upsert_replica(self, r):
        now = time.time()
        self._x("""INSERT INTO replicas(replica_id,primary_id,site_id,state,generation,sync_seq,last_sync,
                   demand,created_at,updated_at,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(replica_id) DO UPDATE SET site_id=excluded.site_id,state=excluded.state,
                   generation=excluded.generation,sync_seq=excluded.sync_seq,last_sync=excluded.last_sync,
                   demand=excluded.demand,updated_at=excluded.updated_at,metadata=excluded.metadata""",
                (r["replica_id"], r["primary_id"], r["site_id"], r["state"], int(r.get("generation", 1)),
                 int(r.get("sync_seq", 0)), r.get("last_sync"), json.dumps(r["demand"]),
                 r.get("created_at", now), now, json.dumps(r.get("metadata", {}))))

    def delete_replica(self, replica_id):
        self._x("DELETE FROM replicas WHERE replica_id=?", (replica_id,))

    def list_replicas(self):
        return self._q("SELECT * FROM replicas ORDER BY replica_id")

    # ------------------------------------------------------ reservations
    def upsert_reservation(self, r):
        self._x("""INSERT INTO reservations(res_id,owner,op_id,kind,purpose,site_id,path,demand,state,created_at,
                   released_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(res_id) DO UPDATE SET owner=excluded.owner,
                   op_id=excluded.op_id,purpose=excluded.purpose,demand=excluded.demand,state=excluded.state,
                   released_at=excluded.released_at""",
                (r["res_id"], r["owner"], r.get("op_id"), r["kind"], r["purpose"], r.get("site_id"),
                 json.dumps(r.get("path")) if r.get("path") is not None else None,
                 json.dumps(r["demand"]), r["state"], r["created_at"], r.get("released_at")))

    def held_reservations(self):
        rows = self._q("SELECT * FROM reservations WHERE state='HELD' ORDER BY created_at")
        for r in rows:
            if r.get("path"):
                r["path"] = json.loads(r["path"])
        return rows

    # -------------------------------------------------------- operations
    def begin_operation(self, op):
        now = time.time()
        self._x("INSERT INTO operations VALUES(?,?,?,?,?,?,?,?,?)",
                (op["operation_id"], op["operation_type"], op["state"], op.get("source_site"),
                 op.get("target_site"), op["instance_id"], now, now, json.dumps(op.get("payload", {}), default=str)))

    def update_operation(self, op_id, state, payload=None):
        """Change an operation's state; ``payload`` entries are *merged* into the stored payload."""
        with self.lock:
            cur = self._q("SELECT payload FROM operations WHERE operation_id=?", (op_id,))
            merged = dict(cur[0]["payload"]) if cur and isinstance(cur[0]["payload"], dict) else {}
            merged.update(payload or {})
            self._x("UPDATE operations SET state=?,updated_at=?,payload=? WHERE operation_id=?",
                    (state, time.time(), json.dumps(merged, default=str), op_id))

    def operation(self, op_id):
        r = self._q("SELECT * FROM operations WHERE operation_id=?", (op_id,))
        return r[0] if r else None

    def operations(self, operation_type=None):
        if operation_type:
            return self._q("SELECT * FROM operations WHERE operation_type=? ORDER BY started_at", (operation_type,))
        return self._q("SELECT * FROM operations ORDER BY started_at")

    def incomplete_operations(self):
        q = ",".join("?" * len(TERMINAL_OP_STATES))
        return self._q(f"SELECT * FROM operations WHERE state NOT IN ({q}) ORDER BY started_at", TERMINAL_OP_STATES)

    @staticmethod
    def _dict(row):
        d = dict(row)
        for k in ("payload", "metadata", "demand", "capacity"):
            if k in d and isinstance(d[k], str):
                try:
                    d[k] = json.loads(d[k])
                except ValueError:
                    pass
        return d
