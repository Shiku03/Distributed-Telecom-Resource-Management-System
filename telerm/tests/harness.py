"""In-process multi-node harness for integration tests: a real Manager and real
Site objects talking over real TCP sockets on free local ports (one process,
many threads).  The scenario script runs the same thing as separate OS processes."""
import json
import os
import shutil
import socket
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import protocol                      # noqa: E402
from common.site_node import Site                # noqa: E402
from manager import Manager                      # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def make_config(**overrides):
    with open(os.path.join(ROOT, "config.json")) as f:
        cfg = json.load(f)
    cfg["manager"]["port"] = free_port()
    for s in cfg["sites"].values():
        s["port"] = free_port()
    cfg["heartbeat_interval_s"] = 0.2
    cfg["failure_detector"] = {"suspect_after_heartbeats": 3, "failed_after_heartbeats": 6,
                               "site_lease_heartbeats": 3, "failed_instance_ttl_s": 60,
                               "missing_threshold_heartbeats": 3}
    cfg["replication"] = {"sync_interval_s": 0.3, "sync_bw_fraction": 0.1, "max_replicas": 2}
    cfg["placement_policy"] = "edge_first"
    cfg.update(overrides)
    return cfg


class Cluster:
    def __init__(self, cfg=None, results_dir=None, start_sites=True):
        self.cfg = cfg or make_config()
        self.dir = results_dir or tempfile.mkdtemp(prefix="telerm_")
        self.mgr = Manager(self.cfg, self.dir)
        self.mgr.start()
        self.sites = {}
        self.mhost, self.mport = self.cfg["manager"]["host"], self.cfg["manager"]["port"]
        if start_sites:
            for sid in self.cfg["sites"]:
                self.start_site(sid)

    def start_site(self, sid):
        s = Site(sid, self.cfg, self.mhost, self.mport)
        s.start()
        self.sites[sid] = s
        return s

    def crash_site(self, sid):
        self.sites.pop(sid).crash()

    def restart_manager(self):
        self.mgr.stop()
        time.sleep(0.3)
        self.mgr = Manager(self.cfg, self.dir)
        self.mgr.start()
        return self.mgr

    def rpc(self, msg, timeout=8.0):
        return protocol.request(self.mhost, self.mport, msg, timeout=timeout)

    def request(self, service="UPF", ingress="edge-A", holding=60.0, **kw):
        spec = self.cfg["services"][service]
        msg = {"type": "REQUEST", "service": service, "ingress": ingress, "holding_s": holding,
               "demand": {"cpu": spec["cpu"], "mem": spec["mem"], "bw": spec["bw"]},
               "max_latency_ms": spec["max_latency_ms"], **kw}
        return self.rpc(msg)

    def wait(self, cond, timeout=15.0, step=0.05):
        end = time.time() + timeout
        while time.time() < end:
            if cond():
                return True
            time.sleep(step)
        return False

    def ledger_ok(self):
        return self.mgr.ledger.verify()

    def close(self, cleanup=True):
        for s in list(self.sites.values()):
            s.stop()
        self.mgr.stop()
        self.mgr.store.close()
        if cleanup:
            shutil.rmtree(self.dir, ignore_errors=True)
