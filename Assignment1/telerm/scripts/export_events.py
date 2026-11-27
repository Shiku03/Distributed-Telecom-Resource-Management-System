#!/usr/bin/env python3
"""Export the durable TeleRM control-plane event log and operations to JSONL/CSV
for Spark / Power BI.  (The manager's WRITE_REPORT/SHUTDOWN also writes these
into <results-dir>/analytics.)"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.durable_store import DurableStore
from common.reporting import export_analytics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="results/telerm_control.sqlite3")
    ap.add_argument("--out", default="results/analytics")
    a = ap.parse_args()
    if not os.path.exists(a.db):
        sys.exit(f"no such database: {a.db}")
    store = DurableStore(a.db)
    n = export_analytics(store, a.out)
    store.close()
    print(f"Exported {n} events to {a.out}")


if __name__ == "__main__":
    main()
