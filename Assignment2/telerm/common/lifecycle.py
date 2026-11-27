"""Explicit process lifecycle states and transition validation for TeleRM.

The manager, the site processes, the service registry and the durable store
all use these states.  ``transition`` is the single place where a state change
is validated, so an invalid change (for example TERMINATED -> RUNNING) raises
instead of silently corrupting the control plane.

Process lifecycle:  CREATE -> LOCATE -> SCHEDULE -> ALLOCATE -> RUN -> RESIZE
-> MIGRATE -> DRAIN -> TERMINATE.  CREATE/LOCATE/SCHEDULE are the CREATED
state (admission and placement are decided before the first reservation);
ALLOCATE is ALLOCATING; RUN is RUNNING; and so on.  STANDBY is the state of a
passive replica: it holds reserved resources and synchronised state but is not
scheduled and does not serve calls.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

STATES = {
    "CREATED", "ALLOCATING", "RUNNING", "RESIZING", "MIGRATING", "STANDBY",
    "DRAINING", "FAILED", "RECOVERING", "TERMINATED",
}

_ALLOWED = {
    "CREATED": {"ALLOCATING", "FAILED", "TERMINATED"},
    "ALLOCATING": {"RUNNING", "STANDBY", "FAILED", "TERMINATED"},
    "RUNNING": {"RESIZING", "MIGRATING", "DRAINING", "FAILED"},
    "RESIZING": {"RUNNING", "FAILED", "DRAINING"},
    "MIGRATING": {"RUNNING", "FAILED", "RECOVERING", "DRAINING"},
    "STANDBY": {"RUNNING", "DRAINING", "FAILED", "TERMINATED"},
    "DRAINING": {"TERMINATED", "FAILED"},
    "FAILED": {"RECOVERING", "TERMINATED"},
    "RECOVERING": {"RUNNING", "FAILED", "TERMINATED"},
    "TERMINATED": set(),
}

# States in which a logical service name may be resolved to an endpoint.
RESOLVABLE = {"RUNNING", "RESIZING"}


class InvalidTransition(ValueError):
    pass


def transition(current: str, target: str) -> str:
    if current not in STATES or target not in STATES:
        raise InvalidTransition(f"unknown lifecycle state: {current!r} -> {target!r}")
    if target not in _ALLOWED[current]:
        raise InvalidTransition(f"transition not allowed: {current} -> {target}")
    return target


def event(instance_id: str, event_type: str, state: str, **details: Any) -> dict:
    """Build a lifecycle event record.

    ``ts`` is epoch seconds (float) and ``timestamp`` the same instant in ISO
    8601 UTC; analysis tools can use either.  Keyword details such as
    ``logical_name``, ``site``, ``operation_id`` and ``generation`` are merged
    into the record unchanged.
    """
    if state not in STATES:
        raise ValueError(f"unknown lifecycle state: {state}")
    now = time.time()
    return {
        "ts": now,
        "timestamp": datetime.fromtimestamp(now, timezone.utc).isoformat(),
        "instance_id": str(instance_id),
        "event": event_type,
        "state": state,
        **details,
    }
