"""Deterministic crash injection.

Used by the queue worker and the streaming runner; lives in ``core`` because
both depend on it and neither should depend on the other.

A correctness claim is only worth what you tried to break it with.  This module
lets a worker be killed at a named point in its loop, on a chosen occurrence,
without editing code between runs:

    DATAPLATFORM_CHAOS="after_write:3" dataplatform-worker

The exit is deliberately violent -- :func:`os._exit` skips ``finally`` blocks,
``atexit`` handlers and buffered flushes, which is what makes it a fair
simulation of ``SIGKILL`` or a lost machine.  A clean shutdown proves nothing,
because a clean shutdown is the case that already works.

Call sites stay cheap and safe: :func:`maybe_crash` is a no-op unless the
environment variable is set, so the hooks can live in the normal code path.
"""
from __future__ import annotations

import logging
import os
import sys
from collections import Counter
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)

ENV_VAR = "DATAPLATFORM_CHAOS"

#: Exit code used for injected crashes -- 128 + SIGKILL(9), the shell's
#: convention for "killed", so it is distinguishable from an application error.
CRASH_EXIT_CODE = 137


class CrashPoint:
    """Named points a worker can be killed at."""

    BEFORE_POLL = "before_poll"
    AFTER_POLL = "after_poll"
    AFTER_WRITE = "after_write"
    BEFORE_COMMIT = "before_commit"
    AFTER_COMMIT = "after_commit"
    BEFORE_HEARTBEAT = "before_heartbeat"

    ALL = (
        BEFORE_POLL,
        AFTER_POLL,
        AFTER_WRITE,
        BEFORE_COMMIT,
        AFTER_COMMIT,
        BEFORE_HEARTBEAT,
    )


_counters: Counter = Counter()

#: Indirection so tests can observe a crash without dying.
_exit_fn: Callable[[int], None] = os._exit


def parse_plan(spec: Optional[str]) -> Dict[str, int]:
    """Parse ``"after_write:3,before_commit:1"`` into ``{point: occurrence}``.

    Unknown point names raise, because a silently ignored chaos plan produces a
    green run that proves nothing.  A bare point name means "the first time".
    """
    plan: Dict[str, int] = {}
    if not spec:
        return plan

    for clause in spec.split(","):
        clause = clause.strip()
        if not clause:
            continue
        point, _, occurrence = clause.partition(":")
        point = point.strip()
        if point not in CrashPoint.ALL:
            raise ValueError(
                "unknown crash point {0!r}; expected one of {1}".format(
                    point, ", ".join(CrashPoint.ALL)
                )
            )
        plan[point] = int(occurrence) if occurrence.strip() else 1
    return plan


def load_plan() -> Dict[str, int]:
    """Read the crash plan from the environment."""
    return parse_plan(os.environ.get(ENV_VAR))


def armed() -> bool:
    """True when a crash plan is configured."""
    return bool(os.environ.get(ENV_VAR))


def reset() -> None:
    """Forget occurrence counters (used between test cases)."""
    _counters.clear()


def maybe_crash(point: str) -> None:
    """Kill the process if *point* has now occurred the configured number of times.

    No-op when the chaos plan is unset, which is every real deployment.
    """
    if not armed():
        return

    plan = load_plan()
    target = plan.get(point)
    if target is None:
        return

    _counters[point] += 1
    if _counters[point] != target:
        return

    message = "CHAOS: killing process at {0} (occurrence {1})".format(point, target)
    logger.critical(message)
    # Bypass logging buffers -- the process is about to disappear.
    sys.stderr.write(message + "\n")
    sys.stderr.flush()
    _exit_fn(CRASH_EXIT_CODE)
