"""The lease the current thread is executing under.

A worker claims a run, then hands execution to
``api.execute_pipeline_background``, which writes the run's terminal status
itself.  That write has to be fenced -- otherwise a worker whose lease expired
can overwrite the outcome of the attempt that replaced it -- but threading a
``worker_id`` and ``attempt`` through every execution signature would touch
half the codebase for a property that belongs to the *thread*, not the call.

So the worker records its lease here for the duration of the run, and
:func:`dataplatform.core.database.set_run_status_in_queue` consults it:

    with lease_context(run_id, worker_id, attempt):
        execute_pipeline_background(config, run_id)

Storage is thread-local.  The embedded executor sets no lease context, so its
writes stay exactly as unfenced as they were before -- there is only one writer
in that mode, and it is the process doing the writing.
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Optional

_state = threading.local()


@dataclass(frozen=True)
class Lease:
    """A claim on one run, held by one worker, for one attempt."""

    run_id: str
    worker_id: str
    attempt: int


@contextmanager
def lease_context(run_id: str, worker_id: str, attempt: int) -> Iterator[Lease]:
    """Record the lease this thread is executing under."""
    previous = getattr(_state, "lease", None)
    lease = Lease(run_id=run_id, worker_id=worker_id, attempt=attempt)
    _state.lease = lease
    try:
        yield lease
    finally:
        _state.lease = previous


def current_lease(run_id: Optional[str] = None) -> Optional[Lease]:
    """Return this thread's lease, or None.

    When *run_id* is given, the lease is returned only if it covers that run --
    a status write for some other run must not borrow this thread's fence.
    """
    lease = getattr(_state, "lease", None)
    if lease is None:
        return None
    if run_id is not None and lease.run_id != run_id:
        return None
    return lease


def clear() -> None:
    """Drop any lease recorded for this thread (used in tests)."""
    _state.lease = None
