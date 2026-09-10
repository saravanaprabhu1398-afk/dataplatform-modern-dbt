"""Naive consumers, so the harness has a "before" column.

The platform has no streaming path yet, so the honest baseline is the two
consumer shapes people actually write when they haven't thought about the
commit boundary:

* ``at_least_once`` -- write the batch, then commit the offset.  A crash in
  between replays the batch on restart, so the sink gains duplicates.
* ``at_most_once``  -- commit the offset, then write the batch.  A crash in
  between skips the batch forever, so the sink loses rows.

Neither is a strawman; both are what a first implementation looks like.  Running
them through the verifier produces the duplicate and loss rates that Week 1's
work has to drive to zero.

    events = generate(GeneratorConfig()).events
    rows, offset = run_baseline(events, mode="at_least_once", crash_after_batches=3)

The crash is simulated rather than a real process kill: the consumer stops
between the two steps exactly where a ``SIGKILL`` would have landed, then
restarts from the last committed offset.  Real process kills arrive in Week 4
via :mod:`dataplatform.core.chaos`.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from dataplatform.streaming.model import StreamEvent

logger = logging.getLogger(__name__)

AT_LEAST_ONCE = "at_least_once"
AT_MOST_ONCE = "at_most_once"
MODES = (AT_LEAST_ONCE, AT_MOST_ONCE)


@dataclass
class BaselineRun:
    """What a naive consumer left behind."""

    mode: str
    rows: List[Dict[str, Any]] = field(default_factory=list)
    committed_offset: int = 0
    crashed_at_offset: Optional[int] = None
    batches_processed: int = 0

    @property
    def crashed(self) -> bool:
        return self.crashed_at_offset is not None


def run_baseline(
    events: Sequence[StreamEvent],
    mode: str = AT_LEAST_ONCE,
    batch_size: int = 100,
    crash_after_batches: Optional[int] = None,
) -> BaselineRun:
    """Consume *events* with a naive consumer, optionally crashing mid-stream.

    Returns the sink contents and the offset the consumer would have restarted
    from.  Feed ``run.rows`` straight into :func:`dataplatform.streaming.verifier.verify`.
    """
    if mode not in MODES:
        raise ValueError("mode must be one of {0}".format(", ".join(MODES)))
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    run = BaselineRun(mode=mode)
    total = len(events)
    committed = 0
    offset = 0
    crashed = False

    while offset < total:
        batch = events[offset:offset + batch_size]
        run.batches_processed += 1
        crash_here = (
            crash_after_batches is not None
            and not crashed
            and run.batches_processed == crash_after_batches
        )

        if mode == AT_LEAST_ONCE:
            run.rows.extend(event.as_dict() for event in batch)
            if crash_here:
                # Killed after the write, before the offset advanced.
                crashed = True
                run.crashed_at_offset = offset + len(batch)
                offset = committed          # restart replays this batch
                logger.info(
                    "baseline crash (at_least_once) after writing offset %d, "
                    "resuming from committed offset %d",
                    run.crashed_at_offset,
                    committed,
                )
                continue
            offset += len(batch)
            committed = offset
        else:  # AT_MOST_ONCE
            committed = offset + len(batch)
            if crash_here:
                # Killed after the offset advanced, before the write landed.
                crashed = True
                run.crashed_at_offset = offset
                offset = committed          # restart skips this batch
                logger.info(
                    "baseline crash (at_most_once) after committing offset %d, "
                    "batch never written",
                    committed,
                )
                continue
            run.rows.extend(event.as_dict() for event in batch)
            offset = committed

    run.committed_offset = committed
    return run
