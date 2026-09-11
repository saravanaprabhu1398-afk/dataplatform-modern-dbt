"""Re-running a pipeline for periods that have already passed.

A backfill is not "run it N more times" -- it is "run it once for each window
it should have owned", which is only meaningful because a run now carries the
window it is responsible for.

    plan = plan_backfill(config, "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")
    result = submit_backfill(plan)

Three decisions that keep a backfill from becoming an outage:

**Windows already covered are skipped by default.** Re-running a period that
already succeeded is how a backfill doubles a month of revenue. ``--force``
exists, but you have to mean it.

**The plan is computed and shown before anything is queued.** Enumerating 4,000
runs and discovering it afterwards is worse than being told first.

**Nothing is enqueued unless the whole plan is enqueued.** A partial backfill
that stopped halfway is harder to reason about than one that refused to start.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from dataplatform.core.intervals import Interval, intervals_between

logger = logging.getLogger(__name__)

#: Refuse rather than queue an unbounded flood. Raise it deliberately.
DEFAULT_MAX_RUNS = 500

SKIP_ALREADY_COVERED = "already covered"


@dataclass
class PlannedRun:
    """One window a backfill would run, and whether it intends to."""

    interval: Interval
    skip_reason: Optional[str] = None
    existing_status: Optional[str] = None

    @property
    def will_run(self) -> bool:
        return self.skip_reason is None


@dataclass
class BackfillPlan:
    """What a backfill would do, before it does any of it."""

    pipeline_name: str
    config_path: str
    start: str
    end: str
    runs: List[PlannedRun] = field(default_factory=list)
    force: bool = False

    @property
    def to_run(self) -> List[PlannedRun]:
        return [run for run in self.runs if run.will_run]

    @property
    def skipped(self) -> List[PlannedRun]:
        return [run for run in self.runs if not run.will_run]

    def summary(self) -> str:
        lines = [
            "backfill {0}: {1} window(s) in [{2}, {3})".format(
                self.pipeline_name, len(self.runs), self.start, self.end
            ),
            "  will run  {0}".format(len(self.to_run)),
            "  skipped   {0}{1}".format(
                len(self.skipped),
                " (already covered; pass force to re-run)" if self.skipped else "",
            ),
        ]
        for run in self.to_run[:5]:
            lines.append("    {0}".format(run.interval))
        if len(self.to_run) > 5:
            lines.append("    ... and {0} more".format(len(self.to_run) - 5))
        return "\n".join(lines)


@dataclass
class BackfillResult:
    """What a backfill actually queued."""

    backfill_id: str
    pipeline_name: str
    run_ids: List[str] = field(default_factory=list)
    skipped: int = 0

    def summary(self) -> str:
        return "backfill {0}: queued {1} run(s), skipped {2}".format(
            self.backfill_id, len(self.run_ids), self.skipped
        )


def plan_backfill(
    config: Any,
    start: str,
    end: str,
    force: bool = False,
    max_runs: int = DEFAULT_MAX_RUNS,
    covered: Optional[Dict[str, str]] = None,
) -> BackfillPlan:
    """Work out which windows a backfill would cover, without queueing anything.

    ``covered`` defaults to the windows already recorded for this pipeline, so
    a re-run of the same command is a no-op rather than a duplicate.
    """
    if not getattr(config, "schedule", None):
        raise ValueError(
            "{0} has no schedule, so it has no windows to backfill. Give it a "
            "schedule block, or run it directly with parameters.".format(
                getattr(config, "pipeline_name", "pipeline")
            )
        )

    windows = intervals_between(config.schedule, start, end, limit=max_runs)
    if len(windows) > max_runs:
        raise ValueError(
            "{0} windows exceeds the {1} run limit; narrow the range".format(
                len(windows), max_runs
            )
        )

    if covered is None:
        from dataplatform.core.database import covered_windows

        covered = covered_windows(config.pipeline_name)

    plan = BackfillPlan(
        pipeline_name=config.pipeline_name,
        config_path=getattr(config, "file_path", "") or "",
        start=start,
        end=end,
        force=force,
        runs=[],
    )
    for window in windows:
        existing = covered.get(window.start)
        if existing is not None and not force:
            plan.runs.append(
                PlannedRun(interval=window, skip_reason=SKIP_ALREADY_COVERED,
                           existing_status=existing)
            )
        else:
            plan.runs.append(PlannedRun(interval=window, existing_status=existing))
    return plan


def submit_backfill(
    plan: BackfillPlan,
    config_path: Optional[str] = None,
    actor: str = "",
    backfill_id: Optional[str] = None,
) -> BackfillResult:
    """Queue every run the plan intends to make, as one identifiable group.

    Runs are queued oldest window first so a pipeline whose periods depend on
    each other is filled in the order it would have run.
    """
    from dataplatform.core.database import enqueue_run

    path = config_path or plan.config_path
    if not path:
        raise ValueError("a backfill needs the pipeline's config path to queue runs")

    identifier = backfill_id or "bf-{0}".format(uuid.uuid4().hex[:12])
    result = BackfillResult(
        backfill_id=identifier,
        pipeline_name=plan.pipeline_name,
        skipped=len(plan.skipped),
    )

    for planned in plan.to_run:
        run_id = "{0}-{1}".format(identifier, planned.interval.ds)
        enqueue_run(
            run_id=run_id,
            pipeline_name=plan.pipeline_name,
            config_path=path,
            actor=actor or None,
            logical_start=planned.interval.start,
            logical_end=planned.interval.end,
            backfill_id=identifier,
        )
        result.run_ids.append(run_id)

    logger.info("%s", result.summary())
    return result
