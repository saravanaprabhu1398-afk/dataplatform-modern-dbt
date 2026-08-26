"""External queue worker for production-style deployments."""
from __future__ import annotations

import logging
import time
from typing import Optional

from dataplatform.core.config import load_config
from dataplatform.core.database import (
    claim_next_queued_run,
    init_db,
    recover_orphaned_runs,
    save_run_status,
    set_run_status_in_queue,
)

logger = logging.getLogger(__name__)


def run_worker_once() -> bool:
    """Claim and execute one queued run. Returns True when work was processed."""
    run = claim_next_queued_run()
    if run is None:
        return False

    run_id = run["run_id"]
    pipeline_name = run["pipeline_name"]
    config_path = run["config_path"]
    logger.info("External worker claimed run_id=%s pipeline=%s", run_id, pipeline_name)

    try:
        from dataplatform.core.api import execute_pipeline_background

        config = load_config(config_path)
        execute_pipeline_background(config, run_id)
    except Exception as exc:
        logger.error("External worker failed run_id=%s: %s", run_id, exc, exc_info=True)
        set_run_status_in_queue(run_id, "failed", error=str(exc))
        save_run_status(
            pipeline_name,
            run_id,
            "failed",
            f"External worker failed before execution: {exc}",
            {"config_path": config_path},
        )

    return True


def run_worker_loop(
    *,
    poll_interval: float = 2.0,
    once: bool = False,
    recover_orphans: bool = True,
    idle_sleep: Optional[float] = None,
) -> None:
    """Poll the persistent queue and execute claimed runs."""
    init_db()
    if recover_orphans:
        recover_orphaned_runs()

    sleep_seconds = poll_interval if idle_sleep is None else idle_sleep
    logger.info(
        "External queue worker started (poll_interval=%ss, once=%s)",
        poll_interval,
        once,
    )

    while True:
        processed = run_worker_once()
        if once:
            return
        if not processed:
            time.sleep(sleep_seconds)
