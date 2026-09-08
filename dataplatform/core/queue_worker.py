"""External queue worker for production-style deployments.

The worker claims a run, takes a lease on it, and renews that lease on a
heartbeat for as long as it is working.  Three properties follow:

* A crashed worker stops heartbeating, so its run is requeued by
  :func:`~dataplatform.core.database.reap_expired_leases` after the lease
  expires -- not after an arbitrary wall-clock timeout.
* A worker that is merely *slow* keeps its run, however long it takes.  Length
  is not evidence of death.
* A worker that stalls past its lease is fenced: its status writes no longer
  apply, because the attempt it holds is no longer the live one.

Run it with::

    python -m dataplatform.core.queue_worker
    DATAPLATFORM_CHAOS="before_heartbeat:2" python -m dataplatform.core.queue_worker
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Optional

from dataplatform.core.config import load_config
from dataplatform.core.database import (
    DEFAULT_LEASE_SECONDS,
    claim_next_queued_run,
    complete_run_with_lease,
    generate_worker_id,
    init_db,
    reap_expired_leases,
    recover_orphaned_runs,
    renew_lease,
    save_run_status,
)
from dataplatform.core.leases import lease_context
from dataplatform.core.chaos import CrashPoint, maybe_crash

logger = logging.getLogger(__name__)

#: Renew at a third of the lease, so two consecutive missed renewals still
#: leave time before another worker can take the run.
HEARTBEAT_DIVISOR = 3


class LeaseHeartbeat:
    """Renews one run's lease in the background until stopped.

    If a renewal fails the run is no longer ours -- the reaper requeued it, or
    another attempt claimed it -- so the heartbeat records the loss and stops.
    The worker checks :attr:`lost` and abandons the run rather than reporting
    an outcome it is no longer entitled to report.
    """

    def __init__(
        self,
        run_id: str,
        worker_id: str,
        attempt: int,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        interval: Optional[float] = None,
    ) -> None:
        self.run_id = run_id
        self.worker_id = worker_id
        self.attempt = attempt
        self.lease_seconds = lease_seconds
        self.interval = interval or max(lease_seconds / HEARTBEAT_DIVISOR, 0.1)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lost = threading.Event()

    @property
    def lost(self) -> bool:
        """True when a renewal failed and this worker no longer holds the run."""
        return self._lost.is_set()

    def start(self) -> "LeaseHeartbeat":
        self._thread = threading.Thread(
            target=self._run, name="lease-{0}".format(self.run_id), daemon=True
        )
        self._thread.start()
        return self

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def beat_once(self) -> bool:
        """Renew the lease once. False means it was lost."""
        maybe_crash(CrashPoint.BEFORE_HEARTBEAT)
        if renew_lease(self.run_id, self.worker_id, self.attempt, self.lease_seconds):
            return True

        self._lost.set()
        logger.error(
            "Lease lost for run_id=%s (worker=%s attempt=%s); abandoning run",
            self.run_id,
            self.worker_id,
            self.attempt,
        )
        return False

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            if not self.beat_once():
                return

    def __enter__(self) -> "LeaseHeartbeat":
        return self.start()

    def __exit__(self, *exc_info) -> None:
        self.stop()


def run_worker_once(
    worker_id: Optional[str] = None,
    lease_seconds: float = DEFAULT_LEASE_SECONDS,
    heartbeat_interval: Optional[float] = None,
) -> bool:
    """Claim and execute one queued run. Returns True when work was processed."""
    worker = worker_id or generate_worker_id()
    run = claim_next_queued_run(worker_id=worker, lease_seconds=lease_seconds)
    if run is None:
        return False

    run_id = run["run_id"]
    pipeline_name = run["pipeline_name"]
    config_path = run["config_path"]
    attempt = int(run.get("attempt") or 1)
    logger.info(
        "Worker %s claimed run_id=%s pipeline=%s attempt=%s",
        worker,
        run_id,
        pipeline_name,
        attempt,
    )

    heartbeat = LeaseHeartbeat(
        run_id, worker, attempt, lease_seconds=lease_seconds, interval=heartbeat_interval
    )
    try:
        with heartbeat, lease_context(run_id, worker, attempt):
            from dataplatform.core.api import execute_pipeline_background

            config = load_config(config_path)
            execute_pipeline_background(config, run_id)
    except Exception as exc:
        logger.error("Worker %s failed run_id=%s: %s", worker, run_id, exc, exc_info=True)
        if heartbeat.lost:
            # Another attempt owns this run now; reporting a failure here would
            # overwrite whatever that attempt is doing.
            logger.warning(
                "Not reporting failure for run_id=%s -- lease was lost", run_id
            )
            return True
        complete_run_with_lease(run_id, worker, attempt, "failed", error=str(exc))
        save_run_status(
            pipeline_name,
            run_id,
            "failed",
            "Worker failed before or during execution: {0}".format(exc),
            {"config_path": config_path, "attempt": attempt},
        )
        return True

    if heartbeat.lost:
        logger.warning(
            "Completed work for run_id=%s but the lease was lost; result discarded",
            run_id,
        )
    return True


def run_worker_loop(
    *,
    poll_interval: float = 2.0,
    once: bool = False,
    recover_orphans: bool = True,
    idle_sleep: Optional[float] = None,
    worker_id: Optional[str] = None,
    lease_seconds: float = DEFAULT_LEASE_SECONDS,
    max_attempts: Optional[int] = None,
) -> None:
    """Poll the persistent queue and execute claimed runs."""
    init_db()
    worker = worker_id or generate_worker_id()
    if recover_orphans:
        recover_orphaned_runs()

    sleep_seconds = poll_interval if idle_sleep is None else idle_sleep
    logger.info(
        "Queue worker %s started (poll_interval=%ss, lease=%ss, once=%s)",
        worker,
        poll_interval,
        lease_seconds,
        once,
    )

    while True:
        # Reap before claiming: a run whose worker died is work available now.
        reap_kwargs = {} if max_attempts is None else {"max_attempts": max_attempts}
        reap_expired_leases(**reap_kwargs)

        processed = run_worker_once(worker_id=worker, lease_seconds=lease_seconds)
        if once:
            return
        if not processed:
            time.sleep(sleep_seconds)


if __name__ == "__main__":  # pragma: no cover
    from dataplatform.core.logging_config import setup_logging

    setup_logging()
    run_worker_loop()
