"""Bounded thread pool for pipeline execution.

Limits concurrent pipeline runs to PIPELINE_WORKERS (default 4) so that
a burst of requests cannot saturate the server's thread pool.

Usage::

    pool = get_worker_pool()
    pool.submit(run_id, my_fn, arg1, arg2)
    pool.cancel(run_id)   # only cancels runs still waiting in the queue
"""
import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Lock
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)

_MAX_WORKERS = int(os.getenv("PIPELINE_WORKERS", "4"))


class PipelineWorkerPool:
    def __init__(self, max_workers: int = _MAX_WORKERS):
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="pipeline-worker",
        )
        self._futures: Dict[str, Future] = {}
        self._lock = Lock()
        logger.info("PipelineWorkerPool started with max_workers=%d", max_workers)

    def submit(self, run_id: str, fn: Callable, *args: Any, **kwargs: Any) -> None:
        """Submit fn(*args, **kwargs) under run_id. Returns immediately."""
        future = self._pool.submit(fn, *args, **kwargs)
        with self._lock:
            self._futures[run_id] = future
        future.add_done_callback(lambda _f: self._cleanup(run_id))

    def cancel(self, run_id: str) -> bool:
        """Cancel a queued (not yet started) run. Returns True if cancelled."""
        with self._lock:
            future = self._futures.get(run_id)
        if future is None:
            return False
        return future.cancel()

    def is_running(self, run_id: str) -> bool:
        """True if the run is actively executing."""
        with self._lock:
            future = self._futures.get(run_id)
        return future is not None and future.running()

    def is_pending(self, run_id: str) -> bool:
        """True if the run is queued but not yet executing."""
        with self._lock:
            future = self._futures.get(run_id)
        return future is not None and not future.running() and not future.done()

    def _cleanup(self, run_id: str) -> None:
        with self._lock:
            self._futures.pop(run_id, None)

    def shutdown(self, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait)


_pool = PipelineWorkerPool()


def get_worker_pool() -> PipelineWorkerPool:
    return _pool
