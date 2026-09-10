"""Demonstrate lease failover and fencing against a real database.

Runs the actual queue functions -- no mocks, no forced SQL -- through the
sequence that used to corrupt state:

    worker A claims a run, heartbeats, then dies mid-run
    the lease expires on its own (wall clock, not a fake update)
    the reaper requeues the run
    worker B claims it as attempt 2 and finishes
    worker A wakes up and tries to report success -- and is fenced out

Run it with::

    python demo/scripts/lease_failover.py
"""
import os
import tempfile
import time
from pathlib import Path

LEASE_SECONDS = 2.0

started = time.time()


def log(message):
    print("[{0:6.2f}s] {1}".format(time.time() - started, message))


def main():
    db_path = Path(tempfile.mkdtemp(prefix="lease-demo-")) / "demo.db"
    os.environ["DATABASE_PATH"] = str(db_path)

    import dataplatform.core.database as db
    from dataplatform.core.queue_worker import LeaseHeartbeat

    db._DB_PATH = db_path
    db._engine = None
    db.init_db()

    db.enqueue_run("demo-run", "nightly_load", "pipelines/nightly_load.yaml")
    log("enqueued demo-run")

    claim_a = db.claim_next_queued_run(worker_id="worker-A", lease_seconds=LEASE_SECONDS)
    log("worker-A claimed demo-run as attempt {0}".format(claim_a["attempt"]))

    heartbeat_a = LeaseHeartbeat("demo-run", "worker-A", claim_a["attempt"], LEASE_SECONDS)
    heartbeat_a.beat_once()
    log("worker-A renewed its lease (still working)")

    log("worker-A dies -- no more heartbeats")
    time.sleep(LEASE_SECONDS + 0.5)

    log("recover_orphaned_runs (embedded sweep) finds nothing to do: {0} run(s)".format(
        db.recover_orphaned_runs(stale_after_seconds=0)
    ))

    counts = db.reap_expired_leases()
    log("reaper: {0} requeued, {1} dead-lettered".format(
        counts["requeued"], counts["dead_lettered"]
    ))

    claim_b = db.claim_next_queued_run(worker_id="worker-B", lease_seconds=30)
    log("worker-B claimed demo-run as attempt {0}".format(claim_b["attempt"]))

    db.complete_run_with_lease("demo-run", "worker-B", claim_b["attempt"], "completed")
    log("worker-B completed the run")

    applied = db.complete_run_with_lease("demo-run", "worker-A", claim_a["attempt"], "failed")
    log("worker-A wakes up and reports failure -> applied={0}".format(applied))

    final = db.get_queue_run("demo-run")
    print("")
    print("final state: status={0} attempt={1} worker={2} error={3}".format(
        final["status"], final["attempt"], final["worker_id"], final["error"]
    ))
    print("the zombie's write did not land" if applied is False else "FENCE FAILED")


if __name__ == "__main__":
    main()
