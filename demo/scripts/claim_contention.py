"""Measure the old claim against the new one under concurrency.

The original ``claim_next_queued_run`` was *safe* -- its conditional UPDATE
made a double-claim impossible -- but it was not *live*: every worker selected
the same head-of-queue row, and the losers returned ``None`` and went back to
sleep while the queue still held work.

This script runs both implementations against the same queue and reports how
much work each one actually picked up.  PostgreSQL only, because the fix
(``FOR UPDATE SKIP LOCKED``) is a PostgreSQL feature:

    DATAPLATFORM_TEST_POSTGRES_URL=postgresql+psycopg2://dp:dp@localhost:55432/dp \\
        python demo/scripts/claim_contention.py
"""
import os
import sys
import threading
import time

URL = os.environ.get("DATAPLATFORM_TEST_POSTGRES_URL")
WORKERS = 12


def legacy_claim(db, worker_id):
    """The claim as it was: SELECT the head, then conditionally UPDATE it."""
    now = db._now_iso()
    with db._get_conn() as conn:
        trans = conn.begin()
        try:
            row = conn.execute(
                db.text(
                    "SELECT * FROM pipeline_queue WHERE status = 'queued' "
                    "ORDER BY queued_at ASC LIMIT 1"
                )
            ).fetchone()
            if row is None:
                trans.commit()
                return None
            run = dict(row._mapping)
            result = conn.execute(
                db.text(
                    "UPDATE pipeline_queue SET status = 'running', started_at = :now "
                    "WHERE run_id = :run_id AND status = 'queued'"
                ),
                {"now": now, "run_id": run["run_id"]},
            )
            trans.commit()
        except Exception:
            trans.rollback()
            raise
    # The losing racer stops here, even though other rows are still queued.
    return run if result.rowcount == 1 else None


def run_round(db, claim_fn, label):
    with db._get_conn() as conn:
        conn.execute(db.text("DELETE FROM pipeline_queue"))
        conn.commit()
    for index in range(WORKERS):
        db.enqueue_run("bench-{0}".format(index), "pipe", "p.yaml")

    claimed, missed = [], []
    lock = threading.Lock()
    barrier = threading.Barrier(WORKERS)

    def worker(name):
        barrier.wait()
        result = claim_fn(db, name)
        with lock:
            (claimed if result else missed).append(name)

    threads = [threading.Thread(target=worker, args=("w{0}".format(i),)) for i in range(WORKERS)]
    started = time.time()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = (time.time() - started) * 1000

    with db._get_conn() as conn:
        remaining = conn.execute(
            db.text("SELECT COUNT(*) FROM pipeline_queue WHERE status = 'queued'")
        ).scalar()

    print(
        "{0:<24} claimed {1:>3}/{2}   idle workers {3:>3}   "
        "left queued {4:>3}   {5:6.1f} ms".format(
            label, len(claimed), WORKERS, len(missed), remaining, elapsed
        )
    )
    return len(claimed)


def main():
    if not URL:
        print("set DATAPLATFORM_TEST_POSTGRES_URL to a PostgreSQL database")
        return 1

    os.environ["POSTGRES_URL"] = URL
    import dataplatform.core.database as db

    db._initialized = False
    db._engine = None
    db.init_db()

    print("{0} workers releasing simultaneously against {0} queued runs\n".format(WORKERS))
    legacy = run_round(db, legacy_claim, "legacy SELECT+UPDATE")
    new = run_round(
        db, lambda d, w: d.claim_next_queued_run(worker_id=w), "SKIP LOCKED + lease"
    )

    print("\nwork picked up per wake-up: {0}/{1} -> {2}/{1}".format(legacy, WORKERS, new))
    return 0


if __name__ == "__main__":
    sys.exit(main())
