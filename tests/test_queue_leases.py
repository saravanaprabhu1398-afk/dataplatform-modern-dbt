"""Tests for lease-based claiming and fencing (Week 1).

The property under test is not "a run gets executed" -- it is "a run is
executed by exactly one live worker, and a worker that lost its lease cannot
report an outcome".  Every test here either takes a lease away from a worker or
makes two workers compete for one.
"""
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import inspect as sa_inspect

import dataplatform.core.database as db
from dataplatform.core.leases import clear as clear_lease_context
from dataplatform.core.leases import lease_context
from dataplatform.core.queue_worker import LeaseHeartbeat
from dataplatform.core import chaos


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Point DATABASE_PATH at a temp file so each test gets a fresh DB."""
    db_file = str(tmp_path / "test_leases.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    db._initialized = False
    db._DB_PATH = Path(db_file)
    db._engine = None
    db.init_db()
    yield
    clear_lease_context()
    db._initialized = False
    db._engine = None


def _expire_lease(run_id, seconds_ago=60):
    """Force a run's lease into the past, as if its worker had stopped."""
    past = (datetime.utcnow() - timedelta(seconds=seconds_ago)).isoformat() + "Z"
    with db._get_conn() as conn:
        conn.execute(
            db.text("UPDATE pipeline_queue SET lease_expires_at = :past WHERE run_id = :r"),
            {"past": past, "r": run_id},
        )
        conn.commit()


class TestSchema:
    def test_lease_columns_exist(self):
        columns = {c["name"] for c in sa_inspect(db._get_engine()).get_columns("pipeline_queue")}
        assert {"worker_id", "lease_expires_at", "heartbeat_at", "attempt"} <= columns

    def test_migration_is_idempotent(self):
        db._ensure_schema_migrations(db._get_engine())
        db._ensure_schema_migrations(db._get_engine())
        columns = {c["name"] for c in sa_inspect(db._get_engine()).get_columns("pipeline_queue")}
        assert "attempt" in columns


class TestClaimTakesLease:
    def test_claim_stamps_worker_lease_and_attempt(self):
        db.enqueue_run("run-1", "pipe", "p.yaml")

        claimed = db.claim_next_queued_run(worker_id="w1", lease_seconds=30)

        assert claimed["run_id"] == "run-1"
        assert claimed["status"] == "running"
        assert claimed["worker_id"] == "w1"
        assert claimed["attempt"] == 1
        assert claimed["heartbeat_at"] is not None
        assert claimed["lease_expires_at"] > db._now_iso()

    def test_claim_returns_none_when_empty(self):
        assert db.claim_next_queued_run(worker_id="w1") is None

    def test_two_workers_take_different_runs(self):
        # The liveness fix: the second worker must get the *other* run, not
        # None because it lost a race for the head of the queue.
        db.enqueue_run("run-a", "pipe", "p.yaml")
        db.enqueue_run("run-b", "pipe", "p.yaml")

        first = db.claim_next_queued_run(worker_id="w1")
        second = db.claim_next_queued_run(worker_id="w2")

        assert first is not None and second is not None
        assert {first["run_id"], second["run_id"]} == {"run-a", "run-b"}

    def test_concurrent_workers_never_share_a_run(self):
        import threading

        for index in range(6):
            db.enqueue_run("run-c{0}".format(index), "pipe", "p.yaml")

        claims = []
        lock = threading.Lock()

        def claim(worker):
            result = db.claim_next_queued_run(worker_id=worker)
            if result is not None:
                with lock:
                    claims.append(result["run_id"])

        threads = [
            threading.Thread(target=claim, args=("w{0}".format(i),)) for i in range(6)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(claims) == 6, "every worker should have found work"
        assert len(set(claims)) == 6, "no run may be claimed twice"

    def test_attempt_increments_on_reclaim(self):
        db.enqueue_run("run-2", "pipe", "p.yaml")
        first = db.claim_next_queued_run(worker_id="w1", lease_seconds=1)
        _expire_lease("run-2")
        db.reap_expired_leases()

        second = db.claim_next_queued_run(worker_id="w2")

        assert first["attempt"] == 1
        assert second["attempt"] == 2
        assert second["worker_id"] == "w2"


class TestRenewAndHold:
    def test_renew_extends_the_lease(self):
        db.enqueue_run("run-3", "pipe", "p.yaml")
        claimed = db.claim_next_queued_run(worker_id="w1", lease_seconds=1)

        assert db.renew_lease("run-3", "w1", claimed["attempt"], lease_seconds=120)
        assert db.get_queue_run("run-3")["lease_expires_at"] > claimed["lease_expires_at"]

    def test_renew_fails_for_wrong_worker(self):
        db.enqueue_run("run-4", "pipe", "p.yaml")
        claimed = db.claim_next_queued_run(worker_id="w1")
        assert not db.renew_lease("run-4", "impostor", claimed["attempt"])

    def test_renew_fails_for_stale_attempt(self):
        db.enqueue_run("run-5", "pipe", "p.yaml")
        db.claim_next_queued_run(worker_id="w1", lease_seconds=1)
        _expire_lease("run-5")
        db.reap_expired_leases()
        db.claim_next_queued_run(worker_id="w2")

        assert not db.renew_lease("run-5", "w1", 1)

    def test_holds_lease_tracks_expiry(self):
        db.enqueue_run("run-6", "pipe", "p.yaml")
        claimed = db.claim_next_queued_run(worker_id="w1", lease_seconds=60)

        assert db.holds_lease("run-6", "w1", claimed["attempt"])
        assert not db.holds_lease("run-6", "w2", claimed["attempt"])

        _expire_lease("run-6")
        assert not db.holds_lease("run-6", "w1", claimed["attempt"])


class TestFencing:
    def test_lease_holder_can_complete(self):
        db.enqueue_run("run-7", "pipe", "p.yaml")
        claimed = db.claim_next_queued_run(worker_id="w1")

        assert db.complete_run_with_lease("run-7", "w1", claimed["attempt"], "completed")
        assert db.get_queue_run("run-7")["status"] == "completed"

    def test_fenced_worker_cannot_complete(self):
        db.enqueue_run("run-8", "pipe", "p.yaml")
        db.claim_next_queued_run(worker_id="w1", lease_seconds=1)
        _expire_lease("run-8")
        db.reap_expired_leases()
        db.claim_next_queued_run(worker_id="w2")

        # The zombie wakes up and tries to declare the run finished.
        applied = db.complete_run_with_lease("run-8", "w1", 1, "completed")

        assert applied is False
        run = db.get_queue_run("run-8")
        assert run["status"] == "running"
        assert run["worker_id"] == "w2"

    def test_status_writes_are_fenced_inside_a_lease_context(self):
        db.enqueue_run("run-9", "pipe", "p.yaml")
        db.claim_next_queued_run(worker_id="w1", lease_seconds=1)
        _expire_lease("run-9")
        db.reap_expired_leases()
        db.claim_next_queued_run(worker_id="w2")

        with lease_context("run-9", "w1", 1):
            applied = db.set_run_status_in_queue("run-9", "failed", error="stale write")

        assert applied is False
        assert db.get_queue_run("run-9")["status"] == "running"

    def test_status_writes_apply_for_the_live_attempt(self):
        db.enqueue_run("run-10", "pipe", "p.yaml")
        claimed = db.claim_next_queued_run(worker_id="w1")

        with lease_context("run-10", "w1", claimed["attempt"]):
            assert db.set_run_status_in_queue("run-10", "completed")

        assert db.get_queue_run("run-10")["status"] == "completed"

    def test_lease_context_does_not_fence_other_runs(self):
        db.enqueue_run("run-11", "pipe", "p.yaml")
        db.enqueue_run("run-12", "pipe", "p.yaml")
        claimed = db.claim_next_queued_run(worker_id="w1")

        with lease_context(claimed["run_id"], "w1", claimed["attempt"]):
            other = "run-12" if claimed["run_id"] == "run-11" else "run-11"
            assert db.set_run_status_in_queue(other, "cancelled")

        assert db.get_queue_run(other)["status"] == "cancelled"

    def test_embedded_writes_stay_unfenced(self):
        db.enqueue_run("run-13", "pipe", "p.yaml")
        db.set_run_status_in_queue("run-13", "running")

        assert db.set_run_status_in_queue("run-13", "completed")
        assert db.get_queue_run("run-13")["status"] == "completed"


class TestReaper:
    def test_requeues_expired_lease(self):
        db.enqueue_run("run-14", "pipe", "p.yaml")
        db.claim_next_queued_run(worker_id="w1", lease_seconds=1)
        _expire_lease("run-14")

        counts = db.reap_expired_leases()

        assert counts == {"requeued": 1, "dead_lettered": 0}
        run = db.get_queue_run("run-14")
        assert run["status"] == "queued"
        assert run["worker_id"] is None
        assert run["started_at"] is None
        assert run["attempt"] == 1  # the next claim increments it

    def test_leaves_live_leases_alone(self):
        db.enqueue_run("run-15", "pipe", "p.yaml")
        db.claim_next_queued_run(worker_id="w1", lease_seconds=300)

        assert db.reap_expired_leases() == {"requeued": 0, "dead_lettered": 0}
        assert db.get_queue_run("run-15")["status"] == "running"

    def test_a_long_run_is_not_killed_for_being_long(self):
        # The bug this week exists to fix: liveness inferred from wall clock.
        db.enqueue_run("run-16", "pipe", "p.yaml")
        claimed = db.claim_next_queued_run(worker_id="w1", lease_seconds=300)
        old_start = (datetime.utcnow() - timedelta(hours=9)).isoformat() + "Z"
        with db._get_conn() as conn:
            conn.execute(
                db.text("UPDATE pipeline_queue SET started_at = :s WHERE run_id = 'run-16'"),
                {"s": old_start},
            )
            conn.commit()

        db.reap_expired_leases()
        db.recover_orphaned_runs(stale_after_seconds=3600)

        run = db.get_queue_run("run-16")
        assert run["status"] == "running"
        assert db.holds_lease("run-16", "w1", claimed["attempt"])

    def test_dead_letters_after_max_attempts(self):
        db.enqueue_run("run-17", "pipe", "p.yaml")
        for _ in range(3):
            db.claim_next_queued_run(worker_id="w1", lease_seconds=1)
            _expire_lease("run-17")
            db.reap_expired_leases(max_attempts=3)

        run = db.get_queue_run("run-17")
        assert run["status"] == "dead_letter"
        assert "Lease expired" in run["error"]
        assert run["attempt"] == 3

    def test_reaped_run_is_claimable_again(self):
        db.enqueue_run("run-18", "pipe", "p.yaml")
        db.claim_next_queued_run(worker_id="w1", lease_seconds=1)
        _expire_lease("run-18")
        db.reap_expired_leases()

        reclaimed = db.claim_next_queued_run(worker_id="w2")
        assert reclaimed["run_id"] == "run-18"
        assert reclaimed["worker_id"] == "w2"


class TestOrphanRecoveryIsLeaseAware:
    def test_skips_runs_with_a_live_lease(self):
        db.enqueue_run("run-19", "pipe", "p.yaml")
        db.claim_next_queued_run(worker_id="w1", lease_seconds=300)

        recovered = db.recover_orphaned_runs(stale_after_seconds=0)

        assert recovered == 0
        assert db.get_queue_run("run-19")["status"] == "running"

    def test_still_fails_leaseless_runs(self):
        # Embedded mode: no lease, the process that owned it is gone.
        db.enqueue_run("run-20", "pipe", "p.yaml")
        db.set_run_status_in_queue("run-20", "running")

        recovered = db.recover_orphaned_runs(stale_after_seconds=0)

        assert recovered == 1
        assert db.get_queue_run("run-20")["error"] == "Server restarted"

    def test_leaves_expired_leases_for_the_reaper(self):
        # An expired lease is recoverable work, not a failure. Sweeping it here
        # would race the reaper and turn a requeue into a dead run.
        db.enqueue_run("run-21", "pipe", "p.yaml")
        db.claim_next_queued_run(worker_id="w1", lease_seconds=1)
        _expire_lease("run-21")

        assert db.recover_orphaned_runs(stale_after_seconds=0) == 0
        assert db.get_queue_run("run-21")["status"] == "running"

        assert db.reap_expired_leases()["requeued"] == 1
        assert db.get_queue_run("run-21")["status"] == "queued"


class TestHeartbeat:
    def setup_method(self):
        chaos.reset()

    def test_beat_renews_while_held(self):
        db.enqueue_run("run-22", "pipe", "p.yaml")
        claimed = db.claim_next_queued_run(worker_id="w1", lease_seconds=30)
        heartbeat = LeaseHeartbeat("run-22", "w1", claimed["attempt"], lease_seconds=120)

        assert heartbeat.beat_once()
        assert not heartbeat.lost
        assert db.get_queue_run("run-22")["lease_expires_at"] > claimed["lease_expires_at"]

    def test_beat_marks_lost_when_fenced(self):
        db.enqueue_run("run-23", "pipe", "p.yaml")
        db.claim_next_queued_run(worker_id="w1", lease_seconds=1)
        _expire_lease("run-23")
        db.reap_expired_leases()
        db.claim_next_queued_run(worker_id="w2")

        heartbeat = LeaseHeartbeat("run-23", "w1", 1)
        assert not heartbeat.beat_once()
        assert heartbeat.lost

    def test_background_thread_renews(self):
        db.enqueue_run("run-24", "pipe", "p.yaml")
        claimed = db.claim_next_queued_run(worker_id="w1", lease_seconds=30)

        with LeaseHeartbeat(
            "run-24", "w1", claimed["attempt"], lease_seconds=60, interval=0.05
        ) as heartbeat:
            deadline = time.time() + 2
            while time.time() < deadline:
                if db.get_queue_run("run-24")["lease_expires_at"] > claimed["lease_expires_at"]:
                    break
                time.sleep(0.02)

        assert not heartbeat.lost
        assert db.get_queue_run("run-24")["lease_expires_at"] > claimed["lease_expires_at"]

    def test_chaos_can_kill_the_heartbeat(self, monkeypatch):
        exits = []
        monkeypatch.setenv(chaos.ENV_VAR, "before_heartbeat:1")
        monkeypatch.setattr(chaos, "_exit_fn", lambda code: exits.append(code))

        db.enqueue_run("run-25", "pipe", "p.yaml")
        claimed = db.claim_next_queued_run(worker_id="w1")
        LeaseHeartbeat("run-25", "w1", claimed["attempt"]).beat_once()

        assert exits == [chaos.CRASH_EXIT_CODE]
