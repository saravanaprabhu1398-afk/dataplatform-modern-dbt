"""Tests for the streaming correctness harness.

These tests exist to prove the *oracle*, not the pipeline.  A verifier that has
never been shown a broken sink is a verifier that will happily pass a broken
pipeline, so every failure mode gets a test that deliberately corrupts a good
run and asserts the verifier notices.
"""
import copy

import pytest

from dataplatform.core import chaos
from dataplatform.streaming.baseline import AT_LEAST_ONCE, AT_MOST_ONCE, run_baseline
from dataplatform.core.chaos import CrashPoint
from dataplatform.streaming.generator import GeneratorConfig, Manifest, generate
from dataplatform.streaming.model import (
    StreamEvent,
    compute_checksum,
    read_jsonl,
    window_start,
    write_jsonl,
)
from dataplatform.streaming.verifier import verify


SMALL = GeneratorConfig(keys=4, events_per_key=25, seed=42)


@pytest.fixture
def stream():
    return generate(SMALL)


@pytest.fixture
def perfect_rows(stream):
    """What a correct exactly-once sink would contain."""
    return [event.as_dict() for event in stream.events]


class TestGenerator:
    def test_is_deterministic(self):
        first = generate(GeneratorConfig(keys=3, events_per_key=10, seed=7))
        second = generate(GeneratorConfig(keys=3, events_per_key=10, seed=7))
        assert [e.as_dict() for e in first.events] == [e.as_dict() for e in second.events]
        assert first.manifest.as_dict() == second.manifest.as_dict()

    def test_seed_changes_the_stream(self):
        first = generate(GeneratorConfig(keys=3, events_per_key=10, seed=7))
        second = generate(GeneratorConfig(keys=3, events_per_key=10, seed=8))
        assert [e.as_dict() for e in first.events] != [e.as_dict() for e in second.events]

    def test_sequences_are_gapless_per_key(self, stream):
        by_key = {}
        for event in stream.events:
            by_key.setdefault(event.key, []).append(event.seq)
        assert len(by_key) == SMALL.keys
        for key, seqs in by_key.items():
            assert sorted(seqs) == list(range(SMALL.events_per_key)), key

    def test_arrival_order_is_out_of_event_order(self, stream):
        # Without inversions the harness cannot tell event-time windowing from
        # processing-time windowing, which is half of what it exists to check.
        assert stream.out_of_order_count > 0

    def test_arrival_order_is_monotonic_by_ingest_time(self, stream):
        ingest_times = [event.ingest_time for event in stream.events]
        assert ingest_times == sorted(ingest_times)

    def test_manifest_totals_match_events(self, stream):
        assert stream.manifest.total_unique == SMALL.keys * SMALL.events_per_key
        assert len(stream.manifest.checksums) == stream.manifest.total_unique
        window_count = sum(w["count"] for w in stream.manifest.windows.values())
        assert window_count == stream.manifest.total_unique

    def test_checksums_are_intact(self, stream):
        assert all(event.is_intact() for event in stream.events)

    def test_duplicate_injection_adds_rows_without_changing_manifest(self):
        config = GeneratorConfig(
            keys=3, events_per_key=20, seed=11, duplicate_fraction=0.5
        )
        dup_stream = generate(config)
        assert dup_stream.manifest.duplicates_injected > 0
        assert len(dup_stream.events) > dup_stream.manifest.total_unique
        # The manifest describes the truth, not the delivery.
        assert dup_stream.manifest.total_unique == 60

    def test_jsonl_roundtrip(self, stream, tmp_path):
        path = str(tmp_path / "events.jsonl")
        written = write_jsonl(path, stream.events)
        rows = read_jsonl(path)
        assert written == len(stream.events) == len(rows)
        assert StreamEvent.from_dict(rows[0]).as_dict() == stream.events[0].as_dict()

    def test_manifest_roundtrip(self, stream, tmp_path):
        path = str(tmp_path / "manifest.json")
        stream.manifest.write(path)
        assert Manifest.read(path).as_dict() == stream.manifest.as_dict()


class TestVerifierAcceptsCorrectSinks:
    def test_perfect_sink_passes(self, stream, perfect_rows):
        report = verify(stream.manifest, perfect_rows)
        assert report.ok, report.summary()
        assert report.duplicate_rate == 0.0
        assert report.loss_rate == 0.0

    def test_row_order_does_not_matter(self, stream, perfect_rows):
        shuffled = list(reversed(perfect_rows))
        assert verify(stream.manifest, shuffled).ok

    def test_extra_sink_columns_are_tolerated(self, stream, perfect_rows):
        rows = [dict(row, _loaded_at="2026-01-01T00:00:00.000000Z") for row in perfect_rows]
        assert verify(stream.manifest, rows).ok

    def test_stringified_numerics_are_tolerated(self, stream, perfect_rows):
        rows = [
            dict(row, seq=str(row["seq"]), amount_cents=str(row["amount_cents"]))
            for row in perfect_rows
        ]
        assert verify(stream.manifest, rows).ok


class TestVerifierCatchesBrokenSinks:
    def test_catches_duplicates(self, stream, perfect_rows):
        rows = perfect_rows + perfect_rows[:5]
        report = verify(stream.manifest, rows)

        assert not report.ok
        assert report.duplicate_total == 5
        assert report.duplicate_rows == 5
        assert report.duplicate_rate == pytest.approx(5 / stream.manifest.total_unique)
        assert report.missing_total == 0

    def test_counts_repeated_duplicates_once_per_identity(self, stream, perfect_rows):
        rows = perfect_rows + perfect_rows[:1] * 3
        report = verify(stream.manifest, rows)
        assert report.duplicate_total == 1     # one identity affected
        assert report.duplicate_rows == 3      # three extra copies

    def test_catches_missing_rows(self, stream, perfect_rows):
        rows = perfect_rows[:-7]
        report = verify(stream.manifest, rows)

        assert not report.ok
        assert report.missing_total == 7
        assert report.loss_rate == pytest.approx(7 / stream.manifest.total_unique)
        assert len(report.missing_examples) == 7

    def test_catches_mutated_payload(self, stream, perfect_rows):
        rows = copy.deepcopy(perfect_rows)
        rows[3]["payload"] = "tampered"
        report = verify(stream.manifest, rows)

        assert not report.ok
        assert report.corrupted_total == 1

    def test_catches_mutation_with_recomputed_checksum(self, stream, perfect_rows):
        # The subtle case: a writer that mutates a row and recomputes its
        # checksum is self-consistent, and only the manifest catches it.
        rows = copy.deepcopy(perfect_rows)
        row = rows[4]
        row["amount_cents"] = row["amount_cents"] + 1
        row["checksum"] = compute_checksum(
            row["key"], row["seq"], row["event_time"], row["amount_cents"], row["payload"]
        )
        report = verify(stream.manifest, rows)

        assert not report.ok
        assert report.corrupted_total == 1
        assert report.window_mismatches, "an altered amount must move a window sum"

    def test_catches_unexpected_rows(self, stream, perfect_rows):
        ghost = StreamEvent.create(
            key="k9999",
            seq=0,
            event_time=perfect_rows[0]["event_time"],
            ingest_time=perfect_rows[0]["ingest_time"],
            amount_cents=500,
            payload="ghost",
        )
        report = verify(stream.manifest, perfect_rows + [ghost.as_dict()])

        assert not report.ok
        assert report.unexpected_total == 1

    def test_catches_unparseable_rows(self, stream, perfect_rows):
        report = verify(stream.manifest, perfect_rows + [{"nonsense": True}])
        assert not report.ok
        assert report.unexpected_total == 1

    def test_catches_window_aggregate_drift(self, stream, perfect_rows):
        rows = copy.deepcopy(perfect_rows)
        dropped = rows.pop(0)
        report = verify(stream.manifest, rows)

        assert not report.ok
        affected = window_start(dropped["event_time"], stream.manifest.window_seconds)
        assert affected in [m.window_start for m in report.window_mismatches]

    def test_catches_offset_drift(self, stream, perfect_rows):
        report = verify(stream.manifest, perfect_rows, committed_offset=len(perfect_rows) + 10)
        assert not report.ok
        assert report.offset_gap == 10

    def test_offset_agreement_passes(self, stream, perfect_rows):
        report = verify(stream.manifest, perfect_rows, committed_offset=len(perfect_rows))
        assert report.ok
        assert report.offset_gap == 0

    def test_examples_are_capped_but_counts_are_exact(self):
        big = generate(GeneratorConfig(keys=4, events_per_key=100, seed=3))
        rows = [event.as_dict() for event in big.events][:100]
        report = verify(big.manifest, rows)

        assert report.missing_total == 300
        assert len(report.missing_examples) == 50
        assert report.truncated
        assert report.loss_rate == pytest.approx(0.75)


class TestBaseline:
    def test_at_least_once_duplicates_on_crash(self, stream):
        run = run_baseline(
            stream.events, mode=AT_LEAST_ONCE, batch_size=10, crash_after_batches=3
        )
        report = verify(stream.manifest, run.rows, committed_offset=run.committed_offset)

        assert run.crashed
        assert report.duplicate_rows == 10
        assert report.missing_total == 0
        assert not report.ok

    def test_at_most_once_loses_on_crash(self, stream):
        run = run_baseline(
            stream.events, mode=AT_MOST_ONCE, batch_size=10, crash_after_batches=3
        )
        report = verify(stream.manifest, run.rows, committed_offset=run.committed_offset)

        assert run.crashed
        assert report.missing_total == 10
        assert report.duplicate_rows == 0
        assert not report.ok

    def test_no_crash_is_clean_in_both_modes(self, stream):
        for mode in (AT_LEAST_ONCE, AT_MOST_ONCE):
            run = run_baseline(stream.events, mode=mode, batch_size=10)
            report = verify(stream.manifest, run.rows, committed_offset=run.committed_offset)
            assert report.ok, "{0}: {1}".format(mode, report.summary())

    def test_rejects_unknown_mode(self, stream):
        with pytest.raises(ValueError):
            run_baseline(stream.events, mode="exactly_once_by_wishing")


class TestChaos:
    def setup_method(self):
        chaos.reset()

    def test_disarmed_by_default(self, monkeypatch):
        monkeypatch.delenv(chaos.ENV_VAR, raising=False)
        assert not chaos.armed()
        chaos.maybe_crash(CrashPoint.AFTER_WRITE)  # must not raise or exit

    def test_parses_plan(self):
        assert chaos.parse_plan("after_write:3,before_commit") == {
            "after_write": 3,
            "before_commit": 1,
        }

    def test_rejects_unknown_point(self):
        with pytest.raises(ValueError):
            chaos.parse_plan("after_lunch:1")

    def test_crashes_on_the_configured_occurrence(self, monkeypatch):
        exits = []
        monkeypatch.setenv(chaos.ENV_VAR, "after_write:3")
        monkeypatch.setattr(chaos, "_exit_fn", lambda code: exits.append(code))

        for _ in range(5):
            chaos.maybe_crash(CrashPoint.AFTER_WRITE)

        assert exits == [chaos.CRASH_EXIT_CODE]

    def test_ignores_other_points(self, monkeypatch):
        exits = []
        monkeypatch.setenv(chaos.ENV_VAR, "after_write:1")
        monkeypatch.setattr(chaos, "_exit_fn", lambda code: exits.append(code))

        chaos.maybe_crash(CrashPoint.BEFORE_COMMIT)
        assert exits == []
