"""The oracle: prove what a sink actually contains.

The verifier never asks the pipeline what it did.  It compares the rows in a
sink against the manifest produced by the generator and reports six independent
failure modes:

===================  ====================================================
duplicates           an identity present more than once
missing              an identity the manifest expects and the sink lacks
corrupted            an identity whose checksum no longer matches
unexpected           a row the manifest never described
window mismatches    event-time aggregates that disagree with a recompute
offset gap           committed offsets that disagree with the row count
===================  ====================================================

Duplicates and missing rows are the exactly-once claim.  Window mismatches are
the event-time claim.  Keeping them separate matters: a pipeline can be free of
duplicates and still window on the wrong clock.

    report = verify(manifest, rows)
    print(report.summary())
    assert report.ok

Counts are always exact.  Only the *example* lists are truncated, so a badly
broken run still reports a usable summary instead of a million-line list.
"""
from __future__ import annotations

import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from dataplatform.streaming.generator import Manifest
from dataplatform.streaming.model import StreamEvent, compute_checksum, window_start

logger = logging.getLogger(__name__)

#: How many offending identities are retained per category for display.
MAX_EXAMPLES = 50


@dataclass
class WindowMismatch:
    """One event-time window whose aggregate disagrees with the manifest."""

    window_start: str
    expected_count: int
    actual_count: int
    expected_sum_cents: int
    actual_sum_cents: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "window_start": self.window_start,
            "expected_count": self.expected_count,
            "actual_count": self.actual_count,
            "expected_sum_cents": self.expected_sum_cents,
            "actual_sum_cents": self.actual_sum_cents,
        }


@dataclass
class VerificationReport:
    """Result of comparing a sink against the manifest.

    ``*_total`` fields are exact.  ``*_examples`` lists are capped at
    :data:`MAX_EXAMPLES` entries and exist only for reading.
    """

    expected_unique: int = 0
    observed_rows: int = 0
    unique_observed: int = 0

    duplicate_rows: int = 0          # extra copies, not distinct identities
    duplicate_total: int = 0         # distinct identities seen more than once
    missing_total: int = 0
    corrupted_total: int = 0
    unexpected_total: int = 0

    duplicate_examples: List[str] = field(default_factory=list)
    missing_examples: List[str] = field(default_factory=list)
    corrupted_examples: List[str] = field(default_factory=list)
    unexpected_examples: List[str] = field(default_factory=list)

    window_mismatches: List[WindowMismatch] = field(default_factory=list)
    offset_gap: Optional[int] = None

    @property
    def truncated(self) -> bool:
        return (
            self.duplicate_total > len(self.duplicate_examples)
            or self.missing_total > len(self.missing_examples)
            or self.corrupted_total > len(self.corrupted_examples)
            or self.unexpected_total > len(self.unexpected_examples)
        )

    @property
    def duplicate_rate(self) -> float:
        """Extra rows as a fraction of the rows that should exist."""
        if not self.expected_unique:
            return 0.0
        return self.duplicate_rows / float(self.expected_unique)

    @property
    def loss_rate(self) -> float:
        if not self.expected_unique:
            return 0.0
        return self.missing_total / float(self.expected_unique)

    @property
    def ok(self) -> bool:
        return (
            self.duplicate_total == 0
            and self.missing_total == 0
            and self.corrupted_total == 0
            and self.unexpected_total == 0
            and not self.window_mismatches
            and not self.offset_gap
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "expected_unique": self.expected_unique,
            "observed_rows": self.observed_rows,
            "unique_observed": self.unique_observed,
            "duplicate_rows": self.duplicate_rows,
            "duplicate_identities": self.duplicate_total,
            "duplicate_rate": round(self.duplicate_rate, 6),
            "missing": self.missing_total,
            "loss_rate": round(self.loss_rate, 6),
            "corrupted": self.corrupted_total,
            "unexpected": self.unexpected_total,
            "window_mismatches": len(self.window_mismatches),
            "offset_gap": self.offset_gap,
            "examples": {
                "duplicates": self.duplicate_examples[:10],
                "missing": self.missing_examples[:10],
                "corrupted": self.corrupted_examples[:10],
                "unexpected": self.unexpected_examples[:10],
                "windows": [w.as_dict() for w in self.window_mismatches[:5]],
            },
        }

    def summary(self) -> str:
        """A short report suitable for a terminal or a CI log."""
        lines = [
            "streaming verification: {0}".format("PASS" if self.ok else "FAIL"),
            "  expected unique   {0}".format(self.expected_unique),
            "  observed rows     {0} ({1} unique)".format(
                self.observed_rows, self.unique_observed
            ),
            "  duplicate rows    {0}  (rate {1:.4%})".format(
                self.duplicate_rows, self.duplicate_rate
            ),
            "  missing rows      {0}  (rate {1:.4%})".format(
                self.missing_total, self.loss_rate
            ),
            "  corrupted rows    {0}".format(self.corrupted_total),
            "  unexpected rows   {0}".format(self.unexpected_total),
            "  window mismatches {0}".format(len(self.window_mismatches)),
        ]
        if self.offset_gap is not None:
            lines.append("  offset gap        {0}".format(self.offset_gap))
        for mismatch in self.window_mismatches[:5]:
            lines.append(
                "    window {0}: count {1}->{2}, sum {3}->{4}".format(
                    mismatch.window_start,
                    mismatch.expected_count,
                    mismatch.actual_count,
                    mismatch.expected_sum_cents,
                    mismatch.actual_sum_cents,
                )
            )
        if self.truncated:
            lines.append("  (examples capped at {0} per category)".format(MAX_EXAMPLES))
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def verify(
    manifest: Manifest,
    rows: Iterable[Dict[str, Any]],
    committed_offset: Optional[int] = None,
) -> VerificationReport:
    """Compare sink *rows* against *manifest* and return a report.

    ``rows`` is any iterable of dicts carrying at least ``key``, ``seq``,
    ``event_time``, ``amount_cents``, ``payload`` and ``checksum`` -- JSONL
    lines, DuckDB rows, or a list built in a test.

    ``committed_offset``, when given, is the pipeline's own record of how many
    unique records it consumed.  A disagreement with the sink means the offset
    store and the data have drifted apart -- the failure that survives every
    other check, because both sides look internally consistent.
    """
    report = VerificationReport(expected_unique=manifest.total_unique)

    seen: Counter = Counter()
    corrupted = set()
    unexpected = set()
    window_actual: Dict[str, Dict[str, int]] = defaultdict(
        lambda: {"count": 0, "sum_amount_cents": 0}
    )

    for raw in rows:
        report.observed_rows += 1
        try:
            event = StreamEvent.from_dict(raw)
        except (KeyError, TypeError, ValueError) as exc:
            unexpected.add("unparseable row: {0}".format(exc))
            continue

        ident = event.identity_str
        seen[ident] += 1

        expected_checksum = manifest.checksums.get(ident)
        if expected_checksum is None:
            unexpected.add(ident)
        else:
            # Recompute from the row's own fields, then compare against the
            # manifest.  Checking both catches a mutated payload whose checksum
            # column was rewritten to match it.
            recomputed = compute_checksum(
                event.key,
                event.seq,
                event.event_time,
                event.amount_cents,
                event.payload,
            )
            if recomputed != expected_checksum or event.checksum != expected_checksum:
                corrupted.add(ident)

        bucket = window_actual[window_start(event.event_time, manifest.window_seconds)]
        bucket["count"] += 1
        bucket["sum_amount_cents"] += event.amount_cents

    duplicates = sorted(ident for ident, count in seen.items() if count > 1)
    missing = sorted(ident for ident in manifest.checksums if ident not in seen)

    report.unique_observed = len(seen)
    report.duplicate_rows = sum(count - 1 for count in seen.values() if count > 1)
    report.duplicate_total = len(duplicates)
    report.missing_total = len(missing)
    report.corrupted_total = len(corrupted)
    report.unexpected_total = len(unexpected)

    report.duplicate_examples = duplicates[:MAX_EXAMPLES]
    report.missing_examples = missing[:MAX_EXAMPLES]
    report.corrupted_examples = sorted(corrupted)[:MAX_EXAMPLES]
    report.unexpected_examples = sorted(unexpected)[:MAX_EXAMPLES]

    report.window_mismatches = _compare_windows(manifest.windows, window_actual)

    if committed_offset is not None:
        report.offset_gap = committed_offset - report.unique_observed

    logger.debug("verification report: %s", report.as_dict())
    return report


def _compare_windows(
    expected: Dict[str, Dict[str, int]], actual: Dict[str, Dict[str, int]]
) -> List[WindowMismatch]:
    """Diff expected against actual event-time window aggregates."""
    mismatches: List[WindowMismatch] = []
    empty = {"count": 0, "sum_amount_cents": 0}

    for window in sorted(set(expected) | set(actual)):
        want = expected.get(window, empty)
        got = actual.get(window, empty)
        if (
            want["count"] != got["count"]
            or want["sum_amount_cents"] != got["sum_amount_cents"]
        ):
            mismatches.append(
                WindowMismatch(
                    window_start=window,
                    expected_count=want["count"],
                    actual_count=got["count"],
                    expected_sum_cents=want["sum_amount_cents"],
                    actual_sum_cents=got["sum_amount_cents"],
                )
            )
    return mismatches
