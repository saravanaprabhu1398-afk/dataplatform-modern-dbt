"""Replayable streaming sources.

A source's only job is to be resumable: hand it the offsets a sink committed
and it must produce exactly the records that follow them.  It deliberately
cannot commit anything -- if a source could advance its own offsets, the commit
boundary would be split across two systems and exactly-once would be gone.

:class:`JsonlSource` replays the harness stream from Week 0.  It shards records
across partitions by key, so per-partition offsets, uneven partition progress
and (in Week 3) per-partition watermarks all behave like a real broker's
without needing one.
"""
from __future__ import annotations

import logging
import zlib
from typing import Any, Dict, List, Optional, Tuple

from dataplatform.plugins.base import Offsets, StreamingSource
from dataplatform.streaming.model import read_jsonl

logger = logging.getLogger(__name__)

PARTITION_KEY = "key"


def partition_for(key: str, partitions: int) -> str:
    """Assign a key to a partition, stably across processes.

    ``hash()`` is salted per process in Python 3, so it cannot be used here:
    two workers would disagree about which partition a key belongs to.
    """
    return str(zlib.crc32(key.encode("utf-8")) % partitions)


class JsonlSource(StreamingSource):
    """Replay a JSONL stream as a partitioned, resumable source."""

    def __init__(self, path: str, partitions: int = 4) -> None:
        if partitions <= 0:
            raise ValueError("partitions must be positive")
        self.path = path
        self.partitions = partitions
        self._by_partition: Dict[str, List[Dict[str, Any]]] = {
            str(index): [] for index in range(partitions)
        }
        self._offsets = Offsets()
        self._load()

    def _load(self) -> None:
        for row in read_jsonl(self.path):
            partition = partition_for(str(row[PARTITION_KEY]), self.partitions)
            self._by_partition[partition].append(row)
        logger.debug(
            "loaded %s into %d partitions: %s",
            self.path,
            self.partitions,
            {p: len(rows) for p, rows in self._by_partition.items()},
        )

    @property
    def total_records(self) -> int:
        return sum(len(rows) for rows in self._by_partition.values())

    def seek(self, offsets: Offsets) -> None:
        """Resume from *offsets*; unknown partitions start at zero."""
        self._offsets = offsets.copy()

    def poll(self, max_records: int) -> Tuple[List[Dict[str, Any]], Offsets]:
        """Take up to *max_records* records, round-robin across partitions.

        Returns the records and the offsets that *would* follow them.  Nothing
        is advanced here -- the caller's sink decides whether those offsets ever
        become durable.
        """
        if max_records <= 0:
            raise ValueError("max_records must be positive")

        batch: List[Dict[str, Any]] = []
        next_offsets = self._offsets.copy()

        exhausted = set()
        while len(batch) < max_records and len(exhausted) < self.partitions:
            for partition, rows in self._by_partition.items():
                if len(batch) >= max_records:
                    break
                position = next_offsets.next_for(partition)
                if position >= len(rows):
                    exhausted.add(partition)
                    continue
                batch.append(rows[position])
                next_offsets.advance(partition, position + 1)

        return batch, next_offsets

    def commit_local(self, offsets: Offsets) -> None:
        """Advance the source's own cursor after a sink committed *offsets*."""
        self._offsets = offsets.copy()
