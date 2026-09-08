from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple


class Plugin(ABC):
    """Base class for all plugins."""

    @abstractmethod
    def execute(self, config: Dict[str, Any]) -> Tuple[bool, Any]:
        """Execute the plugin with given config.

        Returns a (success, data) tuple. success is True on success,
        False on failure. data is plugin-specific output (dict, list, etc.)
        or None.
        """
        pass


class ExecutorPlugin(Plugin):
    """Base class for executor plugins."""
    pass


class TransformerPlugin(Plugin):
    """Base class for transformer plugins."""
    pass

# ---------------------------------------------------------------------------
# Streaming contracts
# ---------------------------------------------------------------------------
#
# ``Plugin.execute()`` is a batch contract: config in, (success, data) out, no
# notion of where a previous run stopped.  Streaming needs a place to put a
# checkpoint, so it gets its own pair of contracts alongside -- additive, so
# every existing plugin keeps working untouched.
#
# The division of labour is the whole design:
#
#   StreamingSource      knows how to resume from an offset
#   TransactionalSink    writes records AND offsets in ONE transaction
#
# Exactly-once is not a flag either of them sets.  It is the property that
# falls out of the sink's commit being atomic and fenced.

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Fence:
    """Proof of the right to write.

    ``attempt`` is the monotonic claim counter from the run queue.  A sink
    rejects a commit whose attempt is older than the one already recorded for
    the stream: that writer was replaced while it was away.
    """

    stream: str
    attempt: int


@dataclass
class Offsets:
    """Per-partition position: the offset of the *next* record to read."""

    positions: Dict[str, int] = field(default_factory=dict)

    def next_for(self, partition: str) -> int:
        return self.positions.get(partition, 0)

    def advance(self, partition: str, next_offset: int) -> None:
        self.positions[partition] = next_offset

    def total(self) -> int:
        """Records consumed across all partitions."""
        return sum(self.positions.values())

    def copy(self) -> "Offsets":
        return Offsets(positions=dict(self.positions))

    def __bool__(self) -> bool:
        return bool(self.positions)


class StreamingSource(Plugin):
    """A source that can be resumed from a set of offsets."""

    def execute(self, config: Dict[str, Any]) -> Tuple[bool, Any]:
        """Streaming sources are driven by the runner, not by execute()."""
        raise NotImplementedError("streaming sources are consumed via poll()")

    def seek(self, offsets: Offsets) -> None:
        """Position the source at *offsets* before the next poll."""
        raise NotImplementedError

    def poll(self, max_records: int) -> Tuple[List[Dict[str, Any]], Offsets]:
        """Return up to *max_records* records and the offsets that follow them.

        The returned offsets must not be committed by the source.  Committing is
        the sink's job, and only as part of the same transaction as the data.
        """
        raise NotImplementedError


class TransactionalSink(Plugin):
    """A sink that can persist records and offsets atomically."""

    def execute(self, config: Dict[str, Any]) -> Tuple[bool, Any]:
        """Transactional sinks are driven by the runner, not by execute()."""
        raise NotImplementedError("transactional sinks are written via commit()")

    def read_offsets(self, stream: str) -> Offsets:
        """Return the offsets last committed for *stream*."""
        raise NotImplementedError

    def committed_attempt(self, stream: str) -> int:
        """Highest fencing token this sink has recorded for *stream*.

        Zero means "nothing committed yet, so nobody is fenced".  The runner
        uses it to refuse work at startup rather than discovering at the first
        commit that it was replaced.
        """
        return 0

    def commit(
        self, records: List[Dict[str, Any]], offsets: Offsets, fence: Fence
    ) -> int:
        """Write *records* and *offsets* in one transaction.

        Must raise :class:`StaleFence` rather than write, if *fence* is older
        than the attempt already recorded for the stream.  Returns the number of
        records written.
        """
        raise NotImplementedError


class StaleFence(RuntimeError):
    """Raised when a writer's fencing token has been superseded."""
