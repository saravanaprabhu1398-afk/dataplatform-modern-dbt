"""Streaming correctness harness.

Week 0 of the exactly-once work: a deterministic generator, an oracle that
verifies a sink against it, naive baseline consumers to measure against, and
crash-injection hooks for later phases.

    from dataplatform.streaming import GeneratorConfig, generate, verify

    stream = generate(GeneratorConfig(seed=7))
    report = verify(stream.manifest, [e.as_dict() for e in stream.events])
    assert report.ok
"""
from dataplatform.streaming.baseline import (
    AT_LEAST_ONCE,
    AT_MOST_ONCE,
    BaselineRun,
    run_baseline,
)
from dataplatform.streaming.generator import (
    GeneratedStream,
    GeneratorConfig,
    Manifest,
    generate,
)
from dataplatform.streaming.model import (
    StreamEvent,
    compute_checksum,
    read_jsonl,
    window_start,
    write_jsonl,
)
from dataplatform.streaming.verifier import (
    VerificationReport,
    WindowMismatch,
    verify,
)

__all__ = [
    "AT_LEAST_ONCE",
    "AT_MOST_ONCE",
    "BaselineRun",
    "GeneratedStream",
    "GeneratorConfig",
    "Manifest",
    "StreamEvent",
    "VerificationReport",
    "WindowMismatch",
    "compute_checksum",
    "generate",
    "read_jsonl",
    "run_baseline",
    "verify",
    "window_start",
    "write_jsonl",
]
