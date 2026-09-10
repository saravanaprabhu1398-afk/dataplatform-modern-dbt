# Streaming correctness

This subsystem processes a stream into a database with an exactly-once
guarantee under process death, and windows the result by event time rather
than arrival time.

Every number below was measured by running the code, not estimated. The
scripts that produce them are in `demo/scripts/` and are listed at the end.

---

## The guarantee

> Records and the offsets that produced them are written in **one database
> transaction**, under a fencing token. A crash before that transaction commits
> leaves nothing behind, so the next poll replays the same records. A crash
> after it commits leaves the offsets advanced, so the next poll moves on.
> There is no window in which one landed without the other.

That is the whole design. Exactly-once is not a setting; it is a property of
where the commit boundary is drawn.

```
at-least-once  =  commit data, then commit offsets   (crash between → duplicates)
at-most-once   =  commit offsets, then commit data   (crash between → data loss)
exactly-once   =  commit data AND offsets in ONE transaction, under a valid fence
```

Measured against the naive alternatives, crashed at the same point in the same
stream (`demo/scripts/failure_matrix.py`, `dataplatform stream baseline`):

| consumer | duplicates | loss |
|---|---|---|
| at-least-once | 5.00% | 0 |
| at-most-once | 0 | 5.00% |
| this pipeline | 0 | 0 |

## What is **not** guaranteed

Stating this precisely is part of the design, not a disclaimer.

- **No exactly-once across arbitrary sinks.** The guarantee comes from the data
  and the offsets sharing a transaction. Against a sink that cannot join that
  transaction, the fallback is idempotent writes keyed on `(key, seq)` —
  effectively-once through deduplication, which does not protect an aggregate.
- **No per-producer clock-skew correction.** Event time is trusted as given. A
  producer whose clock runs fast drags the watermark forward and makes its
  peers' records look late. Measured at 5 minutes of skew across two of eight
  keys: 183 records reclassified as late, and no window matched its expected
  total.
- **No cross-partition ordering.** Order is per key, within a partition.
- **No aggregate correctness for partially-overlapping replays.** Window deltas
  are additive, so the sink skips them on a commit that advances no offset. The
  runner always replays whole batches from committed offsets; a source that
  replayed a partial overlap would double-count.
- **No consensus.** Leases live in PostgreSQL rows. Correctness depends on the
  database being the single source of truth for who holds a run.

---

## Shape

```
JsonlSource ──poll(offsets)──▶ runner ──commit(records, offsets, fence)──▶ SqlTransactionalSink
     ▲                            │                                            │
     └──── seek(committed) ───────┘                                    ONE transaction:
                                                                       records
                                                                       offsets
                                                                       window aggregates
                                                                       corrections
                                                                       side output
```

- **Source** (`streaming/sources.py`) can only *resume*; it cannot commit. If a
  source could advance its own offsets, the commit boundary would span two
  systems and the guarantee would be gone.
- **Sink** (`streaming/sinks.py`) owns the offset store, so that both can be
  written together, and refuses a commit whose fencing token has been
  superseded.
- **Runner** (`streaming/runner.py`) takes offsets from the sink, advances the
  source only after a commit succeeds, refuses to start when already
  superseded, and waits out transient sink outages with bounded backoff.
- **Queue leases** (`core/database.py`, `core/queue_worker.py`) decide who is
  allowed to be that writer. Liveness comes from lease renewal, not from
  wall-clock age: a long job is not a dead job.

## Event time

Three separate ideas, deliberately not conflated:

| knob | means |
|---|---|
| `out_of_orderness` | how far the watermark trails the highest event time seen |
| `allowed_lateness` | how long a window keeps accepting updates after it fires |
| `idle_partition_timeout` | when a silent partition stops holding the watermark back |

A window fires when the watermark passes its end. An arrival after that but
within `allowed_lateness` updates the window and emits a **correction**
carrying the delta, bumping the window's revision so a restatement is visible.
An arrival beyond it goes to the **side output** with its lateness and the
watermark that rejected it.

There is deliberately no `drop` policy. Silently losing records is the failure
this subsystem exists to detect.

Two rules that are easy to get wrong and are pinned by tests:

- **Lateness is judged against the watermark at the start of the batch.** A
  record must not be late because of a watermark its own batch advanced, or
  batch size would change the answer.
- **A window fires when the watermark passes it, whether or not the current
  batch carried a record for it.** A batch of hour-9 traffic closes hour 3.

### Event time versus arrival time

Same 12,000 records, 1,443 arriving out of order:

| | windows correct | records misplaced | records recorded as late |
|---|---|---|---|
| event time | 12 / 12 | 0 | 803 corrections, 58 side output |
| processing time | 0 / 12 | 530, plus 30 phantom buckets | none — it reports nothing |

The phantom buckets are the 48-hour stragglers: an arrival-time pipeline files
them under the day they turned up.

---

## Failure matrix

Every row runs for real — processes killed with `SIGKILL` semantics, leases
allowed to expire, the database taken away mid-run — and the Week 0 oracle
decides whether the result is acceptable.

| injection | simulates | expected | measured |
|---|---|---|---|
| SIGKILL mid-batch | worker dies after write, before commit | 0 dup, 0 loss | exit 137, 0 dup, 0 missing |
| Lease expiry / zombie | stalled worker wakes after being replaced | `StaleFence`, nothing written | fenced, rows 2400 → 2400 |
| Duplicate delivery | broker resends an acknowledged batch | 0 dup, aggregates unchanged | 200 rows, window total unchanged |
| Out-of-order < 6h | arrival after its window fired, inside lateness | window restated, correction emitted | 169 corrections over 12 windows |
| Out-of-order 48h | arrival past allowed lateness | side output, counted, never dropped | 22 set aside; 2378 + 22 = 2400 |
| Idle partition | one partition stops producing | watermark advances anyway | 00:55 → 08:55 |
| Sink unavailable | database away for the first 3 commits | backpressure, no loss | 3 retries, recovered in 1.6s, 0 missing |
| Clock skew 5m | one producer's clock runs fast | **known limitation** | 183 late rows, 0/12 windows match |

Seven handled; one recorded as a limitation. A matrix with no limitations in it
is a matrix that was not run.

## Throughput

12,000 records, 4 partitions, SQLite on local disk, Python 3.14, darwin. Every
run was verified to have committed all 12,000 records.

| batch | windowing | records/s | p50 commit | p99 commit |
|---|---|---|---|---|
| 50 | off | 29,599 | 1.64 ms | 2.77 ms |
| 250 | off | 35,085 | 6.96 ms | 10.90 ms |
| 1000 | off | 36,494 | 26.92 ms | 29.07 ms |
| 50 | on | 19,247 | 2.50 ms | 5.05 ms |
| 250 | on | 23,300 | 10.59 ms | 17.47 ms |
| 1000 | on | 24,269 | 41.33 ms | 47.32 ms |

The event-time aggregate path costs roughly a third of throughput. That is the
price of corrections and a side output, and it is worth knowing rather than
assuming.

**End-to-end lag is not reported.** The harness replays a file whose ingest
timestamps are stream time, not wall-clock arrival, so any end-to-end lag
computed here would describe the generator rather than the pipeline.

## Claim contention

The queue claim was safe before this work — a conditional `UPDATE` made a
double-claim impossible — but it was not *live*: every worker selected the same
head-of-queue row and the losers went back to sleep with work still queued.
12 workers released simultaneously against 12 queued runs, on PostgreSQL:

| claim | work picked up | idle workers | left queued |
|---|---|---|---|
| `SELECT` then conditional `UPDATE` | 3–5 of 12 | 7–9 | 7–9 |
| `FOR UPDATE SKIP LOCKED` | 12 of 12 | 0 | 0 |

---

## Reproducing all of it

```bash
dataplatform stream generate --out data/stream --keys 50 --per-key 200 --seed 7
dataplatform stream baseline --stream data/stream        # the before column
dataplatform stream run --stream data/stream --table events_sink
dataplatform stream verify --stream data/stream --table events_sink

python demo/scripts/failure_matrix.py                    # the matrix above
python demo/scripts/throughput.py                        # the throughput table
python demo/scripts/event_time_vs_processing_time.py     # event vs arrival time
python demo/scripts/lease_failover.py                    # lease handover and fencing
python demo/scripts/claim_contention.py                  # needs PostgreSQL
```

PostgreSQL-only paths (`FOR UPDATE SKIP LOCKED`, the row-locked fence) have
tests that skip unless a database is provided:

```bash
docker run -d --rm -e POSTGRES_PASSWORD=dp -e POSTGRES_USER=dp \
    -e POSTGRES_DB=dp -p 55432:5432 postgres:16-alpine

DATAPLATFORM_TEST_POSTGRES_URL=postgresql+psycopg2://dp:dp@localhost:55432/dp \
    pytest tests/test_queue_leases_postgres.py
```

Crash points can be injected anywhere in the loop:

```bash
DATAPLATFORM_CHAOS="after_write:3" python -m dataplatform.streaming.runner \
    --stream data/stream --table events_sink
```

Points are `before_poll`, `after_poll`, `after_write`, `before_commit`,
`after_commit`, `before_heartbeat`. The exit is `os._exit(137)` — no `finally`
blocks, no flushes — because a clean shutdown proves nothing.
