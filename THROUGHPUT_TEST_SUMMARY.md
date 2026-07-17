# Zero Throughput Test Summary

We now have throughput coverage at three levels:

| Test                                                                                        | What it measures                                                                |
| ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| [E2E throughput harness](apps/zero-throughput/README.md)                                    | PostgreSQL → replicator → ViewSyncer/IVM → synthetic Zero clients               |
| [Relational IVM benchmark](packages/zql-benchmarks/src/zero-throughput-relational.bench.ts) | The 300-query relational workload with networking, CVR, and replication removed |
| [Initial-sync benchmark](packages/zero-cache/src/db/initial-sync.bench.pg.ts)               | PostgreSQL COPY into a SQLite replica across realistic row/payload shapes       |

The E2E harness supports:

- Four application profiles: feed, email, forum, and relationship-heavy relational.
- “Hot” worst-case workloads, where every write impacts every active query.
- “Realistic” partitioned workloads, where only 20–30% of writes affect an active client group.
- Fixed-rate runs and binary-search parameter sweeps.
- Overload/recovery tests.
- Migration tests varying transaction size, transaction rate, concurrency, and durability.

## What We Have Learned

### 1. Fanout and Query Complexity Dominate Steady-State Throughput

The initial hot-workload sweep used three queries per user and 50-row windows. Sustainable rates under the 2-second p99 SLO were approximately:

| Profile    | Users |      1 sync worker | 4 sync workers |
| ---------- | ----: | -----------------: | -------------: |
| Relational |    50 |        12 writes/s |    38 writes/s |
| Email      |    50 |        31 writes/s |    47 writes/s |
| Forum      |    50 |        18 writes/s |    47 writes/s |
| Relational |   200 |         3 writes/s |     6 writes/s |
| Email      |   200 |         9 writes/s |    12 writes/s |
| Forum      |   200 | Below search floor |     9 writes/s |

These results are available in the [hot sweep summary](apps/zero-throughput/results/sweeps/read-heavy-fast-2026-06-30/summary.csv).

The important interpretation is that these are deliberately pathological writes: each logical write touches every active query set. They measure worst-case IVM fanout, not ordinary database write volume. More sync workers help substantially, especially at 50 users, but the improvement is workload-dependent and not linear.

### 2. Smarter Load Shedding Prevents Runaway Incremental Work

The tests found cases where incrementally advancing a standing query was more expensive than discarding and rehydrating it. That led to the projected-cost load-shedding logic now on `main`: advancement is reset when its projected cost materially exceeds hydration, including pathological single-change pushes.

At 100 relational users, 25 hot writes/s, and one worker:

- Baseline p99 lag: 13.7s; lag was growing at +3.09 seq/s.
- With load shedding: p99 lag: 9.7s; lag slope became -0.31 seq/s.

It still missed the 2-second SLO, but the backlog stopped growing. In the realistic fixed-rate comparison, forum p99 improved from about 1.95s to 590ms at 200 writes/s, while simpler profiles were roughly flat. This suggests the benefit is concentrated in expensive or skewed query shapes.

### 3. Resetting Pipelines Was Not Enough; Rehydration Timing Matters

The recovery benchmark exposed reset/rehydrate thrashing during sustained write bursts. Immediately rebuilding pipelines after a reset could cause them to be overwhelmed and reset again.

The experimental branch now waits for a 50ms quiet interval before rehydrating, with a 5-second maximum wait to preserve progress under continuous traffic. Across three relational overload repetitions:

- Forced rehydrations fell from 25–50 per run to zero.
- First catch-up improved from roughly 2.1–2.9s to 1.2–1.6s.
- Stable recovery improved from roughly 3.1–3.9s to 2.2–2.6s.

This is the main reason the overload/recovery benchmark is valuable: steady-state p99 alone did not show the reset storm.

### 4. Transaction Shape Is a Separate Throughput Dimension

The migration benchmark showed very different behavior for one large transaction versus many tiny transactions:

- One 30,000-row transaction recovered stably in about 2.7s.
- Thirty 1,000-row transactions recovered in about 1.4–1.5s.
- Thirty thousand 3-row transactions exposed per-transaction commit overhead in both PostgreSQL ingress and the SQLite replica.

For durable 3-row transactions, producer throughput scaled from roughly 5.8k rows/s with one writer to 53.6k rows/s with 64 writers. Disabling synchronous commit raised the observed upper bound to about 71.8k rows/s, but that is intentionally a non-durable ceiling.

These figures describe producer/commit throughput; Zero’s performance is captured separately by the post-write recovery time.

### 5. Coalescing Replica Transactions Appears to Be a Major Catch-Up Win

The current experimental branch groups queued logical upstream transactions into fewer physical SQLite transactions while preserving logical watermarks and change-log entries.

For a 90,000-row migration made of 30,000 three-row transactions at concurrency 64:

- Before coalescing: stable recovery was about 9.2s, with 100 pipeline resets and 50 forced rehydrations.
- Coalesced runs: stable recovery was 2.8–2.9s, with 50 resets and no forced rehydrations.

That is roughly a 68% reduction in catch-up time. Correctness tests cover successful batching and atomic rollback in [incremental-sync.test.ts](packages/zero-cache/src/services/replicator/incremental-sync.test.ts) and [write-worker.test.ts](packages/zero-cache/src/services/replicator/write-worker.test.ts).

### 6. The 50k Rows/s Ceiling Is Not IVM or Raw SQLite

Follow-up diagnostics on the realistic migration shape (90,000 rows, 30,000
three-row transactions, 64 writers, one client, and load shedding enabled)
separated two ceilings that had previously looked like one:

| Stage or control                                           |                   Measured throughput |
| ---------------------------------------------------------- | ------------------------------------: |
| Durable PostgreSQL producer                                |                    50.9k–54.6k rows/s |
| Non-durable PostgreSQL producer (`synchronous_commit=off`) |                          77.3k rows/s |
| SQLite apply + change log, committing every 3 rows         |                          64.9k rows/s |
| SQLite apply + change log, folding 768 rows per commit     |                           164k rows/s |
| SQLite apply + change log, folding 3,072 rows per commit   |                           207k rows/s |
| `ChangeProcessor` in-process, fully coalesced              |                          94.9k rows/s |
| Write worker, fully coalesced                              |                          84.5k rows/s |
| Write worker during the durable E2E run                    | 44.5k rows/s of worker-call wall time |
| Write worker during the non-durable E2E run                | 48.8k rows/s of worker-call wall time |

The harness's `achievedWriteRate` is the PostgreSQL producer's commit rate, not
the replica's apply rate. Disabling durable commits raised that number from
54.6k to 77.3k rows/s, while Zero's write-worker path remained near 49k rows/s.
This confirms that durable PostgreSQL commits cap the producer in the original
run, but a separate Zero replication bottleneck becomes visible when the
producer is made faster.

#### Larger PostgreSQL transactions change the ceiling

The same durable migration with 1,000 rows per PostgreSQL transaction (900,000
rows total, 64 writers) committed at **286.8k rows/s**—more than five times the
3-row-transaction rate. Its PostgreSQL transaction latency was p50 205ms and
p99 500ms, which is acceptable for a bulk migration but not a replacement for
low-latency transactional writes.

That run made the downstream limit unambiguous: after the writer stopped, Zero
needed 24.4s to catch up 825,000 rows. The write worker spent 10.4s in calls
(86.7k rows/s by worker-call wall time), while the full replication span was
only 27.1k rows/s. Each 1,000-row logical transaction still becomes roughly
1,002 individual change messages and WebSocket frames, so larger PostgreSQL
transactions remove commit overhead without removing the per-row transport
cost. The replication path—not PostgreSQL—is the capacity limit for this bulk
shape.

The strongest evidence points to undersized replication batches plus the
per-message transport/worker path, rather than SQLite itself:

- Coalescing allows up to 256 logical transactions, but instrumented E2E runs
  averaged only 13.4–13.7 transactions per worker call (p50 12–13, p95 29,
  maximum 56).
- The replicator flushes whenever the downstream WebSocket queue is
  momentarily empty. That condition is at
  [incremental-sync.ts](packages/zero-cache/src/services/replicator/incremental-sync.ts#L304),
  and explains why normal streaming traffic rarely reaches the configured
  coalescing limits. Follow-up experiments below show that the gaps are real
  transport/arrival gaps, however, rather than only event-loop scheduling.
- With the migration schema, increasing the worker batch from one logical
  transaction to 4, 16, 64, and 256 raised isolated throughput from 31.5k to
  52.8k, 72.5k, 82.6k, and 85.6k rows/s respectively.
- JSON parsing plus downstream schema validation alone sustained roughly 593k
  messages/s, so parsing is not independently responsible for the ceiling.
- The stream sends each change as an individual WebSocket frame and acknowledges
  each consumed message individually in
  [streams.ts](packages/zero-cache/src/types/streams.ts#L238). Along with
  subscription bookkeeping, structured cloning into the worker, and local CPU
  contention, this accounts for the remaining gap between the isolated worker
  ceiling and E2E performance. The current measurements do not yet assign that
  residual cost to one of those sub-stages.

#### Collection-window experiment

Waiting before an empty-queue flush successfully made worker batches larger,
but did not improve E2E recovery:

| Flush policy                    | Average tx/batch | p50 tx/batch | Worker-call throughput | First catch-up |
| ------------------------------- | ---------------: | -----------: | ---------------------: | -------------: |
| Immediate flush baseline        |             13.7 |           13 |           44.5k rows/s |           1.8s |
| Repeated 2ms idle wait, cap 256 |            243.9 |          256 |           62.2k rows/s |           3.3s |
| Fixed 2ms window, cap 64        |             41.6 |           39 |           55.4k rows/s |           2.3s |
| Fixed 1ms window, cap 64        |             31.8 |           30 |           49.6k rows/s |           2.0s |
| One event-loop yield, cap 64    |             13.8 |           12 |           45.7k rows/s |           1.7s |

The 2ms variants amortized worker and SQLite costs, but the collection delay
more than consumed those savings. A single event-loop yield did not enlarge
batches at all, confirming that the receive queue is empty because messages
have not arrived yet. These are single local runs and catch-up has some
run-to-run variance, but there is no evidence that delaying the consumer is a
net optimization. The experimental source change was therefore reverted.

The next experiment should batch the transport itself: send several change
messages or a complete logical transaction per WebSocket frame and use
cumulative/range acknowledgements. Add timings around change-streamer send,
receive/parse, worker dispatch, worker execution, and response handling so the
remaining transport/IPC cost is separately attributable.

### 7. Initial-Sync Tests Found Measurable Instrumentation Overhead

The new production-shaped initial-sync fixtures cover mixed tables, wide rows, large payloads, and narrow row-heavy workloads. They established repeatable baselines and showed that recording OpenTelemetry counters for every COPY chunk was itself on the hot path.

Batching those metrics into 8 MiB increments produced reported cumulative speedups of roughly 13–35%, depending on the fixture, while preserving totals. The work also added per-table source-wait versus destination-processing timing, which lets us distinguish PostgreSQL delays from parser, decoding, SQLite, or metric overhead.

The tests also invalidated an earlier assumption: synthetic fixed-size COPY rechunking did not model production PostgreSQL behavior because COPY data is row-aligned by the time Zero receives it. Those misleading chunk-boundary performance profiles were removed; arbitrary fragmentation remains covered as parser correctness testing.

## Important Caveats Before Presenting Capacity Numbers

- The realistic high-rate sweeps currently report the requested target as the “best write rate,” even when the writer cannot achieve it. For example, targets of 7,813 writes/s only achieved roughly 786–1,017 writes/s depending on profile. Those results are valid latency observations at the achieved rate, but not proof of 7.8k sustainable throughput.
- We should add a rate-attainment requirement to sweep pass/fail before publishing a headline capacity number.
- `apps/zero-throughput/results/` is gitignored. Some recovery and coalescing artifacts record the preceding commit SHA because the tested changes were still uncommitted. They are strong directional evidence, but should be rerun from a clean, identified commit for a formal team benchmark.
- Most recent migration results are local single-machine experiments, not dedicated-hardware or multi-environment measurements.
- The recovery and transaction-coalescing work is currently on `mlaw/tput`; the E2E harness and projected-cost load shedding are already on `main`.

## Takeaway

The tests have identified four distinct limits: query fanout, reset/rehydrate
thrashing, durable PostgreSQL commit throughput for tiny transactions, and an
undersized-batch/per-message replication path before SQLite. Raw SQLite is not
the current 50k rows/s ceiling when changes are effectively coalesced. The work
has already driven improvements in load shedding, recovery behavior,
replication batching, and initial-sync instrumentation overhead; the next
replication optimization should focus on transport batching and stage-level
transport and worker measurements, rather than adding a replica-side flush
delay.
