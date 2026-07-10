# Zero E2E Throughput Benchmark Design

## Purpose

We need a repeatable benchmark that answers how much write throughput Zero can
sustain before connected clients fall behind.

The benchmark should measure the full path:

1. PostgreSQL accepts committed writes.
2. The replicator observes and applies WAL changes.
3. The view-syncer evaluates standing queries and produces diffs.
4. Synthetic clients receive and apply query results over the Zero protocol.

The primary output is the maximum sustainable PostgreSQL write rate for a given
application profile, user count, and standing-query mix.

## Questions

- At a fixed user count and query mix, what writes/sec can Zero sustain while
  keeping client-visible lag bounded?
- Which stage becomes the bottleneck: PostgreSQL, replication, view syncing,
  network fanout, or client-side apply?
- How do throughput and lag change as we vary:
  - concurrent users
  - standing queries per user
  - rows matched per query
  - write batch size
  - payload size
  - hot-set updates versus append-only inserts
  - simple table queries versus multi-table relationship queries

## Non-Goals

- This is not a correctness replacement for existing integration tests.
- This is not initially a broad database benchmark. PostgreSQL is the source of
  writes, but the benchmark exists to characterize Zero E2E behavior.
- This is not initially a browser rendering benchmark. Synthetic clients should
  speak the protocol directly unless a browser client becomes necessary for a
  specific measurement.

## Location

The benchmark app should live at:

```text
apps/zero-throughput
```

It should be runnable locally and in CI-like environments, with configuration
for workload profile, scale, duration, and output path.

## High-Level Harness

The harness owns process lifecycle and measurement collection:

1. Start PostgreSQL.
2. Apply benchmark schema and seed data.
3. Start the Zero replicator.
4. Start the Zero view-syncer.
5. Start synthetic protocol clients.
6. Register standing queries for each synthetic user.
7. Start a controlled PostgreSQL write generator.
8. Increase or hold write rate according to the selected run mode.
9. Collect stage metrics, client observations, and final summaries.
10. Stop all processes and write a machine-readable result file.

If replicator and view-syncer cannot initially be launched as independent
processes, the first implementation can launch the existing zero-cache process
and still record stage metrics where available. The benchmark should preserve
the conceptual split so we can separate the processes later.

## Workload Model

Each benchmark run chooses one application profile. A profile defines schema,
seed data, standing queries, write operations, and scale parameters.

Initial profiles:

| Profile           | Shape                                                             | Purpose                                             |
| ----------------- | ----------------------------------------------------------------- | --------------------------------------------------- |
| `feed-append`     | Append-only rows with many users watching recent rows             | Measures high-fanout insert throughput              |
| `email`           | Inbox threads, message lists, unread thread/message relationships | Models email-client list/detail query fanout        |
| `forum`           | Category, thread, post, and author relationship queries           | Models forum-style hierarchical discussion activity |
| `relational`      | Org/account/contact/activity relationship queries                 | Measures query planning and IVM cost for joins      |
| `feed-update-hot` | Updates over a small hot working set                              | Measures invalidation and repeated diff cost        |
| `issue-list`      | Users subscribe to filtered issue lists                           | Models zbugs-style list views                       |
| `dashboard`       | Each user registers several narrow standing queries               | Measures many-query-per-user overhead               |

Scale dimensions:

- `users`: number of concurrent synthetic clients.
- `queriesPerUser`: number of standing queries each client registers.
- `rowsPerQuery`: approximate result-set size per standing query.
- `writeRate`: target committed writes per second.
- `batchSize`: rows written per PostgreSQL transaction.
- `payloadBytes`: size of user payload per row.
- `hotSetSize`: number of rows updated when using hot-set profiles.
- `duration`: measured run duration after warmup.
- `warmup`: time allowed for clients to connect and reach initial sync.

## Benchmark Schema Signals

Rows used for lag calculation should include both a monotonic sequence and a
server-side timestamp.

Example columns:

```sql
id text primary key,
profile text not null,
shard int not null,
bucket int not null,
seq bigint not null,
payload jsonb not null,
written_at timestamptz not null default clock_timestamp(),
updated_at timestamptz not null default clock_timestamp()
```

Use `seq` as the primary catch-up signal. It gives an unambiguous ordering for
written data and lets clients report the maximum sequence observed for each
query.

Use `written_at` as the wall-clock latency signal. It should be generated by
PostgreSQL, not the write-generator process. If we later expose WAL commit
timestamps or LSN timestamps cheaply, those should be preferred for stage-level
latency because `clock_timestamp()` records statement execution time rather than
commit visibility.

## Lag and Throughput Calculation

For each run, record:

- highest committed `seq`
- highest observed `seq` per client query
- client receive time for observed rows
- write commit rate
- diff receive rate
- diff byte rate

Primary lag signals:

- `seqLag = highestCommittedSeq - minObservedSeqAcrossRequiredQueries`
- `timeLag = clientReceivedAt - written_at`
- `lagSlope = changeInSeqLag / time`

A write rate is sustainable for a profile when:

- all required clients remain connected
- initial sync completes before the measured window
- p95 and p99 client-visible lag stay under the configured SLO
- `seqLag` remains bounded during the measured window
- `lagSlope` is approximately zero or negative after warmup

The benchmark should report the highest sustainable writes/sec for each profile
and user count under a declared SLO, for example:

```text
p99 client-visible lag < 2s for 10 minutes, with no positive lag slope
```

## Stage Metrics

E2E numbers are necessary but not sufficient. The harness should collect enough
stage metrics to identify the bottleneck.

PostgreSQL:

- committed transactions/sec
- committed rows/sec
- transaction latency p50/p95/p99
- WAL bytes/sec
- replication slot lag, confirmed flush LSN, and restart LSN
- CPU, memory, and disk IO

Replicator:

- WAL records/sec
- rows applied/sec
- source LSN and applied LSN
- apply queue depth
- apply latency
- CPU and RSS

View-syncer:

- changed rows received/sec
- queries invalidated/sec
- queries evaluated/sec
- diffs produced/sec
- diff rows/sec
- diff bytes/sec
- query evaluation latency
- output queue depth
- CPU and RSS

Synthetic clients:

- connected clients
- registered queries
- initial sync latency
- diffs received/sec
- diff bytes/sec
- max observed `seq` per query
- receive-to-apply latency
- protocol errors and reconnects
- CPU and RSS

## Run Modes

### Fixed Rate

Run a profile at a specified write rate for a specified duration. This is useful
for regression testing and comparing commits.

### Sweep

Run the same profile across a list of write rates and user counts. This is
useful for producing throughput curves.

### Search

Use a bounded search to find the maximum sustainable write rate for a selected
profile and user count. This is useful for headline capacity numbers.

## Output

Each run should write a JSON result file and a concise human-readable summary.

The JSON should include:

- git commit
- profile name
- benchmark configuration
- process versions and command lines
- environment details
- time-series metric samples
- final p50/p95/p99 latency summaries
- maximum lag and lag slope
- pass/fail against the configured SLO

The summary should include:

- profile and scale
- target write rate and achieved write rate
- p95 and p99 client-visible lag
- max `seqLag`
- lag slope
- likely bottleneck stage when detectable
- links or paths to detailed output

## Implementation Phases

### Phase 1: Minimal E2E Harness

- Create `apps/zero-throughput`.
- Define a single append-only profile.
- Start PostgreSQL, Zero, and one synthetic client.
- Register one standing query.
- Generate writes at a fixed rate.
- Report committed `seq`, observed `seq`, and E2E lag.

### Phase 2: Scale and Profiles

- Add concurrent synthetic clients.
- Add configurable users, queries per user, write rate, batch size, and duration.
- Add the `feed-update-hot`, `issue-list`, and `dashboard` profiles.
- Write JSON result files.

### Phase 3: Stage Metrics

- Collect PostgreSQL replication metrics.
- Add replicator and view-syncer queue, latency, and throughput metrics.
- Add client diff byte and apply metrics.
- Emit time-series samples.

### Phase 4: Capacity Search

- Add write-rate sweep mode.
- Add bounded search mode for maximum sustainable writes/sec.
- Add pass/fail SLO configuration.
- Produce comparison summaries across profiles and user counts.

### Phase 5: CI and Regression Tracking

- Add a small fixed-rate smoke benchmark suitable for CI.
- Add a longer opt-in benchmark for release or nightly runs.
- Store historical result artifacts for comparison.

## Open Decisions

- Whether to launch replicator and view-syncer as separate processes from the
  start or begin with the current zero-cache process boundary.
- Whether synthetic clients should use the production Zero client, a lower-level
  protocol client, or both.
- Which SLO should be the default headline number.
- Which metrics already exist versus which need to be added to Zero internals.
- Whether to run PostgreSQL through Docker, an existing local instance, or both.
