# SQLite Change Log — Detailed Implementation Plan

Status: proposed

Companion design: [`SQLITE_CHANGE_LOG_DESIGN.md`](./SQLITE_CHANGE_LOG_DESIGN.md)

## 1. Purpose

This plan turns the scoped SQLite change-log design into independently
mergeable and testable changes. The immediate objective is to add a local
SQLite change log to the current replication-manager without changing which
component owns logical replication, subscriber catchup, or replication-slot
ACKs. Later slices can prove SQLite reads in production and make the PG change
log removable once the separate RMv2 lifecycle prerequisites are present.

The plan deliberately separates mechanical changes from correctness-sensitive
integration. In particular:

- serialization is extracted before the worker protocol changes;
- the worker protocol changes before the writer is enabled;
- the reader is tested independently before it participates in subscriber
  handoff;
- purge SQL is tested independently before purge is routed to the writer
  process;
- content comparison starts only after the reader exists; and
- disabling PG writes is a release gate, not part of the SQLite component
  implementation.

Every numbered slice below should be a separate pull request unless a slice is
explicitly described as an operational rollout step.

## 2. Scope and success criteria

### In scope

- `_zero.changeLogStream` schema, migration, and seed rows;
- a canonical downstream-message codec shared with the PG log;
- atomic replica apply + SQLite stream-log append + state-version update;
- batched SQLite catchup reads;
- a gap-free catchup-to-live handoff in the current migration topology;
- transaction-aligned incremental purge;
- cross-process scheduling of purge on the canonical SQLite writer;
- shadow-write, dark-compare, and canary-read rollout controls; and
- observability and rollback gates for each production phase.

### Out of scope

- replica epochs, generations, and IDs;
- Initial Sync, Fork, and Resumption;
- replication-slot ownership, invalidation, and cleanup;
- changing slot ACKs from PG change-log commits to verified backup watermarks;
- cross-replica routing; and
- deletion of the PG schema before the RMv2 prerequisites in the design hold.

### Done for this workstream

The SQLite workstream is complete when all of the following are true:

1. A configured canonical replica atomically maintains
   `_zero.changeLogStream` with its applied data and `stateVersion`.
2. PG and SQLite catchup output have been compared over a full retention
   window with no unexplained divergence.
3. Selected replica IDs can serve SQLite catchup with an immediate
   configuration rollback to PG.
4. Incremental purge keeps the configured window without splitting
   transactions or materially stalling replication.
5. The remaining reasons PG writes cannot be disabled are exclusively the
   broader RMv2 lifecycle and slot-ACK gates listed in the companion design.

## 3. Current topology and migration constraints

Today the write and serving responsibilities are split across processes:

```text
Postgres logical slot
        |
        v
change-streamer worker
  +-- PG Storer: durable PG log, catchup, purge, current source ACKs
  +-- Forwarder: live subscriber fan-out and per-subscriber backlog
        |
        +------------------------------+
        |                              |
        v                              v
canonical replicator              other subscribers
(`backup` when litestream is       (`serving` replicator and
configured, otherwise `serving`)   view-syncer traffic)
        |
        v
SQLite replica write worker
```

The SQLite writer belongs in the canonical replicator's existing
`ChangeProcessor` transaction. The SQLite reader can open the canonical replica
file from the change-streamer process, but purge must be sent to the canonical
replicator so it is serialized with replica writes.

Two migration-only coordination rules follow from this topology.

### 3.1 Required-head barrier for SQLite catchup

The change-streamer can have forwarded through commit `F` while the canonical
replicator has only committed SQLite through `S`, where `S < F`. If a new
subscriber is registered after `F`, messages in `(S, F]` are neither in the
SQLite log yet nor newly emitted to that subscriber's live backlog. Pinning a
SQLite head at `S` would therefore create a gap.

Before SQLite is eligible for reads, `ChangeStreamerImpl` must initialize its
committed head from the PG Storer's current `lastWatermark`; tracking only
commits seen since process startup is insufficient. For every SQLite catchup
registration it then captures a `requiredHead`:

- outside an upstream transaction, use the last forwarded commit watermark;
- during an upstream transaction, capture its completion promise because
  `Forwarder.add()` queues the subscriber until the transaction ends. After a
  commit, use that commit watermark; after rollback, use the prior committed
  head.

The coordinator then:

1. registers the subscriber with `Forwarder`;
2. waits until SQLite `stateVersion >= requiredHead`;
3. pins the SQLite catchup head and reads through it; and
4. calls `setCaughtUp()` so the existing backlog drains.

Messages emitted after registration are in the live backlog. Messages through
`requiredHead` are in SQLite before the head is pinned. Duplicate commit ranges
are harmless because `Subscriber` already deduplicates by committed watermark.

The initial implementation should poll `stateVersion` with an abort signal,
short interval, and bounded timeout. This keeps the migration seam small and
can later be replaced with an event notification. Select the PG path before
registering the subscriber if SQLite is unavailable or not yet eligible. Once
a subscriber is registered on the SQLite path, a barrier/read failure closes
that subscription; it must not fall back in place to PG because the PG
Storer/Forwarder lockstep boundary has already passed.

In steady-state RMv2, where the canonical writer commits before live commit
fan-out, the barrier should normally be satisfied immediately. Keep it as a
checked invariant rather than adding an RMv2-only fast path initially.

### 3.2 Writer-serialized purge

The change-streamer currently computes cleanup eligibility from backup
verification and subscriber ACKs. The canonical replicator owns the SQLite
writer. A direct SQLite write from the change-streamer would introduce a second
writer and could race `ChangeProcessor`.

Purge therefore needs a request/response path:

```text
backup monitor + Forwarder ACKs
        |
        v
ChangeStreamerImpl computes safe floor
        |
        v
dispatcher routes maintenance request
        |
        v
canonical ReplicatorService / IncrementalSyncer
        |
        v
write worker runs one purge batch between source transactions
```

`ThreadWriteWorkerClient` currently permits one pending RPC. Maintenance must
not call it concurrently with `processMessage()`. `IncrementalSyncer` should
coalesce pending purge requests and invoke the worker only after processing a
commit/rollback, or during an explicitly serialized idle turn.

Snapshot reservation protection remains in the existing backup monitors: they
do not advance `scheduleCleanup()` while a reservation is active. Slice 9 also
adds a pause barrier so a reservation waits for any already-dispatched SQLite
purge before reading/advertising its change-log bounds, then prevents new purge
requests until the reservation ends. The change-streamer combines the last
verified backup watermark with subscriber ACKs before sending the SQLite
floor.

The same barrier protects new SQLite catchups. A catchup waits for an in-flight
purge, blocks new purge dispatch, registers with `Forwarder` so its requested
watermark appears in `getAcks()`, and then releases the block. Without this
ordering, a previously computed floor could delete the new subscriber's range
before its ACK was visible. PG-only subscribers do not need to block SQLite
purge.

## 4. Adjustments made for implementation

The companion design's conceptual slices are refined here in five ways:

1. Codec extraction, worker-protocol changes, and atomic writing are separate
   PRs so each can land without activating a new persistence path.
2. The pure reader and the catchup-to-live coordinator are separate PRs so
   range correctness can be tested without timing races.
3. The current split-process topology gets the required-head barrier described
   above, including startup initialization and rollback handling.
4. Purge SQL and purge scheduling/IPC are separate PRs; the latter also closes
   the snapshot-reservation race around already-dispatched work.
5. Content comparison depends on the reader and therefore follows shadow
   writing; PG replacement remains an RMv2 milestone rather than a SQLite PR.

## 5. Proposed code structure

The names below are concrete enough for consistent pull requests but may be
adjusted during implementation if an existing abstraction provides a better
home.

| Responsibility                                  | Proposed location                                               |
| ----------------------------------------------- | --------------------------------------------------------------- |
| Canonical serialize/extract/reconstruct helpers | `services/change-streamer/change-log-codec.ts`                  |
| SQLite DDL and seed helper                      | `services/replicator/schema/change-log-stream.ts`               |
| Transaction-scoped append logic                 | `services/replicator/change-log-stream-writer.ts`               |
| Pure read/bounds API                            | `services/change-streamer/sqlite-change-log-reader.ts`          |
| Subscriber handoff coordinator                  | `services/change-streamer/sqlite-change-log-catchup.ts`         |
| Pure purge SQL                                  | `services/replicator/sqlite-change-log-purger.ts`               |
| Purge IPC payloads and routing                  | `types/processes.ts`, `server/main.ts`, `workers/replicator.ts` |
| Rollout configuration                           | `config/zero-config.ts`                                         |
| Integration and metrics                         | `services/change-streamer/change-streamer-service.ts`           |

Avoid exporting these through `mod.ts`. Import each implementation directly.

### 5.1 Shared serialized envelope

Use a single envelope at the replicator/write-worker boundary:

```ts
export type SerializedChangeStreamData = {
  data: ChangeStreamData;
  json: string;
};
```

`json` is produced once by the change-streamer with
`BigIntJSON.stringify(data)`. It is the representation used for forwarding and
for extracting the `change` text stored by PG and SQLite. The change-streamer
client preserves the exact application-level JSON while parsing and validating
`data`, so `IncrementalSyncer` does not stringify the message again. The parsed
`data` remains available to `ChangeProcessor`; the worker does not parse JSON a
second time.

Move `extractChangeSubstring()` and the inverse reconstruction helper out of
`storer.ts`. The codec tests become the compatibility contract for both stores.
If moving `WatermarkedChange` avoids a dependency from the codec back to
`change-streamer-service.ts`, place that low-level type in
`change-streamer.ts` rather than creating a cycle.

### 5.2 Writer ownership

Add an explicit required option to `ChangeProcessor` construction, for example:

```ts
type ChangeProcessorOptions = {
  logsChangeStream: boolean;
};
```

All call sites must choose deliberately. During the current migration:

- enable it for `ReplicaFileMode === 'backup'`;
- enable it for `ReplicaFileMode === 'serving'` only when that is the canonical
  file because no backup replicator exists; and
- disable it for `serving-copy`, initial-sync-only processors, tests that do
  not exercise it, and non-canonical view-syncer replicas.

If a process tree has neither a backup replicator nor a primary serving
replicator, it has no canonical incremental SQLite writer and must reject every
mode other than `off`.

Pass this choice from `server/replicator.ts` through
`ThreadWriteWorkerClient.init()` to `write-worker.ts`. Do not infer canonical
ownership solely from `ReplicatorMode`, because both `serving` and
`serving-copy` currently map to the same mode.

### 5.3 Reader API

Keep storage classification separate from subscriber policy:

```ts
type CatchupPlan =
  | {kind: 'range'; minWatermark: string; headWatermark: string}
  | {kind: 'ahead'; headWatermark: string}
  | {
      kind: 'too-old';
      minWatermark: string;
      headWatermark: string;
    };

interface SQLiteChangeLogReader {
  plan(fromWatermark: string): CatchupPlan;
  read(
    fromWatermark: string,
    throughWatermark: string,
    batchSize: number,
  ): AsyncIterable<readonly WatermarkedChange[]>;
}
```

`plan()` reads `stateVersion`, minimum retained watermark, and the requested
transaction boundary. The catchup coordinator maps `too-old` to
`WatermarkTooOld` for serving and `AutoResetSignal` for backup mode.

Each `read()` batch uses a short read transaction and continues by the last
`(watermark, pos)` pair. The query must be strictly after the requested
watermark and at or below the pinned head. Do not hold one read snapshot for
the whole WebSocket catchup. Select the tag with
`json_extract(change, '$.tag')` (or parse the stored change text with the shared
codec if benchmarks favor that path) and validate it before reconstruction;
the schema intentionally does not duplicate the tag in another column.

### 5.4 Purger API

Keep SQL selection/deletion independent of policy and IPC:

```ts
type PurgeBatchResult = {
  deletedRows: number;
  deletedThrough: string | undefined;
  moreEligible: boolean;
};

interface SQLiteChangeLogPurger {
  purgeBatch(opts: {
    externalFloor: string;
    retentionCutoffMs: number;
    maxRows: number;
  }): PurgeBatchResult;
}
```

The purger selects the indexed time floor, combines it with `externalFloor` and
the current head, deletes complete transactions only, and treats `maxRows` as a
soft limit for one oversized oldest transaction. The change-streamer computes
external backup/subscriber safety; SQL code computes file-local time/head
safety and executes one bounded write transaction.

## 6. Rollout configuration

Add hidden options under `changeStreamer` with safe defaults. Suggested names:

| Option                            |            Default | Purpose                                       |
| --------------------------------- | -----------------: | --------------------------------------------- |
| `sqliteChangeLogMode`             |              `off` | `off`, `write`, `compare`, or `serve`         |
| `sqliteChangeLogReadPercent`      |                `0` | Stable canary percentage when mode is `serve` |
| `sqliteChangeLogRetentionMs`      |            `60000` | Time-based minimum retention                  |
| `sqliteChangeLogReadBatchRows`    |   benchmark result | Rows per short read snapshot                  |
| `sqliteChangeLogPurgeBatchRows`   |   benchmark result | Target rows per write batch                   |
| `sqliteChangeLogBarrierTimeoutMs` | operational result | Maximum required-head wait                    |

Mode semantics are cumulative:

- `off`: no SQLite stream writes, reads, compare, or purge;
- `write`: write and purge SQLite while PG remains authoritative;
- `compare`: additionally perform sampled dark reads and compare with PG; and
- `serve`: additionally allow a stable percentage of eligible catchup
  subscriptions to use SQLite.

Reject invalid combinations during normalization, such as a non-zero read
percentage outside `serve`. A separate hidden PG-write switch should only be
introduced in the final RMv2 replacement gate; coupling it to `serve` would
make rollback unsafe.

Configuration is inherited by child processes today. Pass only the derived
writer boolean and tuning values into the write worker rather than teaching the
worker about rollout modes.

## 7. Dependency graph

```text
Slice 0: benchmark baseline

Slice 1: codec --------> Slice 3: worker envelope --+
Slice 2: schema ------------------------------------+--> Slice 4: writer
        |                                                   |
        +--> Slice 6: reader --> Slice 7: handoff            +--> Slice 5: shadow wiring
        |                                                   |
        +--> Slice 8: purger -------------------------------+--> Slice 9: purge IPC
                                                                    |
Slice 5 + Slice 6 + Slice 9 ----------------------------------------+--> Slice 10: compare
Slice 7 + Slice 9 + Slice 10 ------------------------------------------> Slice 11: canary reads
                                                                          |
                                                                          v
                                                               Slice 12: RMv2 PG-write gate
                                                                          |
                                                                          v
                                                               Slice 13: PG cleanup
```

Slices 1 and 2 can proceed in parallel after the benchmark shape is agreed.
Slices 6 and 8 can proceed in parallel once the schema exists. Slice 0 can
continue collecting numbers while mechanical slices are reviewed. Slice 4 must
not be enabled in production until its go/no-go thresholds are met, and
sustained production shadow writing waits for Slice 9 so retention is bounded.

## 8. Pull-request slices

### Slice 0 — Establish performance and progress gates

**Objective:** make performance expectations explicit before production writes
are enabled.

**Code changes**

- Extend
  `services/replicator/sqlite-change-log-ceiling.bench.ts` rather than creating
  an unrelated benchmark harness.
- Require `sqliteTxRows === logicalTxRows` for go/no-go results so each logical
  upstream transaction incurs its real SQLite commit cost.
- Use the proposed table, primary key, partial time index, and real message-size
  distributions.
- Add cases for:
  - small high-frequency transactions;
  - mixed row/schema messages;
  - a transaction larger than the purge target;
  - a large catchup scan on a second connection;
  - small between-commit purge batches;
  - idle purge drains; and
  - litestream v5/checkpoint pressure when the executable is available.

**Measurements**

- transactions and changes per second;
- p50, p95, and p99 commit latency;
- reader batch and end-to-end catchup latency;
- purge transaction duration and rows deleted;
- main DB, WAL, and freelist size; and
- upstream-loop stall time attributable to SQLite.

**Tests and validation**

- Add a deterministic correctness mode that asserts row counts, final head,
  complete transaction boundaries, and purge progress.
- Keep wall-clock benchmark thresholds out of normal CI. Record the hardware,
  SQLite build, journal mode, and exact invocation with published results.

**Exit criteria**

- Owners record target workloads and acceptable latency/throughput regression.
- Default reader and purge batch sizes are chosen, or remain required rollout
  settings if results are workload-dependent.
- The oldest-oversized-transaction case completes rather than looping.

**Production effect:** none.

### Slice 1 — Extract and lock down the canonical codec

**Objective:** make PG and SQLite use one serialization contract without
changing stored or forwarded bytes.

**Code changes**

- Add `change-log-codec.ts`.
- Move `extractChangeSubstring()` from `storer.ts` and export a reconstruction
  helper equivalent to current `toDownstream()`.
- Add `serializeChangeStreamData()` using `BigIntJSON.stringify()`.
- Change `Storer.store()` and its catchup path to call those helpers.
- Move only low-level types needed to break cycles; do not create a re-export
  module.

**Tests**

- Add `change-log-codec.test.ts` with begin, data, schema, truncate, backfill,
  commit, and rollback messages.
- Include nested big integers, strings containing escaped NUL, quotes,
  backslashes, Unicode, and empty objects/arrays.
- Assert exact output strings, extract/reconstruct round trips, and parity with
  the pre-extraction PG representation.
- Run existing `storer.pg.test.ts` and `change-streamer-service.pg.test.ts`
  unchanged as regression tests.

**Exit criteria**

- No PG schema or runtime behavior changes.
- Every supported downstream message can be serialized once and reconstructed
  byte-for-byte.

**Production effect:** none. **Rollback:** revert the mechanical extraction.

### Slice 2 — Add v14 schema and seed invariant

**Objective:** ensure every fresh, migrated, and restored current replica has a
valid but inert SQLite stream-log schema.

**Code changes**

- Add `services/replicator/schema/change-log-stream.ts` with:
  - table DDL;
  - partial retention-index DDL;
  - a prepared seed helper; and
  - schema constants used by tests and migration.
- Append migration v14 to
  `services/change-source/common/replica-schema.ts`.
- Reconfirm immediately before merge that v14 is still the next unused replica
  schema version; renumber the migration and design references if another
  migration lands first.
- In `migrateSchema`, create the table and index.
- In `migrateData`, read the current `stateVersion` and `writeTimeMs`, then
  insert the synthetic begin/commit pair idempotently.
- Include the DDL in fresh creation from
  `services/replicator/schema/replication-state.ts` and call the seed helper
  after `_zero.replicationState` is initialized.
- Do not raise `minSafeVersion` or `AutoResetSignal`; this is a backward-
  compatible additive table.

**Tests**

- Add focused schema tests for exact columns, primary key, partial index, and
  seed rows.
- Extend `replication-state.test.ts` for fresh creation.
- Extend `replica-schema.test.ts` for v13 to v14 migration, rollback to a build
  that ignores the table, and roll-forward without duplicate seed rows.
- Verify seed `max(watermark) === stateVersion` and that a reader starting at
  the seed would receive no rows.

**Exit criteria**

- Fresh and upgraded replicas contain exactly one valid synthetic transaction.
- Existing replica data and `_zero.changeLog2` are unchanged.

**Production effect:** a small inert table/index in replica files.
**Rollback:** older code ignores the additive table.

### Slice 3 — Carry canonical JSON through the write-worker boundary

**Objective:** make the canonical string available to the SQLite writer while
preserving existing processing behavior.

**Code changes**

- Add `SerializedChangeStreamData` in the lowest non-cyclic protocol module
  that can be directly imported by both the replicator and write worker.
- Add a `streamInStringified()` counterpart to `streamOutStringified()` that
  preserves the exact application-level JSON alongside its parsed, validated
  value without changing the WebSocket protocol.
- Change the client-side `ChangeStreamer` interface and alternate
  implementations to return parsed downstream data with its preserved JSON.
- Change `WriteWorkerClient.processMessage()`, `ArgsMap`, `Request`, and worker
  dispatch to accept the envelope.
- At the replication-manager ingress in `IncrementalSyncer`, send each parsed
  data-plane message plus its preserved JSON to the worker without
  reserialization.
- In this slice, `ChangeProcessor` consumes `data` and intentionally ignores
  `json`.
- Keep status/error/control messages outside the write-worker envelope because
  they are not part of the persisted stream log.

**Tests**

- Update `write-worker.test.ts` and `incremental-sync.test.ts` fixtures.
- Add a worker-thread round-trip test proving bigint and escaped-NUL JSON cross
  structured-clone without alteration.
- Assert the HTTP client preserves the exact application-level JSON for bigint
  and escaped-NUL messages and that the worker receives that string unchanged.
- Confirm all existing commit results, notification timing, abort, and worker
  error propagation are unchanged.

**Exit criteria**

- Production behavior is unchanged with the extra ignored string and no second
  canonical serialization in the replicator.
- The worker receives the same canonical JSON covered in Slice 1.

**Production effect:** small IPC payload increase and one fewer canonical
serialization per data-plane message in each replicator. **Rollback:** restore
the old change-streamer client result and worker argument after
draining/restarting workers; there is no persisted-format dependency yet.

### Slice 4 — Add the atomic SQLite writer behind a disabled flag

**Objective:** implement apply + stream-log + state update in one existing
SQLite transaction.

**Code changes**

- Add `change-log-stream-writer.ts` with prepared insert statements and a
  transaction-local `pos` counter.
- Add `ChangeProcessorOptions.logsChangeStream` and thread it through all
  constructors.
- Create one stream writer per `ChangeProcessor`, but issue writes only when
  enabled.
- On begin:
  - assert no stream transaction is open;
  - reset `pos` to zero; and
  - insert the begin row in the same transaction opened by
    `TransactionProcessor`.
- On data/schema/truncate/backfill:
  - increment the independent stream position; and
  - insert the canonical change substring before returning from
    `processMessage()`.
- On commit:
  - capture one `writeTimeMs`;
  - insert the commit row with `precommit` and that timestamp;
  - update `_zero.replicationState` with the same timestamp; and
  - commit the existing transaction.
- On rollback, processing failure, abort, or source interruption, use the
  existing transaction rollback so no stream rows survive.
- Do not alter `_zero.changeLog2`'s counter or semantics.

**Wiring**

- Add hidden mode/tuning config with default `off`.
- Pass a derived `logsChangeStream` boolean to `write-worker.init()`.
- Initially enable only for the canonical file-mode cases described in §5.2.
- Add a startup assertion that the flag cannot be enabled for `serving-copy`.
- Reject writer enablement if the dispatcher did not create a canonical
  replicator for this process tree.

**Tests**

- Focused `change-log-stream-writer.test.ts` unit tests for positions and fields.
- Extend `change-processor.test.ts` for every message family and multi-message
  transactions.
- Extend `write-worker.test.ts` for real worker/reopen verification.
- Test explicit rollback, processing error, worker abort, source disconnect
  mid-transaction, and restart.
- Test an arbitrarily large transaction without buffering its messages in
  memory.
- After every commit/reopen, assert:
  - replica data matches the stream;
  - `max(changeLogStream.watermark) === stateVersion`;
  - begin/data/commit positions are contiguous;
  - the commit is the only row with `precommit` and `writeTimeMs`; and
  - the state row and commit row use the same `writeTimeMs`.
- Run all tests once with logging disabled to prove old behavior is unchanged.

**Exit criteria**

- The invariant holds across normal commit, rollback, thrown errors, and
  process restart.
- Enabling the writer does not change live delivery or PG ACK behavior.
- Slice 0 thresholds are approved before production enablement.

**Production effect:** none by default. **Rollback:** set mode to `off`; the
extra table remains harmless.

### Slice 5 — Enable shadow writes and writer observability

**Objective:** accumulate production SQLite history while PG remains the only
reader and source of ACK durability.

**Code changes**

- Support `sqliteChangeLogMode=write` on selected replica managers.
- Add metrics for:
  - SQLite stream rows and estimated retained bytes;
  - SQLite head and head lag relative to the last received PG commit;
  - per-message processing and per-commit latency;
  - transaction row-count distribution;
  - rollback count; and
  - invariant failures.
- Add structured startup logging for file mode, writer enabled state, schema
  version, and seed/head watermark.
- Treat an atomic-writer error like any other canonical replica-apply error;
  stop/recover the replicator rather than continuing with a partial log.

**Tests**

- Add a current-topology integration test: PG change-streamer emits a stream,
  canonical replicator consumes it, and PG/SQLite heads eventually converge.
- Verify temporary head skew is observable but not reported as corruption.
- Verify PG catchup, PG purge, slot ACKs, and subscriber output are unchanged.

**Rollout**

1. Enable on development and load-test environments.
2. A short, explicitly disk-bounded production probe is permitted if needed to
   validate workload shape.
3. Do not begin a sustained production soak or broad rollout until Slice 9 can
   enforce retention.

**Exit criteria**

- No invariant failures.
- Head lag returns to zero in load tests and any bounded probe.
- Commit-latency and WAL/source-lag gates remain within Slice 0 thresholds.

**Rollback:** set mode to `off`. PG remains authoritative throughout.

### Slice 6 — Implement and test the pure SQLite reader

**Objective:** prove range classification and reconstruction without changing
subscriber routing.

**Code changes**

- Add `sqlite-change-log-reader.ts` using a dedicated read connection in the
  file's configured WAL/WAL2 mode.
- Implement `plan()` for:
  - seed/exact head;
  - catchable retained boundary;
  - too-old boundary;
  - missing interior transaction boundary; and
  - subscriber-ahead watermark.
- Pin `headWatermark` from `_zero.replicationState` before iteration.
- Read strictly after `fromWatermark` and through the pinned head.
- Continue batches by `(watermark, pos)` and close each short read snapshot
  before yielding/awaiting WebSocket flow control.
- Reconstruct `WatermarkedChange` with the Slice 1 codec.
- Provide `close()`/abort behavior so shutdown and canceled subscriptions do
  not retain a read connection or statement.

**Tests**

- Add `sqlite-change-log-reader.test.ts` using a real temporary SQLite file.
- Cover exact seed, minimum, middle, exact head, too old, missing boundary, and
  ahead.
- Cover a transaction spanning several batches and several transactions in one
  batch.
- Append and purge concurrently from another connection while reading; output
  must end at the originally pinned head and contain complete transactions.
- Cancel between batches and assert statements/transactions are released.
- Assert byte-identical output for bigint, escaped NUL, schema, backfill, and
  truncate representations.
- Use `EXPLAIN QUERY PLAN` in a focused test or benchmark assertion to catch an
  accidental full scan for continuation/time-bound queries.

**Exit criteria**

- Reader tests pass independently of `ChangeStreamerImpl` and PG.
- A slow iterator does not hold one SQLite read transaction for its lifetime.

**Production effect:** none; no caller selects the reader yet.

### Slice 7 — Add the required-head catchup-to-live coordinator

**Objective:** make SQLite catchup gap-free in the current split-process
topology, still dark by default.

**Code changes**

- Track in `ChangeStreamerImpl`:
  - `lastForwardedCommitWatermark`, initialized from the PG Storer head before
    SQLite becomes read-eligible; and
  - a current-transaction completion promise that resolves to committed or
    rolled back.
- Add a read-only canonical-replica path to change-streamer construction.
- Add `sqlite-change-log-catchup.ts` to own:
  - required-head capture;
  - Forwarder registration ordering;
  - barrier polling with timeout/cancellation;
  - `CatchupPlan` to serving/backup error-policy mapping;
  - batched delivery with existing subscriber flow control; and
  - `setCaughtUp()` transition.
- Preserve the current PG path exactly when SQLite is not selected.
- Choose PG versus SQLite before calling `Forwarder.add()`.
- Before `Forwarder.add()` on the SQLite path, enter the Slice 9 cleanup guard:
  wait for any in-flight purge, block new dispatch, add the subscriber so its
  ACK is visible, then release the guard.
- After SQLite registration, fail closed on barrier/read error; do not switch
  that same subscriber to PG.
- Keep `Forwarder`'s mid-transaction queued-subscriber behavior unchanged.

**Tests**

- Unit-test the coordinator with controllable forwarded and SQLite heads.
- Add integration cases where registration occurs:
  - between transactions with SQLite current;
  - between transactions with SQLite behind;
  - in the middle of a transaction;
  - in a transaction that subsequently rolls back;
  - while multiple commits arrive during catchup;
  - with a transaction larger than one read batch;
  - with an ahead subscriber; and
  - during timeout, cancellation, reader error, and process shutdown.
- For every race case, record observable commit messages and assert no missing
  or duplicate committed transaction.
- Add the restore-skew scenario: forward through `C`, restore SQLite at
  `B < C`, subscribe from `C`, replay `B..C`, and deliver only changes after
  `C` to the subscriber.
- Retain existing PG `change-streamer-service.pg.test.ts` as the compatibility
  suite.

**Exit criteria**

- The handoff proof holds under all registration timing tests.
- Barrier timeouts close only the selected subscription and emit a metric.
- No production subscription selects SQLite yet.

**Production effect:** dark code only.

### Slice 8 — Implement the pure incremental purger

**Objective:** prove retention-floor SQL and bounded progress independently of
process scheduling.

**Code changes**

- Add `sqlite-change-log-purger.ts` with prepared queries for:
  - current head;
  - indexed time floor from commit rows;
  - oldest eligible transaction;
  - candidate row at the target offset;
  - next distinct watermark for an oversized oldest transaction; and
  - prefix deletion below the chosen ceiling.
- Cap `effectiveFloor` at head so the latest transaction is retained.
- Keep `maxRows` a soft target only for one oversized oldest transaction.
- Return structured progress data for scheduling and metrics.
- Do not depend on `SQLITE_ENABLE_UPDATE_DELETE_LIMIT`.

**Tests**

- Add `sqlite-change-log-purger.test.ts` with a real SQLite database.
- Test the indexed time floor, caller-supplied external floor, and head cap
  independently and in every limiting order.
- Test zero eligible rows, fewer than target rows, exact target, many small
  transactions, and one oversized oldest transaction.
- Assert every remaining watermark starts with begin and ends with commit.
- Repeatedly call until `moreEligible` is false and assert monotonic progress.
- Run a reader on a second connection while purging and verify its pinned range
  remains valid.

**Exit criteria**

- Purge never removes head, never splits a transaction, and always progresses.
- The time-floor query uses the partial index.

**Production effect:** none; the purger has no scheduler yet.

### Slice 9 — Route purge to the canonical writer

**Objective:** schedule SQLite maintenance without adding a second writer or
racing the worker RPC.

**Code changes**

- Reserve typed IPC message names in `types/processes.ts`, for example
  `sqliteChangeLogMaintenanceRequest` and
  `sqliteChangeLogMaintenanceResponse`.
- Add request IDs and payload validation. A request contains at least safe
  floor, batch target, and request time; a response contains the structured
  batch result or serialized error.
- In `server/main.ts`, retain the canonical replicator worker reference and
  route requests from the local change-streamer:
  - `backup` when litestream backup is configured;
  - otherwise the primary `serving` replicator; and
  - no request to `serving-copy`; reject enablement if neither canonical worker
    exists.
- Extend `ReplicatorService`/`IncrementalSyncer` with a maintenance request API.
- Coalesce the latest verified-backup target and retain the newest tuning
  values, but recompute the final floor from current subscriber ACKs immediately
  before dispatch. A newly connected older subscriber must be allowed to lower
  a not-yet-dispatched floor.
- Execute a small batch only when no `processMessage()` RPC is pending,
  preferably immediately after commit/rollback.
- Add an idle-drain trigger that is serialized through the same
  `IncrementalSyncer`; never invoke `ThreadWriteWorkerClient` concurrently.
- Extend `WriteWorkerClient`, `ArgsMap`, `ResultMap`, and `write-worker.ts` with
  `purgeChangeLog()`.
- In shadow mode, keep PG purge unchanged and schedule SQLite purge
  independently from the same safe inputs.
- Add an asynchronous cleanup pause/resume barrier to the change-streamer
  service used by backup monitors. Starting a snapshot reservation first blocks
  new SQLite purge requests and waits for any in-flight request, then reads the
  bounds returned to the snapshot client. Ending the reservation resumes
  scheduling. Multiple reservations use reference-counted/task-ID state.
- Use the same cleanup coordinator for SQLite catchup registration. Hold the
  block only until `Forwarder.add()` exposes the subscriber's initial ACK; the
  ACK then protects the remaining catchup range without a long-lived lock.
- If routing or purge fails, emit an error/metric and retry later; do not block
  PG cleanup or stop replication unless SQLite disk growth becomes unsafe.

**Floor calculation in the current topology**

1. Backup monitors continue to call `scheduleCleanup(verifiedWatermark)` only
   when their snapshot-reservation rules permit it.
2. `ChangeStreamerImpl` takes the minimum of the eligible verified watermark,
   any `Forwarder.getAcks()` values, and head. With no subscribers, the ACK
   constraint is positive infinity.
3. The writer-side purger combines that supplied floor with its indexed time
   floor and current SQLite head.
4. The result is conservative if any external input is stale. An unverifiable
   backup does not advance the supplied floor.

**Tests**

- Add IPC request/response and worker-dispatch tests.
- Test requests while a transaction is open, between transactions, while idle,
  and during shutdown.
- Assert `ThreadWriteWorkerClient` never sees concurrent calls.
- Test coalescing several backup targets, plus a new older subscriber lowering
  the final floor before dispatch.
- Test absent/dead canonical worker, worker error, timeout, and retry.
- Test a reservation racing an already-dispatched purge: the reservation does
  not advertise bounds until that request completes, and no later purge runs
  until the reservation ends.
- Add an end-to-end test with live apply, SQLite catchup reads, and repeated
  purge batches.

**Exit criteria**

- Only the canonical write worker mutates `_zero.changeLogStream`.
- Sustained input and purge do not trigger the worker's concurrent-call assert.
- A quiet source still drains eligible history eventually.

**Rollout**

1. Enable sustained shadow writes for one replica ID.
2. Hold for multiple retention windows and at least one backup/restore cycle.
3. Expand while monitoring retained rows/bytes, purge latency, writer latency,
   WAL growth, and source lag.

**Production effect:** enabled only with SQLite `write` mode.
**Rollback:** disable the SQLite mode; PG purge remains intact.

### Slice 10 — Add sampled dark comparison

**Objective:** compare complete PG and SQLite ranges without serving SQLite
results.

**Code changes**

- Enable `sqliteChangeLogMode=compare` only after the local log has accumulated
  the configured retention window.
- Compare only complete committed ranges through
  `min(pgHead, sqliteHead)`; head skew alone is not divergence.
- Select stable sampled ranges by replica/shard and watermark so retries compare
  the same data.
- Stream comparison in batches. Compare canonical `(watermark, tag, json)`
  rows or incremental hashes; do not load a retention window into memory.
- Classify mismatch metrics by missing PG row, missing SQLite row, tag mismatch,
  byte mismatch, bound mismatch, and reader error.
- Log bounded diagnostic samples with replica version and range, avoiding full
  row payloads that may contain customer data.
- Add eligibility checks:
  - schema is v14+;
  - SQLite has at least a full retention window or an explicitly recorded
    warm-up boundary;
  - requested range exists in both stores; and
  - replica version matches.

**Tests**

- Exact parity over all message types and multi-batch ranges.
- Expected temporary head lag with no mismatch.
- Inject one mutation for every mismatch classification.
- Ensure purge racing comparison produces an inconclusive/retry result rather
  than a false divergence when common bounds change before pinning.
- Verify sampling and payload redaction.

**Rollout**

1. Compare one shard after warm-up.
2. Investigate every divergence; do not aggregate unexplained mismatches away.
3. Expand sampling and hold for at least one complete deploy/restore cycle.

**Exit criteria**

- No unexplained content or bounds divergence for the agreed observation
  period.
- Reader latency and connection/WAL effects are within target.

**Rollback:** return mode to `write`; SQLite history continues accumulating.

### Slice 11 — Canary SQLite catchup reads

**Objective:** serve a controlled subset of catchup traffic from SQLite while
PG remains a complete rollback path.

**Code changes**

- Implement `sqliteChangeLogMode=serve` and a stable canary selector using
  replica/shard plus subscriber identity. Avoid random choice on every retry.
- Select SQLite only when all eligibility checks pass before Forwarder
  registration. Otherwise use PG and increment a labeled ineligibility metric.
- Add a local health circuit breaker. A barrier/reader failure marks SQLite
  temporarily ineligible so the failed connection closes and a normal client
  retry selects PG; a bounded background probe can restore eligibility.
- Route selected subscriptions through the Slice 7 coordinator.
- Keep `/snapshot` and `/changes` on the same replica/version routing contract;
  do not use this flag to route among arbitrary RMv2 siblings.
- Pin the catchup-source choice by snapshot `taskID`: a snapshot selected for
  SQLite retains its cleanup reservation and its subsequent `/changes` request
  uses SQLite; a snapshot selected for PG continues on PG. Consume/expire this
  state through the existing reservation lifecycle so retries cannot silently
  switch stores across an incompatible minimum watermark.
- Put this state in a small read-source router used by both
  `BackupMonitor.startSnapshotReservation()` and
  `ChangeStreamerImpl.subscribe()`. The router returns bounds from the chosen
  store for `/snapshot`, then consumes the same choice for the matching initial
  `/changes` request; unreserved/reconnect requests perform a fresh eligibility
  decision.
- Report catchup source, request classification, rows/bytes, barrier wait,
  catchup latency, backlog peak, timeout/reset rate, and ahead rate.

**Tests**

- Deterministic canary selection and configuration validation.
- Warm versus unwarmed replica behavior.
- Successful PG fallback before registration.
- SQLite failure after registration closes the connection and succeeds on a
  normal client retry, without in-place PG fallback.
- Snapshot reservation and subscriber-ACK protection through catchup.
- Process restart with a litestream-restored v14 replica.
- Mixed PG/SQLite subscribers receiving the same committed sequence.

**Rollout**

1. Start at zero percent with eligibility metrics enabled.
2. Increase to a small percentage on selected replica IDs.
3. Hold through disconnect storms, deploys, backup restores, and schema changes.
4. Increase gradually to 100% SQLite reads while keeping PG writes/read code.
5. Keep at least one release with 100% SQLite reads and immediate PG rollback.

**Exit criteria**

- Catchup/reset/error rates are no worse than PG within agreed bounds.
- No missing committed transaction is observed.
- Required-head barrier wait remains bounded and normally near zero.
- Purge retains all ranges required by active catchups and advertised backups.

**Rollback:** set read percentage to zero or mode to `compare`; new
subscriptions use PG immediately.

### Slice 12 — RMv2 integration gate for disabling PG writes

**Objective:** disable PG change-log writes only when SQLite is no longer being
used inside the RMv1 ownership/ACK contract.

This is an operational and integration milestone, not another SQLite storage
component. Before adding or enabling a PG-write-off flag, verify with tests and
production evidence that:

- each RM exclusively owns its replica ID and replication slot;
- startup establishes source position from replica/backup/slot state without
  reading PG change-log state;
- slot ACKs cannot pass the verified backed-up replica watermark;
- Fork, Resumption, Auto Reset, and cleanup do not consult `cdc.changeLog`;
- snapshot/change routing preserves replica ID/generation compatibility;
- all catchup traffic has used SQLite successfully for at least one release;
  and
- the rollback plan accounts for PG history no longer being populated.

**Implementation once gates hold**

- Add a separate PG-write option defaulting to enabled.
- Refuse to disable it unless the process is operating in the qualifying RMv2
  topology and SQLite serving is enabled.
- Canary by replica ID, not by individual request.
- Monitor slot lag, backup lag, SQLite head/retention, catchup errors, resets,
  and disk growth.
- Re-enable PG writes only as forward recovery: the PG log must warm for a full
  retention window before it can again serve old catchup requests.

**Exit criteria**

- Selected and then all RMv2 replica IDs operate without PG change-log reads or
  writes through deploy, crash, resumption/fork, restore, and auto-reset tests.
- The rollback/warm-up behavior is documented operationally.

### Slice 13 — Remove the PG change log

**Objective:** delete obsolete code and schema only after the rollback window.

**Code changes**

- Remove PG persistence/catchup/purge responsibility from `storer.ts`; retain
  or relocate any still-needed metadata ownership logic.
- Remove PG change-log configuration and metrics that have no remaining
  consumer.
- Remove dual-reader/comparator code and temporary migration flags.
- Drop `cdc.changeLog` in a separately reviewed, backward-compatible upstream
  migration after confirming no supported older zero-cache can require it.
- Update architecture docs and runbooks.

**Tests**

- Full RMv2 startup, steady replication, catchup, rolling replacement, crash
  recovery, restore, reset, and cleanup suites without a PG change-log table.
- Mixed-version deployment test or an explicit deployment-order assertion that
  prevents old binaries from starting after the table is dropped.

**Exit criteria**

- No code, query, dashboard, alert, or supported rollback references the PG
  change log.

## 9. Cross-cutting invariants

Every slice that touches the relevant path must preserve these assertions:

1. After each committed logging transaction,
   `max(changeLogStream.watermark) === replicationState.stateVersion`.
2. Replica data, `_zero.changeLog2`, stream rows, and state version commit or
   roll back together.
3. Commit is not forwarded as live until the canonical SQLite commit has
   completed in the steady-state writer-before-forward topology.
4. Migration catchup does not pin its read head until SQLite reaches the
   required forwarded head.
5. A transaction is either wholly retained or wholly purged.
6. The latest committed transaction is always retained.
7. No SQLite commit directly ACKs the upstream slot during this workstream.
8. Subscriber-ahead is a supported state for a compatible replica, not
   automatically a gap/reset.
9. Only the canonical replica writer mutates the SQLite stream log.
10. PG remains authoritative until the explicit Slice 12 gate.

Prefer executable assertions in schema/writer/reader/purger tests over relying
only on prose.

## 10. Failure and restart matrix

The integration suites should cover this matrix before canary reads:

| Failure point                                   | Required result                                               |
| ----------------------------------------------- | ------------------------------------------------------------- |
| Before begin is processed                       | No SQLite transaction or log rows                             |
| During data apply/log                           | Existing SQLite transaction rolls back                        |
| After commit row insert, before state update    | Whole SQLite transaction rolls back                           |
| After state update, before SQLite commit        | Whole SQLite transaction rolls back                           |
| After SQLite commit, before next stream message | Replica/log/state agree after restart                         |
| Source disconnect mid-transaction               | Rollback leaves no stream rows for that transaction           |
| Subscriber disconnect during catchup            | Reader snapshot closes; ACK protection is removed             |
| SQLite barrier timeout                          | Selected subscription closes; no in-place PG fallback         |
| Purge request during transaction                | Request is deferred/coalesced until a safe boundary           |
| Canonical replicator exits during purge         | Request fails/retries; no second writer takes over implicitly |
| Restore from v13 backup                         | v14 migration seeds at restored `stateVersion`                |
| Restore from v14 backup                         | Existing stream bounds and state remain consistent            |

## 11. Observability and operational gates

Use low-cardinality labels such as shard, mode, and result. Do not label metrics
by subscriber, watermark, or replica ID if that creates unbounded cardinality.

Minimum signals:

- `sqlite_change_log.write_duration`;
- `sqlite_change_log.transaction_rows`;
- `sqlite_change_log.head_lag`;
- `sqlite_change_log.retained_rows` and estimated retained bytes;
- `sqlite_change_log.reader_batch_duration` and catchup duration;
- `sqlite_change_log.barrier_wait_duration` and timeout count;
- `sqlite_change_log.purge_duration`, deleted rows, and deferred/coalesced count;
- `sqlite_change_log.compare_result` by bounded reason;
- subscriber catchup source and outcome; and
- existing replica DB/WAL size, source lag, and flow-control backlog metrics.

Alert or halt rollout on:

- any atomicity invariant failure;
- any unexplained content divergence;
- repeated required-head timeouts under healthy replication;
- unbounded retained-row/WAL growth;
- sustained commit latency beyond the Slice 0 threshold;
- purge that reports eligibility without making progress; or
- elevated reset/catchup failure rates versus PG.

## 12. Standard validation for every implementation PR

Run the narrow tests named by the slice first, preferably with coverage. Then
run the package checks required by the repository:

```bash
pnpm --filter zero-cache run test <changed-test-file> --coverage
pnpm --filter zero-cache run format
pnpm --filter zero-cache run lint
pnpm --filter zero-cache run check-types
```

For changes to shared codec behavior or process protocols, also run all directly
affected suites, including:

```bash
pnpm --filter zero-cache run test storer.pg.test
pnpm --filter zero-cache run test change-streamer-service.pg.test
pnpm --filter zero-cache run test write-worker.test
pnpm --filter zero-cache run test incremental-sync.test
```

For migration changes, run both fresh-schema and upgrade tests. For any
benchmark-affecting slice, publish the exact benchmark command and before/after
results in the pull request rather than treating a successful benchmark process
exit as sufficient evidence.

## 13. Suggested review ownership

Each PR should request review from the owners of the seam it changes:

- codec and subscriber behavior: change-streamer owners;
- schema, writer, and worker protocol: replicator/SQLite owners;
- migration: replica-schema and rollback-compatibility owners;
- purge IPC: process lifecycle and backup-monitor owners; and
- rollout gates: RMv2 lifecycle and operations owners.

The PR description should repeat its production default, enablement command,
rollback command, new metrics, and the next slice it unblocks. This keeps a
mechanically mergeable slice from being mistaken for authorization to advance
the production rollout.
