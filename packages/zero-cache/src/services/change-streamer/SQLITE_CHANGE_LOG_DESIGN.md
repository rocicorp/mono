# SQLite Change Log — Scoped Design and Rollout Slices

Status: draft for review

## 1. Scope and relationship to RMv2

This document covers one deliberately small part of RMv2: replacing the
Postgres-backed change log (`storer.ts` + `cdc."changeLog"`) with a local
SQLite change log that is maintained alongside a replication-manager's
canonical replica.

It defines:

- the SQLite schema and migration;
- writing the change stream atomically with replica application;
- catchup reads and the catchup-to-live handoff;
- bounded, incremental purge;
- a shadow-write and dual-read rollout; and
- the gates for eventually disabling and removing the PG change log.

It intentionally does **not** define:

- replica epochs, generations, or IDs;
- Initial Sync, Fork, or Resumption;
- replica-slot creation, ownership, invalidation, or cleanup;
- how backed-up watermarks are converted into replication-slot ACKs;
- cross-replica request routing; or
- CVR-in-SQLite, which has its own database and writer.

Those concerns belong to the full RMv2 design. This component has two operating
contexts:

1. **Migration:** the existing change-streamer still owns the slot and the PG
   change log remains authoritative. A replication-manager consumes that stream
   and shadow-writes the same changes to SQLite.
2. **Steady-state RMv2:** each replication-manager exclusively manages one
   replica ID, replication slot, and backup path. It applies the slot's stream to
   its replica and maintains that replica's local change log.

The local change log is scoped to a **replica ID**. It is not a globally shared
coordination mechanism, and a replication-manager does not need it to establish
ownership of or resume a replication slot. It is included in the replica's
litestream backup because it shares the replica file, but the backup and slot
are the sources of recovery truth.

The broader RMv2 routing layer must either pin a snapshot/changes session to a
compatible replica ID or explicitly guarantee cross-sibling watermark
compatibility. This document does not assume that independently running sibling
replicas have byte-identical local logs.

## 2. Goal and core invariant

For every transaction processed by a logging replication-manager:

1. apply its row and schema changes to the canonical SQLite replica, including
   `_zero.changeLog2` for IVM;
2. append its downstream begin/data/commit messages to an ordered SQLite change
   log; and
3. update `_zero.replicationState.stateVersion` to the commit watermark.

These writes happen in **one SQLite transaction**. For every externally visible
committed snapshot of a logging replica:

> `max(changeLogStream.watermark) == replicationState.stateVersion`

The invariant applies after commit, not while the writer has an open
transaction, and only to modes configured to maintain the stream log. Purge
must always retain the latest transaction so that the log remains non-empty and
the invariant remains observable.

Atomic apply + log + watermark makes every local file and litestream restore
self-consistent. It does **not** imply that every subscriber is at or behind the
restored log. Live delivery can outrun litestream backup, and sibling replicas
can be at different heads. Catchup therefore retains an explicit
subscriber-ahead state (§5.2).

## 3. Architecture

### 3.1 Reuse vs. replace

**Reused**:

| Concern                                           | Component                                       |
| ------------------------------------------------- | ----------------------------------------------- |
| Live fan-out and subscriber backlog               | `forwarder.ts`, `broadcast.ts`, `subscriber.ts` |
| Subscriber flow control                           | `broadcast.ts`                                  |
| Snapshot reservations and subscriber ACK tracking | `change-streamer-service.ts`                    |
| Worker-thread replica writes                      | `write-worker.ts`, `write-worker-client.ts`     |
| Replica apply transaction                         | `ChangeProcessor` (`change-processor.ts`)       |
| Backup-watermark verification                     | the litestream backup monitor                   |

**Replaced**:

| Old                                         | New                                                         |
| ------------------------------------------- | ----------------------------------------------------------- |
| PG persistence/catchup/purge in `storer.ts` | `ChangeLogWriter`, `ChangeLogReader`, and `ChangeLogPurger` |
| `cdc."changeLog"`                           | `_zero.changeLogStream` in the replica file                 |
| Heap-proportional PG persistence queue      | direct, flow-controlled SQLite processing                   |

### 3.2 Live fan-out: transaction streaming without transaction buffering

The local log is for catchup, not for steady-state fan-out. The service does not
buffer an entire upstream transaction before forwarding it.

Messages are processed and forwarded in lockstep:

- For `begin` and `data`, the write worker applies/logs the message in the open
  SQLite transaction, then the service immediately forwards that message.
- For `commit`, the worker appends the commit row, updates `stateVersion`, and
  commits SQLite before the service forwards the commit message.
- For `rollback`, the worker rolls back SQLite before the service forwards the
  rollback message. No rows from the aborted transaction remain in the log.

This preserves streaming for arbitrarily large transactions without an
unbounded transaction buffer. A subscriber never observes a commit watermark
before the corresponding local SQLite commit, although it can observe the
transaction's begin/data messages while that transaction is open.

There is no separate persistence queue. A slow SQLite writer or slow subscriber
flow control directly backpressures stream consumption. The benchmark in Slice
0 must validate this combined path at the target transaction and change rates;
operational monitoring must cover upstream WAL retention during stalls.

### 3.3 One SQLite file

The change log lives in the same SQLite file as the canonical replica.

- Apply + log + watermark commit atomically.
- One litestream target produces a self-consistent restore.
- There is no coordination between a replica file and an attached log file.

A separate attached database is intentionally not used. In particular, SQLite
does not provide crash-atomic commits across attached databases when WAL mode is
in use.

Incremental purge returns deleted pages to SQLite's freelist. Under a steady
workload, later inserts can reuse those pages without requiring `VACUUM`; the
file's high-water size is still determined by the largest retained window or
transaction it has seen.

## 4. Schema and serialization

### 4.1 Table and retention index

Migration **v14** (the current `CURRENT_SCHEMA_VERSION` is 13).

Name: **`_zero.changeLogStream`** (placeholder). It must remain distinct from:

- retained legacy `_zero.changeLog`; and
- `_zero.changeLog2`, the rolling latest-op-per-row index used for IVM.

```sql
CREATE TABLE "_zero.changeLogStream" (
  "watermark"   TEXT NOT NULL,
  "pos"         INTEGER NOT NULL,
  "change"      TEXT NOT NULL,
  "precommit"   TEXT,
  "writeTimeMs" INTEGER,
  PRIMARY KEY ("watermark", "pos")
);

CREATE INDEX "_zero.changeLogStream_writeTimeMs"
  ON "_zero.changeLogStream" ("writeTimeMs", "watermark")
  WHERE "writeTimeMs" IS NOT NULL;
```

`pos` is the intra-transaction sequence: begin is zero, data/schema changes are
one through N, and commit is last. `precommit` and `writeTimeMs` are populated
only for commit rows.

`change` is TEXT rather than a typed JSON column. This matches the PG change-log
lesson that JSONB cannot hold the NUL Unicode character and lets the reader
serve the stored representation without database-side JSON reserialization.

The partial `(writeTimeMs, watermark)` index makes selection of the time-based
retention floor proportional to the small set of matching commit rows rather
than the total number of retained change rows. Slice 0 measures its write cost.

### 4.2 Seed invariant

A fresh or newly migrated replica seeds a synthetic transaction at its current
`stateVersion`:

```text
(stateVersion, 0, {"tag":"begin"},  NULL,         NULL)
(stateVersion, 1, {"tag":"commit"}, stateVersion, now)
```

This keeps the log non-empty and gives it a valid minimum catchup boundary.
These rows are never sent to a subscriber starting at that same watermark.

### 4.3 Migration for existing replicas

Existing replicas have no historical raw stream to backfill. Migration creates
the table and index, then seeds the log at the current `stateVersion`. It does
not raise `AutoResetSignal`.

A subscriber older than the seed cannot be caught up from SQLite and follows
the normal too-old path. During shadow rollout the PG reader remains available
as the fallback until SQLite has accumulated the configured retention window.

### 4.4 Canonical serialization

The ingress service serializes every downstream message exactly once with
`BigIntJSON.stringify`. The same canonical string is:

- passed to the forwarder; and
- passed alongside parsed `ChangeStreamData` through the write-worker API.

`ChangeProcessor` does not currently receive raw wire bytes, so Slice 2 extends
the worker boundary explicitly rather than assuming the string is available.
The writer stores the change substring using the same
`extractChangeSubstring` scheme as `storer.ts`; the reader reconstructs the
downstream tuple using the same `toDownstream` representation.

In migration mode, the upstream WebSocket message has already been parsed. The
RM reserializes it at its ingress boundary using the same canonical serializer
as the PG change-streamer. Dual-write tests must prove that this
parse/re-serialize path is byte-identical for all supported message types,
including big integers and escaped NUL characters.

## 5. Components

### 5.1 `ChangeLogWriter`

`ChangeLogWriter` is a small extension of `ChangeProcessor`, not an independent
transaction owner. For every begin/data/schema/commit message it inserts one
row using the transaction's commit version and a dedicated stream `pos`
counter. This counter is separate from `_zero.changeLog2`'s row-operation
counter because the stream log also counts begin, commit, schema, truncate, and
backfill messages.

The commit path uses one captured `writeTimeMs` for both the commit log row and
`_zero.replicationState`. The commit row is appended immediately before
`updateReplicationWatermark` and `commit()`.

Writing is gated by an explicit `logsChangeStream` configuration. It is enabled
for the canonical replica owned by the replication-manager and disabled for
view-syncer replicas and unrelated `ChangeProcessor` uses.

### 5.2 `ChangeLogReader`

```ts
type CatchupBounds = {
  minWatermark: string;
  headWatermark: string;
};

interface ChangeLogReader {
  bounds(): CatchupBounds;

  // Reads entries after `fromWatermark` through a head pinned when read starts.
  // Every batch uses its own short SQLite read snapshot.
  read(
    fromWatermark: string,
    opts: {batchSize: number},
  ): AsyncIterable<{
    changes: Downstream[];
    lastCommitWatermark: string | undefined;
  }>;
}
```

The reader uses a separate read connection in WAL mode. It first pins
`headWatermark` from `replicationState` and validates the requested watermark,
then scans `(watermark, pos)` through that ceiling. Each batch opens its own
short snapshot and continues after the last `(watermark, pos)` pair. This avoids
a slow subscriber pinning the WAL and blocking checkpoints.

The service distinguishes three cases:

1. **Too old:** `fromWatermark < minWatermark`, or it lies within the retained
   range but the required transaction boundary is absent. Serving mode returns
   `WatermarkTooOld`; backup mode raises `AutoResetSignal` and marks reset
   required.
2. **Catchable:** `minWatermark <= fromWatermark <= headWatermark` and the
   boundary exists. Stream changes strictly after it through the pinned head.
3. **Ahead:** `fromWatermark > headWatermark`. This is valid after restoring an
   older backup and can occur when routing among compatible siblings. No catchup
   rows are sent; the subscriber becomes live and deduplicates replayed changes
   until the manager passes its watermark.

The replica generation/ID routing contract is checked before these watermark
rules. An ahead watermark is not permission to cross an incompatible replica
generation.

### 5.3 Catchup-to-live handoff

The subscriber is registered with the forwarder before the reader pins its
head. Live messages are buffered in the subscriber's existing bounded backlog
while catchup runs. After the reader reaches the pinned commit boundary,
`setCaughtUp()` drains that backlog and transitions to direct live delivery.

Registration and head pinning must be ordered so that every transaction is
either in the catchup range or in the live backlog. Existing subscriber ACKs
remain at the requested watermark until commit messages are consumed, which
also protects the unread catchup tail from purge.

For the ahead case, catchup completes without rows. Replayed live commits at or
below the subscriber's watermark are discarded by `Subscriber`; delivery
resumes when the manager advances beyond it.

### 5.4 `ChangeLogPurger`

```ts
interface ChangeLogPurger {
  // maxRows is a target. One oversized oldest transaction may exceed it so
  // that purge always makes progress without splitting a transaction.
  purgeBatch(floor: string, maxRows: number): number;
}
```

#### Retention floor

For the current replica ID:

```text
floor = min(
  timeFloor,
  verifiedBackupWatermark,
  earliestSnapshotReservation,
  earliestSubscriberAck,
  headWatermark,
)

retain everything >= floor
```

- `timeFloor` is the earliest commit whose `writeTimeMs` is at or after
  `now - retentionWindow`. If no commit is in the window, it is
  `headWatermark`.
- `verifiedBackupWatermark` ensures a replica restored from the currently
  advertised backup can catch up. Backup verification remains conservative:
  an unverifiable or stale backup blocks advancement of this constraint.
- `earliestSnapshotReservation` protects the particular backup watermark being
  restored by an in-flight `/snapshot` request, even if a newer backup appears.
- `earliestSubscriberAck` protects connected subscribers, including catchup in
  progress.
- Missing reservation/subscriber constraints are treated as positive infinity,
  not as a reason to disable time-based cleanup.

This backup constraint is about serving catchup from an advertised backup. It
is separate from the broader RMv2 rule that replication-slot ACKs are driven by
backed-up replica watermarks.

#### Progress-safe bounded prefix deletion

`SQLITE_ENABLE_UPDATE_DELETE_LIMIT` is not required. In one short write
transaction:

1. Set `effectiveFloor = min(floor, headWatermark)` so the latest transaction is
   never eligible.
2. Find the oldest eligible watermark below `effectiveFloor`. If none exists,
   stop.
3. Inspect the row at `OFFSET maxRows` within the eligible ordered prefix.
4. If no such row exists, use `effectiveFloor` as the delete ceiling; the whole
   eligible prefix contains at most `maxRows` rows.
5. If that row belongs to a later transaction, use its watermark as the ceiling.
6. If it still belongs to the oldest transaction, that transaction alone is
   larger than `maxRows`; use the next distinct watermark as the ceiling (or
   `effectiveFloor` if there is none) and delete the oversized transaction as
   one soft-limit batch.
7. Delete rows with `watermark < ceiling`.

This guarantees progress, never splits a transaction, normally deletes at most
`maxRows` rows, and exceeds the target only for one oversized oldest
transaction.

Purge uses the same single writer as replica application. Small batches run
between upstream transactions; larger opportunistic drains run only while the
writer is idle. Slice 0 measures both schedules under write and catchup load.

## 6. Implementation and rollout slices

Each slice is independently testable and can ship dark behind configuration.
Slices 1–4 are topology-independent SQLite components. Slices 5–7 integrate
them with the current architecture without taking ownership of replica
lifecycle or slot ACK policy.

### Slice 0 — Performance and progress de-risking

- Extend `replicator/sqlite-change-log-ceiling.bench.ts` with:
  - one upstream transaction per SQLite transaction
    (`sqliteTxRows === logicalTxRows` for go/no-go results);
  - both small high-frequency and very large transactions;
  - incremental purge, including an oldest transaction larger than the target
    purge batch;
  - concurrent catchup reads on a second connection; and
  - litestream v5 shipping/checkpoint pressure when available.
- Report transactions/sec, changes/sec, commit latency percentiles, WAL size,
  and catchup/purge latency.
- Choose explicit target workloads and an acceptable regression threshold
  before enabling the writer in production.

**Dependencies:** none. **Topology:** none.

### Slice 1 — Schema and v14 migration

- Add `_zero.changeLogStream`, the partial retention index, and the seed
  invariant to `schemaVersionMigrationMap`.
- Seed fresh replicas during initial state creation.
- Migrate v13 replicas by seeding at their current `stateVersion`, without a
  reset.
- Test fresh creation, v13→v14 migration, rollback/roll-forward compatibility,
  non-empty bounds, and the post-commit max-watermark invariant.

**Dependencies:** none. **Topology:** none. **Default:** inert unless writing is
enabled.

### Slice 2 — Canonical serialization and combined writer

- Extend the write-worker boundary to carry parsed `ChangeStreamData` and its
  one canonical serialized representation.
- Add `ChangeLogWriter` to `ChangeProcessor`, gated by `logsChangeStream`.
- Add a dedicated position counter and commit `precommit`/`writeTimeMs` fields.
- Test row, schema, truncate, backfill, rollback, huge-transaction, bigint, and
  escaped-NUL messages.
- Assert exact canonical parity with `storer.ts`, atomic rollback, and
  apply/log/state consistency after reopening the file.

**Dependencies:** Slice 1. **Topology:** none. **Default:** disabled.

### Slice 3 — Reader and catchup handoff

- Implement short-snapshot batched reads with `(watermark, pos)` continuation.
- Implement too-old, catchable, and ahead validation.
- Reuse the existing forwarder/subscriber backlog for the catchup-to-live
  boundary.
- Test head, middle, seed, too-old, missing interior boundary, exact head, and
  ahead requests; live arrivals during catchup; a transaction larger than one
  reader batch; and byte-identical reconstructed output.
- Add a restore-skew test: forward through `C`, restore a backup at `B < C`,
  reconnect at `C`, replay `B..C`, and verify exactly-once observable delivery.

**Dependencies:** Slices 1–2. **Topology:** none for the reader; handoff uses
existing in-process subscriber components.

### Slice 4 — Incremental purger

- Implement indexed time-floor selection and all active floor constraints.
- Implement progress-safe, transaction-aligned prefix batches with a soft limit
  for one oversized transaction.
- Run small batches between commits and idle drains behind separate tuning
  options.
- Test every floor independently; no subscribers/reservations; stale backup;
  undersized final prefix; oversized oldest transaction; latest-transaction
  protection; concurrent catchup; and repeated calls making eventual progress.

**Dependencies:** Slices 1 and 3. **Topology:** none.

### Slice 5 — Shadow-write integration

The existing PG change-streamer remains unchanged and authoritative: it owns the
slot, writes `cdc.changeLog`, serves subscribers, and drives the existing slot
ACK policy.

- Enable SQLite logging in selected replication-managers consuming the existing
  stream.
- Keep all SQLite reads dark.
- Compare committed ranges only, up to `min(pgHead, sqliteHead)`; transient head
  skew is expected.
- Emit metrics for head lag, min watermark, retained bytes/rows, write latency,
  and sampled content divergence.
- Roll back by disabling `logsChangeStream`; no source, ACK, or subscriber
  behavior changes.

**Dependencies:** Slices 2 and 4.

### Slice 6 — Dual-read and canary serving

- Add a per-replica read flag selecting PG or SQLite catchup while PG remains
  fully intact.
- Enable SQLite reads only after the local log has accumulated a full retention
  window and the corresponding replica backups contain the v14 schema.
- Keep snapshot reservations and `/changes` requests pinned according to the
  broader routing contract; do not assume an arbitrary sibling has the same
  local history.
- Compare PG and SQLite output for common committed ranges and compare reset,
  catchup-latency, and ahead-state rates.
- Canary SQLite reads, expand gradually, and retain immediate rollback to PG.

**Dependencies:** Slice 5.

### Slice 7 — PG change-log replacement gate

This slice is an integration gate, not an implementation of the full RMv2
lifecycle. PG writes can be disabled only for deployments where the broader
RMv2 system already guarantees:

- each running RM exclusively owns its replica ID and replication slot;
- the RM can establish source position from its replica/backup without the PG
  change log;
- replication-slot ACKs never advance past the replica's verified backed-up
  watermark;
- Fork/Resumption and Auto Reset do not consult `cdc.changeLog`;
- snapshot/change routing satisfies the replica compatibility contract; and
- SQLite read canaries have completed successfully for at least one release.

When those gates hold:

1. canary selected replica IDs with PG change-log writes disabled;
2. verify slot lag, backup lag, subscriber catchup, ahead handling, and reset
   rates;
3. expand gradually while retaining a flag that restores PG writes; and
4. only after the rollback window, remove `storer.ts` and drop
   `cdc.changeLog` in a separate cleanup change.

There is no same-slot “promotion” protocol in this document. Normal RMv2
rolling replacement uses Fork or Resumption from the full design; migration of
the first RMv2 replica must be specified by that rollout plan.

**Dependencies:** Slice 6 and the listed RMv2 integration contracts.

## 7. Correctness invariants

- After every committed logging transaction,
  `max(changeLogStream.watermark) == replicationState.stateVersion` and the
  applied rows correspond to the logged stream.
- Rollback leaves neither applied changes nor stream-log rows.
- Every restored SQLite snapshot is internally consistent across replica data,
  stream log, and `stateVersion`.
- Live transaction messages are not held in an unbounded transaction buffer;
  commit is forwarded only after the local SQLite commit.
- Catchup distinguishes too-old, catchable, and ahead watermarks. Ahead is a
  normal recovery/routing state, not a gap.
- Catchup output is canonical and byte-identical between PG and SQLite for the
  compared migration range.
- Purge retains the minimum required by time, advertised backup, snapshot
  reservations, subscriber ACKs, and the current head.
- Every non-empty eligible purge prefix eventually makes progress, including
  when its oldest transaction exceeds the target batch size.
- Purge never removes the latest transaction or splits a begin…commit sequence.
- A local SQLite commit never directly ACKs the upstream replication slot. Slot
  ACK durability is supplied by the full RMv2 backup contract.
- No correctness rule assumes two replica IDs have identical local retention or
  independently generated auxiliary transactions.

## 8. Decisions and remaining questions

Resolved here:

- The log shares the canonical replica file.
- Apply + log + watermark are one SQLite transaction.
- Live begin/data messages stream without waiting for transaction commit; the
  commit message follows the local commit.
- There is no separate heap-based persistence queue.
- Catchup retains the subscriber-ahead path.
- Purge uses an indexed time floor, transaction-aligned prefix deletion, and a
  soft limit for one oversized transaction.
- The rollout is shadow write → dual read → RMv2-gated PG removal. This document
  does not define a same-slot promotion.

To settle empirically or before enabling production writes:

1. Final table and index names.
2. Retention-window default (initial proposal: 60 seconds).
3. Purge target rows, between-commit cadence, and idle-drain size.
4. Slice 0 target workloads and acceptable regression thresholds.
