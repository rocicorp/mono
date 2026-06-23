# Replication Manager v2 — Incremental Build Path

> **Status:** Planning. Authored 2026-06-23.
> **Scope:** Engineering build path for RMv2 (multi-RM availability, no PG change-log).
> **Companion:** the RMv2 architecture doc (goals, terminology, Fork/Resumption/Auto-Reset semantics). This document is the _how/when_, grounded in the current code, not the _what/why_.

## 1. Goals (recap)

RMv2 exists to:

- **Availability** — run multiple replication-managers concurrently for crash/eviction failover.
- **Cost** — enable AWS Spot + Karpenter bin-packing by removing the restrictive single-RM PDB.
- **Throughput / stability** — remove the intermediary Postgres change-log, a consistent bottleneck and outage source.

The two structural changes that deliver this: **(1)** the change-log moves from a globally-durable Postgres DB to a per-RM ephemeral SQLite DB, and **(2)** the upstream replication slot is ACKed from the **litestream backup watermark** rather than from change-log durability.

## 2. Current state (RMv1) — what we build on

A surprising amount of the RMv2 substrate already exists. Phases below are mostly _extend-and-repoint_, not greenfield.

### 2.1 Process composition

- Dispatcher `server/main.ts` forks workers. A process is a **replication-manager** when
  `runChangeStreamer = changeStreamerMode === 'dedicated' && changeStreamerURI === undefined`
  (`server/main.ts:104`); otherwise it's a **view-syncer** that connects to a remote change-streamer.
- RM forks: change-streamer worker (`server/change-streamer.ts`), replicator in `'backup'` mode (`server/replicator.ts`), litestream backup subprocess, optional shadow-syncer. `numSyncWorkers = 0`.
- View-syncer forks: replicator in `'serving'` mode + N syncer workers.
- **Data flow:** PG WAL → change-source (logical-replication slot, in change-streamer process) → change-streamer stores to PG change-log + forwards → backup-replicator (subscriber, `mode='backup'`) applies to the SQLite replica file → litestream backs the replica up to S3. View-syncers are also subscribers (`mode='serving'`) that restore from a backup then catch up + stream live.

### 2.2 Change-log storage (Postgres) — the thing RMv2 removes

- Schema `{app}_{shard}/cdc` (`types/shards.ts` `cdcSchema()`); tables in `change-streamer/schema/tables.ts`:
  - `changeLog(watermark TEXT, pos INT8, change JSON, precommit TEXT, PK(watermark,pos))`
  - `replicationState(lastWatermark, owner, ownerAddress, lock PK CHECK(lock=1))` — handoff/ownership.
  - `replicationConfig(replicaVersion, publications TEXT[], resetRequired BOOL, lock …)` — compatibility + the auto-reset flag.
  - `tableMetadata`, `backfilling`.
- Versioned migrations (`change-streamer/schema/init.ts`), currently v6, run under `pg_advisory_xact_lock(hashtext('migrate-schema:…'))`.
- **Purge** (`change-streamer-service.ts` `#purgeOldChanges` / `storer.ts` `purgeRecordsBefore`) is a bulk `DELETE` gated by **both** the backup watermark (`scheduleCleanup`) and the earliest subscriber ACK (`forwarder.getAcks()`), keeps the latest row, and aborts if `owner` changed.
- **Reservation against purge** while a subscriber restores: `PurgeLock` (`FOR SHARE` on the first `changeLog` row, `storer.ts:989+`) and the `/snapshot` reservation map (`vfs-backup-monitor.ts`).
- **Flow control:** `Broadcast` consensus (majority + padding) to subscribers; `Storer` heap-proportion backpressure buffer to absorb PG slowness (`storer.ts:344-382`); socket-flush threshold to upstream.

### 2.3 Replication slot lifecycle + the `replicas` table (already exists!)

- `change-source/pg/schema/shard.ts` defines upstream **`{app}_{shard}.replicas`**: `id` (PK, `Date.now()`/uuid), `rank` (BIGSERIAL), `slot`, `version` (= replicaVersion = **generation**), `initialSchema`, `initialSyncContext`, `subscriberContext`.
- **Slot pool**: `{app}_{shard}_a/_b/…` (`replication-slots.ts` `slotPoolSuffix`). Slot creation under `pg_advisory_xact_lock(hashtext('replication-slot-management:…'))` (`replication-slots.ts:139`) — **advisory-lock coordination already exists**.
- **Generation** = `consistent_point` LSN of the slot at creation, converted via `lsn.ts` `toStateVersionString()` → lexicographically-sortable watermark. Also the initial row version for all rows.
- **Resync** today: `createReplicaAndSlot` again (new rank, new version), then `dropOldReplicasAndSlots(rank < currentRank)` drops older replicas + inactive slots (retries draining every 30s).
- **Auto-reset** today: `markResetRequired()` sets `cdc.replicationConfig.resetRequired = true`; next startup throws `AutoResetSignal`.
- **Slot ACK** today: change-source `Acker` (`change-source.ts:582-642`) advances `confirmed_flush_lsn` from **downstream change-log durability** (status acks from the storer). **This is what RMv2 repoints to the backup watermark.**

### 2.4 Litestream (v3 today, v5 landed)

- v3: `litestream replicate` child process (`litestream/commands.ts`), poll the rocicorp-fork `/metrics` `litestream_replica_progress{watermark}` gauge (`litestream3-backup-monitor.ts`), verify vs S3 object times. ~15-min interval.
- **v5 landed** (commits `2adca291b`, `448862949`, `df604304d`): `backupUsingV5` / `restoreUsingV5` / `executableV5` flags, VFS extension path + probe interval. **VFS backup-watermark reader** — a forked `backup-watermark-reader` process (`server/backup-watermark-reader.ts`) opens the backup read-only (`file:zero-backup.db?vfs=litestream&mode=ro`) and reads `_zero.replicationState` → `{stateVersion(watermark), writeTimeMs, litestream_txid(), litestream_lag()}` (`vfs-watermark-reader.ts`), polled by `VfsBackupMonitor` (default 30s). `shouldStartWorker()` (`types/processes.ts:179`) fixes single-process forking.
- Gates (`litestream/normalize.ts:45-62`): `backupUsingV5 ⟹ restoreUsingV5 ⟹ executable flipped to v5`.
- **Gap:** the backup watermark currently feeds **change-log purge only**, never the slot ACK.

### 2.5 Subscriber protocol (mostly stays)

- `/replication/v{n}/snapshot` (`change-streamer-http.ts`) returns `SnapshotStatus{backupURL, replicaVersion, minWatermark}` (`snapshot.ts`); the open WS is the purge reservation (`vfs-backup-monitor.ts:165-171`).
- `/replication/v{n}/changes` streams `Downstream = ['status',…] | ChangeStreamData | ['error',{type}]`; errors `WrongReplicaVersion(1)` / `WatermarkTooOld(2)` trigger restore-and-retry. `SubscriberContext{protocolVersion, taskID, id, mode, replicaVersion, watermark, initial}`.
- The RM's own backup-replicator (`replicator/incremental-sync.ts`) subscribes via the same `/changes` with `mode='backup'`.
- `changeStreamerMode: 'discover'` + `replicationState.ownerAddress` is the seed for multi-RM discovery.

## 3. The dependency spine (why the ordering is fixed)

**(A) The slot-ACK source is the linchpin.** RMv2's "no PG change-log" only works once the slot is ACKed from the backup watermark instead of change-log durability. Until the ACK is repointed, the change-log can't be removed. → **Phase 2 first.**

**(B) SQLite change-log and the new handoff are inseparable.** The PG change-log is globally shared and single-`owner`. Once it's process-local SQLite, an incoming RM can't read the outgoing RM's log to catch up — it must initialize via slot+backup (Resumption/Fork). And concurrent RMs (Fork) each need their own log → SQLite. You cannot ship SQLite-change-log with RMv1 handoff, nor Fork with a shared log. **They cut over together (Phase 6).** _(Confirmed with stakeholder.)_

**(C) Cross-sibling watermark identity** makes multi-RM serving coherent: replicas of the same generation replicate the same WAL, so their commit watermarks and change-log entries are byte-identical. Therefore any RM of generation G can catch up any subscriber of generation G — this is what enables view-syncer failover and Fork (poll any sibling slot `confirmed_flush_lsn ≥ new_slot_start` ⟹ that sibling's backup is ≥ new*slot_start, since ACK happens \_after* backup).

### Load-bearing invariants to encode + test

- **Ready invariant:** `backup_watermark ≥ slot.confirmed_flush_lsn` always (ACK after backup).
- **Resumption ordering:** take over the slot _first_, then restore the backup (taking the slot stops further ACKs, so the backup is guaranteed ≥ slot).
- **Active-replication ordering:** backup _first_, then ACK.
- **Ownership = active slot subscription** (PG-native exclusivity), replacing `replicationState.owner` + forceful takeover.

## 4. Build path at a glance

Two tracks converge. Track 1 (v5 rollout) is the prod-enablement critical path; Track 2 (RMv2 mechanism) develops/merges in parallel behind flags, _enabled_ only after Track 1.

| #   | Phase                                                         | Delivers                                                                                     | Gated by      |
| --- | ------------------------------------------------------------- | -------------------------------------------------------------------------------------------- | ------------- |
| 1   | Finish litestream v5 rollout                                  | Frequent LTX backups + VFS watermark in prod                                                 | (in progress) |
| 2   | Backup-driven slot ACK + internalized keepalives              | Proves invariant (A); slot advances from backup                                              | 1             |
| 3   | `replicas` table v2 schema migration                          | epoch/stage/deadline/backupURL columns; publish `replicas`                                   | —             |
| 4   | SQLite change-log store                                       | Per-RM ephemeral change-log component                                                        | —             |
| 5   | Init state machine (Fork/Resume/Init-sync/coordinated-resync) | The "brain"; stage transitions; deadline heartbeat                                           | 3             |
| 6   | **Cutover** (single active RM on new substrate)               | PG change-log gone; SQLite + backup-ACK + Resumption handoff                                 | 2,4,5         |
| 7   | True multi-RM concurrency                                     | Fork, concurrent serving, view-syncer failover, background cleanup → **availability + cost** | 6             |
| 8   | Epoch (manual resync) + Invalid-stage auto-reset              | User-controlled resync; reset without PG signal                                              | 5,6           |
| 9   | Decommission                                                  | Remove PG change-db, v3 litestream, forceful takeover, `resetRequired`                       | 6,7,8 stable  |

## 5. Phases in detail

### Phase 1 — Finish the litestream v5 rollout _(Track 1; mostly rollout)_

Code is landed. Remaining is staged enablement (per the arch roadmap): `RESTORE_USING_V5` (hobby→pro→BYOC→default-in-code), then flip `--litestream-executable` to v5 + `BACKUP_USING_V5` (same staging). The `normalize.ts` gates already enforce the safe order. **Prerequisite for everything** — RMv2 needs 5-30s backups + VFS watermark polling.

### Phase 2 — Backup-driven slot ACK _(de-risks invariant A inside the RMv1 substrate)_

Repoint the change-source `Acker` to advance `confirmed_flush_lsn` from the `VfsBackupMonitor` watermark rather than change-log durability. Fold in the arch roadmap's second item: move PG keepalive/status-ACK management _into_ the change-source (driven by the committed-and-backed-up watermark) and stop emitting `status` messages downstream.

- **Landable/validate:** flag `ackSlotFromBackupWatermark` (requires `backupUsingV5`). **Shadow first** — metric the backup-driven ack watermark vs the change-log-driven one; confirm convergence before flipping the real ACK.
- **Risk:** the slot now lags to backup cadence → WAL growth if backups stall. Mitigate via `max_slot_wal_keep_size` headroom + alerts on the existing `replica.backup_lag` metric; define a stall policy. PG change-log is still intact here, so the change is isolated and reversible.

### Phase 3 — `replicas` table v2 schema _(pure migration; columns inert initially)_

Add to `change-source/pg/schema/shard.ts` + a migration: `epoch INT`, `stage TEXT` (`Initializing|Ready|Invalid`), `initializationDeadline TIMESTAMPTZ`, `backupURL TEXT`, `ownerAddress TEXT` (discovery). Backfill existing rows → `epoch=0, stage='Ready'`, derive `backupURL` from config. **Add `replicas` to the metadata publication `_{app}_metadata_{shard}`** — required for Fork's LSN-advance trick (writing the new-replica row generates WAL that forces sibling slots forward). Reinterpret `ZERO_LITESTREAM_BACKUP_URL` as a **prefix**; per-replica path = `{prefix}/{shard}/{id}` (already surfaced to subscribers via `/snapshot`'s `backupURL`).

- **Landable/validate:** ships alone; inert columns; migration tested across the pg-15..18 matrix.

### Phase 4 — SQLite change-log store _(big isolated component)_

Implement `SQLiteStorer` to the existing `Storer` interface (store/catchup/purge). Differences from PG `Storer` that matter:

- **Incremental purges** in small batches via `SQLITE_ENABLE_UPDATE_DELETE_LIMIT` (replace bulk `DELETE`) so maintenance never blocks replication.
- **Time-sliced catchup reads** (arch Option 1): scan in the main loop, yield every ~64kb on WS flush. Prefer over per-catchup workers for v1; revisit only if large catchups starve replication.
- **Unified flow control:** drop the heap-proportion buffer (`storer.ts:344-382`); change-log write + subscriber dispatch share one ack-based path (SQLite write ≤ replica-apply cost).
- **Landable/validate:** unit + throughput bench vs PG `Storer` (`storer-bench.pg.test.ts` as a template); optional **dual-write shadow** (write both, serve from PG) to validate equivalence on live traffic pre-cutover.

### Phase 5 — Init state machine _(the brain)_

`initReplicaV2()` replacing the top of `initializePostgresChangeSource`, expressing: find **eligible replicas** (same epoch, latest generation) → **Fork** (active sibling slot: create new slot+replica, poll a sibling `confirmed_flush_lsn ≥ new_slot_start`, restore that backup) / **Resumption** (eligible but inactive slot: subscribe _then_ restore) / **Initial Sync** (none eligible) / **Coordinated Resync** (advisory lock → re-check → one winner syncs, losers Fork). Plus **stage transitions** (Initializing→Ready only by the active subscriber after first backup; Ready→Invalid) and the **`initializationDeadline` heartbeat**. Reuses `createReplicaAndSlot`, the slot pool, the advisory lock, and `pg_replication_slots` queries.

- **Landable/validate:** depends on the **multi-RM integration harness** (§6) — build that first. Encode the Resumption ordering + Ready invariants as tested properties. Flag-gated; not in the live path yet.

### Phase 6 — Cutover: single active RM on the new substrate _(the flip)_

Wire change-streamer to `SQLiteStorer` + backup-ACK (Phase 2) + `initReplicaV2` (Phase 5). Replace `owner`/`ownerAddress` takeover with **ownership = active slot subscription**; drop the `pg_terminate_backend`-style takeover. Keep **one active RM at a time** here — rolling restart uses **Resumption** (brief handoff gap, ~RMv1) so we validate storage + handoff + backup-ACK in prod _before_ concurrent serving. View-syncers discover the current `Ready` RM from the `replicas` row (reuse `discover` mode). Adapt `dropOldReplicasAndSlots` to drop the prior slot post-resumption.

- **Landable/validate:** master flag, staged hobby→pro→BYOC. High-risk PR but small — Phases 2-5 pre-built and pre-tested every piece. **Delivers the throughput/stability goal on its own** (PG change-log bottleneck gone).

### Phase 7 — True multi-RM concurrency _(delivers availability + cost)_

Enable **Fork** (2+ RMs serve concurrently); **view-syncer discovery + seamless failover** across `Ready` rows in `replicas` (extend `discover` to a multi-target registry with health + generation match + reconnect); **background cleanup task** in live RMs (poll `replicas` + `pg_replication_slots`; drop `Invalid`, expired-`Initializing`, and `Ready`-inactive-older-generation replicas/slots). Unlocks Spot/Karpenter + failover.

- **Risk/validate:** chaos tests (kill an RM mid-serve; concurrent startup) on the harness; verify no slot leaks; verify seamless view-syncer reconnection.

### Phase 8 — Epoch + Invalid-stage auto-reset

`ZERO_REPLICA_EPOCH` config; eligibility filtered by epoch; a bump forces coordinated initial-sync of a new generation while ignoring older epochs. Replace the `resetRequired` boolean with **Auto Reset via `stage=Invalid`** (mark Invalid while subscribed → drop slot → delete row; incoming RM re-checks stage _after_ subscribing, for the take-over-before-drop race).

### Phase 9 — Decommission

Remove the PG change-db `Storer` + cdc `changeLog` schema/migrations + `ZERO_CHANGE_DB` semantics; remove v3 litestream + `litestream3-backup-monitor.ts` + the v3 executable + the `executableV5` flag-flip scaffolding; remove `resetRequired`, `owner`/`ownerAddress`, `terminateChangeDBLockHolders`. Trails once 6-8 are stable.

## 6. Cross-cutting work

- **Multi-RM test harness (build before Phase 5):** N change-streamer instances against one PG with `fakeReplicator`/`ReplicationMessages`; simulate crash/eviction/concurrent-start. Phases 5-7 depend on it.
- **Observability:** `backup_lag` now gates the slot — promote to a primary SLO with alerting; add metrics for stage distribution, live slot count, fork/resume/resync events, catchup duration.
- **Flag strategy:** dev sub-flags (`ackSlotFromBackupWatermark`, `changeLogStore: pg|sqlite|dual`, `initV2`) collapsing into one master rollout flag at Phase 6.

## 7. Open decisions (with recommendations)

1. **Catchup reads:** time-slicing vs workers → **recommend time-slicing** for v1; revisit if large catchups starve replication.
2. **Litestream backup GC** (arch open question): no `litestream delete` → **recommend** periodic full snapshots + S3 lifecycle expiry per replica path as the interim.
3. **View-syncer discovery across RMs** (under-specified): use `replicas` as the registry (Ready rows + `ownerAddress`), client-side load-balance + failover. Worth a focused design before Phase 7.
4. **`rank` vs `(epoch, generation)`** for "latest": derive ordering from `(epoch, version)`; keep `rank` only as a tiebreaker.
5. **Backup-stall policy:** define explicitly (slot grows with lag): retention headroom + alarm vs. force-reset thresholds.

## Appendix — key file index

| Area                                  | Path                                                                                                           |
| ------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Dispatcher / RM-vs-VS decision        | `server/main.ts:104`                                                                                           |
| Change-streamer worker                | `server/change-streamer.ts`                                                                                    |
| Backup-replicator worker              | `server/replicator.ts`, `services/replicator/incremental-sync.ts`                                              |
| Change-streamer orchestration         | `services/change-streamer/change-streamer-service.ts`                                                          |
| PG change-log store (Storer)          | `services/change-streamer/storer.ts`                                                                           |
| Change-log purge                      | `change-streamer-service.ts` `#purgeOldChanges`, `storer.ts` `purgeRecordsBefore`                              |
| Forwarder / flow control              | `services/change-streamer/forwarder.ts`, `broadcast.ts`, `subscriber.ts`                                       |
| cdc schema + migrations               | `services/change-streamer/schema/tables.ts`, `schema/init.ts`                                                  |
| `/snapshot` + `/changes` HTTP         | `services/change-streamer/change-streamer-http.ts`, `snapshot.ts`                                              |
| Purge reservation (VFS)               | `services/change-streamer/vfs-backup-monitor.ts`                                                               |
| Slot lifecycle + pool + advisory lock | `services/change-source/pg/replication-slots.ts`                                                               |
| `replicas` table + shard schema       | `services/change-source/pg/schema/shard.ts`                                                                    |
| Initial sync                          | `services/change-source/pg/initial-sync.ts`                                                                    |
| Slot ACK (to repoint)                 | `services/change-source/pg/change-source.ts:582-642` (Acker)                                                   |
| LSN ↔ watermark                       | `services/change-source/pg/lsn.ts`                                                                             |
| Litestream commands (backup/restore)  | `services/litestream/commands.ts`, `normalize.ts`                                                              |
| v3 watermark monitor                  | `services/change-streamer/litestream3-backup-monitor.ts`                                                       |
| v5 VFS watermark reader               | `server/backup-watermark-reader.ts`, `services/litestream/vfs-watermark-reader.ts`, `vfs-watermark-worker*.ts` |
| Config options                        | `config/zero-config.ts`                                                                                        |
