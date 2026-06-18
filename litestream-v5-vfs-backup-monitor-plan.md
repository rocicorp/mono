# Litestream v5 VFS Backup Monitor Plan

## Goal

Move Zero's replication-manager backup cleanup logic from the Rocicorp
Litestream v3 fork watermark metric to a Litestream v5 backup monitor that
queries the actual S3 backup through the Litestream SQLite VFS.

The key correctness property is that changelog pruning is based on the
`_zero.replicationState.stateVersion` that is actually readable from the
durable backup, not on a metric, local state sample, or inferred txid mapping.

## Recommendation

Use a long-lived out-of-process Node helper based on the working POC.

The helper process loads the Litestream VFS extension once, opens the S3-backed
SQLite database through `vfs=litestream`, and periodically queries
`_zero.replicationState`.

This keeps the native VFS extension out of the change-streamer process in
production, while avoiding the repeated page-index setup cost of spawning
`sqlite3` for every probe. In `SINGLE_PROCESS` dev mode, the helper can run
in-process through the existing `childWorker()` behavior, which matches the POC
and is acceptable for local development.

Keep the `sqlite3` CLI approach as a fallback/debug path, not the primary
design.

## Design

Keep two backup monitor modes:

- Litestream v3: keep the current fork metric path that parses
  `litestream_replica_progress`.
- Litestream v5: add a VFS helper worker that opens the backup directly from S3
  and queries Zero's replication state.

The v5 monitor should not use Litestream txids for pruning. It may record
`litestream_txid()` for logs or debugging, but the cleanup watermark should be
the `stateVersion` read from `_zero.replicationState`.

The production topology should be:

```text
change-streamer process
  VfsBackupMonitor
    owns reservations, cleanup delay, metrics, and scheduleCleanup()
    requests "read backup watermark" from the helper

forked VFS helper process
  sets LITESTREAM_REPLICA_URL
  loads litestream-vfs once
  keeps one readonly VFS SQLite connection open
  returns {watermark, writeTimeMs, txid, lagSeconds, observedAt}
```

## VFS Helper Worker

Add a worker entrypoint under `packages/zero-cache/src/server/` or
`packages/zero-cache/src/workers/`, following the existing `runWorker()` /
`childWorker()` pattern.

The POC shape is the right primitive:

```ts
const sqlite = new Sqlite3Database(':memory:');
sqlite.loadExtension(config.litestream.vfsExtensionPath);

const db = new Database(lc, `file:zero-backup.db?vfs=litestream&mode=ro`, {
  readonly: true,
});
```

Before loading the extension, set the child process environment:

- `LITESTREAM_REPLICA_URL`: the effective v5 backup URL, including endpoint
  parameters when configured.
- `LITESTREAM_LOG_LEVEL`: normally `WARN` or `ERROR`.
- `LITESTREAM_LOG_FILE`: optional, to avoid native VFS logs mixing with
  zero-cache JSON logs on stdout.

The helper should load the VFS exactly once per process. It can either:

- keep the `:memory:` loader connection open for the worker lifetime, or
- close it only after verifying that the extension remains registered for new
  connections on all supported platforms.

The helper should keep the VFS `Database` connection open for the worker
lifetime. That lets the VFS retain its page index and page cache across polls.

Use a prepared statement:

```sql
SELECT
  stateVersion AS watermark,
  writeTimeMs,
  litestream_txid() AS txid,
  litestream_lag() AS lagSeconds
FROM "_zero.replicationState";
```

Parse the result with valita and return:

- `watermark: string`
- `writeTimeMs: number | null`
- `txid: string`
- `lagSeconds: number`
- `observedAt: Date`

The worker should close the VFS database in a `finally` block in `run()`.
`stop()` should only signal the `RunningState`; it should not close the database
while a query may be in progress.

## IPC

The change-streamer process should own cleanup decisions. The helper should only
read backup state.

Add request/response IPC messages with request IDs:

- request: read the current backup watermark
- response: successful probe result or structured error

The monitor should enforce a response timeout. On timeout or worker failure,
cleanup is skipped and `replica.purge_blocked` is incremented. The process
manager can restart the helper, or the monitor can recreate it, but no failure
path may advance cleanup.

## Monitor Behavior

Add a v5 monitor in the change-streamer process that reuses the existing safety
model:

- snapshot reservations pause cleanup
- cleanup delay remains in force
- cleanup advances monotonically
- failed backup checks increment `replica.purge_blocked`
- cleanup is skipped, not guessed, when the probe fails

On each interval:

1. Request the latest backup watermark from the VFS helper.
2. If the returned watermark is newer than the last scheduled watermark, record
   it with `observedAt`.
3. After reservation and cleanup-delay checks, call
   `changeStreamer.scheduleCleanup(watermark)`.
4. Keep `writeTimeMs`, `txid`, and `lagSeconds` for logs and metrics.

The backup lag metric should use the backed-up row's `writeTimeMs` compared to
the probe observation time. This is more direct than the v3 metric timestamp.

## Optional Forced Sync

The first version should be VFS-only: it observes whatever Litestream has
already made durable in S3. This is correct but cleanup lags by the normal
Litestream replication cadence.

If lower cleanup lag matters, add an optional mode that runs:

```text
litestream-v5 sync -wait -json -socket <socket> <replica.db>
```

before the VFS probe. Even then, pruning should still be based on the watermark
queried through the VFS after the sync, not on the txid returned by `sync`.

## Config

Add v5-specific config, defaulting to the current v3 behavior:

- `litestream.backupUsingV5`
- `litestream.vfsExtensionPath`, default `/usr/local/lib/litestream-vfs.so`
- `litestream.vfsProbeIntervalMs`, default around 30000
- `litestream.vfsProbeTimeoutMs`
- `litestream.vfsLogFile`, optional
- optional `litestream.v5ConfigPath`
- optional debug fallback: `litestream.vfsProbeExecutable`, default `sqlite3`

Keep `restoreUsingV5` separate from v5 backup mode. During migration, restore
and backup may be enabled independently.

## Packaging

Update `packages/zero/Dockerfile` to:

1. Use Litestream `0.5.12` for the v5 binary.
2. Build or copy the matching `litestream-vfs.so` loadable extension.
3. Keep the current v3 Rocicorp fork binary until the migration is complete.
4. Copy both v3 and v5 Litestream config files into the image.
5. Optionally install the Alpine `sqlite` package for the CLI fallback/debug
   probe.

The VFS extension should be version-pinned with the v5 Litestream binary.

## Rollout

1. Ship code with v3 backup as the default.
2. Enable v5 restore everywhere first, so rollback targets can restore v5-format
   backups.
3. Add v5 backup config that writes to a new S3 prefix. Do not mix v3 WAL-format
   backups and v5 LTX-format backups in the same prefix.
4. Canary v5 backup plus the VFS monitor on one shard or deployment.
5. Verify:
   - late joiners restore and catch up
   - the VFS monitor reads the expected watermark
   - cleanup only advances after the VFS-visible watermark advances
   - probe failures block cleanup
6. Roll v5 backup out broadly.
7. After the rollback window passes, remove the v3 fork binary, fork-only
   config, and `litestream_replica_progress` parsing path.

## Tests

Add unit tests for:

- VFS helper startup environment
- VFS extension loading path
- watermark row parsing
- helper request/response IPC
- helper timeout behavior
- helper process failure blocking cleanup
- endpoint handling in `LITESTREAM_REPLICA_URL`
- v5 monitor cleanup scheduling
- reservation and cleanup-delay behavior
- probe failures blocking cleanup

For integration coverage, start with a fake helper worker. If feasible, add a
real local-file Litestream VFS integration test later.

After code changes, run the relevant package checks:

```text
pnpm --filter zero-cache run format
pnpm --filter zero-cache run lint
pnpm --filter zero-cache run check-types
pnpm --filter zero-cache run test --coverage
```
