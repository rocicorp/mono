# RMv2 local soak

A local, reproducible end-to-end exercise of the SQLite change log in `serve`
mode against a _real_ litestream v5 backup (minio/S3), with real view-syncer
restores, disconnects and reconnects.

Implements `~/workspace/zero/plans/rmv2-local-soak-plan.md`. Section numbers
below refer to that plan.

## Why this exists, and what `zero-cache-compare` does not cover

`pnpm run zero-cache-compare` runs a single-node zero-cache in `compare` mode
with no backup URL. Its backup watermark comes from the `ReplicaPoller`, which
polls the _local_ replica, so the purge floor is pinned to the replica head and
the log is drained as fast as it fills. With the floor there, `minWatermark`
never has to cover anything, so neither the snapshot-reservation path nor the
demotion path can fire, and no follower ever restores.

This harness replaces that with a real topology:

```
docker:  postgres:16 (6434, existing)     minio (9000/9001) + mc bucket-init
         |                                 ^
         | logical replication             | s3
         v                                 |
  replication-manager  ---- litestream v5 backup ----
  port 4850  (change-streamer :4851, litestream metrics :4852)
  NUM_SYNC_WORKERS=0
  SQLITE_CHANGE_LOG_MODE=serve / read 100 / cold 100 / compare 100
         |  ws://localhost:4851/
         +--------------+--------------+
         v              v              v
      vs-0           vs-1           vs-2      each: own replica dir,
      :4860          :4870          :4880     own TASK_ID, own restore
```

Three view-syncers rather than one, so a demotion of one is visibly _not_ a
demotion of the others and the purge floor has more than one ack to be held by.

## Prerequisites

```bash
# one-time: go, then the three native binaries into .litestream/bin (gitignored)
brew install go
pnpm run build-litestream        # idempotent; --force to rebuild
```

| Binary          | Source                                   | Why                                                                                                                                              |
| --------------- | ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `litestream-v3` | `rocicorp/litestream` @ `zero@v0.0.10`   | the `PurgeLocker` branch is gated on `litestream.executable` -- the _v3_ path. Omitting it silently skips the purge lock and diverges from prod. |
| `litestream-v5` | `rocicorp/litestream` @ `v0.5.17-zero.1` | does all of the backing up and restoring                                                                                                         |
| `vfs-query`     | `mono/go`, `make build`                  | reads the backup watermark back out of S3 through the litestream VFS                                                                             |

Both litestream binaries link `mattn/go-sqlite3` via cgo, so they must be built
with the host toolchain; they cannot be lifted out of the Docker build.

Docker must be running. The soak brings up postgres and minio itself, from
`docker/docker-compose.yml` + `docker/docker-compose.minio.yml` as one project.

The zbugs database must be migrated and seeded; pass `--seed` to do it.

## Running

```bash
cd apps/zbugs

pnpm run rmv2-soak                               # the full run
pnpm run rmv2-soak -- --scale 0.05 --chaos none  # a two-minute smoke test
pnpm run rmv2-soak -- --chaos all                # adds C9 and the rollback drills
pnpm run rmv2-soak -- --baseline                 # adds the phase-0 A/B control
pnpm run rmv2-soak -- --help
```

Everything lands in `.soak/<run-id>/`: per-incarnation JSON logs under `logs/`,
each replica under its node's directory, and `report.json`. The summary is also
printed. A non-zero exit means the gate failed.

## What it checks

**The correctness gate (7.1)** is the only one that survives the cutover: for
every replica and every replicated table, at a defined transaction bound,

```
pi(replica.T)  ==  liteRow(PG.T)     as multisets keyed by a unique key
```

`liteRow` is the replicator's own PG->SQLite value mapping, so both sides are
canonicalized identically; `pi` drops `_0_version` and the `_zero.*` schema. The
bound comes from **quiescence** (7.2): writes stopped, one sentinel transaction
round-tripped through every replica, and every replica's `stateVersion` equal.
It runs after every phase _and_ after every chaos action, never only at the end
-- a final-only comparison is nearly worthless, because a later restore heals a
divergence.

Two stated blind spots: a bug _inside_ `liteRow` is invisible here, and equality
at a bound cannot see a transient wrong state that heals.

Comparing the replication-manager's replica as well as each view-syncer's
bisects for free (7.3): RM clean and followers dirty is the change-log/catchup
path; both dirty points at the replicator or initial sync.

**Coverage (7.6)** is a positive requirement, not an absence check. A soak that
quietly routes everything to PG would pass the gate while proving nothing, so
the report lists each required route with its count or an explicit
`NOT EXERCISED`.

**Resource bounds (7.7)** sample the change log's local disk, the upstream
replication slot's retained WAL, each replica's footprint and the backup's
size, and summarize purge-pass stop reasons. A run that sits chronically at
`continuation: 'immediate'` / `stopped: 'batch-limit'` is a purger losing to the
write rate.

## Where the numbers come from

Counters, histograms and gauges come from OpenTelemetry: each task exports
OTLP/HTTP JSON to an in-process receiver on its own path (`otlp.ts`). That is
the census of record -- `sqlite_change_log.catchup_routes{source,reason}`,
purge probe outcomes, compare results, log file bytes, backup lag -- and it
needs no collector.

The JSON log stream (`logs.ts`) supplies _events with a time_: when the log
reseeded and why, when a reservation opened and when it was confirmed, which
task was demoted. `ZERO_LOG_LEVEL=debug` on the replication-manager is what
makes the SQLite route visible at all (`serving <id> from SQLite catchup` is a
debug line); the view-syncers stay at `info` so the burst phase's log volume
does not perturb the timings it is measuring.

## The headline measurement

Section 1.4: **the residual wait is a reseed window, not a restore window.**

The purge floor is capped at the backup watermark, so `log.minWatermark <=
backupWatermark` holds for any log that held the history (invariant 14), and a
view-syncer restoring from a backup the replication-manager published is
covered by construction. Only a log that _just reseeded_ sits above the backup
watermark. Today that ends in a free demotion to PG; after PG is retired it
becomes a hold.

So the report separates the two populations:

- `reservationHoldMsC3` -- C3 restores against a live log, and should read ~0.
- `reseedWindowMsC6C13` -- C6 and C13 restore against a log that just reseeded.
  This is the number that converts to a view-syncer hold at the cutover.

Every demotion and delayed confirmation is also reported with both
`minWatermark` and `seedWatermark` against the backup watermark, because
`#confirmReservations` compares `minWatermark` where `seedWatermark` would be
exact (section 1.5). A population where `seedWatermark <= backupWatermark`
consistently held is the evidence that the check can be tightened.

## Chaos matrix

`--chaos` takes ids, `none`, or `all`. The default is C1-C8 and C13.

| #   | Action                                                               | Expected route                                   |
| --- | -------------------------------------------------------------------- | ------------------------------------------------ |
| C1  | SIGTERM a view-syncer (graceful drain), restart                      | `sqlite/selected`                                |
| C2  | SIGQUIT a view-syncer (abrupt), restart                              | `sqlite/selected`                                |
| C3  | Kill a view-syncer, delete its replica, restart                      | restore, then `sqlite`; **must not demote**      |
| C4  | Kill mid-burst; a short outage, then one past retention              | `sqlite/selected`, then `pg/watermark-uncovered` |
| C5  | SIGTERM the replication-manager, restart                             | a valid log resumes from its own head            |
| C6  | Delete only the change log, restart, then wipe a view-syncer replica | forced `created` reseed; the 1.4 measurement     |
| C7  | SIGSTOP the replication-manager 30s, then SIGCONT                    | disconnect and reconnect, no data gap            |
| C8  | SIGKILL the replication-manager mid-burst, restart                   | reconcile by _truncation_, not reseed            |
| C9  | Stop minio under sustained writes, then restart it                   | log bytes and slot WAL bounded, and both recover |
| C10 | `readPercent` 100 -> 0, restart                                      | every route becomes `pg/percentage`              |
| C11 | `serve` -> `compare` -> `write`, restart each time                   | the writer stays, reads stop, comparison stops   |
| C12 | `write` -> `off`, restart                                            | does turning it off actually free the disk       |
| C13 | C4 and C6 together                                                   | a follower already behind, meeting a reseed      |

`GRACEFUL_SHUTDOWN = ['SIGTERM','SIGINT']` and `FORCEFUL_SHUTDOWN =
['SIGQUIT','SIGABRT']` are genuinely different paths in `life-cycle.ts`, which
is why C1 and C2 are two actions and not the same test twice.

C10-C12 always run last regardless of the order they are requested in: C11 and
C12 leave the change log rolled back to `write` and then `off`, which is not a
state the rest of the run can continue from.

## Gotchas worth knowing

**Every zero-cache worker is its own process group.** `childWorker` forks with
`detached: true` so that SIGINT is not propagated automatically. A signal to the
dispatcher's process group therefore reaches only the dispatcher, and a
dispatcher that dies without draining leaves its change-streamer,
backup-replicator, litestream and vfs-query processes running -- still serving
view-syncers, and still holding the replica's locks. The next start then fails
several layers down with `SQLITE_BUSY: journal_mode = delete`. `node.ts` tracks
the whole process tree while the node is alive and reaps it on stop; the run
also refuses to start if any of its ports are already held.

**The shared `.env`.** `apps/zbugs/.env` sets
`ZERO_CHANGE_STREAMER_SQLITE_CHANGE_LOG_MODE='write'`. dotenvx does not override
a variable that is already set, but it _does_ inject one that is absent, so the
view-syncer environment says `off` explicitly rather than relying on omission.
(A view-syncer that inherits a writing mode now warns rather than refusing to
start, since #6412, but it still runs no log.)

**The backup path is not the configured path.**
`initializePostgresChangeSource` derives a destination backup URL whose last
segment is a generation id -- the replica fork/resumption identity -- and logs
it as `setting up backup to s3://<bucket>/<generation>`. The configured
`s3://zero-replica/zbugs` path is therefore not a prefix of anything, so the
harness owns and clears the whole bucket instead.

**`--retention-ms 0` is not a thing.** It is asserted `> 0`; it is the purger's
retention floor, not a routing knob. The dial that zeroes the warm-up wait is
`--cold-read-percent 100`, which requires `mode=serve` and a nonzero read
percentage.

**Litestream config paths.** `litestream.configPath` defaults to a path relative
to the process's cwd (`./src/services/litestream/...`), which only resolves from
`packages/zero-cache`. The harness passes absolute paths.

## Known limitations

- `snapshotBackupIntervalHours` defaults to 4, so no snapshot occurs during a
  soak and restores replay from the initial snapshot plus L0/L1. Snapshot-
  boundary restores are **not** covered; `--snapshot-hours` can be dialled down
  for a separate short run.
- `config-v5.yml` tunes its L1/L2/L3 compaction ladder against a 15s
  monitor-interval, and `--backup-interval-seconds 2` produces roughly seven
  times the L0 files. Fine for a short soak; run one pass at 15 s before
  drawing conclusions about compaction cost.
- `initFromPgChangeLog` is hardcoded `true`. The most direct answer to "what
  does the cutover feel like" is a run with it flipped to `false` behind a local
  patch; the harness is what makes that observable.
- The oracle is an end-state comparison. It cannot see a transient wrong state
  that heals -- changes served out of order, or an idempotent duplicate. The
  dark comparator and `Subscriber` dedup cover those today; post-retirement
  nothing does, which is a gap to raise rather than one this harness closes.

## Layout

| File                                    | Slice   | Contents                                               |
| --------------------------------------- | ------- | ------------------------------------------------------ |
| `../build-litestream.sh`                | L1      | the three native binaries, idempotent                  |
| `../../docker/docker-compose.minio.yml` | L2      | minio + one-shot bucket init                           |
| `../change-log-traffic.ts`              | L3      | the stage driver, the quiet phase, and the residue mix |
| `../rmv2-soak.ts`                       | L4      | the orchestrator                                       |
| `config.ts`                             | L4      | paths, ports and the environment matrices              |
| `node.ts`, `cluster.ts`                 | L4      | process lifecycle and topology                         |
| `infra.ts`                              | L2      | docker compose, minio and the S3 helpers               |
| `oracle.ts`                             | L5      | quiescence, `liteRow` canonicalization, keyed diffs    |
| `workload.ts`                           | L3      | the phase shapes                                       |
| `chaos.ts`                              | L6, L10 | C1-C13                                                 |
| `logs.ts`                               | L7      | JSON log events and tripwires                          |
| `otlp.ts`                               | L7, L8  | the OTLP metrics receiver                              |
| `resources.ts`                          | L8      | disk, slot and purge sampling                          |
| `report.ts`                             | L7, L8  | the census, the gate and the report                    |
