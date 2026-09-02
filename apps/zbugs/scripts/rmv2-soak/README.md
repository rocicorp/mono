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

node scripts/rmv2-soak.ts                               # the full run
node scripts/rmv2-soak.ts --scale 0.05 --chaos none     # a two-minute smoke test
node scripts/rmv2-soak.ts --chaos all                   # adds C9 and the rollback drills
node scripts/rmv2-soak.ts --baseline                    # adds the phase-0 A/B control
node scripts/rmv2-soak.ts --help
```

Call `scripts/rmv2-soak.ts` directly rather than through `pnpm run rmv2-soak --`.
pnpm forwards the `--` itself as an argument, and `parseArgs` runs `strict`, so
the pnpm form dies with `ERR_PARSE_ARGS_UNEXPECTED_POSITIONAL`.

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

**Resource bounds (7.7)** include these samples:

- The physical disk footprint and the live/free pages of the change log.
- The upstream replication slots for this soak app.
- The footprint of each replica.
- The size of the backup.

The report also summarizes the stop reasons for purge passes. SQLite normally
reuses freed pages instead of shrinking the physical file. Thus, C9 checks that
live pages decrease after recovery. C9 does not require file truncation. A run
fails when the purger repeatedly reports `continuation: 'immediate'` and
`stopped: 'batch-limit'`.

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
covered by construction. Only a log that _just reseeded_ can sit above the
backup watermark. The replication-manager readiness gate absorbs that window:
it waits for a backup that covers the startup replica before serving followers.

The report gives that window two numbers, because one number would lie:

- **`reseedToCoveringBackupMs`** -- reseed to the first backup the vfs poller
  observes at or above the seed point. This is the window itself. Nothing can
  be served from the log before it, so it is the exposure whether or not a
  follower happens to be waiting on it. Its floor is one litestream
  `monitor-interval` plus one vfs poll interval, so it scales with those
  settings rather than being a fixed cost.
- **`reservationHoldMs`** -- what a follower actually waited, from
  `created snasphot reservation` to `reserving change-log entries since`. It is
  ~0 whenever the follower arrived after the covering backup had landed.

`reseedToConfirmMs` is also recorded, but only for reference: it spans the
replication-manager's own restart as well, so it overstates the stall by
seconds. `reservationHoldMsC3` is the control -- C3 restores against a _live_
log, and reads ~1 ms.

Every demotion and delayed confirmation is also reported with both
`minWatermark` and `seedWatermark` against the backup watermark, because
`#confirmReservations` compares `minWatermark` where `seedWatermark` would be
exact (section 1.5). A population where `seedWatermark <= backupWatermark`
consistently held is the evidence that the check can be tightened.

## Chaos matrix

`--chaos` takes ids, `none`, or `all`. The default is C1-C8, C13 and C14.

| #   | Action                                                               | Expected route                                                   |
| --- | -------------------------------------------------------------------- | ---------------------------------------------------------------- |
| C1  | SIGTERM a view-syncer (graceful drain), restart                      | `sqlite/selected`                                                |
| C2  | SIGQUIT a view-syncer (abrupt), restart                              | `sqlite/selected`                                                |
| C3  | Kill a view-syncer, delete its replica, restart                      | restore, then `sqlite`; **must not demote**                      |
| C4  | Kill mid-burst; a short outage, then one past retention              | `sqlite/selected`; stale long-gap replica discarded and restored |
| C5  | SIGTERM the replication-manager, restart                             | a valid log resumes from its own head                            |
| C6  | Delete only the change log, restart, then wipe a view-syncer replica | forced `created` reseed; the 1.4 measurement                     |
| C7  | SIGSTOP the replication-manager 30s, then SIGCONT                    | disconnect and reconnect, no data gap                            |
| C8  | SIGKILL the replication-manager mid-burst, restart                   | reconcile by _truncation_, not reseed                            |
| C9  | Stop minio for five minutes under sustained writes, then restart it  | live log pages and app-scoped slot WAL grow, then drain          |
| C10 | `readPercent` 100 -> 0, restart                                      | every route becomes `pg/percentage`                              |
| C11 | `serve` -> `compare` -> `write`, restart each time                   | the writer stays, reads stop, comparison stops                   |
| C12 | `write` -> `off`, restart                                            | does turning it off actually free the disk                       |
| C13 | C4 and C6 together                                                   | a follower already behind, meeting a reseed                      |
| C14 | Wipe the RM's whole volume (replica, litestream state, change log)   | restore from backup into a fresh generation; no follower demoted |

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

**The app identity is isolated.** Zero defaults `ZERO_APP_ID` to `zero`, which
is also what an ordinary local zbugs process uses. Sharing it means sharing the
CDC ownership row and replication slot: another RM startup can set the soak's
owner to `NULL` and terminate its change-streamer. The harness explicitly uses
`rmv2_soak`; its fixed ports already prevent two soak runs from competing with
each other.

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
- **`pg/watermark-uncovered` is not reachable from a restart**, so a run that
  reports it as `NOT EXERCISED` is describing the system rather than a gap in
  the harness. `restoreReplica` runs on every view-syncer start and
  `reserveAndGetSnapshotStatus` hands it the log's `minWatermark` before it
  subscribes, so a replica below the minimum is discarded and re-restored and
  the subscriber always reaches `/changes` at or above the minimum -- C4's long
  gap asserts exactly that conversion (`longGapOutcome`). The route belongs to
  a follower that survives a _stream_ disconnect long enough for the purge
  floor to pass its ack. SIGSTOP does not produce one: the change-streamer
  keeps a frozen subscriber registered (`continuing with N subscriber(s) still
pending`) and lets it drain on resume.
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
