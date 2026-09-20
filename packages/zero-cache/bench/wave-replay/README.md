# Assignment wave replay: concurrent client-group baseline

This harness reproduces the measured baseline behind the current-main zero-cache
latency diagnosis: N fresh client groups each register the goblins educator
assignment page's six desired roots in one same-tick batch, against one sync
worker, and the harness records how the query wave scales with N.

The result it exists to defend is a scaling law, not an absolute time. Query-wave
wall clock grows nearly linearly with concurrent client groups while the CPU-ish
per-group query time stays flat, because every ViewSyncer on a sync worker shares
one cooperative time-slice queue. Any optimization branch should re-run this and
compare ratios, not milliseconds; the milliseconds belong to whatever machine
produced them.

## What one run does

Each client is its own bun process with its own auth token, its own in-memory kv
store and therefore its own client group. It registers, in one tick:

1. `assignment.basic`
2. `assignment.summary`
3. `assignment.roster` (ttl 30s)
4. `assignment.with_problems` (ttl 30s)
5. `problem_trackers.for_assignment` (ttl 30s)
6. `misconduct.for_assignment_count`

A fresh client group also hydrates Zero's internal `lmids` and `mutationResults`
queries, so the server logs eight pipeline additions for a six-root client wave.
`queryCount: 8` in `diagnosis-query-wave-stages` is the signal that the wave is
shaped correctly.

`run-replay-concurrency.ts` asserts wave integrity on every client: all six roots
must complete within 1 ms of each other, because one group poke commits the whole
wave and no root becomes visible before `pokers.end(finalVersion)`. A wider spread
means the run is not exercising single-poke semantics and its numbers are not
comparable, so the driver exits non-zero.

## Prerequisites

Three things have to be running before the harness does anything.

**Postgres with logical replication.** The workload needs its own database; do not
point it at a dev database you care about, because zero-cache creates replication
slots and CVR schemas in it. The fixture assumes the goblins schema, so the
cheapest correct way to get one is to copy a migrated goblins database:

```bash
docker exec goblins-postgres psql -U postgres \
  -c 'CREATE DATABASE zcbaseline TEMPLATE goblins;'
```

**The goblins transform server**, which resolves the six named custom queries.
zero-cache calls it at `ZERO_QUERY_URL` and forwards the client's Zero auth token
as `Authorization: Bearer <token>`; better-auth resolves that token against the
`session` table. It must point at the *same* database as zero-cache, otherwise
auth and rows disagree and every query comes back empty. In the goblins checkout,
`packages/server/server.bun.ts` with `DATABASE_URL` (and
`DATABASE_DIRECT_CONNECTION_URL`, `DATABASE_REPLICA_URL`, `DATABASE_TOOLS_URL`,
`FLOW_DATABASE_URL`) set to the dedicated database, `PORT=49800`, plus the dev
secret set that `goblins up server` normally injects.

**zero-cache from this checkout**, with the instrumentation commit applied.

## Rerun

```bash
# 1. seed the fixture and the per-client session tokens (idempotent)
./seed/apply-seed.sh \
  'postgresql://postgres:postgres@localhost:5432/zcbaseline?sslmode=disable' 32

# 2. start zero-cache on one sync worker, from this checkout
cd packages/zero-cache
ZERO_PORT=49700 \
ZERO_UPSTREAM_DB='postgresql://postgres:postgres@localhost:5432/zcbaseline?sslmode=disable' \
ZERO_REPLICA_FILE="$HOME/work/zcbench/replica/sync_replica.db" \
ZERO_APP_ID=zero_goblins_dev_zcbaseline \
ZERO_APP_PUBLICATIONS=zero_sync \
ZERO_ADMIN_PASSWORD=goblins \
ZERO_MUTATE_URL=http://localhost:49800/zero/push \
ZERO_QUERY_URL=http://localhost:49800/zero/queries \
ZERO_QUERY_FORWARD_COOKIES=true \
ZERO_MUTATE_FORWARD_COOKIES=true \
ZERO_WEBSOCKET_COMPRESSION=true \
ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS=4 \
ZERO_NUM_SYNC_WORKERS=1 \
ZERO_LOG_LEVEL=info \
ZERO_LOG_FORMAT=text \
NODE_ENV=development \
  node ./src/server/runner/main.ts > ~/work/zcbench/zero-cache.log 2>&1

# 3. smoke one client group before spending time on the matrix
cd bench/wave-replay
bun run-replay-concurrency.ts --clients 1 --run 0 --output /tmp/zc-smoke

# 4. run the matrix and aggregate (truncate or rotate the server log first)
bun run-matrix.ts --server-log ~/work/zcbench/zero-cache.log --output ./results
```

`ZERO_NUM_SYNC_WORKERS=1` is the load-bearing setting. This is normal
multiprocess mode with exactly one syncer child, not `SINGLE_PROCESS=1`: the
dispatcher hashes `taskID/clientGroupID` across the syncer list, so with a list of
one every client group lands on the same worker and the same time-slice queue.
`ZERO_LOG_LEVEL=info` and `ZERO_LOG_FORMAT=text` are what the extractor parses;
`debug` adds per-row logging that distorts the very intervals being measured.

Rotate the server log between matrices. `extract-stage-events.mjs` reads the whole
file and `summarize-concurrency.mjs` joins stage events to client groups by id, so
a log carrying an earlier matrix silently mixes runs.

## Layout

| Path | What it is |
|---|---|
| `client/zero-ordered-replay.ts` | One client group. Registers the wave, measures registration → first data → complete per root, prints the settle table. Imports `@goblins/zero`, so it is copied into `$GOBLINS_REPO/.tmp/` before it runs. |
| `run-replay-concurrency.ts` | Starts N clients concurrently, writes per-client output plus one run summary, asserts wave integrity. |
| `run-matrix.ts` | Runs the whole N × runs matrix, then extraction and aggregation. |
| `extract-stage-events.mjs` | Pulls `zeroEvent` payloads and `query pipeline lifecycle` context blocks out of the server log. |
| `summarize-concurrency.mjs` | Joins client settle times to server stage events by client group and computes per-N pooled medians. |
| `seed/seed-emulation.sql` | The 3,365-statement idempotent fixture: 136 students, 5 classes, 24 problems, 973 problem trackers, 973 conversations, 957 mastery assessments. All identities synthetic. |
| `seed/apply-seed.sh` | Applies the fixture and mints one session token per concurrent client. |
| `results/` | Baseline numbers and the raw per-client output they came from. |

Pass `--goblins-repo` to `run-replay-concurrency.ts` if the goblins checkout is
not at `/workspace/goblins`.

## Reading the output

`results/summary.json` carries `summary` (per-N pooled medians) and `rows` (one
row per client sample). The fields that matter:

- `queryWaveMs` — wall time of `#addAndRemoveQueries`, from the first pipeline
  addition through the CVR flush, catch-up and `pokeEnd`. This is the number that
  should grow with N.
- `queryProcessingMs` — `TimeSliceTimer` total, which stops while a ViewSyncer
  yields, so it approximates CPU spent on this group's queries. This is the number
  that should stay flat.
- `trackerHydrateMs` — hydration of `problem_trackers.for_assignment` alone.
- `queryLockWorkMs` — how long the client group's lock stayed held. It tracks
  `queryWaveMs` because the lock is held while its work is descheduled, which is
  not the same thing as contention: each client group has its own lock.
