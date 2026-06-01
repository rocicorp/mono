# RM vs Load Benchmark

This folder is the reproducible e2e harness for storer/changeLog performance
PRs. It exists so future perf work extends one shared scenario shape instead of
inventing a new local script for every hypothesis.

```text
one replication-manager
        |
        v
   Storer/changeLog  ---->  serving replica stream consumer(s)
        |
        `---------------->  optional reconnect catchup
```

Files:

- `e2e.ts` pins the PR-review scenario: 1 RM stream consumer, 1 shared serving
  replica applier, and a reconnecting consumer under heavy load.
- `index.ts` owns the runner and emits JSON summaries.
- `protocol.ts` isolates WebSocket/protocol experiments from the load driver.
- `scenarios.ts` defines named load scenarios and env overrides.
- `fixtures.ts` builds protocol messages and payload shapes.
- `perf-utils.ts` keeps benchmark-only helpers out of production code.
- `types.ts` keeps result shapes stable for scripts that compare branches.

Default review run:

```bash
pnpm --filter zero-cache run perf:rm-vs-load:e2e -- --out /tmp/rm-vs-load.json
```

The default `e2e` wrapper focuses on the steady-state pressure point this
folder is meant to protect:

```text
             15s target: 4k tx/s, 20 rows/tx

  RM -> Storer/changeLog -> live serving-replica stream consumer
          |                    |
          |                    `-> parse + websocket ACK + SQLite apply
          |
          `-> 1 reconnecting consumer starts 500 tx behind during load
              and shares the existing serving replica model
```

The headline question is not whether a quiet recovery path is faster. It is
whether one serving process can keep accepting RM changes while a reconnecting
consumer closes its starting gap instead of adding unbounded lag. Syncer workers
inside that serving process share the serving replica file, so the default
review scenario uses `ZERO_RM_VS_SUBSCRIBERS=1` and
`ZERO_RM_VS_APPLY_LIMIT=1` rather than treating every syncer worker as an
independent SQLite writer.

The JSON summary includes the review metrics that tend to regress when this
path gets changed: load-phase rows/s, reconnect catchup time, VS parse/apply
timings, process CPU, heap/RSS pressure, websocket bytes, websocket frames, and
ACK counts. It also reports insert/update/delete counts so reviewers can tell
whether a run exercised the insert-only ingestion path or the mixed row-churn
path.

Real serving-replica load is not only new rows. Use the mixed scenario when a
change might move costs between SQLite inserts, updates, deletes, and stream
flow control:

```bash
ZERO_RM_VS_SCENARIO=mixed-hot-row-churn \
  pnpm --filter zero-cache run perf:rm-vs-load:e2e -- \
  --out /tmp/rm-vs-load-mixed.json
```

That scenario keeps the same 1 RM / 1 serving-replica applier / reconnecting
consumer shape as the default review run, but the data rows follow a
deterministic 40/40/20 insert/update/delete pattern after the first row exists:

```text
  tx stream: begin -> insert/update/delete x 20 -> commit

  row churn:
    insert: creates a new active row
    update: rewrites an active row with the current tx payload
    delete: removes an active row from the update/delete target set
```

Use `perf:rm-vs-load` directly only when intentionally changing scenario
parameters with `ZERO_RM_VS_*` env vars.

Useful view-syncer digestion knobs:

- `ZERO_RM_VS_SUBSCRIBERS=4|8` changes the number of live RM stream consumers.
  Use `1` for the corrected one-serving-process topology; larger values model
  multiple independent serving replicas consuming the same RM stream.
- `ZERO_RM_VS_APPLY_MODE=direct|worker-message|worker-batch` makes each
  simulated VS parse and apply the downstream stream into its own SQLite
  replica. `worker-message` models the old production shape of one
  write-worker handoff per replication message; `worker-batch` models the
  transaction-batched write-worker path.
- `ZERO_RM_VS_APPLY_LIMIT=N` keeps all simulated stream consumers connected but
  only lets the first N apply into SQLite. The default `1` keeps the live
  serving replica applying while the reconnecting consumer exercises stream
  catchup without pretending it owns a second replica file.
- `ZERO_RM_VS_CONSUMER_RUNTIME=inline|worker` controls where the simulated VS
  websocket/parse/ACK loop runs. Use `worker` for host-shape experiments where
  each VS instance should get its own JS thread, matching deployments that run
  one VS process per vCPU. The default `inline` mode is faster to start and is
  useful for small comparisons, but it intentionally shares one benchmark event
  loop across all VS consumers.
- `ZERO_RM_VS_APPLY_CLIENTS=1` is the legacy shorthand for
  `ZERO_RM_VS_APPLY_MODE=direct`.
- `ZERO_RM_VS_TRANSPORT=websocket` routes each simulated VS through the same
  stringified WebSocket stream and ack protocol used between the change-streamer
  and serving replicas. The default `in-process` mode is faster and useful when
  isolating SQLite apply cost.
- `ZERO_RM_VS_PROTOCOL=v6` pins the benchmark to the production RM stream
  payload: one existing Downstream JSON message per replication message.
- `ZERO_RM_VS_WS_ACK=per-message|cumulative` compares the production
  one-ACK-per-frame stream shape against the lower-churn cumulative ACK mode.
- `ZERO_RM_VS_WS_BATCH_MESSAGES=N` controls how many queued downstream messages
  a production WebSocket stream frame can carry.
- `ZERO_RM_VS_WORKER_BATCH_MESSAGES=N` caps the worker batch size for very
  large upstream transactions.
- `ZERO_RM_VS_MIXED_INSERT_WEIGHT`, `ZERO_RM_VS_MIXED_UPDATE_WEIGHT`, and
  `ZERO_RM_VS_MIXED_DELETE_WEIGHT` adjust the deterministic operation mix for
  `mixed-hot-row-churn`. The default is `4/4/2`, i.e. roughly 40% inserts, 40%
  updates, and 20% deletes.
- `ZERO_RM_VS_WAL_AUTOCHECKPOINT=N` overrides the serving-replica
  `wal_autocheckpoint` pragma applied to each simulated VS write worker. The
  default follows the production serving-replica pragma so the review scenario
  includes the same bounded checkpoint window.
- `ZERO_RM_VS_SQLITE_SYNCHRONOUS=OFF|NORMAL|FULL` overrides the SQLite
  `synchronous` pragma on each simulated VS replica connection. The default
  remains `NORMAL`; use this only to measure durability/performance headroom.
- `ZERO_RM_VS_CLIENT_CPU_US=N` burns `N` microseconds per downstream message to
  model client/query work sharing the VS event loop.
- `ZERO_RM_VS_FLUSH_BYTES=N` controls how many stream payload bytes the RM can
  fan out before awaiting serving-replica flow control. The default mirrors the
  production RM threshold.
- `ZERO_RM_VS_SOURCE_APPLY=1` additionally applies every generated change into
  a local benchmark SQLite replica before storer/fanout. Leave this off for the
  default RM -> serving-replica stream benchmark; enable it only when
  intentionally measuring an extra local apply path.
