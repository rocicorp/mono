# RM vs Load Benchmark

This folder is the reproducible e2e harness for storer/changeLog performance
PRs. It exists so future perf work extends one shared scenario shape instead of
inventing a new local script for every hypothesis.

```text
one replication-manager
        |
        v
   Storer/changeLog  ---->  N live view-syncers
        |
        `---------------->  optional reconnect catchup
```

Files:

- `e2e.ts` pins the PR-review scenario: 1 RM, 16 view-syncers, heavy load.
- `index.ts` owns the runner and emits JSON summaries.
- `protocol.ts` isolates WebSocket/protocol experiments from the load driver.
- `scenarios.ts` defines named load scenarios and env overrides.
- `fixtures.ts` builds protocol messages and payload shapes.
- `perf-utils.ts` keeps benchmark-only helpers out of production code.
- `types.ts` keeps result shapes stable for scripts that compare branches.

Default review run:

```bash
npm --workspace=zero-cache run perf:rm-vs-load:e2e -- --out /tmp/rm-vs-load.json
```

The default `e2e` wrapper focuses on the steady-state pressure point this
folder is meant to protect:

```text
             6s target: 1k tx/s, 20 rows/tx

  RM -> Storer/changeLog -> 16 live VSs
          |                    |
          |                    `-> parse + websocket ACK + SQLite apply
          |
          `-> 1 reconnecting VS starts 500 tx behind during the load window
```

The headline question is not whether a quiet recovery path is faster. It is
whether the system can keep accepting writes while the live VSs are busy, and
whether a VS that fell behind can close its starting gap instead of adding
unbounded lag.

The JSON summary includes the review metrics that tend to regress when this
path gets changed: load-phase rows/s, reconnect catchup time, VS parse/apply
timings, process CPU, heap/RSS pressure, websocket bytes, websocket frames, and
ACK counts.

Use `perf:rm-vs-load` directly only when intentionally changing scenario
parameters with `ZERO_RM_VS_*` env vars.

Useful view-syncer digestion knobs:

- `ZERO_RM_VS_SUBSCRIBERS=4|8` changes the number of live VS consumers.
- `ZERO_RM_VS_APPLY_MODE=direct|worker-message|worker-batch` makes each
  simulated VS parse and apply the downstream stream into its own SQLite
  replica. `worker-message` models the old production shape of one
  write-worker handoff per replication message; `worker-batch` models the
  transaction-batched write-worker path.
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
- `ZERO_RM_VS_PROTOCOL=v6|v7|batch-json|batch-compact` changes the WebSocket
  payload shape. `v6` is the compatibility stream: one existing Downstream JSON
  message per replication message. `v7` is the production row-batch stream: the
  subscriber emits named `change-batch` messages over the same stream/ACK layer.
  `batch-json` keeps the normal downstream JSON messages but wraps several in
  one benchmark-only bounded frame and one ACK. `batch-compact` is a
  benchmark-only compact row sketch; it is useful for estimating protocol
  headroom, but this harness has to parse already-stringified messages before
  compacting them, so it is not a production encoder.
- `ZERO_RM_VS_WS_ACK=per-message|cumulative` compares the production
  one-ACK-per-frame stream shape against the lower-churn cumulative ACK mode.
- `ZERO_RM_VS_WS_BATCH_MESSAGES=N` controls how many queued downstream messages
  a WebSocket transport frame can carry. For `message-json`, this controls the
  existing stream batch size. For `v7`, it controls how many queued
  `change-batch` frames can share one stream send. For
  `batch-json`/`batch-compact`, it controls the bounded protocol frame size.
- `ZERO_RM_VS_WORKER_BATCH_MESSAGES=N` caps the worker batch size for very
  large upstream transactions.
- `ZERO_RM_VS_WAL_AUTOCHECKPOINT=N` overrides the serving-replica
  `wal_autocheckpoint` pragma applied to each simulated VS write worker. The
  default follows the production serving-replica pragma so the review scenario
  includes the same bounded checkpoint window.
- `ZERO_RM_VS_CLIENT_CPU_US=N` burns `N` microseconds per downstream message to
  model client/query work sharing the VS event loop.
