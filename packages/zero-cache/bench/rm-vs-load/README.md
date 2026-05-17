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
- `scenarios.ts` defines named load scenarios and env overrides.
- `fixtures.ts` builds protocol messages and payload shapes.
- `perf-utils.ts` keeps benchmark-only helpers out of production code.
- `types.ts` keeps result shapes stable for scripts that compare branches.

Default review run:

```bash
npm --workspace=zero-cache run perf:rm-vs-load:e2e -- --out /tmp/rm-vs-load.json
```

Use `perf:rm-vs-load` directly only when intentionally changing scenario
parameters with `ZERO_RM_VS_*` env vars.

Useful view-syncer digestion knobs:

- `ZERO_RM_VS_SUBSCRIBERS=4|8` changes the number of live VS consumers.
- `ZERO_RM_VS_APPLY_MODE=direct|worker-message|worker-batch` makes each
  simulated VS parse and apply the downstream stream into its own SQLite
  replica. `worker-message` models the old production shape of one
  write-worker handoff per replication message; `worker-batch` models the
  transaction-batched write-worker path.
- `ZERO_RM_VS_APPLY_CLIENTS=1` is the legacy shorthand for
  `ZERO_RM_VS_APPLY_MODE=direct`.
- `ZERO_RM_VS_TRANSPORT=websocket` routes each simulated VS through the same
  stringified WebSocket stream and ack protocol used between the change-streamer
  and serving replicas. The default `in-process` mode is faster and useful when
  isolating SQLite apply cost.
- `ZERO_RM_VS_WS_ACK=per-message|cumulative` compares the deploy-safe
  one-ack-per-frame stream shape against the cumulative ACK experiment.
- `ZERO_RM_VS_WS_BATCH_MESSAGES=N` controls how many queued downstream messages
  a WebSocket transport frame can carry. `1` models the old one-frame-per-change
  stream shape.
- `ZERO_RM_VS_WORKER_BATCH_MESSAGES=N` caps the worker batch size for very
  large upstream transactions.
- `ZERO_RM_VS_CLIENT_CPU_US=N` burns `N` microseconds per downstream message to
  model client/query work sharing the VS event loop.
