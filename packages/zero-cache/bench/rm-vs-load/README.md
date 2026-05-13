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

Golden path:

```bash
npm --workspace=zero-cache run perf:rm-vs-load:e2e -- --out /tmp/rm-vs-load.json
```

Use `perf:rm-vs-load` directly only when intentionally changing scenario
parameters with `ZERO_RM_VS_*` env vars.
