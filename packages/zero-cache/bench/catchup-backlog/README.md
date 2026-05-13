# Catchup Backlog Benchmark

This benchmark isolates the handoff fixed by the subscriber backlog PR.

```text
storer catchup cursor
       |
       v
subscriber backlog  ---> downstream websocket
```

Files:

- `index.ts` is the executable entrypoint.
- `handoff.ts` models the unsafe fire-and-forget handoff and the intended
  flow-controlled handoff.
- `report.ts` owns the ASCII output table so benchmark mechanics and
  presentation do not blur together.

Golden path:

```bash
npm --workspace=zero-cache run perf:catchup-backlog
```
