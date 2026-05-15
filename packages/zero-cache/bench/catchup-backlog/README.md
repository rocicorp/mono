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
- `memory.ts` measures heap growth for the outage-recovery queue burst.
- `payload.ts` owns the synthetic change payload used by both probes.
- `scenarios.ts` defines the load matrix: small reconnect, baseline backlog,
  larger row payloads, slow downstream consumption, and 16 concurrent VS
  reconnects, including an outage-recovery shape where 16 VSs catch up while
  downstream consumption is still under load.
- `report.ts` owns the ASCII output table so benchmark mechanics and
  presentation do not blur together.

Default review run:

```bash
npm --workspace=zero-cache run perf:catchup-backlog
```

Memory probe for the 16-VS outage-recovery queue burst:

```bash
npm --workspace=zero-cache run perf:catchup-backlog:memory
```

In the benchmark output, `done ms` is when the handoff reports completion.
For the old fire-and-forget shape this corresponds to enqueue completion;
`pending at done` and `hidden ms` show how much downstream debt still remained
after that reported completion.
