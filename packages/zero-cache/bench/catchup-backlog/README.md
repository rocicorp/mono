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
- `load-model.ts` derives backlog size from live transaction rate and catchup
  duration for recovery scenarios.
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

The `16-vs-10s-1000tps` scenario assumes 16 VSs spend 10 seconds catching up
while the RM continues receiving 1000 transactions per second. The harness does
not sleep for those 10 seconds; it derives the expected live backlog
(`1000 tx/s * 10s * 3 downstream messages/tx`) and adds the assumed catchup
window to the reported and actual catchup times.

In the benchmark output, `report ms` is when the handoff reports "caught up"
from reconnect start. For the old fire-and-forget shape this corresponds to
enqueue completion after the catchup replay; `pending@report` and `false ms`
show how much downstream debt still remained after that reported completion.
