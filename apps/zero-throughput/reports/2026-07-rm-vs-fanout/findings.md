# RM → ViewSyncer fan-out: where the time goes

Environment: single 4-vCPU Xeon @2.10 GHz container, 15 GB RAM, local PostgreSQL
16.13, Node 24.9, Zero @ `d87c1c8`. Absolute rates are for **relative**
attribution on this box; re-measure headline ceilings on production-class
hardware (see `runbook.md`). Raw data + scripts: `results/` in the scratchpad,
harness runs under `apps/zero-throughput/results/`.

## Question

The team observed that "adding view-syncers eventually does nothing for pipeline
advancement" — extra view-syncers add hydration capacity but *degrade*
replication throughput. This report runs Matt's throughput assets
(`apps/zero-throughput`, the `zero-throughput-relational` IVM bench, and the
`sqlite-change-log-ceiling` bench), localizes the bottleneck with CPU profiles +
OTLP metrics, adds a purpose-built RM fan-out isolation harness
(`packages/zero-cache/src/scripts/subscriber-sim.ts`), and derives a 10x design
(`design-10x.md`).

Target metrics: (1) sustainable **hot-model writes/s**, (2) **fan-out scaling
efficiency**, (3) raw **RM→VS changes/s**.

## TL;DR

1. **The bottleneck is the view-syncer's per-client-group IVM advance + CVR
   flush, not the RM/replication path.** The RM can produce and fan out changes
   at 20k–95k/s; the hot workload's view side sustains ~5 logical writes/s/core.
   A 3–4 order-of-magnitude gap.
2. **Two co-dominant view-side costs**: SQLite reads/scans during IVM advance
   (~58 ms per client-group advance) and **CVR flush to Postgres (~46 ms each)**.
   Under load the **load-shedding heuristic thrashes** — 189 pipeline resets vs
   201 hydrations — burning the syncer on rehydration instead of progress.
3. **Adding view-syncers degrades throughput via a global flow-control gate.**
   A single *mildly* slow subscriber (2–5 ms/msg) collapses every other
   subscriber from 2000/s to ~48/s (40×). Fan-out to healthy subscribers is
   otherwise linear. The consensus-padding knob only partially helps because a
   coarse 1 s progress-monitor tick still gates the global broadcast.

## Reference ceilings (isolated)

### R1 — pure-IVM ceiling (relational hot, 300 views, single-threaded)

`packages/zql-benchmarks/src/zero-throughput-relational.bench.ts`. One iteration =
one logical write (insert activity + edit account + edit org) advancing **all 300
hot views** (100 users × 3 queries, all spanning the one hot org).

| backend | mode | ms/write | logical writes/s |
|---|---|---:|---:|
| zqlite (prod replica store) | push only | 192.2 | **5.2** |
| zqlite | push + flush views | 203.4 | **4.9** |
| memory | push only | 70.3 | 14.2 |
| memory | push + flush views | 73.7 | 13.6 |

→ Hot workload is **IVM-bound at ~5 writes/s/core** on zqlite. Flush adds ~5%.
The memory backend is 2.7× faster → SQLite read/scan per advance is a big share.

### R2 — RM replica-apply + change-log ceiling (single-threaded)

`packages/zero-cache/src/services/replicator/sqlite-change-log-ceiling.bench.ts`,
combined mode (replica row write + change-log append), 1 KiB payload.

| upstream tx fold | SQLite tx fold | µs/change | changes/s |
|---:|---:|---:|---:|
| 1 | 1 | 47.5 | 21,000 |
| 1 | 1000 | 14.6 | 68,600 |
| 100 | 10000 | 10.6 | 94,600 |

→ RM apply+changelog sustains **21k–95k changes/s** — 3–4 orders of magnitude
above the IVM ceiling. Batching SQLite commits ~3×'s it.

## Metric (1) — sustainable hot writes/s vs sync workers (single task)

relational hot, 50 users, 3 q/user, 50 rows/query, 45 s binary-searched runs:

| sync workers | best sustainable wps | p99 client lag @ best | maxSeqLag |
|---:|---:|---:|---:|
| 1 | 3 | 1124 ms | 5 |
| 2 | 6 | 1131 ms | 9 |
| 4 | 6 | 1110 ms | 9 |

1→2 workers = 2.0×; **2→4 = 1.0× (no gain)**. On 4 vCPUs the box is CPU-saturated
at 2 sync workers, so more workers do nothing — "adding capacity does nothing,"
reproduced at the sync-worker level. The 3 wps single-worker knee matches the R1
IVM ceiling once real CVR/WebSocket/replication overhead is layered on.

## Metric (3) — RM→VS fan-out (subscriber-sim, feed-append, writer 2000 changes/s)

Fan-out to **fast** subscribers is linear (wire is not the limit when subscribers
keep up):

| K subscribers | per-subscriber changes/s | total delivered/s |
|--:|--:|--:|
| 1 | 2000 | 2000 |
| 2 | 2000 | 4000 |
| 4 | 2000 | 8000 |
| 8 | 1976 | 15,809 |

(K=1 raw wire ceiling ~18,900 changes/s.)

**One slow subscriber stalls everyone** (K=4, 1 slow, per-message ack delay),
fast-subscriber avg changes/s (writer = 2000):

| slow delay | padding=1 (default) | padding=0 |
|--:|--:|--:|
| 1 ms | 768 | — |
| 2 ms | **48** | 412 |
| 5 ms | 48–96 | 97 |
| 10 ms | 384 | — |
| 20 ms | 1430 | 2116 |
| 50 ms | 2869 (catch-up) | — |

Non-monotonic: a *mildly* slow subscriber (2–5 ms/msg) is worst — it stays in the
"pending" set at every 64 KiB flow-control checkpoint, so the RM waits the full
consensus padding each time. `padding=0` only partially helps because
`Forwarder` runs `checkProgress` on a **1 s progress-monitor tick**
(`forwarder.ts:91`); between ticks the global broadcast is stuck on the slow
subscriber. **No padding value decouples it** — the gate is global, not
per-subscriber.

## Stage / CPU attribution (relational hot, 50 users, 2 workers, 8 wps, saturated)

Genuinely saturated (p99 lag 8.8 s, +0.96 slope). Only **1083 changes / 362 txns
in 45 s (~24 changes/s)** overwhelm the syncers.

Syncer CPU self-time (ex-idle ~30%): SQLite row iteration/exec (`zqlite/db.ts`,
`next` 10.3%) **16.3%**, IVM source reads 3.5%, pipeline-driver 3.5%, row-key
stringify 3.0%, SQL string-building (`@databases/sql`) 3.4%, JSON/bigint
serialization ~3%, `logger.withContext` 1.9%, IVM operators ~2%.

Stage timings (OTLP, cumulative):

| stage | per-op | count | total | note |
|---|--:|--:|--:|---|
| **cvr.flush-time** | 46 ms | 768 | **35.2 s** | flush CVR to Postgres — co-dominant |
| **advance-time** (per client-group) | 58 ms | 542 | **31.2 s** | advance all queries for a CG |
| hydration-time | 85 ms | 212 | 17.9 s | (re)materialize a pipeline |
| ivm.advance-time (per change) | 3 ms | 4038 | 10.2 s | single-change advance |
| cvr.load_duration | 172 ms | 28 | 4.8 s | CVR load |
| view_syncer_lag (e2e) | 5.68 s | — | — | serving lag at saturation |

**Load-shedding thrash:** `pipeline-resets = 189` vs `hydration = 201` — the
eac60e7 heuristic projects advance cost > hydration and resets pipelines on
nearly every batch, forcing constant 85 ms rehydration instead of progress.
Amplification: 24 changes/s in → **84,348 CVR rows flushed + 125,882 poke rows**
out over the run.

## Hypothesis verdicts

| # | hypothesis | verdict | evidence |
|---|---|---|---|
| a | ACK/consensus flow-control gating stalls the stream | **CONFIRMED (fan-out case)** | 1 slow subscriber → 40× collapse; 1 s tick gates even padding=0 |
| b | per-change ws envelope + per-message ACK cost dominates | **REFUTED at realistic rates** | fan-out linear to K=8 fast subscribers; wire does ~19k/s vs ~24 changes/s needed |
| c | storer PG change-DB writes are the bottleneck | **REFUTED** | R2 shows 21k–95k changes/s apply; RM idle in hot runs |
| d | VS double-parse + thread clone dominates | **MINOR** | serialization ~3% of syncer CPU; real cost is SQLite reads |
| e | per-client-group changelog re-read + IVM advance duplication | **CONFIRMED (primary)** | advance-time 58 ms/CG × 50 CGs; SQLite reads 16%+ of CPU; R1 ceiling |
| f | CVR flush to PG is expensive | **CONFIRMED (co-primary)** | cvr.flush-time 46 ms × 768 = 35 s, co-dominant with advance |
| — | load-shedding rehydration thrash | **CONFIRMED (new)** | 189 resets / 201 hydrations at saturation |

## Fan-out efficiency (metric 2) — synthesis

Two independent mechanisms cap fan-out scaling, both reproduced:
- **Per-node**: each view-syncer redoes the full O(changes × client-groups ×
  views) advance + CVR flush against its own replica; nothing is shared, so N
  view-syncers multiply total system work rather than dividing it.
- **Cross-node**: the RM's global consensus flow-control couples all subscribers
  to the slowest-in-majority; one lagging view-syncer (GC pause, big advance,
  catchup) throttles the whole fan-out and every other view-syncer with it.

A full multi-VS-task E2E harness (`--vs-tasks K`) is the recommended
production-hardware validation of the combined effect; the mechanism is already
isolated by the subscriber-sim + single-task profiling above.

See `design-10x.md` for the improvement plan.
