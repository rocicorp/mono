# Improving RM → ViewSyncer throughput by an order of magnitude

Grounded in the measurements in `findings.md`. The 10x is pursued on three axes,
each attacking a *measured* dominant cost, not a guessed one.

## The cost model (measured)

Per logical write in the hot model, the view-syncer does, **for every affected
client-group independently**:

```
re-read change-log from SQLite ─┐
scan the query's rows (SQLite)  ├─ IVM advance    ~58 ms / client-group
advance IVM operators           ─┘
flush the CVR to Postgres          ~46 ms / flush   (co-dominant)
poke connected clients
```

Total work = **O(writes × client-groups × views)**, serialized inside a sync
worker, and — under load — periodically thrown away by load-shedding rehydration
(189 resets / 201 hydrations observed). Meanwhile the RM produces changes at
20k–95k/s (R2) and fans out linearly to healthy subscribers, so it is *not* the
limiter — until a slow subscriber trips the global flow-control gate and drags
the whole fan-out down 40×.

So there are two independent ceilings to lift:
- **A. Per-node view work** — the O(writes × client-groups) advance + CVR flush
  (caps metric 1, and makes each added view-syncer expensive).
- **B. Cross-node fan-out coupling** — the global consensus flow-control gate
  (caps metrics 2 & 3 the moment any subscriber lags).

## Levers, by measured leverage

Estimated multipliers are back-of-envelope from the measured per-stage costs;
each must be validated behind a flag with the harness before being claimed.

### A. Lift the per-node view ceiling (metric 1, and makes fan-out cheaper)

**A1 — Share advance across client-groups with identical transformed queries.**
Today 50 client-groups each independently re-read the change-log and advance
their own pipelines for the *same* org changes. Group CGs by `transformationHash`,
advance the shared pipeline **once**, and fan the resulting diff to every
subscribing CG's CVR/poke. This is the "query de-dupe / work-stealing queue"
idea in the Notion *View Syncer Catchup* notes.
- *Impact:* the hot model is the extreme — all 300 views are the same shape on
  the same org, so advance work drops ~50× (advance once, not per-CG). Realistic
  workloads with query overlap get a smaller but still large win.
- *Maps to:* metric (1) **primary**, metric (2) (shared work → adding
  view-syncers stops re-doing everyone else's advances).
- *Touches:* `view-syncer.ts` (`#advancePipelines`), `pipeline-driver.ts`,
  `snapshotter.ts` (one changelog scan per worker, shared), CVR fan-out.

**A2 — Coalesce / de-serialize CVR flushes.** CVR flush to Postgres is
co-dominant (46 ms × 768 = 35 s). Two independent wins: (a) batch the per-CG
row-record writes into fewer, larger PG transactions per flush cycle; (b) take
CVR flush off the synchronous advance path — advance + poke first, flush the CVR
asynchronously and ack the watermark on flush completion (the RMv2 pattern of
"ack on durability, not inline"). Karavil's #6071 (storer changeDB batching) is
prior art for the batching half.
- *Impact:* 2–3× on the view side (removes a co-dominant serial stage).
- *Maps to:* metric (1).
- *Touches:* `cvr-store.ts`, `row-record-cache.ts`, `view-syncer.ts` flush path.

**A3 — Stop the load-shedding thrash.** 189 pipeline resets for 201 hydrations
means the eac60e7 projection is resetting when incremental advance would still
win over the batch. Add hysteresis / amortize the projection over the whole
pending backlog rather than single pathological changes, and coordinate with the
in-flight *View Syncer Catchup* redesign so they don't fight.
- *Impact:* recovers the ~18 s wasted on rehydration → ~1.3–1.5×.
- *Maps to:* metric (1).
- *Touches:* `pipeline-driver.ts` `#shouldAdvanceYieldMaybeAbortAdvance`.

**A4 — Cut per-advance SQLite + string overhead.** The syncer CPU is 16%+ SQLite
row iteration plus row-key stringify (3%) and SQL string building (3.4%) *rebuilt
per advance*. Reuse prepared statements keyed by query shape, cache the compiled
SQL, and share the single changelog scan across the CGs advanced in a batch (ties
to A1). Karavil's #6072/#6073 (VS apply batching, worker-message batching) and
#6074 (local apply, skip the thread hop) are prior art.
- *Impact:* 1.2–1.5×, and compounds with A1.
- *Maps to:* metric (1), (3).

### B. Lift the fan-out coupling ceiling (metrics 2 & 3)

**B1 — Per-subscriber decoupled flow control.** Replace the global consensus
broadcast with a bounded per-subscriber send queue. The RM never blocks the whole
stream on any one subscriber; a subscriber whose queue fills is dropped to
change-log catchup (cheap under RMv2's local SQLite change-log) while healthy
subscribers proceed at full rate. This directly removes the measured 40× collapse
(48/s → 2000/s) from one mildly-slow subscriber.
- *Impact:* restores linear fan-out under partial degradation — up to ~40× in the
  degraded regime that occurs whenever any view-syncer briefly lags (GC, big
  advance, catchup). This is the direct fix for "adding view-syncers degrades
  throughput."
- *Maps to:* metric (2) **primary**, (3).
- *Touches:* `change-streamer/forwarder.ts`, `broadcast.ts`, `subscriber.ts`
  (already has a `ByteBackpressureGate` from bb87037 — extend it to be the
  primary control instead of the global consensus gate),
  `change-streamer-service.ts` `forwardWithFlowControl`.

**B2 — Event-driven flow-control release (kill the 1 s tick).** Even with
`padding=0`, release is gated by the 1 s `Forwarder` progress-monitor tick
(`forwarder.ts:91`). Make release event-driven (resolve the moment the
per-subscriber high-water / majority condition is met) instead of polling.
- *Impact:* removes the sub-second stalls that cap throughput whenever a
  straggler is pending; complements B1.
- *Maps to:* metric (2), (3).

**B3 — Frame the RM→VS stream + windowed ACKs.** Batch changes into
transaction-framed messages with one ACK per frame instead of per change. Reduces
per-change envelope/parse/ACK overhead and the ACK volume feeding the flow
controller. Minor at hot-model change rates (~24/s) but material for high-change
workloads and for B1/B2's accounting. Karavil's #6075 (majority-release) / #6076
(head-indexed queue) are prior art.
- *Maps to:* metric (3).
- *Touches:* `types/streams.ts`, `forwarder.ts`.

## Sequencing against in-flight work

- **RMv2** (local SQLite change-log, ack-on-backup): land first where it overlaps
  — it makes per-subscriber catchup cheap, which is the *enabler* for B1 (a
  decoupled subscriber must be able to fall back to catchup without the PG
  change-DB). Frame B1/B2 as RMv2-compatible.
- **View Syncer Catchup redesign** (drop-pipelines-and-rehydrate): A3 must be
  co-designed with it — both touch the advance-vs-rehydrate decision.
- **Karavil #6070–6076**: mine as prior art (batching, majority-release, queue
  helpers, local apply). Re-benchmark the individual perf PRs against `main` if
  picking any up; do not rebuild what RMv2 obsoletes.

## Recommended first prototypes (behind flags)

Biggest measured leverage for the stated 10x, in order:

1. **A1 (shared advance across identical transformed queries)** — single largest
   lever for hot/overlapping workloads; validate on the `--profile relational
   --model hot` sweep (expect the per-worker sustainable wps to rise toward the
   memory-backend R1 ceiling and beyond as CG-duplication is removed).
2. **B1 (per-subscriber decoupled flow control)** — single largest lever for
   fan-out; validate with `subscriber-sim.ts` slow-subscriber runs (expect fast
   subscribers to hold ~2000/s regardless of a slow peer).
3. **A2 (CVR flush coalescing) + A3 (load-shed retune)** — recover the co-dominant
   CVR stage and the rehydration thrash.

Each ships behind a config flag, is measured with the same harness
(`apps/zero-throughput` sweep for metric 1; `subscriber-sim` for metrics 2/3),
and is kept only if the measured multiplier holds. Stacked, A1×A2×A3 plausibly
reach ~10x on hot-model writes/s on real hardware, and B1×B2 restore fan-out
scaling so that added view-syncers add capacity instead of subtracting
throughput.

## How to reproduce every number here

See `runbook.md`. In short: `apps/zero-throughput` sweep for metric (1),
`packages/zero-cache/src/scripts/subscriber-sim.ts` for metrics (2)/(3), the two
reference benches for R1/R2, and `--cpu-prof` + an OTLP sink for the stage
attribution.
