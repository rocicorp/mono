# `Bound should be set` — Take over UnionFanIn is not yield-safe

**TL;DR:** A `'yield'` landing inside a `Take` maintenance fetch that reads
through a `UnionFanIn` corrupts the take's window state. The next edit through
that pipeline trips `assert(takeState.bound, 'Bound should be set')` and kills
the whole client group. Needs an `OR` containing a flipped `whereExists` plus a
`limit`. Present on `main` and in 1.9.0.

## Prod symptom

```
"errorMsg": "Bound should be set",
  at assert                       shared/src/asserts.ts:3
  at Take.#pushEditChange         zql/src/ivm/take.ts:448
  at Take.push                    zql/src/ivm/take.ts:249
  at pushAccumulatedChanges       zql/src/ivm/push-accumulated.ts:181
  at UnionFanIn.fanOutDonePushing zql/src/ivm/union-fan-in.ts:209
  at UnionFanOut.push             zql/src/ivm/union-fan-out.ts:32
```

`pipeline-driver.ts:1470-1480` logs and rethrows, so every client on the group
disconnects, and it re-arms on rehydrate.

## Pipeline shape

`builder.ts:449` builds `UnionFanOut`/`UnionFanIn` only for an `OR` that
contains a flipped correlated subquery. `builder.ts:374` puts `Take` directly
above it:

```
source -> [Skip] -> UnionFanOut -> [filter branch | flipped-exists branch(es)]
       -> UnionFanIn -> Take -> related(...) joins
```

Join flipping is **cost-based** (`planner-graph.ts:246` enumerates 2^n flip
patterns), so whether the union is built at all depends on SQLite table stats.
The crash appears and disappears as data grows, and will not reproduce on a
small dev dataset unless the flip is forced with `{flip: true}`.

## Minimal repro — 2 operations

Seed `c1(lastMessageAt: null, mode: 'b')`, `c2(null, 'b')`.
Query: `or(mode = 'a', exists(messages where body = 'x', {flip: true}))`,
`orderBy lastMessageAt desc, id desc`, `limit 1`.

1. `+message(chatId: c1, body: 'x')` — c1 starts matching via the exists branch
2. `c1.lastMessageAt → 75` — the ordinary "a message arrived" update

Fires on the first yield schedule tried, on both source implementations.

## Root-cause isolation

2,000 yield schedules per cell, counting `Bound should be set`. Numbers are
**identical** on `MemorySource` and the SQLite `TableSource` — the defect is in
the shared IVM layer; the source only decides whether yields happen at all.

| Yield injected into                          | Failures / 2000 |
| -------------------------------------------- | --------------- |
| fetch + push, both tables                     | 985             |
| **fetch only**, both tables                   | **1083**        |
| **push only**, both tables                    | **0**           |
| fetch, **parent (`chat`) source only**        | 991             |
| fetch, **child (`message`) source only**      | **0**           |
| fetch/parent, **during hydration only**       | **0**           |
| fetch/parent, **after hydration only**        | 1000            |
| *control:* no flip → no union                 | **0**           |
| *control:* no subquery at all                 | **0**           |

Four necessary conditions:

1. the yield is in a **fetch** stream (never push)
2. in the **parent** source (never the flipped join's child)
3. **after** hydration — inside a maintenance fetch issued while handling a push
4. with a **flipped exists in the OR**, i.e. `UnionFanIn` on the fetch path

## Where the fix belongs

**`UnionFanIn`, not `Take`** — the read path, despite the assert firing in
`Take`. Start with `UnionFanIn.fetch` / `mergeFetches` in
`zql/src/ivm/union-fan-in.ts`. Two specifics to look at (neither confirmed as
the defect):

- `mergeFetches` is **one-ahead**: it calls `iter.next()` on the min branch
  before yielding `minNode`, so a `'yield'` from that call reaches the consumer
  ahead of the node already pulled.
- `UnionFanIn.fetch` is not a generator — it eagerly creates every branch's
  iterable, and `mergeFetches`'s `finally` calls `iter.return?.()` on all of
  them when the consumer breaks early. `Take`'s maintenance fetches break after
  at most one node, every time.

## What this is NOT

- **Not #6121** (zqlite NULL cursor-bound lowering). That, plus #6184 and
  #6189, shipped in 1.9.0. `optional` is derived truthfully on `main`
  (`lite.ts:121`, `nullableUpstream`). No NULL bound is needed here.
- **Not #6122** (edit onto an empty *hydrated* window; closed unmerged).
  Hydration-only yields never fail — the take hydrates correctly and is then
  corrupted.
- **Not schema drift.** Reproduces with a consistent source and a truthful
  schema.

`assert(takeState.bound, ...)` at `take.ts:448` was deliberately left in as a
tripwire when #6122 was closed ("push and fetch are allowed to disagree is a
dangerous precedent"). It is doing its job — this is a producer bug #6121 did
not cover. Its siblings at `take.ts:597` / `take.ts:649`
(`newBoundNode`/`afterBoundNode must be found during fetch`) fire from the same
defect.

## Reproducing

```bash
# minimal repro (both sources)
pnpm --filter zql            run test take-union-bound.repro
pnpm --filter zqlite-zql-test run test take-union-bound.repro

# root-cause isolation table
cd packages/zql && SWEEP=1 npx vitest run take-union-yield-mode.diag --disable-console-intercept

# broad sweep: SWEEP_LEN / SWEEP_YIELD / SWEEP_YIELD_MODE / SWEEP_SHAPES /
# SWEEP_SEEDS / SWEEP_DIRS / SWEEP_LIMITS / SWEEP_OPS / SWEEP_CONTINUE
SWEEP=1 SWEEP_LEN=2 SWEEP_YIELD=4 pnpm --filter zqlite-zql-test run test take-union-empty-window
```

Files: `packages/zql/src/query/take-union-*.test.ts`,
`packages/zql/src/ivm/test/mode-yield-source.ts`.

Sweep scale for reference: zqlite depth-2 with 4 yield schedules =
620,928 runs, 1,888 failures, 1,397 of them `Bound should be set`, spread
across all 7 flipped shapes; the non-flipped control shape stayed at 0.
Without yields, 3M+ zqlite runs at depth 3–4 found nothing.
