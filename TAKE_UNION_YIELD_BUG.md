# `Bound should be set` — Take over UnionFanIn is not yield-safe

> **STATUS: root-caused and fixed.** `UnionFanIn.#pushInternalChange` read the
> `'yield'` sentinel as a row. See [Root cause](#root-cause--found).

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

## Root cause — FOUND

`union-fan-in.ts:173`. `UnionFanIn` read the `'yield'` sentinel as if it were a
row.

```ts
const fetchResult = input.fetch({constraint});

if (first(fetchResult) !== undefined) {   // <-- the defect
  // Another branch has the row, so the add/remove is not needed.
  return;
}
```

`first()` (`stream.ts`) is `it.next().value`, generic over `T`. Here `T` is
`Node | 'yield'`, so the call is type-correct and semantically wrong: the
question is *"does a sibling branch hold this row?"*, but the answer computed
is *"was the stream non-empty?"*. `'yield'` is not `undefined`, so **an empty
branch that happens to emit a scheduling sentinel is misread as a branch that
holds the row**, and the `add`/`remove` is silently dropped.

It is not corruption while *sending* a yield — `UnionFanIn` never gets that
far. It is corruption from *reading* one, on a fetch that happens to sit on the
push path.

Walking the 2-op repro:

1. `+message(c1,'x')` — the flipped join emits `add(c1)` out of the exists
   branch. `#fanOutPushStarted` is false (the change originated *inside* the
   sub-graph), so `#pushInternalChange` runs.
2. It probes the sibling `mode='a'` branch with `constraint {id:'c1'}`.
   `c1.mode='b'`, so the branch is genuinely **empty** — but the source's
   fetch emits a trailing `'yield'`. `first()` returns `'yield'` → early
   `return`.
3. `add(c1)` never reaches `Take`. `takeState` stays
   `{size: 0, bound: undefined}` — while `Take.fetch`, reading through
   `mergeFetches`, happily reports `c1`. Push and fetch now disagree.
4. `c1.lastMessageAt → 75` enters via `UnionFanOut` → accumulate →
   `fanOutDonePushing` → `pushAccumulatedChanges` → `Take.#pushEditChange`
   → `assert(takeState.bound, 'Bound should be set')`. The prod stack,
   frame for frame.

The `remove` direction is the same defect with a quieter symptom: a spurious
yield suppresses the remove and `Take` keeps a row that no longer matches.

This accounts for all four necessary conditions above: the corrupted stream is
`input.fetch(...)` **called from within a push** (so fetch-mode yields only,
never push-mode); the sibling branches probed all read the *parent* source (so
never the flipped join's child); `#pushInternalChange` only runs during a push
(so post-hydration only); and its add/remove arm is only reachable for a change
originating inside the fan-out/fan-in sub-graph, which only a flipped join
produces (so the flip is required).

`mergeFetches` is clean. It drains `'yield'` correctly in both loops, and the
one-ahead `iter.next()` and the `finally`/`iter.return?.()` early-break path
are both benign — every consumer of `UnionFanIn.fetch`, `Take` included
(`take.ts:595`, `take.ts:649`), checks `node === 'yield'` and continues.

## The fix

`first(skipYields(fetchResult))`. `skipYields` (`zql/src/ivm/skip-yields.ts`)
is the established idiom for this hazard — `run-ast.ts:101` and
`pipeline-driver.ts:555` already carry comments about it. It preserves today's
yield-emission profile exactly, since `first()` swallowed the sentinel anyway.

`first` has exactly one importer in the repo, so the blast radius is that one
call site; its docstring in `stream.ts` now warns about the sentinel.

**Verified:**

| Check | Before | After |
| ----- | ------ | ----- |
| `take-union-bound.repro`, 400 schedules x 2 sources | fails on first seed, both | **0** |
| `take-union-yield-mode.diag`, `SWEEP=1` (10 cells x 2000) | 985 / 1083 / 1000 / 991 | **0 in all 10** |
| `take-union-empty-window`, zqlite `SWEEP_LEN=2 SWEEP_YIELD=4` (620,928 runs) | 1,888 failures | **0** |
| full `zql` + `zqlite-zql-test` suites | — | 1316 pass, 0 fail (each) |

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

## Why the fuzzer never found it

The chinook fuzzer (`packages/zql-integration-tests/src/chinook/fuzz/`) has
both a flip lane and a yield lane. They are orthogonal and never intersect, and
this bug lives in the uncovered cell — **flip x push x yield**.

1. **The yield lane never builds a union.** `checkYield` (`driver.ts`) lowers
   with `lower(s)`, which calls `.whereExists(rel, sub)` with **no flip
   option**, and the fuzz driver never runs the planner — the only mention of
   the planner anywhere under `fuzz/` is a doc comment in `flip.ts`. So `flip`
   stays undefined, every EXISTS lowers as a semi-join,
   `applyFilterWithFlips` is never entered, and no `UnionFanOut`/`UnionFanIn`
   is ever constructed in a yield-lane pipeline. Pinned by
   `take-union-shape.test.ts`.
2. **The flip lane never pushes and never yields.** `checkFlipInvariance`
   enumerates all `2^k` flip assignments but routes them to
   `checkHydrateCases` — hydrate only, straight (unwrapped) sources. This bug
   needs a post-hydration maintenance fetch during a push; hydration-only
   yields never fail (0/2000 above).
3. **The scale yield lane is hydrate-only too.** Per `checkYieldTail`'s own
   doc: "Push is mini-fixture-only ... so this scale path is hydrate-only."

Contributing: `flip` is not one of the covering-array axes at all
(`AXES = filter x exists x order x limit x start`, `axes.ts:333`), so it is
never part of t-wise coverage — it is a bolt-on lane. The backbone yield lane
also runs `enumerate({depth: 1, related: 1, exists: 1})`.

Note the oracle would not have had to catch a silent divergence here: the bug
**throws**. The fuzzer simply never built the pipeline.

### Fixed (this branch)

Three changes, because closing the cell needed all three:

1. **`flip` is now a real axis** (`axes.ts`): `FLIP_VALS = ['none', 'flip']`.
   It carries the space's only inter-axis constraint — `flip` is meaningful
   only on a *positive* gate — so `flipRealizable` / `tupleRealizable` /
   `assignmentRealizable` teach both the covering-array builder and the
   coverage report about it. Unrealizable cells are never generated and never
   counted in the denominator, so the "100%" gate stays meetable.
2. **L1 runs at strength 3** (`l1QueryCases(data, t = 3)`). Pairwise is not
   enough: a flipped gate only builds a fan-in when it sits *inside an OR*, and
   a `Take` only sits above that fan-in when the query is *limited*. That is a
   3-way interaction. Measured: at `t=2` the greedy cover realizes
   `flip x exists_or x limit` in **0** rows; at `t=3`, in **17**. Cost: 112 ->
   494 rows, 4,640 L1 cases (~35s wall on the backbone), still 100% t-wise.
3. **A new push lane** (`checkYieldPush` + `fanInTakeCases`). Coverage alone
   was not enough: the covering array only fed *hydrate* lanes, while
   `checkYield` ran decoration-free skeletons (no `limit`, hence no `Take`).
   The new lane takes the decorated L1 cases that actually build a `Take` over
   a `UnionFanIn` and drives them through the four-phase push walk with both
   IVM sources yield-wrapped. `checkYield` itself also now runs every flip
   assignment, not just the builder default.

**Result:** the backbone lane finds it. 4 failures in 48 selected cases —
one `Take: afterBoundNode must be found during fetch`, and three *silent
result divergences* against the PG oracle on
`exists_or x flip x limit=small x start=mid_exclusive` (wrong answers, no
throw — a second defect this cell was hiding).

After the `skipYields` fix that lane is at **2/48**: the two `exists_or`
divergences on `track` and `invoice` are gone. What remains is **not this
bug**, and not the yield class at all — re-running the lane with yields
forced off (`() => 1` as the wrapper's rng) reproduces both failures
identically:

- `genre | exists_top | flip | limit=large | start=none` —
  `Take: afterBoundNode must be found during fetch`. `exists_top` is not an
  OR, so `builder.ts:449` builds **no union**; the stack runs
  `FlippedJoin.#pushParent` straight into `Take.push`. A `Take`-over-
  `FlippedJoin` defect with no `UnionFanIn` involved.
- `employee | exists_or | flip | limit=small | start=mid_exclusive` — a
  silent divergence that survives with yields disabled.

Both are pre-existing flip/take defects that the new lane surfaced on its own
merits, and both want their own investigation.

## Reproducing

`take-union-bound.repro.test.ts` is now a **regression test** — it asserts
that no yield schedule breaks `Take`, so it fails if the fix is reverted.

```bash
# regression test (both sources)
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
