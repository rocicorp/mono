# Predicate Pushdown via FetchRequest

## Context

For the bug query:

```ts
bigTable.where(({or, cmp, exists}) =>
  or(cmp('id', id), exists('smallTable', {flip: true})),
);
```

we want the simple branch's `id = ?` to reach `bigTable`'s source as an indexed lookup, rather than being applied post-scan as a runtime predicate.

Today's pipeline:

```
bigTable.connect (no filter — transformFilters strips the OR because of the CSQ)
   ↓
UnionFanOut
   ├→ Filter(cmp(id, ?))                              [branch A]
   └→ FlippedJoin(parent=UFO, child=smallTable.connect) [branch B]
   ↓
UnionFanIn
```

Branch A's filter is applied **post-scan** by the runtime `Filter` operator — `bigTable` is fully scanned. Branch B already gets indexed access via `FlippedJoin`'s constraint translation.

## Plan

Keep the pipeline shape exactly as above, and add a `filter` field to `FetchRequest`. Operators pass the field up via the existing `req` spread; the source applies it during its scan via the same `primaryKeyConstraintFromFilters` index path it already uses for connection-time filters.

Branch A's `fetch` then becomes:

```
FilterEnd.fetch(req)
  → FilterStart.fetch(req with filter = cmp(id, ?))
    → UnionFanOut.fetch(req with filter = cmp(id, ?))
      → bigTable.connect.fetch(req with filter = cmp(id, ?))
        → indexed PK lookup, returns 1 row
```

Pushes are unchanged. The runtime `Filter.push` + `filterPush` machinery already rejects non-matching changes incrementally — there's no perf gap to close on the push path.

## Implementation

### 1. Extend `FetchRequest` with a `filter` field

`packages/zql/src/ivm/operator.ts` (currently lines 46-53):

```ts
export type FetchRequest = {
  readonly constraint?: Constraint | undefined;
  readonly start?: Start | undefined;
  readonly reverse?: boolean | undefined;
  readonly filter?: NoSubqueryCondition | undefined; // NEW
};
```

Semantically `filter` means "the consumer only wants rows matching this predicate." It's an additive constraint: the receiving source (or intermediate operator) must AND it with whatever filtering it already applies.

`NoSubqueryCondition` already exists in `packages/zql/src/builder/filter.ts`. Import it here.

### 2. Have `FilterStart` carry the chain's condition and add it to fetch

`packages/zql/src/ivm/filter-operators.ts`:

- `buildFilterPipeline(input, delegate, pipeline)` gains an optional 4th param: `condition: NoSubqueryCondition | undefined`. Callers pass the AST condition that the filter sub-graph implements.
- `FilterStart`'s constructor stores that condition.
- `FilterStart.fetch(req)` computes:
  ```ts
  const mergedFilter =
    this.#condition && req.filter
      ? {type: 'and', conditions: [req.filter, this.#condition]}
      : (this.#condition ?? req.filter);
  ```
  and forwards `{...req, filter: mergedFilter}` to `this.#input.fetch(...)`. The `Filter` operator's runtime `predicate` evaluation stays as-is (it's still needed for the `push` path and as a safety net for the `fetch` path when a source can't fully apply the filter).

`Filter` itself (`packages/zql/src/ivm/filter.ts`) needs no changes — it's a `FilterOperator` that doesn't see fetch requests. The `FilterStart` is the bridge.

### 3. Update `buildFilterPipeline` callers in the builder

`packages/zql/src/builder/builder.ts`:

- `applyWhere` (line 367): when dispatching to the non-flipped path:
  ```ts
  const noSub = transformFilters(condition).filters;
  return buildFilterPipeline(
    input,
    delegate,
    filterInput => applyFilter(filterInput, condition, delegate, name),
    noSub,
  );
  ```
  `transformFilters` (`packages/zql/src/builder/filter.ts:165-204`) already strips correlated subqueries; reuse it directly.
- `applyFilterWithFlips` `or` case (line 410-449): when building the non-flipped branches' filter pipeline, do the same transform on `{type: 'or', conditions: withoutFlipped}` and pass the result.
- `applyFilterWithFlips` `and` case (line 386-409): same for `{type: 'and', conditions: withoutFlipped}`.

When `transformFilters` returns `{filters: undefined, conditionsRemoved: true}` (the OR-with-CSQ scenario), we pass `undefined` and the source falls back to scanning — same as today. The win comes when `transformFilters` returns a real condition for an OR branch's purely simple sub-condition (the bug case).

### 4. Apply `req.filter` at the sources

`packages/zql/src/ivm/memory-source.ts`:

- In `#fetch` (line 256+), AND `req.filter` into the predicate evaluation. Build a runtime predicate with `createPredicate(req.filter)` if present, and combine with `conn.filters?.predicate` (logical AND).
- Reuse `primaryKeyConstraintFromFilters` (line 264-267) on the merged condition so PK lookups work when the pushed filter targets the primary key. This is the path that turns `cmp('id', ?)` into an indexed scan.
- The combined filter is also passed into `generateWithOverlay` (line 349) so push/overlay correctness is preserved.

`packages/zqlite/src/table-source.ts` + `packages/zqlite/src/query-builder.ts`:

- `buildSelectQuery` already takes `filters: NoSubqueryCondition | undefined` and ANDs them into the WHERE clause via `filtersToSQL`. Add a parallel `fetchFilters?: NoSubqueryCondition` parameter that gets ANDed in alongside.
- `TableSource.#fetch`: pass `req.filter` as `fetchFilters` to `buildSelectQuery`.

For both sources: the request-time filter is conceptually identical to the connection-time filter, just applied per-fetch.

### 5. Forward `req.filter` through `Join` and `FlippedJoin`

Most operators that wrap fetch already pass `req` unchanged via spread (`Skip`, `Take`, `UnionFanOut`, `UnionFanIn`'s per-branch fetch loop) — they need no change.

- `packages/zql/src/ivm/join.ts:119-127`: `#parent.fetch(req)` already forwards `req`. The `req.filter` rides along to the parent. Verify no constraint manipulation strips it.
- `packages/zql/src/ivm/flipped-join.ts:124-190`: when computing the parent fetch (line 184-189), the `...req` spread already preserves `filter`. The `constraint` is augmented from the child key. So `req.filter` reaches the parent source. No change needed beyond a doc comment.

These two operators are the hot path for the bug query: `Branch B` (`FlippedJoin`) ends up doing a parent fetch with `{constraint: {id: smallTableRow.id}, filter: undefined}`, and the constraint already produces an indexed lookup. `Branch A` (`Filter` over `UnionFanOut`) sends `{filter: cmp('id', ?), constraint: undefined}` upward through `UnionFanOut` → `bigTable.connect`, hitting the PK index via `primaryKeyConstraintFromFilters`.

### 6. Optional: log fetch filters in `Snitch` for tests

`packages/zql/src/ivm/test/source-snitch.ts` (or the equivalent — the `Snitch` operator that the `TestBuilderDelegate` wraps everything in): include `req.filter` in the log entry so tests can assert that the source received the pushed-down predicate, not just that the result is correct.

### 7. Planner: per-branch filter propagation

So the cost model can value the optimization (and pick `flip:true` automatically when the math says so), the planner needs to model the per-branch filter that actually arrives at the connection at fetch time.

The audit found **no structural blockers**. The mechanism mirrors the existing per-branch _constraint_ propagation:

#### 7a. Admit simple branches in `processOr`

`packages/zql/src/planner/planner-builder.ts:149-190`. Today `processOr` filters its branches to subquery-bearing ones at lines 157-159, so simple `cmp` branches are invisible to the planner. Stop filtering: every branch becomes an input of the `PlannerFanIn`, and the existing UFI mode (planner-fan-in.ts:148-177) will assign a unique `branchPattern = [i, ...parent]` to each one.

#### 7b. New `PlannerFilter` node

`packages/zql/src/planner/planner-filter.ts` (**new**). A thin pass-through node that holds a `NoSubqueryCondition` and:

- Forwards `propagateConstraints` to its single input (the connection or whatever is below) and **also** registers its filter at the receiving connection under the current `branchPattern`.
- Forwards `estimateCost` straight through.
- Forwards `propagateUnlimitFromFlippedJoin` (no-op for filters; the existing `'propagateUnlimitFromFlippedJoin' in input` guard at planner-fan-in.ts:71-76 already tolerates nodes that lack it).

Add `'filter'` to the `PlannerNode` discriminated union (planner-node.ts:11-16) and to `getNodeName` (planner-join.ts:450-462).

`processOr` builds one `PlannerFilter(connectionInput, transformFilters(branch).filters)` per simple branch and feeds it into the FanIn alongside the existing CSQ-branch joins.

#### 7c. Per-branch filter on `PlannerConnection`

`packages/zql/src/planner/planner-connection.ts`:

- Add `#perBranchFilters: Map<string, NoSubqueryCondition | undefined>` parallel to `#constraints` (line 92). Key is the same `branchPattern.join(',')`.
- Add a setter `setPerBranchFilter(path: number[], filter: NoSubqueryCondition)` that `PlannerFilter.propagateConstraints` calls when it reaches the connection.
- In `estimateCost` (line 187-242), compute `effectiveFilters = AND(this.#filters, this.#perBranchFilters.get(key))` and pass that to `this.#model(...)` instead of `this.#filters`. Cache the cost per-branch as today (the cache key already includes the branchPattern, so per-branch filters don't break it).
- Reset `#perBranchFilters` in `reset()` and capture/restore in `capturePlanningSnapshot` / `restorePlanningSnapshot` (planner-graph.ts:136-241) alongside `connectionConstraints`.

#### 7d. Cost-model `filters` argument

`ConnectionCostModel` (planner-connection.ts:340-345) already takes `filters: Condition | undefined`. Both implementations (`packages/zero-cache/src/services/view-syncer/sqlite-cost-model.ts:44-49` and `packages/zql/src/planner/test/helpers.ts` `simpleCostModel`) currently _ignore_ the `filters` argument. After this change:

- The sqlite cost model should incorporate filter selectivity (it likely already does via SQLite's stat tables — verify). If not, that's a follow-up; for now passing AND'd filters is a no-op there and the optimization fires correctly anyway because the _runtime_ picks indexes from the merged condition.
- `simpleCostModel` ignoring filters means existing planner tests stay stable (behavior unchanged for queries without simple OR branches). To exercise the new path in tests, add a small filter-aware variant or add a tiny "selectivity-per-condition" factor to the test model — this is the only minor task surfaced by the audit (item 7).

#### 7e. Why no `applyPlansToAST` change is needed

`applyPlansToAST` (planner-builder.ts:357-382) walks `plans.plan.joins` to write back `flip` annotations. It never touches simple branches and the AST doesn't need to encode the pushdown decision — runtime pushdown happens at the IVM layer via `req.filter`. Nothing to change here.

## Critical files

Runtime (steps 1-6):

- `packages/zql/src/ivm/operator.ts` — add `filter` to `FetchRequest`.
- `packages/zql/src/ivm/filter-operators.ts` — `buildFilterPipeline` signature + `FilterStart` stores and merges condition.
- `packages/zql/src/builder/builder.ts` — call sites that pass the condition.
- `packages/zql/src/ivm/memory-source.ts` — apply `req.filter` (predicate + PK constraint extraction).
- `packages/zqlite/src/table-source.ts`, `packages/zqlite/src/query-builder.ts` — accept and AND `fetchFilters` into the SQL WHERE.
- (No changes) `union-fan-in.ts`, `union-fan-out.ts`, `flipped-join.ts`, `join.ts`, `skip.ts`, `take.ts` — fetch already forwards `req` unchanged.

Planner (step 7):

- `packages/zql/src/planner/planner-filter.ts` — **new**. `PlannerFilter` node.
- `packages/zql/src/planner/planner-node.ts` — extend `PlannerNode` union with `'filter'`.
- `packages/zql/src/planner/planner-builder.ts` — `processOr` admits simple branches and wraps each in a `PlannerFilter`.
- `packages/zql/src/planner/planner-connection.ts` — add `#perBranchFilters` map, setter, and AND into `effectiveFilters` in `estimateCost`. Update `reset` / capture / restore.
- `packages/zql/src/planner/planner-graph.ts` — extend `PlanState` snapshot to include per-branch filters (parallel to `connectionConstraints`).
- `packages/zql/src/planner/planner-fan-in.ts` (audit only) — already polymorphic; no change.
- `packages/zql/src/planner/test/helpers.ts` — optionally add a tiny filter-selectivity factor to `simpleCostModel` so tests can verify per-branch filters affect plan cost (otherwise the model is filter-blind and the new behavior is invisible to tests).

## Reused utilities

- `transformFilters` (`packages/zql/src/builder/filter.ts:165-204`) — strips CSQs from a `Condition` to produce `NoSubqueryCondition`. Use at every `buildFilterPipeline` call site.
- `createPredicate` (`packages/zql/src/builder/filter.ts:26-94`) — runtime predicate builder. Use in `MemorySource.#fetch` to evaluate `req.filter`.
- `primaryKeyConstraintFromFilters` (already used in `MemorySource.#fetch:264-267`) — drives index selection from a filter AST. Apply to the merged (`conn.filters` + `req.filter`) condition.
- `filtersToSQL` / `buildSelectQuery` (`packages/zqlite/src/query-builder.ts`) — SQL generation already handles a `NoSubqueryCondition` filter; just add a second one.

## Verification

End-to-end:

- `npm --workspace=zql run check-types`, `npm --workspace=zqlite run check-types`
- `npm --workspace=zql run test`, `npm --workspace=zqlite run test`
- `npm --workspace=zero-cache run test -- --project=zero-cache/pg-18`
- `npm run lint && npm run format`

New tests to add:

- `packages/zql/src/ivm/predicate-pushdown.test.ts` (new):
  - **OR-with-flipped-and-simple bug shape** — assert via `Snitch` log that `bigTable.connect.fetch` was called with `req.filter = cmp('id', ?)` (i.e., the predicate reached the source) AND the result is correct (initial fetch + push-driven add/remove/edit).
  - **Plain `where(cmp).orderBy()` with a non-PK filter** — confirm the request-time path is equivalent to connection-time filter for non-OR cases (regression check).
  - **Filter chained with `Skip`/`Take`** — assert filter survives the spread through both.
  - **`FlippedJoin` with a filter on the parent post-join** — `bigTable.where(exists('children', {flip: true})).where(cmp('status', 'open'))` should push `cmp('status', 'open')` down to the source even when the flipped join is in the path.
- `packages/zql/src/ivm/memory-source.test.ts`: direct `connect(...).fetch({filter: ...})` cases — non-PK filter, PK filter (index path), AND with connection-time filter, EDIT/ADD/REMOVE pushes still respect connection-time filter.
- `packages/zqlite/src/table-source.pg.test.ts` (or whatever the existing TableSource test file is): same coverage against SQLite, including verifying SQL output via `buildSelectQuery`.
- `packages/zql/src/ivm/filter-operators.test.ts` (or co-locate in an existing file): `FilterStart` correctly merges its condition with `req.filter` (AND-combine, both-undefined fallthrough, only-one-defined fallthrough).
- `packages/zql/src/planner/planner-builder.test.ts`: for `or(cmp('id', ?), exists('smallTable', {flip: true}))`, the plan graph now includes a `PlannerFilter` for the simple branch, the FanIn assigns it a unique `branchPattern`, and `PlannerConnection.estimateCost` for that pattern reflects the per-branch filter (assert via `getConstraintCostsForDebug` / `planDebugger` events).
- `packages/zql/src/planner/planner-connection.test.ts` (existing): cover `setPerBranchFilter` + `estimateCost` interaction (per-branch filter ANDed with connection-time filter, cache invalidation on filter change, snapshot capture/restore round-trip).
- `packages/zql/src/planner/planner-fan-in.test.ts` (existing): UFI mode with mixed `PlannerFilter` and `PlannerJoin` inputs.

Manual verification:

- Run `npm run transform-query` / `npm run run-query` (`packages/zql/tool/`) on the bug shape against a representative dataset; confirm via the planner debugger output that the OR's simple branch shows up in the plan graph and that its branch's connection-cost line in the debug log reflects the per-branch filter.

## Risks

- **Predicate evaluation correctness across overlay/edit paths.** `MemorySource.#fetch` already threads `conn.filters?.predicate` through `generateWithOverlay`. We need the same plumbing for the merged `req.filter` + `conn.filters` predicate. Mitigate with explicit overlay/edit unit tests on `MemorySource`.
- **Subquery conditions accidentally leaking into `req.filter`.** Anything we pass via `req.filter` must already be a `NoSubqueryCondition`. The contract is enforced at every call site by routing through `transformFilters`. Add a `// TYPE-LEVEL` assertion to keep this honest.
- **Double filtering.** Once the source applies `req.filter`, the `Filter` operator's post-fetch predicate runs again on the same rows — wasteful but not incorrect (the predicate is pure). If hot-path benchmarks show this matters, add a "fully applied" hint on the source's response (mirroring `fullyAppliedFilters` for connection-time filters) and let `FilterStart` skip its predicate. Out of scope for v1.
- **`zqlite`/`MemorySource` divergence.** Two source implementations need parallel changes; missing one would silently regress half the deployments. Mitigate with parallel tests and shared `NoSubqueryCondition` shape.
- **Planner snapshot round-trip.** `PlannerGraph.capturePlanningSnapshot` / `restorePlanningSnapshot` (planner-graph.ts:136-241) already serialize per-connection constraints and join states for backtracking. Adding `#perBranchFilters` requires extending both. Missing the restore path would cause silent plan corruption when the planner backtracks during enumeration. Mitigate with a round-trip unit test in `planner-graph.test.ts`.
- **Cost model filter-blindness.** The production sqlite cost model and the test `simpleCostModel` both currently _ignore_ the `filters` argument (audit item 1, 7). Per-branch filter pushdown is a no-op at the planner level until the cost model honors filter selectivity. This means the new planner code is correct but inert until the cost model is updated — the runtime optimization still fires regardless. Plan a follow-up to teach `simpleCostModel` (and verify the production sqlite model) to apply per-condition selectivity.

## Out of scope

- Pushing filter down through the `Exists` operator's child (the related subquery). `Exists` lives in the filter pipeline; its fetch path doesn't touch arbitrary parent predicates. Not needed for the bug.
- `and(shared, or(...))` distribution / OR-of-OR flattening. The request-time approach naturally handles whatever WHERE shape arrives, so distribution isn't necessary for correctness.
- Teaching the production sqlite cost model to incorporate filter selectivity beyond what it already does (see Risks). The planner code lands ready to use it; the cost model upgrade can ride a follow-up PR.
