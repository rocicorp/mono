# Planner: Per-Branch Filter Cost Modeling

## TL;DR

**Does the planner already account for the `req.filter` pushdown? Partially — and what it does today is _misleading_, not just incomplete.** The runtime fix (predicate pushdown via `FetchRequest.filter`) lands cleanly without planner changes, but the planner's cost view of OR-with-CSQ shapes is currently _biased toward the unflipped plan_ in a way that masks the runtime win.

This plan closes that gap by:

1. Modeling simple OR branches as `PlannerFilter` nodes.
2. Routing each branch's filter to the `PlannerConnection` under that branch's `branchPattern`.
3. AND-merging the per-branch filter with the connection-time filter when calling the cost model.

It does _not_ change the cost-model implementations — `createSQLiteCostModel` already honors filter selectivity via SQLite's `scanstatus`. The test `simpleCostModel` ignores filters; we leave that alone but add a small filter-aware variant for the new tests.

## Diagnosis: what the planner sees today

For the bug shape:

```ts
bigTable.where(({or, cmp, exists}) =>
  or(
    cmp('id', id),
    exists('smallTable', {flip: undefined}), // planner chooses flip
  ),
);
```

### Today's planner graph

`processOr` (planner-builder.ts:149-190) admits **only** subquery-bearing branches. The simple `cmp('id', X)` branch has _zero representation_ in the planner graph. The graph is:

```
PlannerConnection(bigTable, filters = or(cmp, exists))
  ↓
PlannerFanOut
  └→ PlannerJoin(bigTable ⋈ smallTable, semi|flipped)  ← only branch represented
  ↓
PlannerFanIn
  ↓
PlannerTerminus
```

### Today's cost-model call

`PlannerConnection.estimateCost` calls the cost model with the full `ast.where` (including the CSQ):

```ts
this.#model(
  this.table,
  this.#sort,
  this.#filters /* = or(cmp, exists) */,
  mergedConstraint,
);
```

The `createSQLiteCostModel` then pre-processes via `removeCorrelatedSubqueries` (sqlite-cost-model.ts:102-128). For an OR like `or(cmp, exists)`:

```ts
case 'or': {
  const filtered = condition.conditions
    .map(c => removeCorrelatedSubqueries(c))
    .filter((c): c is NoSubqueryCondition => c !== undefined);
  if (filtered.length === 1) return filtered[0];  // ← returns just `cmp`!
}
```

So the cost model is asked: _"how much does it cost to scan `bigTable` with `WHERE id = ?`"_. SQLite's `scanstatus` says **1 row** (PK lookup).

### Why this is wrong

At runtime, the _unflipped_ OR plan does **not** push `cmp('id', X)` to the source. It scans the full `bigTable` and applies the OR via `FanOut` + `Filter`/`Exists` per row. The cost model's "1 row" answer reflects neither plan accurately:

- For the **unflipped** plan: actual scan is `O(bigTable)`, model says `O(1)`. **Massive under-estimate.**
- For the **flipped** plan (UFO/UFI with `req.filter` pushdown): branch A is `O(1)` (PK lookup), branch B is `O(smallTable)` (FlippedJoin). Model's "1 row" answer is closer for branch A but still under-estimates branch B.

### What the planner ends up choosing

`PlannerJoin.estimateCost` for the bigTable⋈smallTable join (planner-join.ts:265-411):

- **semi**: `parent.cost + parent.scanEst * child_cost ≈ 1 + 1 × child_cost ≈ small`
- **flipped**: `child.cost + child.scanEst * parent_cost ≈ smallTable + smallTable × 1 ≈ smallTable`

Conclusion: today's planner _prefers semi_ on the bug shape because `removeCorrelatedSubqueries` makes the parent look like a 1-row PK lookup. **The runtime fix has no effect on this choice** — the planner picks semi, the runtime builds the regular FanOut/FanIn pipeline, and `req.filter` is _never set_ (no `FilterStart` is created in that pipeline shape, because the OR has no flipped branch). The user has to manually set `flip: true` for the runtime win to materialize.

So the runtime fix and planner fix are tightly coupled: **without the planner fix, the runtime fix only helps users who manually annotate `flip: true`.**

## Goal

Teach the planner that:

> "If we flip the EXISTS, branch A becomes a `WHERE id = ?` lookup at the source (1 row), and branch B becomes `WHERE id IN (smallTable.parentField)` at the source (smallTable rows). The flipped plan is much cheaper."

We achieve this by giving the planner a per-branch view of what filter actually lands at each source connection.

## Plan

### 1. New `PlannerFilter` node

`packages/zql/src/planner/planner-filter.ts` (**new**, ~80 lines).

```ts
export class PlannerFilter {
  readonly kind = 'filter' as const;
  readonly #input: Exclude<PlannerNode, PlannerTerminus>;
  readonly #condition: NoSubqueryCondition | undefined;
  #output?: PlannerNode | undefined;

  constructor(
    input: Exclude<PlannerNode, PlannerTerminus>,
    condition: NoSubqueryCondition | undefined,
  ) {
    this.#input = input;
    this.#condition = condition;
  }

  setOutput(node: PlannerNode): void { this.#output = node; }
  get output(): PlannerNode { return must(this.#output); }
  closestJoinOrSource(): JoinOrConnection { return this.#input.closestJoinOrSource(); }

  propagateConstraints(
    branchPattern: number[],
    constraint: PlannerConstraint | undefined,
    from?: PlannerNode,
    planDebugger?: PlanDebugger,
  ): void {
    // Forward constraint AND register the per-branch filter at any
    // PlannerConnection reachable through the input.
    if (this.#condition) {
      const conn = findConnection(this.#input);
      conn?.setPerBranchFilter(branchPattern, this.#condition);
    }
    this.#input.propagateConstraints(branchPattern, constraint, this, planDebugger);
  }

  estimateCost(s: number, branchPattern: number[], dbg?: PlanDebugger): CostEstimate {
    return this.#input.estimateCost(s, branchPattern, dbg);
  }

  propagateUnlimitFromFlippedJoin(): void {
    if ('propagateUnlimitFromFlippedJoin' in this.#input) {
      (this.#input as ...).propagateUnlimitFromFlippedJoin();
    }
  }

  reset(): void { /* no-op: condition is immutable */ }
}
```

Why pass-through for `estimateCost`: the cost the _connection_ computes (with the per-branch filter ANDed in) is what flows up. The filter node itself adds no execution cost.

### 2. Extend `PlannerNode` union and helpers

`packages/zql/src/planner/planner-node.ts:11-16`: add `| PlannerFilter` to `PlannerNode`.

`packages/zql/src/planner/planner-join.ts:450-462` `getNodeName`: add `case 'filter': return 'filter';`.

`packages/zql/src/planner/planner-builder.ts:22-35` `wireOutput`: add `case 'filter': from.setOutput(to); break;`.

### 3. Admit simple OR branches in `processOr`

`packages/zql/src/planner/planner-builder.ts:149-190`. Replace the filter at lines 157-159 with:

```ts
const fanOut = new PlannerFanOut(input);
graph.fanOuts.push(fanOut);
wireOutput(input, fanOut);

const branches: Exclude<PlannerNode, PlannerTerminus>[] = [];
for (const subCondition of condition.conditions) {
  let branch: Exclude<PlannerNode, PlannerTerminus>;
  if (
    subCondition.type === 'correlatedSubquery' ||
    hasCorrelatedSubquery(subCondition)
  ) {
    branch = processCondition(
      subCondition,
      fanOut,
      graph,
      model,
      parentTable,
      getPlanId,
    );
  } else {
    // Simple branch — wrap in a PlannerFilter so its filter is registered
    // at the connection under this branch's pattern.
    const transformed = transformFilters(subCondition).filters;
    branch = new PlannerFilter(fanOut, transformed);
    graph.filters.push(branch);
    wireOutput(fanOut, branch);
  }
  branches.push(branch);
  fanOut.addOutput(branch);
}

const fanIn = new PlannerFanIn(branches);
graph.fanIns.push(fanIn);
for (const branch of branches) {
  wireOutput(branch, fanIn);
}
```

Add `filters: PlannerFilter[] = []` to `PlannerGraph` (`packages/zql/src/planner/planner-graph.ts:50-53`) and reset it in `resetPlanningState` (no-op, but keeps the pattern consistent).

### 4. Per-branch filter on `PlannerConnection`

`packages/zql/src/planner/planner-connection.ts`:

- Add `#perBranchFilters: Map<string, NoSubqueryCondition>` parallel to `#constraints` (line 92).
- New method:
  ```ts
  setPerBranchFilter(path: number[], filter: NoSubqueryCondition): void {
    this.#perBranchFilters.set(path.join(','), filter);
    this.#cachedConstraintCosts.clear();
  }
  ```
- In `estimateCost` (line 187-242):
  ```ts
  const perBranchFilter = this.#perBranchFilters.get(key);
  const effectiveFilters: Condition | undefined =
    this.#filters && perBranchFilter
      ? {type: 'and', conditions: [this.#filters, perBranchFilter]}
      : (this.#filters ?? perBranchFilter);
  const {startupCost, fanout, rows} = this.#model(
    this.table,
    this.#sort,
    effectiveFilters,
    mergedConstraint,
  );
  ```
- Reset `#perBranchFilters` in `reset()` (line 270-275). **Don't** reset on `propagateConstraints` — the per-branch filter is structural, not search state. (See §6 for the snapshot question.)

### 5. Selectivity recomputation

`PlannerConnection`'s `selectivity` field (line 72, 124-136) is computed once at construction from `this.#filters`. With per-branch filters, selectivity is _per-branch_. Either:

- **(a)** Make `selectivity` per-branch (Map keyed on branchPattern). The `PlannerFanIn` already iterates branches with their own patterns; it would call into the connection per branch and read per-branch selectivity correctly.
- **(b)** Keep selectivity global (using only `this.#filters`) and let the rows-count change in `estimateCost` carry the per-branch selectivity implicitly.

Recommend **(b)** — `selectivity` is only used for EXISTS child connections (`limit !== undefined && filters`), which is downstream of `PlannerFilter`. Per-branch filters at the parent connection don't affect EXISTS-child selectivity. Keeping `selectivity` global avoids a bigger refactor.

### 6. Snapshot capture/restore

`packages/zql/src/planner/planner-graph.ts:18-24` `PlanState`. Two options:

- **(a)** Add `connectionPerBranchFilters: Array<Map<string, NoSubqueryCondition>>` parallel to `connectionConstraints`. Capture/restore alongside.
- **(b)** Don't capture per-branch filters. They're set by `PlannerFilter.propagateConstraints` deterministically every time `propagateConstraints` runs (which happens once per planning attempt).

Looking at `planner-graph.ts:300-307`:

```ts
this.propagateConstraints(planDebugger); // sets per-branch filters
```

runs every iteration. So **(b)** works: per-branch filters are re-derived from graph topology each pass and don't need snapshotting. Pick **(b)**.

### 7. Test cost model: filter-aware variant

`packages/zql/src/planner/test/helpers.ts`. The existing `simpleCostModel` ignores filters. Adding filter-awareness to it would change snapshots in many existing tests. Instead, add:

```ts
export const filterAwareCostModel: ConnectionCostModel = (
  _table,
  _sort,
  filters,
  constraint,
) => {
  const constraintCount = constraint ? Object.keys(constraint).length : 0;
  const baseRows = Math.max(
    1,
    BASE_COST - constraintCount * CONSTRAINT_REDUCTION,
  );
  // PK-equality filter on `id` → 1 row. Other simple cmp → halve. Otherwise unchanged.
  const filterFactor = filterSelectivity(filters);
  return {
    startupCost: 0,
    rows: Math.max(1, Math.round(baseRows * filterFactor)),
    fanout,
  };
};
```

Use this in the new planner test for OR-with-mixed branches.

### 8. AST writeback

`applyPlansToAST` (planner-builder.ts:357-382) walks `plans.plan.joins` for `flip` annotations. **No change needed** — `PlannerFilter` doesn't influence the AST output. The runtime layer's `applyWhere` → `applyFilterWithFlips` decides which OR branches go through `FilterStart` (with `req.filter`) based on the AST's `flip` annotations alone. The planner's job ends with picking flips correctly.

## Files

**New:**

- `packages/zql/src/planner/planner-filter.ts`

**Modified:**

- `packages/zql/src/planner/planner-node.ts` — extend `PlannerNode` union
- `packages/zql/src/planner/planner-builder.ts` — `processOr` admits simple branches; `wireOutput` handles `'filter'`
- `packages/zql/src/planner/planner-graph.ts` — add `filters` array; `getNodeName` mention if needed
- `packages/zql/src/planner/planner-connection.ts` — per-branch filter map, `setPerBranchFilter`, AND into `estimateCost`
- `packages/zql/src/planner/planner-join.ts` — `getNodeName` adds `'filter'` case (debug only)
- `packages/zql/src/planner/test/helpers.ts` — `filterAwareCostModel` (additive)

**No change:**

- `applyPlansToAST` — flip annotations only
- `createSQLiteCostModel` — already honors filter selectivity
- Runtime IVM — unchanged, the predicate-pushdown PR is sufficient

## Verification

### Unit

- `planner-builder.test.ts`: for `or(cmp('id', X), exists('child'))`, plan graph contains `PlannerFilter(fanOut, cmp(id, X))` for the simple branch; FanIn assigns it `branchPattern = [N]` distinct from the join branch.
- `planner-connection.test.ts`: `setPerBranchFilter` + `estimateCost` for that branch ANDs the filter, cost reflects selectivity, cache invalidates on `setPerBranchFilter`.
- `planner-fan-in.test.ts`: UFI mode with mixed `PlannerFilter`/`PlannerJoin` inputs reports per-branch costs and aggregate selectivity.

### Integration

- `packages/zql-integration-tests/src/chinook/planner.pg.test.ts`: add a query of the form `albums.where(or(cmp(id, ?), exists('tracks').where(...)))` (no manual flip). Assert the planner picks `flip: true` for the EXISTS via `getAST` inspection.

### Manual

- Run `npm run transform-query` on the bug shape with `flip: undefined`. Confirm:
  1. The planner debugger logs `flip: true` for the EXISTS.
  2. `PlannerConnection.getConstraintCostsForDebug()` shows distinct branch entries — branch A's cost is much smaller than branch B's.

## Why the SQLite cost model is _almost_ doing the right thing already

`removeCorrelatedSubqueries` strips CSQs and _keeps_ the OR's remaining simple branches. So when the planner today asks "scan bigTable with `or(cmp, exists)`?", SQLite is asked "scan bigTable with `cmp`?". This _accidentally_ gives the right answer for the **flipped + branch A** case, but it gives the _same_ answer for the **unflipped** case (which actually scans full bigTable). The cost model can't distinguish between plans because the planner only asks one question per connection.

With per-branch filters, we ask the cost model two questions:

- Branch A's cost (filter = cmp): tiny.
- Branch B's cost (filter = `<correlation key>`-derived constraint, no filter): smallTable rows.

And the unflipped plan is no longer represented this way — when the join is in `semi` mode, the cost model is asked **without** the per-branch filters (because the simple branch's `PlannerFilter` only registers under the UFI's branch pattern; in semi mode the FanOut→FanIn is FO→FI and uses a single shared pattern, so the filter would still register but overlap with the join's path). This needs care — see §9.

### 9. FO vs UFO: when is the per-branch filter live?

In `FI` (semi) mode, all branches share `branchPattern = [0, ...parent]`. The `PlannerFilter` for the simple branch registers `cmp(id, X)` at the connection under `[0, ...parent]`. The `PlannerJoin` branch _also_ propagates constraints (the EXISTS correlation) under `[0, ...parent]`. So the connection sees both.

But in semi mode, the connection isn't actually filtered by the simple branch at runtime — it's the FanOut feeding both branches with the same rows. So overstating the filter at the source under-estimates cost in semi mode.

**Fix**: only register the per-branch filter when the FanIn is `UFI` (i.e., when at least one of its branches is flipped). Concretely: `PlannerFilter.propagateConstraints` walks up to find the enclosing FanIn; if that FanIn is `FI`, skip the `setPerBranchFilter` call.

Even simpler: register the filter unconditionally, but in `PlannerConnection.estimateCost`, _only honor_ the per-branch filter for branchPatterns that are unique (UFI mode generates unique patterns per branch; FI mode shares one). Detection: a per-branch filter is "live" iff its branchPattern is the only pattern in the per-branch map matching that prefix.

Defer the exact mechanism to implementation — the test `expect plan to flip` will catch it either way.

## What this plan does _not_ do

- **Cost model rewrites**: out of scope. The SQLite cost model already accepts filters; we just feed it more accurate ones.
- **Selectivity per-branch on `PlannerConnection.selectivity`**: kept global; only used by EXISTS-child connections, which aren't affected.
- **Modeling the simple branch's contribution to FanIn aggregate selectivity**: the `PlannerFanIn.estimateCost` (planner-fan-in.ts:81-193) already computes `noMatchProb *= 1 - cost.selectivity` per input. With `PlannerFilter` as an input, its `cost.selectivity` is whatever the connection reports for that branch — which is correct under our changes.
- **Pushing filter through nested ORs**: `processOr` is called recursively, so nested ORs naturally compose. The per-branch filter from an inner OR registers at the connection alongside the outer OR's filter.

## Risks

- **§9 (FI vs UFI live filter)**: if we register per-branch filters in FI mode, costs are wrong in the unflipped plan. Mitigate with the "UFI only" check + a unit test that compares costs across FI/UFI states for the same graph.
- **Cache invalidation**: `setPerBranchFilter` clears `#cachedConstraintCosts`. Calling it during `propagateConstraints` (every planning iteration) means more cache churn than constraints alone. Likely fine — constraints already churn per iteration.
- **Snapshot drift**: if §6 turns out wrong (per-branch filters need snapshotting), `restorePlanningSnapshot` would silently restore stale per-branch filters. Mitigate with a round-trip test.
- **Pre-existing bug in `removeCorrelatedSubqueries`**: it returns `cmp` for `or(cmp, exists)`, which is _semantically_ a strict subset, not a superset. This causes today's cost model to under-estimate scan rows for OR-with-CSQ. The new per-branch filter approach replaces this lossy simplification with explicit per-branch filters, so the bug is moot for OR. But it remains for AND/non-OR uses of the cost model — not our problem to fix here, but worth noting.

## Out of scope

- Teaching `simpleCostModel` to honor filters in _all_ planner tests. The new tests use `filterAwareCostModel`; existing tests are unchanged.
- Changing `PlannerJoin.estimateCost` formulas. The cost flow already AND's per-branch filter selectivity in via the connection's lower `rows` value.
- Auto-detecting and rewriting `or(cmp, exists)` AST shapes at parse time. Out of scope and unnecessary — the planner's flip choice does the same job.
