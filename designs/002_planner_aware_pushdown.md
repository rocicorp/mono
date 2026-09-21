# 002: Planner-aware correlated predicate pushdown

- **Status:** Implemented (steps 1–2; parent-side accounting in step 3 is deferred)
- **Date:** 2026-09-21
- **Packages:** `zql` (planner, builder), `zero-cache` (flag)
- **Builds on:** [001](001_correlated_predicate_pushdown.md)

## Summary

001 copies a parent's condition on a join column into the child subquery.
The pass runs after `planQuery`, so the planner never sees the pushed
conditions. As a result, the planner does not know when a pushed condition
makes a flipped join cheap.

This design runs the pass before `planQuery`. It also tells the planner when
a pushed condition removes rows and when it removes nothing. The rule is
short: count each pushed condition once, on the side of the join that is the
outer loop.

Only zero-cache runs the planner. The client is not affected.

## Problem

### The planner does not see cheaper flips

`planner-exec` compares the estimated cost of each plan with the rows that
the plan reads. With the pass on, two of its tests fail. Take
`film.where('id', 1).whereExists('actors')`, which is film → film_actor →
actor. The pass copies `film_id = 1` into the film_actor subquery.

| Plan             | Estimate | Rows read, pass off | Rows read, pass on |
| ---------------- | -------- | ------------------- | ------------------ |
| No flip (chosen) | 4        | 16                  | 16                 |
| Flip pattern 1   | 193      | 408                 | 408                |
| Flip pattern 2   | 10,240   | 5,673               | **41**             |
| Flip pattern 3   | 10,752   | 5,663               | **211**            |

Patterns 2 and 3 flip the film → film_actor join. In those plans, the join
reads film_actor with no constraint. The pushed condition is then the only
restriction on that read, so it removes almost every row. The estimates stay
the same because the planner never sees the condition.
`playlist.where('id', 1).whereExists('tracks')` shows the same pattern.

For these two queries, the planner still picks the cheapest plan. Other
queries are different. In this query, a flip is the best plan only because of
the pushdown:

```ts
reading
  .where('workID', W)
  .whereExists('work', w => w.where('public', true))
  .limit(10);
```

If W is not public, the semi-join reads every reading of W, and every EXISTS
check fails. The flipped plan reads `work WHERE public = true AND id = W`,
gets no rows, and stops. Without the pushed `id = W`, the flipped plan reads
every public work. So today the planner never picks the flip.

### Why the pass cannot move before the planner as it is

In a semi-join, the join fetches the child once for each parent row, with a
constraint on the join column. Every parent that reaches the join satisfies
the parent's condition. So the pushed condition is true for every child row
that the fetch returns, and it removes nothing.

If the planner reads the pushed condition as an ordinary filter, it gets the
semi-join wrong:

- `PlannerConnection.#computeSelectivity` divides rows with filters by rows
  without filters. It ignores the constraint. For `film_id = 1` on
  film_actor, the result is about 0.001.
- `PlannerJoin.estimateCost` uses `1 - (1 - child.selectivity) ^ fanout` as
  the chance that a parent passes the EXISTS. At 0.001, this chance is close
  to zero. The real chance does not change when the pass runs.
- That chance flows up as `downstreamChildSelectivity`. Parent estimates such
  as `limit / downstreamChildSelectivity` then grow by orders of magnitude.
- The SQLite cost model gets `film_id = ? AND film_id = 1` for a constrained
  fetch. SQLite can treat the two terms as independent and lower its row
  estimate again.

These errors make semi-joins look much worse than they are. The planner then
flips for the wrong reason. This is why 001 runs the pass after planning.

## Design

### The rule

Each push makes a pair: the fact F on the parent, and its copy F′ on the
child. For every parent and child row that the join matches, F and F′ have
the same value. So a plan must count the pair once. Count it on the outer
loop, where it removes rows:

| Join type | Outer loop | Count F on the parent?              | Count F′ on the child?              |
| --------- | ---------- | ----------------------------------- | ----------------------------------- |
| Semi      | Parent     | Yes                                 | No: the constraint binds its column |
| Flipped   | Child      | No: the constraint binds its column | Yes                                 |

"Count" means that the condition goes into `selectivity` and into the filters
that `PlannerConnection` sends to the cost model.

Both halves reduce to one test on a connection: a condition is implied when
the current constraint binds its column and the other side of that
constraint holds its pair. Only pushed pairs have this property. A
user-written condition on a constrained column is not implied, so the
planner must keep counting it.

### Marks

The pass must record what it pushed. Each record needs the pushed condition
(F′), the fact (F), and the edge (the `CorrelatedSubquery` or the EXISTS
condition).

Use a side table that `pushDownCorrelatedPredicates` returns and
`planQuery` takes. The other option is a symbol-keyed property on F′, as
`planIdSymbol` is on EXISTS conditions. The side table is better because the
F half needs the reverse lookup: from a join to the facts pushed into its
child.

The records survive between the pass and the planner:

- `simplifyCondition` and `applyPlansToAST` return simple conditions
  unchanged.
- `buildPlanGraph` gives the AST's `where` to `PlannerConnection` without a
  copy.
- Pushed conditions are top-level conjuncts. So they are always in
  `PlannerConnection.#filters`, never in a per-branch filter
  (`setPerBranchFilter`).

The records do not cross the wire. The pass runs after the AST arrives.

### Changes, in order

Steps 1 and 2 give most of the gain. Measure after step 2, before you start
step 3.

1. **Builder.** In `buildPipeline`, move `pushDownCorrelatedPredicates` above
   `planQuery`. Give its records to `planQuery`. Keep the order of 001 when
   the new flag is off.
2. **The child side (F′).**
   - In the `PlannerConnection` constructor, compute `selectivity` without
     pushed conditions. The semi-join uses it as the pass rate, and there the
     pushed conditions are always implied. A flipped join gets the reduction
     through the child's `returnedRows` instead.
   - In `PlannerConnection.estimateCost`, remove each pushed condition whose
     column the merged constraint binds. Then call the cost model. Keep the
     condition when its column is not bound.
3. **The parent side (F).** In a flipped join, the parent lookup is
   constrained on F's column, and every child row satisfies F′. So F removes
   nothing there. Remove F from the parent's cost call only when the
   constraint comes from a flipped join whose child holds F′. Constraints are
   column sets (`PlannerConstraint`), and `mergeConstraints` combines them from
   several flipped joins. So the connection needs to know which join bound
   which column. For a primary-key lookup, F changes little. Measure before
   you build this step.
4. **Tests.** Remove the `withoutCorrelatedPredicatePushdown` wrapper from
   `packages/zql-integration-tests/src/helpers/planner-exec.ts`.
5. **Flag.** Add a hidden zero-cache flag for the new order, separate from
   `enableCorrelatedPredicatePushdown`. This step changes plans, and 001 does
   not. With a separate flag, we can roll back the plan changes and keep the
   pushdown.

## Testing

- **The semi-join invariant.** For a plan with no flipped joins, the estimate
  with the pass on equals the estimate with the pass off. This is the main
  unit test. It shows that the planner ignores implied conditions. Run it on
  the `planner-exec` queries and on the chinook skeletons.
- **Flipped estimates.** For a flipped EXISTS into a pinned child, the child's
  estimated rows fall to the rows that match the pin.
- **Plan choice.** For the `reading`/`work` query above, the planner picks the
  flip when W is not public.
- **`planner-exec`.** Run with the pass on. The two tests that fail today must
  pass at their current thresholds. Compare the summary tables before and
  after. The correlation of each other test must not fall.
- **Fuzz.** The chinook flip-invariance lane and the pinned-push lane must
  still pass. They compare results with Postgres, so they catch a wrong
  pipeline. They do not catch a bad estimate.

## Open questions

1. How does the SQLite cost model estimate `c = ? AND c = 1`? Measure with
   `createSQLiteCostModel` before step 3. If it does not lower the estimate,
   step 3 has little value.
2. How many more flips does the planner pick on zbugs? A flipped pipeline
   pushes changes along a different path than a semi-join. Compare push and
   advance times on zbugs with the new flag on and off.
3. Facts from EXISTS (001, Future work 3) add pushes in the other direction,
   from child to parent. The rule still holds, but the records need a
   direction. Decide this before that work starts.
