# Test Coverage Gaps — `req.filter` (predicate pushdown)

Roughly in order of risk.

## 1. Source × overlay × `req.filter` (highest risk)

None of the three new `memory-source.test.ts` cases exercise `req.filter`
with an in-progress push (overlay set). The code path _does_ feed the
merged predicate into `generateWithOverlay`
(`memory-source.ts:374`) and `overlaysForFilterPredicate`, but nothing
locks it in. Same gap on `table-source.ts:295-298`. This is the most
intricate IVM interaction and the most likely to silently regress.

Worth adding:

- Fetch-with-`req.filter` while an ADD overlay is present (matching and
  non-matching).
- REMOVE overlay, EDIT overlay (old-row matches filter, new-row doesn't,
  and vice versa).
- Same trio in unordered mode (`generateWithOverlayUnordered`).

## 2. SQLite path has zero unit coverage for `req.filter`

`packages/zqlite/src/table-source.test.ts` has no test that passes a
`req.filter`. The memory-source tests don't catch SQLite-specific issues
(NULL semantics, column-type coercion in `filtersToSQL`, the
`fetchFilters` AND-merge in `buildSelectQuery`). At minimum:

- A `table-source.test.ts` mirror of the three new `memory-source.test.ts`
  cases.
- A `query-builder.test.ts` case asserting the emitted SQL when both
  `filters` and `fetchFilters` are present.

## 3. `req.filter` with other fetch-time inputs in `MemorySource`

The new tests use only the basic path. Untested combinations:

- `req.filter` + `req.reverse: true` (different code path through
  `indexComparator`).
- `req.filter` + `req.start` (the `pkConstraint` short-circuit may
  interact with start).
- `req.filter` + `req.multiConstraints` — `#fetchMulti` spreads `req` so
  `filter` propagates into the recursive `#fetch` call, but nothing
  verifies it.

## 4. Pass-through operators not covered in `predicate-pushdown.test.ts`

`Take` and `Join` get reasoned about in the contract comment on
`operator.ts:81-110`, but no test asserts they preserve `req.filter`.
They're the kind of operator someone could easily refactor and silently
drop the filter. Quick `RecordingInput`-style tests for both would be
cheap insurance.

`Cap` (`cap.ts:115`) intentionally builds a fresh `{constraint}` request
and drops `req.filter`. That's safe today because Cap is only used as a
non-flipped EXISTS child whose only consumer is a Join with no filter —
but it's an undocumented restriction. Either add an assert that
`req.filter === undefined`, or document the carve-out next to the new
contract block in `operator.ts`.

## 5. `PlannerFilter` only tested through the graph builder

Direct unit tests would catch regressions earlier:

- `propagateConstraints` with `from.kind === 'fan-in' && from.type === 'FI'`
  does **not** call `setPerBranchFilter` (the "FI mode is skipped" rule).
- `propagateConstraints` with `from.kind === 'fan-in' && from.type === 'UFI'`
  **does** call it, with the right branchPattern.
- `findParentConnection` bails on nested `fan-in` (the "deeply nested OR
  loses the optimization" carve-out).
- `findParentConnection` walks through `join.parent` correctly.

## 6. No end-to-end runtime correctness test for the full pipeline

The planner-controlled tests verify _plan_ shape;
`predicate-pushdown.test.ts` verifies _forwarding_ shape. There's no test
that builds a real pipeline for `or(simple, exists)` with a flipped CSQ
and asserts the **result rows are correct** when the simple branch's
filter actually rides `req.filter` to the source. If the planner picks
UFI but a downstream operator silently drops the filter, plan tests pass
and results are wrong. One integration test in `zql-integration-tests`
for this shape would catch a whole category of regressions.

## Priorities

If only two are done, do **#1** (overlay × filter — highest correctness
risk and intricate) and **#6** (end-to-end runtime — catches integration
drift across the stack). **#2** is also worth it given SQLite is the
production path.
