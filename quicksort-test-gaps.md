# `mlaw/quicksort` — test coverage gaps

Branch: `mlaw/quicksort` (commits `d55c039b0` quicksort of flipped-join fetches on pk, `ef237a592` unique key prop)

The implementation in `packages/zql/src/ivm/flipped-join.ts` looks correct. The gaps below are in **test coverage of the new code paths**, not in the logic.

## Gaps

| Gap                                     | Severity | Notes                                                                                                                                                                                                                                                                                                                                                            |
| --------------------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Non-PK unique-index path                | Medium   | No test exercises a `FlippedJoin` where `parentKey` matches a _non-primary_ unique index. Existing fetch tests use `parentKey: ['id']` (= PK) or `parentKey: ['ownerID']` (no unique index → merge-sort). The new branch in `#parentKeyIsUnique` at `flipped-join.ts:90-93` is reachable only via `TableSource`, and there's no zqlite-level `FlippedJoin` test. |
| `req.reverse` with quicksort            | Medium   | Zero hits for `reverse: true` in `flipped-join.fetch.test.ts` / `flipped-join.more-fetch.test.ts`. Predates this branch, but the new sort uses `dir = req.reverse ? -1 : 1` at `flipped-join.ts:222-223`, so a regression here would go undetected.                                                                                                              |
| `req.start` with quicksort              | Medium   | No `start: {…}` in any flipped-join test. Per-child fetch propagates `req.start` (`flipped-join.ts:202-207`), but the sort + start-cursor interaction is novel.                                                                                                                                                                                                  |
| Multi-column unique parentKey           | Low      | The compound-key test at `flipped-join.fetch.test.ts:1546` uses `parentKey: ['a1', 'a2']` with default PK `['id']` → falls through to merge-sort. Quicksort path with compound unique keys is untested.                                                                                                                                                          |
| `TableSource.getSchema().uniqueIndexes` | Low      | `table-source.test.ts` doesn't assert the new schema property. A trivial assertion test would lock the contract.                                                                                                                                                                                                                                                 |

## Suggested additions (priority order)

1. **pg integration test (or zqlite-rooted unit test) with a non-PK unique index** as parent-side correlation — the only path that depends on the new `uniqueIndexes` schema plumbing.
2. **`req.reverse: true` in `#fetchQuicksort`** — at least one fetch test, ideally with children appearing in a different order than parents.
3. **`req.start` cursor in both directions** — verify per-fetch `start` propagation interacts correctly with the post-sort yield.
4. **Multi-column compound unique parentKey** — variant of the existing multi-column test with PK aligned to the join key.

## Notes from review

- `MemorySource` (`memory-source.ts:148-158`) does **not** populate `uniqueIndexes`. On the client the quicksort path triggers only when `parentKey === primaryKey`. On server `TableSource` it triggers for all unique indexes. Intentional, safe (both paths produce identical output), but worth documenting if downstream consumers ever assume parity.
- The `flipped-join.sibling.test.ts` snapshot diff is purely fetch-trace reordering (sequential per-child vs. interleaved iterators) — same fetches, different order. Expected.
