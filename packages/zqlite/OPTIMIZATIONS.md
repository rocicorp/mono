# table-source.ts row-pump optimizations

Investigation context: profiling the flipped+semi plan for the zbugs
"gatewaycore + labels=[api-gateway, async-processing]" query showed it spent
260 s wall-time, 96.6% of which was inside `next()` (the better-sqlite3 →
JS row marshalling path).

After fixing the O(N²) bug in `Debug.rowVended` (separate change), the same
query runs in **65 s** with the same plan and the same row counts
(475,051 rows vended, 1,098,099 rows scanned, 211 synced, 50 returned).

96.6% of that 65 s is now `next` self-time — i.e. ~132 µs/row marshalled
across the C++/JS boundary plus JS-side row reconstruction. This document
lists optimizations that shave that per-row cost **without changing the
plan** or the join shape.

## Options

| #   | what                                                                                                                                                                              | scope                                           | expected     |
| --- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- | ------------ |
| 1   | Drop `safeIntegers(true)` when the schema doesn't need BigInts                                                                                                                    | 1-line in `#fetch`                              | small–medium |
| 2   | Replace `timeSampled(…, () => next(), …, () => msg)` with an inline fast path that skips both closure allocations when not sampling                                               | ~10 lines in `#mapFromSQLiteTypes`              | small        |
| 3   | Use `statement.raw(true)` + a pre-built per-source column extractor instead of `fromSQLiteTypes`                                                                                  | ~30 lines in `table-source.ts`                  | **medium**   |
| 4   | Slim-row projection in `flipped-join.ts`: only request `id, modified` from SQLite for the rows passing through the join, then re-fetch the surviving 50 by PK to get the full row | structural; touches table-source + flipped-join | large        |
| 5   | Collapse the `mapFromSQLiteTypes → generateWithOverlay → generateWithStart → generateWithYields` generator chain into a single loop for the no-overlay no-start fast path         | ~50 lines in `table-source.ts`                  | medium       |

Per-run cost (warm cache, tera.db): ~1 min per measurement. After each
optimization we re-profile and record the wall-time + `next` self-share.

## Constraints

- Plan stays the same: same SQL, same row counts, same join order. Planner
  fixes are a separate workstream.
- Behavior unchanged: same conversions, same null-handling, same
  BigInt-bounds errors.

## Status

- **#3 in progress** — `raw(true)` mode + per-source extractor.
