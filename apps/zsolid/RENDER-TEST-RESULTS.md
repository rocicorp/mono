# Zero + Solid Render Optimization Test Results

**Date**: 2026-01-23
**Test**: Verify that Zero's immutable applyChange optimization works in Solid

## Summary

**Verdict: PASS**

When ONE issue is updated via psql, ONLY that issue's row re-renders. Other rows do not re-render.

## Test Procedure

1. Started zsolid app with 20 issues loaded (query state: complete)
2. Recorded video of the test
3. Took screenshot of initial render counts (all rows at 1)
4. Updated one issue title via psql: `UPDATE issue SET title='MODIFIED BY PSQL' WHERE id='HdpMkgbHpK3_OcOIiQOuW';`
5. Waited 3 seconds for Zero sync
6. Took screenshot of final render counts
7. Compared render counts before/after

## Results

### Render Count Comparison

```
+-----+--------------------------------------------------+---------+---------+
| Row | Title                                            | Before  | After   |
+-----+--------------------------------------------------+---------+---------+
|  1  | Leaking listeners on AbortSignal [TEST 2]       |    1    |    2*   |
|     | -> MODIFIED BY PSQL                             |         |         |
+-----+--------------------------------------------------+---------+---------+
|  2  | Sort and remove duplicates from refs            |    1    |    1    |
+-----+--------------------------------------------------+---------+---------+
|  3  | UnknownError: Internal error opening backing... |    1    |    1    |
+-----+--------------------------------------------------+---------+---------+
|  4  | Presence keys not removed when room is inv...   |    1    |    1    |
+-----+--------------------------------------------------+---------+---------+
|  5  | Buffer tail messages during authHandler...      |    1    |    1    |
+-----+--------------------------------------------------+---------+---------+
|  6  | TODO: Add unit tests for statement              |    1    |    1    |
+-----+--------------------------------------------------+---------+---------+
|  7  | zql: Limit needs to pull more data              |    1    |    1    |
+-----+--------------------------------------------------+---------+---------+

* Only the modified row (Row 1) increased from 1 -> 2
```

### Console Output

```
[log] [update #2] IssueRow: HdpMkgbH - "MODIFIED BY PSQL"
[log] [update #2] IssueRow: HdpMkgbH - "MODIFIED BY PSQL"
```

Only the modified issue (ID: HdpMkgbHpK3_OcOIiQOuW, short: HdpMkgbH) triggered a re-render.
No other IssueRow logs appeared.

## Evidence

| Artifact          | Path                    |
|-------------------|-------------------------|
| Video recording   | /tmp/render-test.webm   |
| Before screenshot | /tmp/before.png         |
| After screenshot  | /tmp/after.png          |
| Working app       | /tmp/zsolid-working.png |

## Conclusion

Zero's immutable applyChange optimization works correctly with SolidJS:

1. **Modified row re-rendered**: Render count 1 → 2
2. **Other rows unchanged**: All remain at render count 1
3. **Console confirms**: Only one IssueRow component logged a re-render

Solid's fine-grained reactivity combined with Zero's immutable change detection ensures minimal re-renders. This is the expected behavior for a well-optimized reactive system.

## Technical Notes

During setup, we discovered that with `enableLegacyQueries: false` in the Zero schema, you must use named queries defined with `defineQuery`/`defineQueries` that are deployed to the Zero server. Raw builder queries won't sync data.

The fix was to import and use the existing `queries.issueList()` from the zbugs app instead of creating raw builder queries.
