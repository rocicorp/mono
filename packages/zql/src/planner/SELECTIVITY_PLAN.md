# Semi-Join Selectivity Forward Plan

## Problem Statement

The current planner implementation conflates two fundamentally different concepts when estimating costs for semi-joins (EXISTS clauses with LIMIT):

1. **Filter Selectivity**: The fraction of child rows that pass filter predicates
2. **Semi-Join Selectivity**: The fraction of parent rows that have at least one matching child

This conflation leads to incorrect cost estimates for queries with LIMIT clauses on the outer table.

## Background

### Example Scenario

Consider the query:

```sql
SELECT * FROM users
WHERE EXISTS (
  SELECT 1 FROM posts
  WHERE posts.userId = users.id
  AND posts.published = true
)
LIMIT 10
```

The planner needs to estimate: **How many users must we scan to find 10 users that have at least one published post?**

### Current Implementation (Incorrect)

```typescript
// In planner-connection.ts:129-139
const costWithFilters = model(table, sort, filters, undefined);
const costWithoutFilters = model(table, sort, undefined, undefined);
this.selectivity = costWithFilters / costWithoutFilters;

// In planner-join.ts:210-214
scanEst = Math.min(scanEst, parentCost.limit / childCost.selectivity);
```

**What it calculates**: If 50% of posts are published → selectivity = 0.5
**What it estimates**: Need to scan 10 / 0.5 = 20 users

**Problem**: This assumes 50% of users have published posts, but the actual probability depends on how posts are distributed across users (the "fan-out").

### The Two Selectivities Compared

| Scenario                                                                                      | Filter Selectivity                 | Semi-Join Selectivity                             | Reality                                 |
| --------------------------------------------------------------------------------------------- | ---------------------------------- | ------------------------------------------------- | --------------------------------------- |
| Even distribution<br/>1000 users, 10 posts each<br/>50% published                             | 0.5<br/>(50% of posts pass filter) | 0.999<br/>(P(user has ≥1 published) = 1 - 0.5^10) | Almost all users have ≥1 published post |
| Skewed distribution<br/>900 users with 0 posts<br/>100 users with 100 posts<br/>50% published | 0.5<br/>(still 50% of posts pass)  | 0.1<br/>(only 100/1000 users have any posts)      | Only 10% of users have posts at all     |

**Key insight**: Filter selectivity tells us about child rows; semi-join selectivity tells us about parent rows. They're only equal when fan-out = 1 (foreign key from parent to child).

## The Fan-Out Formula

The relationship between filter and semi-join selectivity is:

```
semiJoinSelectivity = 1 - (1 - filterSelectivity)^fanOut
```

Where **fan-out** = average number of child rows per parent row.

### Proof Intuition

For a parent with N children, each with independent probability P of matching:

- Probability NO children match: (1 - P)^N
- Probability ≥1 child matches: 1 - (1 - P)^N

### Edge Cases

**When fan-out = 1** (foreign key from parent → child):

```
semiJoinSelectivity = 1 - (1 - filterSelectivity)^1
                    = filterSelectivity
```

Current implementation is correct! ✅

**When fan-out = 10** (typical one-to-many):

- filterSelectivity = 0.5 → semiJoinSelectivity = 0.999
- filterSelectivity = 0.1 → semiJoinSelectivity = 0.651
- filterSelectivity = 0.01 → semiJoinSelectivity = 0.096

**When fan-out = 100** (high cardinality):

- filterSelectivity = 0.5 → semiJoinSelectivity ≈ 1.0
- filterSelectivity = 0.1 → semiJoinSelectivity ≈ 1.0
- filterSelectivity = 0.01 → semiJoinSelectivity = 0.634

## Solution: Query sqlite_stat1 for Fan-Out

### What sqlite_stat1 Provides

After running `ANALYZE`, SQLite populates `sqlite_stat1` with index statistics:

```sql
SELECT tbl, idx, stat FROM sqlite_stat1;

-- Example result:
-- tbl='posts', idx='idx_posts_userId', stat='10000 100'
--                                             ^     ^
--                                             |     |
--                                    total rows   avg rows per distinct userId
```

The `stat` column format: `"totalRows avgRowsPerDistinct [avgRowsPerDistinct2 ...]"`

For an index on `posts.userId`:

- First number: Total rows in the index (10,000 posts)
- Second number: Average rows per distinct value of userId (100 posts per user)
- **The second number IS the fan-out!**

### Implementation Strategy

```typescript
function getFanOutFromStats(
  db: Database,
  tableName: string,
  columnName: string,
): number | undefined {
  // 1. Find indexes containing the column
  const indexes = db
    .prepare(
      `
    SELECT name FROM sqlite_master
    WHERE type='index'
    AND tbl_name=?
    AND sql LIKE '%' || ? || '%'
  `,
    )
    .all(tableName, columnName);

  if (indexes.length === 0) {
    return undefined; // No index, can't get stats
  }

  // 2. Query sqlite_stat1 for the first matching index
  for (const {name: indexName} of indexes) {
    const result = db
      .prepare(
        `
      SELECT stat FROM sqlite_stat1
      WHERE tbl=? AND idx=?
    `,
      )
      .get(tableName, indexName);

    if (result && result.stat) {
      const parts = result.stat.split(' ');
      if (parts.length >= 2) {
        return parseInt(parts[1], 10); // Average rows per distinct value
      }
    }
  }

  return undefined; // Stats not available (ANALYZE not run)
}
```

### Fallback Strategy

```typescript
const DEFAULT_FANOUT = 3; // Conservative middle ground

const fanOut = getFanOutFromStats(db, table, joinColumn) ?? DEFAULT_FANOUT;
const semiJoinSelectivity = 1 - Math.pow(1 - filterSelectivity, fanOut);
```

**Why 3?**

- SQLite's default is 10 (might be too optimistic)
- fan-out = 1 is common for FK relationships
- fan-out = 3 is a conservative middle ground
- Overestimating selectivity → slightly less efficient but still correct
- Underestimating selectivity → risk of bad plans

## Design Decisions

### Default Fan-Out Value

**Options**:

- 1: Conservative, assumes FK relationships (current behavior)
- 3: Moderate, safe middle ground
- 10: SQLite's default, optimistic

**Recommendation**: Use 3

- Safer than 10 for sparse relationships
- More accurate than 1 for typical one-to-many
- Easy to tune based on production data

## Edge Cases and Considerations

### 1. No Index on Join Column

If the child table has no index on the join column:

- `getFanOutFromStats()` returns `undefined`
- Fall back to `DEFAULT_FANOUT`
- Cost estimates may be less accurate but still safe

### 2. ANALYZE Not Run

If `sqlite_stat1` is empty:

- `getFanOutFromStats()` returns `undefined`
- Fall back to `DEFAULT_FANOUT`
- Document recommendation to run ANALYZE periodically

### 3. Composite Indexes

If join column is part of a multi-column index:

- `sqlite_stat1` provides stats at each position
- Use the stat corresponding to the join column's position
- May require more sophisticated parsing

### 4. Multiple Indexes on Same Column

If multiple indexes contain the column:

- Use first index found (arbitrary but consistent)
- Could enhance to prefer indexes where column is leftmost

### 5. Zero Selectivity

Current code has a bug where `semiJoinSelectivity = 0` (EXISTS can never succeed):

```typescript
if (childCost.semiJoinSelectivity === 0) {
  // EXISTS can never succeed - result set is empty
  return {
    rows: 0,
    runningCost: parentCost.runningCost, // Cost to discover emptiness
    filterSelectivity: 0,
    semiJoinSelectivity: 0,
    limit: parentCost.limit,
  };
}
```

This should be fixed in the same PR.

### 6. NOT EXISTS

For `NOT EXISTS`, we don't short-circuit on first match:

- Must scan ALL children to verify none match
- Don't apply limit or use semi-join selectivity
- Current code correctly doesn't set limit for NOT EXISTS

## Future Enhancements

### Enhancement 1: Use sqlite_stat4 for Distribution

**Problem**: `sqlite_stat1` provides average fan-out, but distributions can be highly skewed.

**Solution**: Sample actual distribution from `sqlite_stat4`:

```typescript
function getSemiJoinSelectivityWithStat4(
  db: Database,
  childTable: string,
  childColumn: string,
  filterSelectivity: number,
): number {
  // Query sqlite_stat4 for distribution samples
  const samples = db
    .prepare(
      `
    SELECT CAST(neq AS INTEGER) as fanOut
    FROM sqlite_stat4
    WHERE tbl=? AND idx LIKE '%' || ? || '%'
  `,
    )
    .all(childTable, childColumn);

  if (samples.length === 0) {
    return fallbackToStat1();
  }

  // Calculate selectivity for each sample's fan-out
  const selectivities = samples.map(
    s => 1 - Math.pow(1 - filterSelectivity, s.fanOut),
  );

  // Average across the distribution
  return selectivities.reduce((a, b) => a + b, 0) / selectivities.length;
}
```

**Challenge**: `stat4` only samples rows that exist in the index. Parents with 0 children don't appear. Need to adjust for missing parents.

## References

### PostgreSQL Documentation

- [Semi-Join Planning](https://postgrespro.com/blog/pgsql/5969618)
- [compute_semi_anti_join_factors](https://doxygen.postgresql.org/costsize_8c.html)
  - Calculates `outer_match_frac`: fraction of outer rows that have matches
  - Uses NDV (number of distinct values) from statistics

### SQLite Documentation

- [sqlite_stat1 Format](https://sqlite.org/fileformat2.html#stat1tab)
- [Query Planning Overview](https://sqlite.org/optoverview.html)
- [ANALYZE Command](https://sqlite.org/lang_analyze.html)

### Academic References

- "Access Path Selection in a Relational Database Management System" (Selinger et al., 1979)
  - Original paper on cost-based optimization
  - Discusses selectivity estimation for joins

## Open Questions

1. **Should we fix zero selectivity edge case in same PR?**
   - Pro: Related to selectivity handling
   - Con: Separate concern, could be separate PR

2. **What's the right default fan-out?**
   - 1: Current behavior (conservative for FK, wrong for one-to-many)
   - 3: Moderate (recommended)
   - 10: SQLite's default (optimistic)

3. **Should we add warnings/logging when falling back to default?**
   - Could help users identify missing ANALYZE or indexes
   - Might be noisy for intentionally simple cost models
