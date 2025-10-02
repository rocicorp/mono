# LIMIT + ORDER BY Antipatterns: Quick Reference

## The Core Pattern That Fails

```sql
SELECT * FROM table
WHERE [complex/selective filter]
ORDER BY [indexed column]
LIMIT n;
```

**What Postgres does:**
1. Scan table in ORDER BY order (using index)
2. Filter each row
3. Stop after finding n matches

**When this fails:** Filter is selective (few matches) or expensive to evaluate

---

## Antipattern Matrix

| Scenario | Query Pattern | What Happens | Why It's Bad | Fix |
|----------|---------------|--------------|--------------|-----|
| **Rare Value** | `WHERE col = 'rare'`<br>`ORDER BY other`<br>`LIMIT 100` | Scans all rows in `other` order, filters each | Rare value → scans entire table | Use composite index `(col, other)` |
| **Correlated Join** | `WHERE EXISTS (join)`<br>`ORDER BY col`<br>`LIMIT 100` | Scans in `col` order, evaluates EXISTS for each | Can't use statistics, assumes high selectivity | Use literal value or app-side lookup |
| **Wrong Index** | `WHERE filter_col = X`<br>`ORDER BY sort_col`<br>`LIMIT 100` | Uses index on `sort_col`, filters after | Should filter first if selective | Composite index `(filter_col, sort_col)` |
| **Expensive Filter** | `WHERE expensive_function()`<br>`ORDER BY col`<br>`LIMIT 100` | Evaluates function for every row in sort order | Function called potentially millions of times | Filter with index first, then expensive check |
| **JOIN + DISTINCT** | `SELECT DISTINCT ... FROM joins`<br>`ORDER BY col`<br>`LIMIT 100` | Sorts by ALL columns, processes all rows | Can't stop at LIMIT until dedupe complete | Use EXISTS instead of JOIN |
| **Text Search** | `WHERE body LIKE '%text%'`<br>`ORDER BY created`<br>`LIMIT 10` | Scans newest, does text search on each | No index, sequential scan per row | Filter first or use full-text index |

---

## Real Examples from zbugs Dataset

### ❌ Antipattern 1: Rare Creator

```sql
-- TestUser has 0 issues
SELECT * FROM issue
WHERE "creatorID" = (SELECT id FROM "user" WHERE name = 'TestUser')
ORDER BY created
LIMIT 100;
```

**Cost:** 168 (but scans all 242,400 rows finding nothing)

**Fix:**
```sql
-- Get ID in application, use as literal
SELECT * FROM issue
WHERE "creatorID" = 'test-user-123'  -- MCV lookup → estimates 1 row
ORDER BY created
LIMIT 100;
```

**New cost:** 8.39 (uses composite index, sorts 0 rows)

---

### ❌ Antipattern 2: Activity Feed Without Index

```sql
SELECT c.*, i.title FROM comment c
JOIN issue i ON i.id = c."issueID"
ORDER BY c.created DESC
LIMIT 50;
```

**Before index:** Cost 285,182
- Parallel seq scan both tables
- Hash join 2.4M rows
- Sort all to get top 50

**After `CREATE INDEX comment_created_desc_idx`:** Cost 19.55
- Index scan top 50 comments
- Nested loop lookup 50 issues
- 14,600x faster!

---

### ❌ Antipattern 3: Comment Count Aggregation

```sql
SELECT i.*, COUNT(c.id) FROM issue i
LEFT JOIN comment c ON c."issueID" = i.id
WHERE i.open = true
GROUP BY i.id
ORDER BY i.modified DESC
LIMIT 100;
```

**Cost:** 510,513
- Joins ALL 2.4M comments
- Groups all results
- Sorts all groups
- Then limits

**Fix with Subquery:**
```sql
SELECT i.*, (
  SELECT COUNT(*) FROM comment c WHERE c."issueID" = i.id
) as count
FROM issue i
WHERE i.open = true
ORDER BY i.modified DESC
LIMIT 100;
```

**New cost:** 944
- Get top 100 issues first
- Count only their comments
- 540x faster!

---

### ❌ Antipattern 4: Label Filter

```sql
SELECT * FROM issue i
WHERE EXISTS (
  SELECT 1 FROM "issueLabel" il
  JOIN label l ON l.id = il."labelID"
  WHERE il."issueID" = i.id AND l.name = 'engineering'
)
ORDER BY modified DESC
LIMIT 100;
```

**Cost:** 4,420
- Can't use composite index (don't know labelID until runtime)
- Generic selectivity estimate
- Scans by modified, checks EXISTS for each

**No perfect fix** (junction table design), but acceptable for 1,803 matches

---

### ⚠️ Antipattern 5: Assignee with Wrong Index

```sql
SELECT * FROM issue
WHERE "assigneeID" = 'FzHEjzbGL1' AND open = true
ORDER BY modified DESC
LIMIT 100;
```

**Current:** Cost 227
- Uses `(open, modified)` index
- Filters by assigneeID (19,836 rows = 16%)
- Assumes finds 100 in ~600 rows

**Composite index ignored:**
```sql
CREATE INDEX ON issue("assigneeID", open, modified DESC);
-- Postgres won't use it - existing plan is actually optimal for high-frequency assignee
```

**When composite helps:** Rare assignees with few issues

---

## Detection Heuristics

### Signs of LIMIT + ORDER BY Problems

1. **High cost estimate but "should be fast"**
   - Cost > 10,000 for simple-looking query
   - Actual execution much slower than estimate

2. **Sequential scan with LIMIT**
   - `Seq Scan ... Filter: ...` with `Limit` on top
   - Should use index if one exists

3. **Sort node before Limit**
   - Sorts entire result set, then limits
   - Should limit first when possible

4. **Nested Loop with high outer rows**
   - Outer relation has 100,000+ rows
   - Inner relation evaluated for each
   - With LIMIT, should reduce outer rows first

5. **Actual rows >> estimated rows in EXPLAIN ANALYZE**
   - Estimated 100 rows, actually scanned 100,000
   - Selectivity estimate was way off

### Query Plan Red Flags

```
❌ BAD:
Limit
  ->  Sort  (cost=X rows=1000000)  -- Sorting before limit!
        ->  Seq Scan  -- No index use
              Filter: [complex condition]

❌ BAD:
Limit
  ->  Nested Loop  (cost=X rows=100)
        ->  Index Scan (rows=1000000)  -- High row count on outer
        ->  Materialize/Join

✅ GOOD:
Limit
  ->  Index Scan using composite_idx
        Index Cond: (filter AND sort)  -- Both in index!

✅ GOOD:
Limit
  ->  Nested Loop
        ->  Index Scan (rows=100)  -- Low row count on outer
        ->  Index Seek (rows=1)
```

---

## Fix Decision Tree

```
Query has LIMIT + ORDER BY + WHERE?
│
├─ Is WHERE column in index with ORDER BY column?
│  ├─ YES → ✅ Should use composite index
│  │         └─ If not used: Check statistics, consider ANALYZE
│  └─ NO → Continue
│
├─ Is WHERE filter selective (<1% of rows)?
│  ├─ YES → ❌ Problem! Options:
│  │         1. Create composite index (filter_col, sort_col)
│  │         2. Rewrite to filter first, sort after
│  │         3. Use literal values (enables MCV lookup)
│  └─ NO → Continue
│
├─ Is WHERE filter a join/EXISTS/IN?
│  ├─ YES → ❌ Likely problem! Options:
│  │         1. Convert to literal value (app does lookup)
│  │         2. Use CTE to materialize first
│  │         3. Accept suboptimal plan if result set small
│  └─ NO → Continue
│
├─ Is WHERE filter expensive (function, text search)?
│  ├─ YES → ❌ Problem! Options:
│  │         1. Add index on function result
│  │         2. Use cheaper filter first, expensive after
│  │         3. Remove LIMIT, filter in application
│  └─ NO → Continue
│
└─ Check EXPLAIN plan:
   ├─ Cost > 10,000? → Investigate
   ├─ Sequential scan? → Add index
   ├─ Sort before Limit? → Reorder operations
   └─ Otherwise → ✅ Probably OK
```

---

## Quick Fixes Cheat Sheet

| Problem | Quick Fix | Better Fix |
|---------|-----------|------------|
| Rare value filter | Use literal value | Composite index |
| Correlated subquery | App-side lookup | Two-step query |
| Wrong index used | Force with pg_hint_plan | Better composite index |
| Expensive filter | CTE to filter first | Index on expression |
| JOIN + DISTINCT | Use EXISTS | Denormalize |
| No index on sort | Add index on sort col | Composite index |
| Text search | LIMIT without ORDER | Full-text index |
| Large result set | Cursor/keyset pagination | Materialized view |

---

## Prevention: Index Design Patterns

### Pattern 1: Filter + Sort Composite
```sql
CREATE INDEX table_filter_sort_idx ON table(filter_column, sort_column DESC);
```
**Enables:** Direct index scan, already sorted

### Pattern 2: Partial Index for Common Filters
```sql
CREATE INDEX table_active_sorted_idx
ON table(user_id, created DESC)
WHERE active = true;
```
**Enables:** Skip inactive rows entirely

### Pattern 3: Expression Index for Computed Filters
```sql
CREATE INDEX users_name_lower_idx ON users(LOWER(name));
```
**Enables:** Case-insensitive search with index

### Pattern 4: Covering Index
```sql
CREATE INDEX table_filter_sort_covering_idx
ON table(filter_col, sort_col)
INCLUDE (other_selected_columns);
```
**Enables:** Index-only scan, no table lookup

---

## Testing Checklist

Before deploying queries with LIMIT + ORDER BY:

- [ ] Run `EXPLAIN ANALYZE` with production-like data
- [ ] Test with low-selectivity values (rare users, old dates)
- [ ] Check actual rows vs estimated rows
- [ ] Verify index is used (not seq scan)
- [ ] Test with and without LIMIT (should be similar strategy)
- [ ] Check for Sort node before Limit (red flag)
- [ ] Measure actual execution time, not just cost
- [ ] Test at different LIMIT values (100, 1000, 10000)

**Golden rule:** If removing LIMIT changes the plan dramatically, the LIMIT plan is probably wrong.
