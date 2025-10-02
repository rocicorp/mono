# When LIMIT + ORDER BY Causes Bad Query Plans

## Overview

The combination of `LIMIT` and `ORDER BY` causes Postgres to make fundamentally different planning decisions than queries without `LIMIT`. This document catalogs all the cases where this leads to suboptimal plans.

---

## The Core Problem: Early Termination Assumption

When Postgres sees `LIMIT n`, it assumes it can stop execution after finding `n` rows. This leads to a preference for:
1. **Scanning in the ORDER BY order** (using index on sort column)
2. **Filtering as it goes** (applying WHERE conditions to each row)
3. **Stopping early** when it hits the limit

This strategy works well when:
- Matches are common (high selectivity)
- Matches are evenly distributed across the sort order
- The filter is cheap to evaluate

**It fails catastrophically when:**
- Matches are rare (low selectivity)
- Matches are clustered at the end of the sort order
- The filter requires expensive operations (joins, subqueries)

---

## Case 1: Rare Values with Non-Selective Index

### The Query
```sql
SELECT * FROM issue
WHERE "creatorID" = 'test-user-123'  -- User with 0 issues
ORDER BY created
LIMIT 100;
```

### What Postgres Does (WRONG)

**With LIMIT:**
```
Limit  (cost=0.42..203.82 rows=100 width=453)
  ->  Nested Loop  (cost=0.42..70469.36 rows=34629 width=453)
        ->  Index Scan using issue_created_idx on issue
              rows=242400  -- ALL ROWS!
        ->  Materialize
              ->  Seq Scan on "user"
                    Filter: ((name)::text = 'TestUser'::text)
```

**Strategy:**
1. Scan ALL 242,400 issues in `created` order
2. For each row, check if creatorID matches TestUser
3. Stop after finding 100 matches

**Reality:** TestUser has 0 issues, so it scans ALL 242,400 rows finding nothing!

**Cost estimate:** 203.82 (assumes will find 100 quickly)
**Actual cost:** Scans entire table

### What Postgres SHOULD Do

**Without LIMIT (correct plan):**
```
Gather Merge
  ->  Sort
        ->  Hash Join
              ->  Parallel Seq Scan on issue
              ->  Hash
                    ->  Seq Scan on "user"
                          Filter: name = 'TestUser'
```

**Strategy:**
1. Find TestUser's ID
2. Hash join to get all matching issues
3. Sort results
4. Return all rows

**Why this is better:** Filters first, only sorts matching rows (0 in this case)

### Root Cause

**The problem:** Postgres uses **generic selectivity estimate** for correlated joins
- Can't look up TestUser's actual frequency at plan time
- Assumes uniform distribution: `1 / n_distinct = 1 / 6 = 16.7%`
- Estimates ~34,629 matching rows
- Thinks scanning will quickly find 100 matches

**The fix requires:** Literal value instead of join
```sql
WHERE "creatorID" = 'test-user-123'  -- Postgres can check MCV, finds 0, estimates 1 row
```

---

## Case 2: Join Preventing Index Use

### The Query
```sql
SELECT * FROM issue
WHERE EXISTS (
  SELECT 1 FROM "user"
  WHERE name = 'Naomi' AND "user".id = issue."creatorID"
)
ORDER BY modified
LIMIT 100;
```

### What Postgres Does (SUBOPTIMAL)

**Plan:**
```
Limit  (cost=0.42..203.82 rows=100 width=453)
  ->  Nested Loop
        Join Filter: ((issue."creatorID")::text = ("user".id)::text)
        ->  Index Scan using issue_modified_idx on issue
              rows=242400  -- Scans in modified order
        ->  Materialize
              ->  Seq Scan on "user"
                    Filter: name = 'Naomi'
```

**Strategy:**
1. Scan issues in `modified` order
2. For each issue, check if creator is Naomi via correlated subquery
3. Stop at 100 matches

**Why it's suboptimal:**
- Must evaluate EXISTS for every row scanned
- Can't use composite index `(creatorID, modified)` because doesn't know creatorID until runtime
- Estimates 34,629 total matches, so assumes will find 100 quickly

### Better Alternative: Scalar Subquery

```sql
WHERE "creatorID" = (SELECT id FROM "user" WHERE name = 'Naomi')
```

**Still not optimal (uses generic estimate), but slightly better:**
```
Limit
  InitPlan 1 (returns $0)
    ->  Seq Scan on "user"
  ->  Index Scan using issue_modified_idx
        Filter: ("creatorID" = ($0))
```

**Why still suboptimal:** Can't use MCV for parameter `$0`

### Optimal: Two-Step Approach

```sql
-- Step 1: Get the creatorID (application does this)
-- Step 2: Use literal
SELECT * FROM issue
WHERE "creatorID" = 'lHr1oe7FW0'  -- Literal enables MCV lookup
ORDER BY modified
LIMIT 100;
```

**Plan with composite index:**
```
Limit
  ->  Index Scan using issue_creatorid_modified_idx
        Index Cond: ("creatorID" = 'lHr1oe7FW0')
        rows=46403  -- Accurate from MCV!
```

**Why optimal:**
- Direct index scan on `(creatorID, modified)`
- Results already sorted
- Knows exact row count from MCV
- Can make correct cost decision

---

## Case 3: Filter Column Not in Sort Index

### The Query
```sql
SELECT * FROM issue
WHERE "assigneeID" = 'FzHEjzbGL1'  -- 19,836 issues
AND open = true
ORDER BY modified DESC
LIMIT 100;
```

### What Postgres Does

**Plan:**
```
Limit  (cost=0.42..222.52 rows=100 width=453)
  ->  Index Scan Backward using issue_open_modified_idx on issue
        Index Cond: (open = true)
        Filter: (("assigneeID")::text = 'FzHEjzbGL1'::text)
        rows=19836  -- Accurate estimate from MCV
```

**Strategy:**
1. Scan backwards on `(open, modified)` index
2. Filter each row by assigneeID
3. Stop after finding 100 matches

**Why it's chosen:**
- Index is `(open, modified)` not `(open, assigneeID, modified)`
- Postgres estimates 19,836 matching rows (16% of open issues)
- Assumes will find 100 matches in first ~600 rows scanned
- Cost: 222.52

### Why This Can Be Wrong

**Worst case:** Assignee's issues clustered at beginning of time
- Scan might need to check 100,000+ rows to find 100 for this assignee
- If assignee recently joined and only has old issues

**Best case:** Assignee's issues evenly distributed
- Finds 100 matches in ~600 rows
- Current plan is optimal

**The problem:** Postgres assumes even distribution but reality varies

### When Composite Index Would Help

```sql
CREATE INDEX issue_assignee_open_modified_idx
ON issue("assigneeID", open, modified DESC);
```

**New plan:**
```
Limit
  ->  Index Scan using issue_assignee_open_modified_idx
        Index Cond: ("assigneeID" = 'FzHEjzbGL1' AND open = true)
```

**Why better:**
- Direct lookup by assigneeID + open
- Results already sorted by modified
- No filtering needed
- Guaranteed to find matches immediately

**Why Postgres doesn't use it:**
- Cost model says `(open, modified)` + filter is cheaper
- For high-frequency assignees (16%), this may be correct
- For rare assignees, composite would be better

---

## Case 4: Expensive Filter with LIMIT

### The Query
```sql
SELECT * FROM issue
WHERE EXISTS (
  SELECT 1 FROM comment
  WHERE comment."issueID" = issue.id
  AND comment.body LIKE '%critical bug%'
)
ORDER BY modified DESC
LIMIT 10;
```

### What Postgres Does (VERY WRONG)

**Plan:**
```
Limit
  ->  Nested Loop Semi Join
        ->  Index Scan Backward using issue_modified_idx on issue
              rows=242400
        ->  Seq Scan on comment
              Filter: ("issueID" = issue.id AND body LIKE '%critical bug%')
```

**Strategy:**
1. Scan issues in modified DESC order (newest first)
2. For EACH issue, scan comments looking for matching text
3. Stop after finding 10 issues

**Why catastrophically bad:**
- If matches are rare, scans thousands of issues
- For each issue, performs expensive text search on comments
- 242,400 issues × avg 10 comments each = 2.4M comment scans in worst case

### Why LIMIT Makes It Worse

**Without LIMIT:**
```
Hash Join
  ->  Seq Scan on issue
  ->  Hash
        ->  Seq Scan on comment
              Filter: body LIKE '%critical bug%'
```

**Strategy:**
1. Scan all comments once, find matches (maybe 100 matches)
2. Hash join with issues
3. Sort results
4. Return all

**Much better:** Scans 2.4M comments once vs. potentially 2.4M times

### The Fix

**Option 1: Filter comments first**
```sql
SELECT i.* FROM issue i
WHERE i.id IN (
  SELECT DISTINCT "issueID" FROM comment
  WHERE body LIKE '%critical bug%'
)
ORDER BY modified DESC
LIMIT 10;
```

**Option 2: Full-text search index**
```sql
CREATE INDEX comment_body_fts ON comment USING gin(to_tsvector('english', body));

SELECT * FROM issue
WHERE EXISTS (
  SELECT 1 FROM comment
  WHERE "issueID" = issue.id
  AND to_tsvector('english', body) @@ to_tsquery('critical & bug')
)
ORDER BY modified DESC
LIMIT 10;
```

---

## Case 5: Multiple Table Join with LIMIT

### The Query
```sql
SELECT DISTINCT i.*
FROM issue i
INNER JOIN "issueLabel" il ON il."issueID" = i.id
INNER JOIN label l ON l.id = il."labelID"
WHERE l.name = 'engineering'
ORDER BY i.modified DESC
LIMIT 100;
```

### What Postgres Does

**Plan:**
```
Limit  (cost=4171.29..4174.29 rows=100 width=453)
  ->  Unique
        ->  Sort  (cost=4171.29..4175.79 rows=1803 width=453)
              Sort Key: i.modified DESC, i.id, [all columns]
              ->  Nested Loop
                    ->  Nested Loop
                          ->  Seq Scan on label l
                                Filter: name = 'engineering'
                          ->  Index Only Scan on "issueLabel" il
                    ->  Index Scan on issue i
```

**Strategy:**
1. Find 'engineering' label
2. Get all issueLabel rows for that label (~1,803)
3. Join each with issue
4. Sort ALL 1,803 results by modified + all columns (for DISTINCT)
5. Unique to remove duplicates
6. Return first 100

### The DISTINCT Problem

**Why DISTINCT is needed:** An issue might have the label multiple times (shouldn't happen but JOIN produces duplicates if it does)

**Why it's expensive:**
- Must sort by ALL columns (not just modified)
- Can't stop at 100 until after deduplication
- Must process all 1,803 rows

### Better Without DISTINCT

```sql
SELECT i.*
FROM issue i
WHERE EXISTS (
  SELECT 1 FROM "issueLabel" il
  JOIN label l ON l.id = il."labelID"
  WHERE il."issueID" = i.id AND l.name = 'engineering'
)
ORDER BY modified DESC
LIMIT 100;
```

**Plan:**
```
Limit
  ->  Sort  (cost=4420.54..4420.79 rows=100 width=453)
        Sort Key: i.modified DESC  -- Only sort by modified!
        ->  Nested Loop
              ->  HashAggregate  -- Deduplicates issue IDs
              ->  Index Scan on issue
```

**Better because:**
- EXISTS prevents duplicates at source
- Only sorts by modified (cheaper)
- HashAggregate deduplicates issue IDs before fetching issue rows

---

## Case 6: LIMIT Prevents Parallel Execution

### The Query
```sql
SELECT * FROM issue
WHERE open = true
ORDER BY modified DESC
LIMIT 1000;
```

### What Postgres Does

**Plan:**
```
Limit
  ->  Index Scan Backward using issue_open_modified_idx on issue
        Index Cond: (open = true)
        rows=121200
```

**Strategy:**
- Use index scan (sequential, no parallelism)
- Scan backwards
- Stop at 1,000

**Cost:** Low because of early termination

### Without LIMIT

**Plan:**
```
Gather Merge
  Workers Planned: 2
  ->  Sort
        ->  Parallel Index Scan using issue_open_modified_idx
```

**Strategy:**
- Parallel scan (2 workers)
- Each worker sorts its partition
- Merge sorted results

**For large limits (10,000+):** Parallel might be faster
**For small limits (100):** Sequential with early termination is faster

**The problem:** LIMIT prevents Postgres from considering parallel plans even when they'd be faster for larger limits

---

## Summary: When LIMIT + ORDER BY Goes Wrong

### Pattern 1: Low Selectivity Filter
**Problem:** Scans entire table in sort order looking for rare matches
**Example:** User with 0 issues, rare label combinations
**Fix:** Filter first, then sort; or use composite index

### Pattern 2: Correlated Subquery/Join Filter
**Problem:** Can't use MCV statistics, gets generic estimate
**Example:** EXISTS, IN with joins
**Fix:** Use literal values or scalar subqueries with application-side lookup

### Pattern 3: Wrong Index Choice
**Problem:** Uses sort index, filters after; should use filter index, sort after
**Example:** Filter by assigneeID, sort by modified, no composite index
**Fix:** Create composite index `(filter_col, sort_col)`

### Pattern 4: Expensive Filter Evaluation
**Problem:** Evaluates expensive filter (text search, complex subquery) for every row in sort order
**Example:** Full-text search in EXISTS with LIMIT
**Fix:** Filter first with simpler/indexed predicate, then expensive filter

### Pattern 5: JOIN + DISTINCT + LIMIT
**Problem:** Must sort by all columns for DISTINCT, processes all rows before LIMIT
**Example:** Issue labels join with DISTINCT
**Fix:** Use EXISTS to prevent duplicates, or subquery to deduplicate IDs first

### Pattern 6: Prevents Parallelism
**Problem:** Small LIMIT favors sequential index scan over parallel execution
**Example:** Moderate LIMIT (1000+) on large result sets
**Fix:** Consider removing LIMIT for internal aggregations, apply in application

---

## The Fundamental Issue

**Postgres's cost model assumes:**
1. Early termination saves significant work
2. Matches are uniformly distributed in sort order
3. Filter selectivity estimates are accurate

**Reality:**
1. Early termination fails if matches are rare or clustered
2. Data is often skewed (recent activity, popular users, etc.)
3. Estimates fail without per-value statistics (MCV only works for literals)

**Result:** LIMIT + ORDER BY causes Postgres to optimize for the common case (high selectivity, even distribution) which fails badly for the uncommon case (low selectivity, skewed distribution).

---

## Decision Framework

**Use LIMIT + ORDER BY scan-and-filter when:**
- ✅ High selectivity (>10% of rows match)
- ✅ Even distribution across sort column
- ✅ Cheap filter evaluation
- ✅ Small LIMIT relative to result set

**Avoid LIMIT + ORDER BY scan-and-filter when:**
- ❌ Low selectivity (<1% of rows match)
- ❌ Skewed distribution (matches clustered)
- ❌ Expensive filter (text search, complex joins)
- ❌ Large LIMIT relative to selectivity

**Alternative strategies:**
1. **Composite index** `(filter_col, sort_col)` - works if filter is on indexed column
2. **CTE approach** - Filter first, materialize, then sort and limit
3. **Application-side** - Get filter value first, use as literal in second query
4. **Cursor/keyset pagination** - Better for large offsets anyway



---


The Fundamental Issue:

  Postgres's cost model for LIMIT queries assumes:
  1. ✅ Early termination saves significant work
  2. ✅ Matches are uniformly distributed in sort order
  3. ✅ Filter selectivity estimates are accurate

  When these assumptions fail (rare values, skewed distribution, bad estimates), the plans are catastrophically wrong.
