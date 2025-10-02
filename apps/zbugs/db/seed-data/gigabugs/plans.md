# Query Plans Analysis

## Dataset Statistics

**Total Issues:** 242,400

**Issue Distribution by Creator:**
- Naomi (`lHr1oe7FW0`): 46,504 issues (19.2%)
- Holden (`FzHEjzbGL1`): 38,304 issues (15.8%) - also most common assignee with 19,796 assignments
- Clarissa (`XtQju79aJj`): 38,168 issues (15.7%) - fewest creator
- Others: ~40,000 issues each

**Total Users:** 6

**Available Indices on `issue` table:**
- `issue_pkey` - Primary key on `id`
- `issue_created_idx` - Index on `created`
- `issue_modified_idx` - Index on `modified`
- `issue_open_modified_idx` - Composite index on `(open, modified)`
- `issue_project_idx` - Unique composite index on `(id, projectID)`

**Available Indices on `user` table:**
- `user_pkey` - Primary key on `id`
- `user_githubid_idx` - Unique index on `githubID`
- `user_login_idx` - Unique index on `login`
- `user_name_idx` - Index on `name` (added during testing)

---

## Query: Find Issues by Creator Name

### Test Case 1: Naomi (46,504 issues)

#### Query with EXISTS (Before adding index on user.name)

```sql
SELECT * FROM issue
WHERE EXISTS (
  SELECT 1 FROM "user"
  WHERE name = 'Naomi' AND "user".id = issue."creatorID"
)
ORDER BY modified
LIMIT 100;
```

**Query Plan:**
```
Limit  (cost=0.58..5409.43 rows=100 width=454)
  ->  Nested Loop  (cost=0.58..72857.81 rows=1347 width=454)
        ->  Index Scan using issue_modified_idx on issue  (cost=0.42..66800.00 rows=242400 width=454)
        ->  Memoize  (cost=0.16..0.18 rows=1 width=32)
              Cache Key: issue."creatorID"
              Cache Mode: logical
              ->  Index Scan using user_pkey on "user"  (cost=0.15..0.17 rows=1 width=32)
                    Index Cond: ((id)::text = (issue."creatorID")::text)
                    Filter: ((name)::text = 'Naomi'::text)
```

**Execution Strategy:**
1. Scans `issue` table using the `issue_modified_idx` index (ordered by `modified`)
2. For each issue row, performs a nested loop join with `user` table
3. Uses memoization to cache user lookups by `creatorID` (avoids repeated lookups for the same creator)
4. Looks up user by primary key and filters for `name = 'Naomi'`
5. Stops after finding 100 matching rows

---

#### Query with EXISTS (After adding index on user.name)

```sql
SELECT * FROM issue
WHERE EXISTS (
  SELECT 1 FROM "user"
  WHERE name = 'Naomi' AND "user".id = issue."creatorID"
)
ORDER BY modified
LIMIT 100;
```

**Query Plan:**
```
Limit  (cost=0.42..174.77 rows=100 width=454)
  ->  Nested Loop  (cost=0.42..70437.08 rows=40400 width=454)
        Join Filter: ((issue."creatorID")::text = ("user".id)::text)
        ->  Index Scan using issue_modified_idx on issue  (cost=0.42..66800.00 rows=242400 width=454)
        ->  Materialize  (cost=0.00..1.08 rows=1 width=32)
              ->  Seq Scan on "user"  (cost=0.00..1.07 rows=1 width=32)
                    Filter: ((name)::text = 'Naomi'::text)
```

**Changes:**
- The plan structure changed - now doing a Seq Scan on `user` (not using the new index)
- However, it **Materializes** the user result (finds Naomi once and caches it)
- Then performs the nested loop join with all issues
- The estimated cost for the limit is much lower: **174.77** vs **5409.43**
- Estimated matching rows increased from 1,347 to 40,400 (more accurate)

**Note:** The planner chose a sequential scan over the index because the `user` table is very small (only 6 rows), so scanning it sequentially is faster than using an index.

---

#### Alternative Query Formulations

**INNER JOIN:**
```sql
SELECT issue.*
FROM issue
INNER JOIN "user" ON "user".id = issue."creatorID"
WHERE "user".name = 'Naomi'
ORDER BY issue.modified
LIMIT 100;
```

**IN Subquery:**
```sql
SELECT * FROM issue
WHERE issue."creatorID" IN (
  SELECT id FROM "user" WHERE name = 'Naomi'
)
ORDER BY modified
LIMIT 100;
```

**Query Plan (Same for both):**
```
Limit  (cost=0.42..174.77 rows=100 width=454)
  ->  Nested Loop  (cost=0.42..70437.08 rows=40400 width=454)
        Join Filter: (("user".id)::text = (issue."creatorID")::text)
        ->  Index Scan using issue_modified_idx on issue  (cost=0.42..66800.00 rows=242400 width=454)
        ->  Materialize  (cost=0.00..1.08 rows=1 width=32)
              ->  Seq Scan on "user"  (cost=0.00..1.07 rows=1 width=32)
                    Filter: ((name)::text = 'Naomi'::text)
```

**Result:** All three query formulations (EXISTS, INNER JOIN, IN) produce identical query plans.

---

### Test Case 2: Clarissa (38,168 issues - fewest creator)

```sql
SELECT * FROM issue
WHERE EXISTS (
  SELECT 1 FROM "user"
  WHERE name = 'Clarissa' AND "user".id = issue."creatorID"
)
ORDER BY modified
LIMIT 100;
```

**Query Plan:**
```
Limit  (cost=0.42..174.77 rows=100 width=454)
  ->  Nested Loop  (cost=0.42..70437.08 rows=40400 width=454)
        Join Filter: ((issue."creatorID")::text = ("user".id)::text)
        ->  Index Scan using issue_modified_idx on issue  (cost=0.42..66800.00 rows=242400 width=454)
        ->  Materialize  (cost=0.00..1.08 rows=1 width=32)
              ->  Seq Scan on "user"  (cost=0.00..1.07 rows=1 width=32)
                    Filter: ((name)::text = 'Clarissa'::text)
```

**Result:** Identical plan to Naomi query - no optimization based on selectivity.

---

### Test Case 3: TestUser (0 issues - edge case)

To test whether Postgres adapts its plan for users with extremely low issue counts, we created a new user with no issues:

```sql
INSERT INTO "user" (id, login, name, avatar, role, "githubID", email)
VALUES ('test-user-123', 'testuser', 'TestUser', 'https://example.com/avatar.png', 'user', 999999, 'test@example.com');
```

**Verification:**
```sql
SELECT COUNT(*) FROM issue WHERE "creatorID" = 'test-user-123';
-- Result: 0
```

#### Query Plans (All Three Formulations)

**EXISTS:**
```sql
SELECT * FROM issue
WHERE EXISTS (
  SELECT 1 FROM "user"
  WHERE name = 'TestUser' AND "user".id = issue."creatorID"
)
ORDER BY modified
LIMIT 100;
```

**IN Subquery:**
```sql
SELECT * FROM issue
WHERE issue."creatorID" IN (
  SELECT id FROM "user" WHERE name = 'TestUser'
)
ORDER BY modified
LIMIT 100;
```

**INNER JOIN:**
```sql
SELECT issue.*
FROM issue
INNER JOIN "user" ON "user".id = issue."creatorID"
WHERE "user".name = 'TestUser'
ORDER BY issue.modified
LIMIT 100;
```

**Query Plan (Identical for all three, even after ANALYZE):**
```
Limit  (cost=0.42..203.82 rows=100 width=453)
  ->  Nested Loop  (cost=0.42..70436.64 rows=34629 width=453)
        Join Filter: ((issue."creatorID")::text = ("user".id)::text)
        ->  Index Scan using issue_modified_idx on issue  (cost=0.42..66799.55 rows=242400 width=453)
        ->  Materialize  (cost=0.00..1.09 rows=1 width=11)
              ->  Seq Scan on "user"  (cost=0.00..1.09 rows=1 width=11)
                    Filter: ((name)::text = 'TestUser'::text)
```

**Critical Findings:**

1. **Same Plan Structure:** Even with a user who has created **0 issues**, Postgres uses the exact same execution strategy - scanning all issues in `modified` order first.

2. **Inaccurate Row Estimates:** The planner estimates ~34,629 rows will match, despite TestUser having 0 issues. This demonstrates that Postgres doesn't maintain per-value statistics (i.e., "this specific `creatorID` has 0 issues").

3. **No Adaptation Based on Actual Data:** Even after running `ANALYZE` to update statistics, the planner doesn't detect that TestUser has no issues and optimize accordingly.

4. **Why This Happens:** Postgres tracks general distribution statistics across the entire column (e.g., "on average, each creatorID appears in 16% of rows") but doesn't track per-value statistics. The planner assumes TestUser has a similar distribution to other users.

5. **Query Formulation Doesn't Matter:** All three query formulations (EXISTS, IN, INNER JOIN) produce identical plans, proving that rewriting the query syntax doesn't help without addressing the underlying index and statistics issues.

**Worst-Case Scenario:** This query would scan through **all 242,400 issues** looking for 100 matches that don't exist, never finding any results despite the LIMIT clause.

---

## Analysis: Non-Optimal Query Plans

### The Problem

**Current Execution Strategy:**
1. Scan through **all issues** in `modified` order (using `issue_modified_idx`)
2. For each issue, check if its `creatorID` matches the target user
3. Stop after finding 100 matches

This means potentially scanning through many thousands of issues before finding 100 that match.

**Optimal Execution Strategy:**
1. Find the target user's ID (e.g., Naomi's ID = `lHr1oe7FW0`)
2. Find **all issues** where `creatorID = 'lHr1oe7FW0'`
3. Sort those issues by `modified`
4. Take the first 100

### Why Postgres Chooses the Non-Optimal Plan

1. **LIMIT 100 Influences Planning:** The planner assumes it will quickly find 100 matching rows by scanning issues in modified order, given that each user created ~40,000 issues (16-17% of total).

2. **Missing Index:** There's no index on `(creatorID, modified)` that would allow efficient retrieval of a specific user's issues already sorted by modification time.

3. **Statistics:** The planner estimates it will find matches frequently enough that scanning by modified order is efficient.

4. **Small User Table:** With only 6 users, the user lookup is trivial (cost ~1.08), so the optimization focus is on the issue scan.

### Potential Solutions

1. **Add composite index:** Create an index on `issue(creatorID, modified)` to support efficient filtering by creator and ordering by modified time.

2. **Rewrite without ORDER BY:** If the modified ordering isn't critical, filtering first would be more efficient.

3. **Use explicit join order hints:** Force Postgres to filter by user first (though this requires query rewriting or optimizer hints).

### Dataset Characteristics Contributing to This Behavior

- Very even distribution: Each user created 38,000-47,000 issues (15-19% each)
- Small user table: Only 6 users total (7 after adding TestUser)
- Large issue table: 242,400 total issues
- High match probability: ~16% of any random issue will match any given user

### Key Insight: Postgres Doesn't Adapt to Per-Value Selectivity

**Critical Discovery:** Even when we created a user (TestUser) with **0 issues**, Postgres still chose the same plan structure and estimated ~34,629 matching rows. This proves that:

1. **No per-value statistics:** Postgres doesn't track statistics like "creatorID = 'test-user-123' appears in 0 rows"
2. **General distribution assumptions:** The planner uses overall column statistics to estimate that any given creatorID will appear in ~14-16% of rows
3. **Plan consistency:** The same non-optimal plan is chosen regardless of actual selectivity (0 issues vs 46,504 issues)

**Implication:** Simply having a more skewed distribution or more users won't solve this problem. Without a composite index on `(creatorID, modified)` or extended statistics, Postgres will continue to choose the "scan by modified order first" strategy for these queries, even in scenarios where it's highly inefficient.

---

## The LIMIT Clause Problem

### Queries WITHOUT LIMIT (Optimal Plans)

To understand the impact of the `LIMIT` clause, we tested the same queries without limiting results:

**EXISTS (No LIMIT):**
```sql
SELECT * FROM issue
WHERE EXISTS (
  SELECT 1 FROM "user"
  WHERE name = 'Naomi' AND "user".id = issue."creatorID"
)
ORDER BY modified;
```

**IN Subquery (No LIMIT):**
```sql
SELECT * FROM issue
WHERE issue."creatorID" IN (
  SELECT id FROM "user" WHERE name = 'Naomi'
)
ORDER BY modified;
```

**INNER JOIN (No LIMIT):**
```sql
SELECT issue.*
FROM issue
INNER JOIN "user" ON "user".id = issue."creatorID"
WHERE "user".name = 'Naomi'
ORDER BY issue.modified;
```

**Query Plan (Identical for all three - OPTIMAL!):**
```
Gather Merge  (cost=21286.58..24653.58 rows=28858 width=453)
  Workers Planned: 2
  ->  Sort  (cost=20286.55..20322.62 rows=14429 width=453)
        Sort Key: issue.modified
        ->  Hash Join  (cost=1.10..16328.75 rows=14429 width=453)
              Hash Cond: ((issue."creatorID")::text = ("user".id)::text)
              ->  Parallel Seq Scan on issue  (cost=0.00..15902.00 rows=101000 width=453)
              ->  Hash  (cost=1.09..1.09 rows=1 width=11)
                    ->  Seq Scan on "user"  (cost=0.00..1.09 rows=1 width=11)
                          Filter: ((name)::text = 'Naomi'::text)
```

**Execution Strategy (WITHOUT LIMIT):**
1. **Find the target user** (Naomi) via sequential scan on the small `user` table
2. **Build a hash table** with the user's ID
3. **Hash Join** - Parallel scan of all issues, joining only those matching the target creatorID
4. **Sort** the filtered results by `modified`
5. **Gather Merge** - Merge results from 2 parallel workers

**This is exactly the optimal strategy we wanted!** Filter by creator first, then sort.

### TestUser (0 issues) Without LIMIT

```sql
SELECT * FROM issue
WHERE EXISTS (
  SELECT 1 FROM "user"
  WHERE name = 'TestUser' AND "user".id = issue."creatorID"
)
ORDER BY modified;
```

**Query Plan:**
```
Gather Merge  (cost=21286.58..24653.58 rows=28858 width=453)
  Workers Planned: 2
  ->  Sort  (cost=20286.55..20322.62 rows=14429 width=453)
        Sort Key: issue.modified
        ->  Hash Join  (cost=1.10..16328.75 rows=14429 width=453)
              Hash Cond: ((issue."creatorID")::text = ("user".id)::text)
              ->  Parallel Seq Scan on issue  (cost=0.00..15902.00 rows=101000 width=453)
              ->  Hash  (cost=1.09..1.09 rows=1 width=11)
                    ->  Seq Scan on "user"  (cost=0.00..1.09 rows=1 width=11)
                          Filter: ((name)::text = 'TestUser'::text)
```

**Result:** Same optimal plan - hash join filters first, then sorts.

---

## Root Cause Analysis: LIMIT is the Culprit

### The Core Problem

**The `LIMIT` clause fundamentally changes Postgres's query planning strategy:**

| Scenario | Plan Strategy | Execution Order |
|----------|---------------|-----------------|
| **With LIMIT 100** | Nested Loop scanning by modified order | 1. Scan issues by `modified`<br>2. Filter by creator<br>3. Stop at 100 matches |
| **Without LIMIT** | Hash Join with parallel execution | 1. Find target creator<br>2. Filter all issues via hash join<br>3. Sort results |

### Why LIMIT Causes Non-Optimal Plans

1. **Early Termination Assumption:** With `LIMIT 100`, Postgres assumes it will quickly find 100 matching rows by scanning in the requested sort order (`modified`). It expects to stop early without scanning all 242,400 issues.

2. **Cost Calculation:** The planner calculates that:
   - **With LIMIT:** Scan ~600 issues in modified order to find 100 matches (16% hit rate) = Low estimated cost
   - **Without LIMIT:** Must process ALL matching issues (~40,000) = Higher cost, so better to filter first

3. **Incorrect Assumptions:** The planner's assumptions are based on:
   - Average distribution (16% hit rate)
   - Uniform distribution across the modified timeline
   - Early termination benefit outweighs the inefficiency of checking each row

4. **Actual Reality:** In practice:
   - For TestUser (0 issues): Would scan **all 242,400 issues** finding nothing
   - For Naomi (46,504 issues): Might scan thousands of issues before finding 100 matches, depending on when her issues appear in the modified timeline
   - The "early termination benefit" only materializes if matches are evenly distributed in the sort order

### The Paradox

**Without LIMIT:** Postgres correctly identifies that filtering first is more efficient
**With LIMIT:** Postgres incorrectly assumes early termination justifies the inefficient scan-and-filter approach

This is a well-known query planner trade-off where LIMIT clauses can cause suboptimal plans when:
- The selectivity is high (many matching rows need to be filtered)
- No composite index exists on (filter_column, sort_column)
- The planner overestimates the benefit of early termination

---

## SQLite Query Plans Comparison

For comparison, we tested the same queries against the SQLite replica database (`/tmp/zbugs-replica.db`) which contains the same 242,400 issues and 7 users.

### EXISTS Query with LIMIT 100 (Naomi)

```sql
SELECT * FROM issue
WHERE EXISTS (
  SELECT 1 FROM user
  WHERE name = 'Naomi' AND user.id = issue.creatorID
)
ORDER BY modified
LIMIT 100;
```

**SQLite Plan:**
```
SCAN issue USING INDEX issue_modified_idx
CORRELATED SCALAR SUBQUERY 1
  SEARCH user USING INDEX user_pkey (id=?)
```

**Strategy:** Scan issues by modified order, for each row run a correlated subquery to check if creator matches.

---

### IN Subquery with LIMIT 100 (Naomi)

```sql
SELECT * FROM issue
WHERE issue.creatorID IN (
  SELECT id FROM user WHERE name = 'Naomi'
)
ORDER BY modified
LIMIT 100;
```

**SQLite Plan:**
```
SCAN issue USING INDEX issue_modified_idx
LIST SUBQUERY 1
  SEARCH user USING INDEX user_name_idx (name=?)
  CREATE BLOOM FILTER
```

**Strategy:** Use the `user_name_idx` to find Naomi, create a Bloom filter, then scan issues by modified order and filter using the Bloom filter.

---

### INNER JOIN with LIMIT 100 (Naomi)

```sql
SELECT issue.*
FROM issue
INNER JOIN user ON user.id = issue.creatorID
WHERE user.name = 'Naomi'
ORDER BY issue.modified
LIMIT 100;
```

**SQLite Plan:**
```
SCAN issue USING INDEX issue_modified_idx
BLOOM FILTER ON user (id=?)
SEARCH user USING INDEX user_pkey (id=?)
```

**Strategy:** Scan issues by modified order with a Bloom filter, then search user table for each match.

---

### EXISTS Query with LIMIT 100 (TestUser - 0 issues)

```sql
SELECT * FROM issue
WHERE EXISTS (
  SELECT 1 FROM user
  WHERE name = 'TestUser' AND user.id = issue.creatorID
)
ORDER BY modified
LIMIT 100;
```

**SQLite Plan:**
```
SCAN issue USING INDEX issue_modified_idx
CORRELATED SCALAR SUBQUERY 1
  SEARCH user USING INDEX user_pkey (id=?)
```

**Result:** Identical plan to Naomi - no adaptation for users with 0 issues.

---

### Queries WITHOUT LIMIT

**EXISTS (No LIMIT):**
```sql
SELECT * FROM issue
WHERE EXISTS (
  SELECT 1 FROM user
  WHERE name = 'Naomi' AND user.id = issue.creatorID
)
ORDER BY modified;
```

**SQLite Plan:**
```
SCAN issue USING INDEX issue_modified_idx
CORRELATED SCALAR SUBQUERY 1
  SEARCH user USING INDEX user_pkey (id=?)
```

**IN Subquery (No LIMIT):**
```sql
SELECT * FROM issue
WHERE issue.creatorID IN (
  SELECT id FROM user WHERE name = 'Naomi'
)
ORDER BY modified;
```

**SQLite Plan:**
```
SCAN issue USING INDEX issue_modified_idx
LIST SUBQUERY 1
  SEARCH user USING INDEX user_name_idx (name=?)
  CREATE BLOOM FILTER
```

**INNER JOIN (No LIMIT):**
```sql
SELECT issue.*
FROM issue
INNER JOIN user ON user.id = issue.creatorID
WHERE user.name = 'Naomi'
ORDER BY issue.modified;
```

**SQLite Plan:**
```
SCAN issue USING INDEX issue_modified_idx
BLOOM FILTER ON user (id=?)
SEARCH user USING INDEX user_pkey (id=?)
```

**Result:** All queries WITHOUT LIMIT use the **same plans** as with LIMIT - SQLite doesn't change strategy.

---

## SQLite vs Postgres: Key Differences

### Consistency vs Adaptability

| Database | With LIMIT | Without LIMIT | Adapts to LIMIT? |
|----------|------------|---------------|------------------|
| **SQLite** | Scan by modified order | Scan by modified order | ❌ No - Always same plan |
| **Postgres** | Nested Loop by modified order | Hash Join then Sort | ✅ Yes - Completely different plans |

### SQLite Behavior

**Consistent Non-Optimal Strategy:**
- SQLite **always** scans issues in `modified` order first
- Query formulation (EXISTS vs IN vs JOIN) produces slightly different plans but same overall strategy
- LIMIT clause has **no impact** on plan selection
- No adaptation based on whether query has LIMIT or not
- Uses Bloom filters for IN/JOIN queries (optimization not available for EXISTS)

**Advantages:**
- Predictable query performance
- Simpler query planner
- Uses available index on `user.name` for IN subquery

**Disadvantages:**
- Non-optimal for all query variations
- Would scan all 242,400 rows for TestUser (0 issues) regardless of LIMIT
- No parallel execution capability
- Doesn't leverage hash joins for filtering

### Postgres Behavior

**Adaptive but Inconsistent:**
- **With LIMIT:** Chooses scan-by-modified-order strategy (similar to SQLite)
- **Without LIMIT:** Chooses optimal hash-join-then-sort strategy
- LIMIT clause **fundamentally changes** the execution plan
- All three query formulations (EXISTS/IN/JOIN) produce identical plans

**Advantages:**
- Optimal plan without LIMIT (hash join, parallel execution)
- Can adapt strategy based on query characteristics

**Disadvantages:**
- LIMIT causes it to choose non-optimal plan
- Less predictable - same query with/without LIMIT behaves completely differently
- Early termination assumption can backfire badly (e.g., TestUser case)

---

## Summary: The Missing Index Problem

**Neither database can execute these queries optimally** because both lack a composite index on `(creatorID, modified)`.

**Ideal Execution Plan (neither achieves this):**
1. Use index to find all issues where `creatorID = 'lHr1oe7FW0'` (already filtered)
2. These results are already sorted by `modified` via the composite index
3. Take first 100 rows

**What actually happens:**

**SQLite:** Always scans in modified order with filtering - consistent but always non-optimal

**Postgres:**
- With LIMIT: Same as SQLite - scan modified order with filtering (non-optimal)
- Without LIMIT: Better strategy (hash join + sort) but still not optimal since it must sort after filtering

**The Solution:** Add composite index `CREATE INDEX issue_creatorid_modified_idx ON issue(creatorID, modified)` to enable both databases to execute these queries optimally.
