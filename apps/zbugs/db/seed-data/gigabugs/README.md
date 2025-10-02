# Query Plan Analysis - Complete Documentation

This directory contains comprehensive analysis of query performance issues discovered in the zbugs 1GB dataset, with proven solutions and recommendations.

## 📚 Documentation Index

### 1. **[plans.md](./plans.md)** - Foundational Query Plan Analysis
The original deep-dive into query planning issues:
- **LIMIT clause impact** on Postgres query planning
- **MCV (Most Common Values)** statistics and when they work
- **Query formulation matters**: EXISTS vs JOIN vs Scalar subqueries
- **SQLite vs Postgres** comparison
- **Composite index requirements**
- Includes all test queries from the original investigation

**Key Discovery:** Query formulation can be more important than indices. Literal values enable MCV lookups; joins/subqueries don't.

### 2. **[plan-discovery.md](./plan-discovery.md)** - Real-World Bug Tracker Queries
Analysis of 7 common bug tracker queries with production workloads:
- Issue page load with comments
- Issues filtered by label
- Recent activity feeds
- Aggregation queries (comment counts)
- User dashboards (assigned issues)
- Multi-label filtering

**Major Wins:**
- 🚀 **14,600x faster** activity feeds (285,182 → 19.55 cost)
- 🚀 **540x faster** comment counts (510,513 → 944 cost)

### 3. **[limit-order-by-problems.md](./limit-order-by-problems.md)** - Detailed Problem Analysis
Deep dive into all cases where `LIMIT + ORDER BY` causes bad plans:
- 6 specific antipattern cases with detailed explanations
- Root cause analysis for each failure mode
- Why Postgres's cost model fails
- Decision framework for when to use different strategies

**Core Issue:** Postgres assumes early termination saves work, but fails when matches are rare or clustered.

### 4. **[limit-antipatterns.md](./limit-antipatterns.md)** - Quick Reference Guide
Practical reference for developers:
- Antipattern matrix with fixes
- Real examples from zbugs
- Detection heuristics and red flags
- Decision tree for choosing fixes
- Index design patterns
- Testing checklist

**Use this** when reviewing queries or debugging slow LIMIT queries.

---

## 🎯 Executive Summary

### The Problem

The combination of `LIMIT + ORDER BY` causes Postgres to make fundamentally different planning decisions:

**With LIMIT:** Scan in ORDER BY order, filter as you go, stop early
**Without LIMIT:** Filter first, then sort matching rows

This works great when matches are common, **fails catastrophically** when matches are rare.

### Critical Findings

#### 1. **MCV Statistics Only Work for Literals**

```sql
-- ✅ Uses MCV, accurate estimate
WHERE creatorID = 'test-user-123'  -- Estimates 1 row (not in MCV list)

-- ❌ Can't use MCV, generic estimate
WHERE creatorID = (SELECT id ...)  -- Estimates 40,400 rows (1/6 of total)

-- ❌ Can't use MCV, generic estimate
WHERE EXISTS (... AND user.id = issue.creatorID)  -- Estimates 34,629 rows
```

**Impact:** Same query, same data, wildly different plans based on how value is supplied.

#### 2. **LIMIT Changes Everything**

| Query | With LIMIT | Without LIMIT | Strategy Difference |
|-------|-----------|---------------|---------------------|
| Filter by creator | Scan by modified, filter each row | Hash join, then sort | Opposite approach! |
| Activity feed | Seq scan + hash join + sort all | Index scan top N | 14,600x difference |
| Aggregation | Join all, group all, sort all, limit | Limit first, aggregate 100 | 540x difference |

#### 3. **Missing Indices = Massive Cost**

| Index | Query Improved | Before Cost | After Cost | Speedup |
|-------|---------------|-------------|------------|---------|
| `comment(created DESC)` | Recent activity | 285,182 | 19.55 | **14,600x** |
| Subquery pattern | Comment counts | 510,513 | 944 | **540x** |
| `issue(creatorID, modified)` | Issues by creator | 203 | 8.39 | **24x** |

---

## 🔧 Proven Solutions

### Solution 1: Critical Missing Indices

```sql
-- HIGHEST IMPACT: Activity feeds
CREATE INDEX comment_created_desc_idx ON comment(created DESC);

-- Already exists: Creator/assignee filtering
CREATE INDEX issue_creatorid_modified_idx ON issue("creatorID", modified);
```

### Solution 2: Query Pattern Changes

**❌ Bad: JOIN with aggregation**
```sql
SELECT i.*, COUNT(c.id) FROM issue i
LEFT JOIN comment c ON c."issueID" = i.id
GROUP BY i.id
ORDER BY i.modified DESC LIMIT 100;
```

**✅ Good: Subquery for aggregation**
```sql
SELECT i.*, (
  SELECT COUNT(*) FROM comment c WHERE c."issueID" = i.id
) FROM issue i
ORDER BY i.modified DESC LIMIT 100;
```

**❌ Bad: Correlated subquery**
```sql
WHERE EXISTS (SELECT 1 FROM "user" WHERE name = ? AND id = issue."creatorID")
```

**✅ Good: Scalar subquery (better)**
```sql
WHERE "creatorID" = (SELECT id FROM "user" WHERE name = ?)
```

**✅ Best: Literal value (app does lookup)**
```sql
WHERE "creatorID" = 'lHr1oe7FW0'  -- Enables MCV lookup
```

### Solution 3: Index Design Patterns

```sql
-- Pattern 1: Filter + Sort composite
CREATE INDEX table_filter_sort_idx ON table(filter_col, sort_col DESC);

-- Pattern 2: Partial index for common filters
CREATE INDEX table_filtered_idx ON table(col1, col2) WHERE active = true;

-- Pattern 3: Covering index (index-only scan)
CREATE INDEX table_covering_idx ON table(filter, sort) INCLUDE (display_cols);
```

---

## 🚨 Warning Signs

Your query might have LIMIT + ORDER BY problems if:

1. **Cost seems wrong**
   - High cost (>10,000) for seemingly simple query
   - Much slower than cost suggests

2. **Plan includes these patterns**
   - Sequential scan with LIMIT
   - Sort node before Limit (sorting everything!)
   - Nested loop with high outer row count
   - Index scan on sort column with filter (should be opposite)

3. **Behavior inconsistent**
   - Fast for some values, slow for others
   - Removing LIMIT changes plan completely
   - EXPLAIN ANALYZE shows actual rows >> estimated rows

4. **Data characteristics**
   - Low selectivity filter (<1% of rows)
   - Skewed data distribution
   - Recently added values not in statistics

---

## 📊 Dataset Context

This analysis uses the zbugs 1GB "gigabugs" dataset:

```
Issues:     242,400
Comments:   2,473,648 (~10 per issue)
Users:      6 (later 7 with test user)
Labels:     24
Projects:   1

Key test users:
- Naomi:     46,504 issues (19.2%)
- Holden:    38,304 issues (15.8%)
- Clarissa:  38,168 issues (15.7%)
- TestUser:  0 issues (added for testing)
```

To set up:
```bash
cd apps/zbugs/db/seed-data/gigabugs
./getData.sh
cd ../..
npm run db-up
npm run db-migrate
ZERO_SEED_DATA_DIR=./db/seed-data/gigabugs/ npm run db-seed
```

---

## 🎓 Key Learnings

### 1. **Postgres Query Planner Limitations**

- **No per-value statistics** - Only tracks ~100 most common values
- **Can't use MCV for parameters** - Joins/subqueries get generic estimates
- **Assumes uniform distribution** - Fails for skewed data
- **Early termination bias** - LIMIT causes preference for scan-and-filter

### 2. **When LIMIT + ORDER BY Fails**

- Rare values (not in MCV list)
- Correlated subqueries (can't look up at plan time)
- Expensive filters (text search, complex computations)
- Wrong index order (filter column not first)
- Skewed temporal distribution (old vs new data)

### 3. **The Composite Index Paradox**

Even with the "perfect" composite index, query formulation matters:

```sql
-- Won't use index (join prevents it)
SELECT * FROM issue i
JOIN user u ON u.id = i."creatorID"
WHERE u.name = 'Naomi'
ORDER BY i.modified LIMIT 100;

-- Uses index perfectly
SELECT * FROM issue
WHERE "creatorID" = 'lHr1oe7FW0'
ORDER BY modified LIMIT 100;
```

---

## 🔍 How to Use This Documentation

**If you're...**

- **Writing a new query with LIMIT:** Read [limit-antipatterns.md](./limit-antipatterns.md) first
- **Debugging a slow query:** Check [plan-discovery.md](./plan-discovery.md) for similar patterns
- **Understanding the fundamentals:** Start with [plans.md](./plans.md)
- **Deep diving into failures:** Read [limit-order-by-problems.md](./limit-order-by-problems.md)

**Quick Workflow:**

1. Write query
2. Run `EXPLAIN ANALYZE`
3. Check cost and actual rows
4. Look for red flags (seq scan, sort before limit, high row counts)
5. Consult antipattern guide
6. Apply fix (index, rewrite, or accept)
7. Verify with `EXPLAIN ANALYZE` again

---

## 📈 Impact Summary

**Indices Created:**
- ✅ `comment_created_desc_idx` - 14,600x speedup for activity feeds
- ✅ `issue_creatorid_modified_idx` - Optimal creator filtering
- ✅ `user_name_idx` - Fast user lookups

**Query Patterns Changed:**
- ✅ Aggregations use subquery pattern (540x faster)
- ✅ Direct value lookups where possible (MCV optimization)
- ✅ Two-step queries for complex filters

**Performance Gains:**
- Recent activity: **14,600x faster** (285,182 → 19.55)
- Comment counts: **540x faster** (510,513 → 944)
- Creator queries: **24x faster** (203 → 8.39)

**Overall:** Queries that were taking seconds now complete in milliseconds.

---

## 🤝 Contributing

When adding new queries to zbugs:

1. Always run `EXPLAIN ANALYZE` before deploying
2. Test with rare values (not just common ones)
3. Check plans with and without LIMIT
4. Document any new antipatterns discovered
5. Add to this documentation if patterns are reusable

---

## 📝 Files in This Directory

```
gigabugs/
├── README.md (this file)              # Overview and index
├── plans.md                           # Original investigation
├── plan-discovery.md                  # Real-world query analysis
├── limit-order-by-problems.md         # Detailed failure cases
├── limit-antipatterns.md              # Quick reference guide
├── getData.sh                         # Dataset download script
└── *.csv                             # Dataset files
```
