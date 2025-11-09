# Streaming Statistics for Query Planning

## Overview

This document explains the statistical data structures needed to improve query planning in Zero's streaming replication system. SQLite's `ANALYZE` only gathers statistics on indexed columns, but our query planner needs statistics on **all columns** for accurate cost estimation.

## The Problem: SQLite ANALYZE Limitations

Current state:

- `sqlite_stat1` and `sqlite_stat4` only cover indexed columns
- Query planner needs NDV (number of distinct values) for fanout calculation
- Need range selectivity for filter estimation on non-indexed columns

Goal: Track statistics on **all columns** during change streaming from PostgreSQL to SQLite.

---

## Count-Min Sketch vs HyperLogLog

### Count-Min Sketch: Frequency Estimation

**What it answers**: "How many times does VALUE X appear?"

**Example queries**:

- "How many posts have `user_id = 5`?" → Answer: ~147 posts
- "How many orders have `status = 'shipped'`?" → Answer: ~2,341 orders
- "What are the top 10 most frequent values?" → Answer: [value1: 500, value2: 450, ...]

**How it works**:

1. Hash each value to multiple counters using different hash functions
2. Increment all counters when inserting a value
3. Query by hashing the target value and returning MIN of all counters
4. Over-counting is possible, but under-counting is impossible

**Space complexity**: O(ε × log(1/δ))

- ε = error bound (e.g., 0.01 for 1%)
- δ = failure probability (e.g., 0.001 for 99.9% confidence)
- Typical: 10-100 KB for 1-5% error

**Critical limitation**: You must specify which value to query. Cannot tell you "how many distinct values exist total" without tracking all values separately (defeats the space-efficiency purpose).

**Use cases for query planning**:

- Hot key detection: "Is `user_id=5` a heavy hitter?"
- Skewed distribution identification
- Equality predicate selectivity: `WHERE user_id = 5`

### HyperLogLog: Cardinality Estimation

**What it answers**: "How many DISTINCT values exist?"

**Example queries**:

- "How many distinct user_ids are there?" → Answer: ~1,234 distinct users
- "How many distinct product categories?" → Answer: ~47 distinct categories
- "What's the NDV (cardinality) of this column?" → Answer: ~10,000

**How it works**:

1. Hash each value to uniformly distributed bit pattern
2. Observe maximum number of leading zeros in binary representation
3. Use harmonic mean across multiple buckets for accuracy
4. Statistical properties of hash distributions estimate cardinality

**Space complexity**: Constant based on precision parameter `p`

- Standard: 1.5 KB for 2% error
- High precision: 6 KB for 1% error
- Can count billions of distinct items

**Typical error**: ~2% with 1.5 KB memory

**Use cases for query planning**:

- Fanout calculation: `fanout = totalRows / distinctValues`
- Semi-join selectivity estimation
- NDV for all columns (not just indexed)

### Why Query Planner Needs HyperLogLog

The planner calculates **fanout** for join cost estimation:

```typescript
// Fanout = average children per parent key
fanout = totalRows / distinctValues;

// Example:
// 10,000 posts, 100 distinct user_ids
// fanout = 10,000 / 100 = 100 posts per user
```

**With Count-Min Sketch** (doesn't work):

```typescript
// You can ask: "How many posts for user_id=5?"
cms.query('user_id', 5); // → ~100 posts

// But you CANNOT ask: "How many distinct user_ids exist?"
// You'd need to either:
// 1. Track every value you've seen (defeats approximation purpose)
// 2. Query every possible value (impossible for large domains)
```

**With HyperLogLog** (correct approach):

```typescript
// You can ask: "How many distinct user_ids exist?"
hll.cardinality(); // → ~100 distinct users

// Then compute fanout:
fanout = rowCount / hll.cardinality(); // = 10,000 / 100 = 100
```

### Concrete Example: Posts Table

Data:

```sql
-- 1000 posts total
-- user_id distribution:
--   user_id=1: 500 posts (hot key!)
--   user_id=2-10: 50 posts each (500 total)
```

**What HyperLogLog tells you**:

```typescript
hll.cardinality() // → ~10 distinct user_ids
fanout = 1000 / 10 = 100 posts/user (average)
```

**What Count-Min Sketch tells you**:

```typescript
cms.query('user_id', 1); // → ~500 posts (hot key!)
cms.query('user_id', 2); // → ~50 posts
cms.topK(5); // → [(1, 500), (2, 50), (3, 50), ...]

// But CMS CANNOT tell you there are 10 distinct users
// unless you track all values separately
```

**Verdict**: Both are useful, but for different purposes!

---

## Range Query Selectivity: Inequality Predicates

For queries like `SELECT * FROM foo WHERE date > 1970`, we need data structures that understand **value distributions**, not just counts or distinct values.

### Option 1: T-Digest (Recommended) ✅

**What it does**: Maintains approximate distribution of values, excellent for quantiles/percentiles

**How it helps with ranges**:

```typescript
// Given: SELECT * FROM foo WHERE date > 1970

// T-Digest can tell you:
percentile = tdigest.percentile(1970); // → "1970 is at the 25th percentile"

// Therefore: ~75% of rows have date > 1970
selectivity = 1 - percentile; // = 0.75
estimatedRows = totalRows * selectivity;
```

**Accuracy**:

- Very accurate for extreme quantiles (<1% error)
- Adaptive compression focuses accuracy at distribution tails
- Better accuracy near min/max values (where range queries often are)

**Memory**: 1-10 KB per column (depends on compression parameter)

**Perfect for**: Timestamp columns, numeric ranges, date filters

**Supported operations**:

- `WHERE date > X`: `selectivity = 1 - tdigest.percentile(X)`
- `WHERE date < X`: `selectivity = tdigest.percentile(X)`
- `WHERE date BETWEEN X AND Y`: `selectivity = tdigest.percentile(Y) - tdigest.percentile(X)`

### Option 2: Histograms (Equi-depth or Equi-width)

**What it does**: Divides value range into buckets with counts

**Equi-depth histogram** (like PostgreSQL `pg_stats.histogram_bounds`):

```typescript
// Example: 100 buckets, each covering ~1% of rows
buckets = [
  {min: 0, max: 1950, count: 1000}, // 1% of rows
  {min: 1950, max: 1975, count: 1000}, // 1% of rows
  {min: 1975, max: 2000, count: 1000}, // 1% of rows
  // ... 97 more buckets
];

// For: date > 1970
// 1. Find bucket containing 1970
// 2. Interpolate within that bucket
// 3. Count all buckets after it
```

**Accuracy**: Depends on bucket count

- 100 buckets ≈ 1% error
- 1000 buckets ≈ 0.1% error

**Memory**: `bucket_count × bucket_size`

- 100 buckets × ~20 bytes = 2 KB
- 1000 buckets × ~20 bytes = 20 KB

**Trade-off**: More buckets = better accuracy but more memory

**When to use**: T-Digest is generally better for streaming updates, but histograms can be simpler to implement if you have bounded domains.

### Option 3: Min/Max + Uniform Distribution Assumption

**Simplest approach** (often used as fallback):

```typescript
// Track exact min/max per column
min = 1900
max = 2024

// Assume uniform distribution
// For: date > 1970
selectivity = (max - 1970) / (max - min)
            = (2024 - 1970) / (2024 - 1900)
            = 54 / 124
            = 0.435 (43.5% of rows)
```

**Accuracy**: Only good if distribution is actually uniform (rarely true in practice)

**Memory**: 16 bytes per column (just min/max values)

**Problem**: Terrible for skewed distributions

- Example: 90% of posts created after 2020, but min=1970, max=2024
- Uniform assumption would estimate only 16% of posts after 2020
- Actual: 90% (off by 5.6x!)

**When to use**: As a fallback when T-Digest/histogram not available, or for truly uniform data

### Option 4: Reservoir Sampling

**What it does**: Maintains random sample of actual values

```typescript
// Keep 1000 random samples of the column
samples = [1945, 1982, 2001, 1967, ...] // 1000 values

// For: date > 1970
// Count samples matching predicate
matchingSamples = samples.filter(v => v > 1970).length
selectivity = matchingSamples / samples.length

// If 750 out of 1000 samples match:
selectivity = 750/1000 = 0.75 (75% of rows)
```

**Accuracy**: Depends on sample size

- 1000 samples ≈ 3% error (95% confidence interval)
- 10000 samples ≈ 1% error

**Memory**: `sample_size × value_size`

- 1000 samples × 8 bytes (int64) = 8 KB
- 1000 samples × avg string size (e.g., 50 bytes) = 50 KB

**Advantages**:

- Works for ANY predicate type (equals, ranges, even complex expressions)
- Simple to implement
- Can answer arbitrary queries without pre-computing specific statistics

**Disadvantages**:

- More memory than T-Digest for same accuracy
- Slower query time (O(sample_size) filter operation vs O(1) lookup)

---

## Recommended Data Structure Combination

For a complete query planning system supporting both equality and range queries:

| Query Type                       | Data Structure   | Use Case                     | Memory/Column | Accuracy |
| -------------------------------- | ---------------- | ---------------------------- | ------------- | -------- |
| **Cardinality** (NDV for fanout) | HyperLogLog      | Distinct count               | 1.5 KB        | 2%       |
| **Equality** (`title = 'mang'`)  | Count-Min Sketch | Frequency estimation         | 10-100 KB     | 1-5%     |
| **Ranges** (`date > 1970`)       | T-Digest         | Percentile-based selectivity | 1-10 KB       | <1%      |
| **Min/Max** (cheap fallback)     | Exact values     | Range bounds                 | 16 bytes      | 0%       |
| **NULL handling**                | Exact counter    | Adjust estimates             | 8 bytes       | 0%       |

### Total Memory Per Column

**Minimal configuration** (solve immediate needs):

- HyperLogLog (1.5 KB) + Min/Max (16 bytes) + NULL count (8 bytes) ≈ **2 KB**

**With range query support**:

- Add T-Digest (5 KB) ≈ **7 KB per column**

**With frequency/skew detection**:

- Add Count-Min Sketch (50 KB) ≈ **57 KB per column**

**Trade-off considerations**:

- 100 columns × 2 KB = 200 KB (minimal)
- 100 columns × 7 KB = 700 KB (with ranges)
- 100 columns × 57 KB = 5.7 MB (full suite)

---

## Query Selectivity Estimation: Complete Example

```sql
SELECT * FROM posts
WHERE user_id = 5           -- equality
  AND created_at > 1970     -- range
  AND status = 'published'  -- equality
```

**Selectivity estimation using multiple structures**:

```typescript
// 1. Equality filter: user_id = 5
// Using Count-Min Sketch:
userId5Count = cms.query('user_id', 5) // → 500 posts
userId5Selectivity = userId5Count / totalRows // = 500 / 10000 = 0.05
// "5% of posts are by user_id=5"

// 2. Equality filter: status = 'published'
// Using Count-Min Sketch:
publishedCount = cms.query('status', 'published') // → 6000 posts
publishedSelectivity = publishedCount / totalRows // = 6000 / 10000 = 0.60
// "60% of posts are published"

// 3. Range filter: created_at > 1970
// Using T-Digest:
percentile1970 = tdigest.percentile('created_at', 1970) // → 0.10
dateSelectivity = 1 - percentile1970 // = 0.90
// "90% of posts are after 1970"

// 4. Combine (assuming independence):
totalSelectivity = userId5Selectivity × publishedSelectivity × dateSelectivity
                 = 0.05 × 0.60 × 0.90
                 = 0.027 (2.7% of rows match)

estimatedRows = totalRows × totalSelectivity
              = 10000 × 0.027
              = 270 rows
```

**Independence assumption caveat**: Assumes predicates are uncorrelated. In reality:

- `user_id=5` might post more frequently (correlation with `created_at`)
- Power users might have different publish rates (correlation with `status`)
- True selectivity might be different (correlation statistics would help)

**Handling correlations** (advanced topic):

- Track multi-column HyperLogLog for correlated pairs
- Use multi-dimensional histograms
- Reservoir samples can answer correlated queries directly
- PostgreSQL uses `pg_stats.correlation` for simple linear correlation

---

## Phased Implementation Approach

### Phase 1: Solve Immediate Needs

**Goal**: Track NDV on all columns for fanout calculation

**Implement**:

- HyperLogLog per column (~1.5 KB each)
- Min/Max per column (16 bytes each)
- NULL counters per column (8 bytes each)
- Row count per table (8 bytes each)

**Integration**:

- Hook into `ChangeStreamerService` at `packages/zero-cache/src/services/change-streamer/change-streamer-service.ts`
- Process INSERT/UPDATE/DELETE from change stream
- Update sketches in-memory during transaction processing
- Periodically flush to `column_stats` SQLite table

**Query planner integration**:

- Extend `SQLiteStatFanout` at `packages/zqlite/src/sqlite-stat-fanout.ts`
- Fallback: stat4 → stat1 → streaming HLL → default (3)

**Memory overhead**: ~2 KB per column

### Phase 2: Improve Range Query Estimation

**Goal**: Better selectivity for inequality predicates

**Implement**:

- T-Digest for numeric/timestamp columns
- Histogram alternative for bounded domains (optional)

**Use cases**:

- `WHERE created_at > X`
- `WHERE age BETWEEN 18 AND 65`
- `WHERE price < 100`

**Memory overhead**: +5 KB per tracked column

**Integration**:

- Add to `ColumnStatistics` class
- Update on INSERT/UPDATE with new numeric values
- Query planner calls `tdigest.percentile()` for range selectivity

### Phase 3: Detect Skew and Hot Values

**Goal**: Identify heavy hitters and skewed distributions

**Implement**:

- Count-Min Sketch for high-cardinality columns
- Top-K tracking for frequent values

**Use cases**:

- Hot key detection: "Is 90% of traffic from user_id=5?"
- Equality predicate selectivity: `WHERE user_id = X`
- Partition strategy: Broadcast hot keys vs shuffle

**Memory overhead**: +50 KB per tracked column

**Integration**:

- Selective tracking (don't need CMS for every column)
- Configuration: which columns to track frequencies
- Query planner uses for equality predicates

### Phase 4: Advanced Optimizations (Future)

**Optional enhancements**:

- Multi-column HyperLogLog for correlated column pairs
- Reservoir sampling for arbitrary predicate evaluation
- Correlation tracking between columns
- Time-decay for temporal data (weight recent data higher)

---

## Implementation Architecture

### Component Overview

```
ChangeStreamer
    ↓
StatisticsAccumulator (new component)
    ├→ Process: INSERT, UPDATE, DELETE
    ├→ Update: HyperLogLog per column (NDV)
    ├→ Update: T-Digest per column (ranges)
    ├→ Update: Count-Min Sketch per column (frequencies)
    ├→ Update: Min/Max per column
    ├→ Update: NULL counters per column
    ├→ Track: Row counts per table
    └→ Periodically: Flush to StatisticsDB
```

### Files to Create

**New files**:

- `packages/zero-cache/src/services/change-streamer/column-statistics.ts`
  - HyperLogLog implementation
  - T-Digest implementation
  - Count-Min Sketch implementation
  - ColumnStatistics container class
  - Serialization/deserialization for persistence

- `packages/zero-cache/src/services/change-streamer/statistics-accumulator.ts`
  - StatisticsAccumulator service
  - Process change messages
  - Extract column values from row data
  - Update sketches
  - Flush to storage

- `packages/zero-cache/src/services/change-streamer/statistics-accumulator.test.ts`
  - Unit tests for accuracy
  - Integration tests with change stream
  - Benchmark memory overhead

**Modified files**:

- `packages/zero-cache/src/services/change-streamer/change-streamer-service.ts`
  - Hook in StatisticsAccumulator
  - Process changes alongside Forwarder/Storer

- `packages/zqlite/src/sqlite-stat-fanout.ts` (or new `streaming-stat-fanout.ts`)
  - Query streaming statistics
  - Fallback chain: stat4 → stat1 → streaming → default
  - Combine baseline ANALYZE stats with streaming deltas

### Database Schema

```sql
CREATE TABLE column_stats (
  table_name TEXT NOT NULL,
  column_name TEXT NOT NULL,

  -- HyperLogLog for NDV
  hll_sketch BLOB,

  -- Min/Max values
  min_value BLOB,
  max_value BLOB,

  -- NULL handling
  null_count INTEGER,

  -- T-Digest for ranges (optional)
  tdigest_sketch BLOB,

  -- Count-Min Sketch for frequencies (optional)
  cms_sketch BLOB,

  -- Metadata
  row_count INTEGER,
  updated_at INTEGER,

  PRIMARY KEY (table_name, column_name)
);
```

### Configuration

```typescript
interface StatisticsConfig {
  // Enable/disable globally
  enabled: boolean;

  // Which columns to track (default: all)
  includeTables?: string[];
  excludeTables?: string[];

  // Which structures to use per column
  trackNDV: boolean; // HyperLogLog (default: true)
  trackMinMax: boolean; // Min/Max (default: true)
  trackRanges: boolean; // T-Digest (default: false)
  trackFrequencies: boolean; // Count-Min Sketch (default: false)

  // Memory limits
  maxColumns?: number; // Limit total columns tracked

  // Flush interval
  flushIntervalMs: number; // How often to persist (default: 60000)
}
```

---

## References and Further Reading

- [HyperLogLog in Practice](https://research.google/pubs/pub40671/) - Google's original paper
- [T-Digest: Accurate Online Accumulation of Rank Statistics](https://arxiv.org/abs/1902.04023)
- [Count-Min Sketch: An Improved Data Stream Summary](https://dl.acm.org/doi/10.1007/978-3-540-30570-5_27)
- [PostgreSQL Planner Statistics](https://www.postgresql.org/docs/current/planner-stats.html) - pg_stats documentation
- [SQLite ANALYZE Documentation](https://www.sqlite.org/lang_analyze.html) - stat1/stat4 format
