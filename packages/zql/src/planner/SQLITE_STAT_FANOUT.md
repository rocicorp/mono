# SQLiteStatFanout Class

A utility for computing accurate join fanout factors from SQLite statistics tables.

## Overview

The `SQLiteStatFanout` class extracts fanout information from `sqlite_stat4` and `sqlite_stat1` to estimate the average number of child rows per parent key in a join operation.

## Key Features

- **Accurate NULL handling**: Uses `sqlite_stat4` histogram to separate NULL and non-NULL samples
- **Compound index support**: Handles multi-column joins with strict prefix matching
- **Automatic fallback**: Falls back to `sqlite_stat1` → default value when better stats unavailable
- **Caching**: Caches results per (table, columns) to avoid redundant queries
- **Median calculation**: Uses median instead of average for skewed distributions

## Problem Statement

`sqlite_stat1` includes NULL rows in its fanout calculation, which can significantly overestimate fanout for sparse foreign keys:

```
Example: 100 tasks (20 with project_id, 80 NULL)
- stat1: "100 17" → fanout = 17 ❌ (includes NULLs)
- stat4: NULL samples (80), non-NULL samples (4) → fanout = 4 ✅
```

## Usage

```typescript
import {SQLiteStatFanout} from './planner/sqlite-stat-fanout.ts';

const calculator = new SQLiteStatFanout(db);

// Single column join: posts.userId → users.id
const result1 = calculator.getFanout('posts', ['userId']);

console.log(`Fanout: ${result1.fanout} (source: ${result1.source})`);
// Output: "Fanout: 4 (source: stat4)"

// Multi-column join: orders.(customerId, storeId)
const result2 = calculator.getFanout('orders', ['customerId', 'storeId']);

console.log(`Fanout: ${result2.fanout} (source: ${result2.source})`);
// Output: "Fanout: 2 (source: stat4)"

// Result includes:
// - fanout: number (average rows per distinct key/combination)
// - source: 'stat4' | 'stat1' | 'default'
// - nullCount?: number (only for stat4)
```

## Requirements

1. **SQLite with ENABLE_STAT4**: Most builds include this
2. **ANALYZE run**: Database must have statistics
   ```sql
   ANALYZE;
   ```
3. **Index on join column**: The join column must have an index
   ```sql
   CREATE INDEX idx_user_id ON posts(user_id);
   ```

## Strategy

The class uses a three-tier fallback strategy:

1. **sqlite_stat4** (best): Histogram with separate NULL/non-NULL samples
   - Queries stat4 for index samples
   - Decodes binary sample values to identify NULLs
   - Returns median fanout of non-NULL samples

2. **sqlite_stat1** (fallback): Average fanout across all rows
   - May overestimate for sparse foreign keys (includes NULLs)
   - Still better than guessing

3. **Default value** (last resort): Configurable constant (default: 3)
   - Used when no statistics available
   - Conservative middle ground between 1 (FK) and 10 (SQLite default)

## Examples

### Example 1: Sparse Foreign Key

```typescript
// 100 tasks: 20 with project_id (4 per project), 80 NULL
const result = calculator.getFanout('task', ['project_id']);
// { fanout: 4, source: 'stat4', nullCount: 80 }
```

### Example 2: Dense One-to-Many

```typescript
// 30 employees evenly distributed across 3 departments
const result = calculator.getFanout('employee', ['dept_id']);
// { fanout: 10, source: 'stat4', nullCount: 0 }
```

### Example 3: No Statistics

```typescript
// No index or ANALYZE not run
const result = calculator.getFanout('table', ['column']);
// { fanout: 3, source: 'default' }
```

## Compound Index Support

The class supports multi-column joins using **flexible prefix matching**. Columns can appear in any order as long as all are present in the first N positions of an index.

### How It Works

When you provide an array of columns, the class:

1. **Finds matching index** where ALL columns exist in the first N positions (order-independent)
2. **Uses depth-based statistics** from stat1/stat4 at depth N (where N = number of columns)

### Index Matching Rules

The matching is **order-independent** when all columns are present in the first N positions of an index.

```typescript
// Given index: CREATE INDEX idx ON orders(customerId, storeId, date)

// ✅ Matches at depth 1
calculator.getFanout('orders', ['customerId']);

// ✅ Matches at depth 2 (both columns in first 2 positions)
calculator.getFanout('orders', ['customerId', 'storeId']);

// ✅ ALSO matches at depth 2 (order doesn't matter!)
calculator.getFanout('orders', ['storeId', 'customerId']);

// ✅ Matches at depth 3
calculator.getFanout('orders', ['customerId', 'storeId', 'date']);

// ❌ Does NOT match - storeId not in first 1 position
calculator.getFanout('orders', ['storeId']);
// Falls back to 'default' source

// ❌ Does NOT match - date not in first 2 positions (gap)
calculator.getFanout('orders', ['customerId', 'date']);
// Falls back to 'default' source
```

### Flexible Matching Examples

```typescript
// Given two indexes:
// - CREATE INDEX idx1 ON orders(customerId, storeId)
// - CREATE INDEX idx2 ON orders(storeId, customerId)

// All of these match BOTH indexes at depth 2:
calculator.getFanout('orders', ['customerId', 'storeId']);
calculator.getFanout('orders', ['storeId', 'customerId']);

// The class picks the first matching index it finds
// Both give the same fanout (combination of customerId and storeId)
```

### No Gaps Allowed

Gaps are **NOT allowed** because SQLite statistics are cumulative from position 0:

```typescript
// Index: (a, b, c)
// Depth 1 = stats for 'a'
// Depth 2 = stats for 'a AND b' (not 'a AND c')
// Depth 3 = stats for 'a AND b AND c'

// ✅ ['a', 'b'] matches at depth 2
// ❌ ['a', 'c'] does NOT match (c not in first 2 positions)
```

### Stat Format Details

**sqlite_stat1** format: `"totalRows avgCol1 avgCol1+2 avgCol1+2+3..."`

- Uses `parts[depth]` to get fanout at specific depth
- Example: `"1000 100 10 5"` means depth 2 has fanout of 10

**sqlite_stat4** neq format: `"N1 N2 N3..."` (space-separated)

- Uses `neqParts[depth-1]` to get fanout (depth is 1-based, array is 0-based)
- Example: `"100 10 5"` means depth 2 has fanout of 10

### Example: Multi-Column Fanout

```typescript
// Schema: orders table with index (customerId, storeId)
// Data: 100 orders, 10 customers, 5 stores, 2 orders per (customer, store) pair

// Single column (depth 1)
const singleCol = calculator.getFanout('orders', ['customerId']);
// Result: { fanout: 10, source: 'stat4' }
// Interpretation: 100 orders / 10 customers = 10 orders per customer

// Two columns (depth 2)
const twoCol = calculator.getFanout('orders', ['customerId', 'storeId']);
// Result: { fanout: 2, source: 'stat4' }
// Interpretation: 100 orders / 50 (customer, store) pairs = 2 orders per pair
```

## Configuration

```typescript
// Custom default fanout
const calculator = new SQLiteStatFanout(db, 10); // Default: 3

// Clear cache after ANALYZE
db.exec('ANALYZE');
calculator.clearCache();
```

## Related Documentation

- [SELECTIVITY_PLAN.md](./SELECTIVITY_PLAN.md) - Design document for semi-join selectivity
- [sqlite_stat4 format](https://sqlite.org/fileformat2.html#stat4tab)
- [Query planner README](./README.md)

## Testing

The class includes comprehensive tests covering:

- Sparse foreign keys with NULLs
- Evenly distributed fanout
- Skewed distributions
- Single-column indexes
- Compound indexes (2-column and 3-column)
- Prefix matching and column order validation
- Edge cases (empty tables, all NULLs, etc.)

Run tests:

```bash
npm -w packages/zql test -- sqlite-stat-fanout.test.ts
```
