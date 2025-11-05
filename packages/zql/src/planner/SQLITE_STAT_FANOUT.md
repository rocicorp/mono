# SQLiteStatFanout Class

A utility for computing accurate join fanout factors from SQLite statistics tables.

## Overview

The `SQLiteStatFanout` class extracts fanout information from `sqlite_stat4` and `sqlite_stat1` to estimate the average number of child rows per parent key in a join operation.

## Key Features

- **Accurate NULL handling**: Uses `sqlite_stat4` histogram to separate NULL and non-NULL samples
- **Automatic fallback**: Falls back to `sqlite_stat1` → default value when better stats unavailable
- **Caching**: Caches results per (table, column) to avoid redundant queries
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

// Get fanout for posts.userId → users.id join
const result = calculator.getFanout('posts', 'userId');

console.log(`Fanout: ${result.fanout} (source: ${result.source})`);
// Output: "Fanout: 4 (source: stat4)"

// Result includes:
// - fanout: number (average rows per distinct key)
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
const result = calculator.getFanout('task', 'project_id');
// { fanout: 4, source: 'stat4', nullCount: 80 }
```

### Example 2: Dense One-to-Many

```typescript
// 30 employees evenly distributed across 3 departments
const result = calculator.getFanout('employee', 'dept_id');
// { fanout: 10, source: 'stat4', nullCount: 0 }
```

### Example 3: No Statistics

```typescript
// No index or ANALYZE not run
const result = calculator.getFanout('table', 'column');
// { fanout: 3, source: 'default' }
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
- Composite indexes
- Edge cases (empty tables, all NULLs, etc.)

Run tests:
```bash
npm -w packages/zql test -- sqlite-stat-fanout.test.ts
```
