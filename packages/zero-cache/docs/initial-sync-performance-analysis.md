# Initial Sync Performance Analysis

## Current Architecture

The initial sync pipeline (`initial-sync.ts:68`) flows as:

```
PostgreSQL COPY TO STDOUT (text/TSV)
    → Node.js Stream pipeline
    → TsvParser (per-byte scanning)
    → Per-value type parsing: pgParser(text) → liteValue(val, type)
    → Buffer (10K rows or 8MB)
    → flush() → batched INSERT (50 rows/batch) via better-sqlite3
    → Index creation (sequential, after all data loaded)
```

**SQLite pragmas during initial sync** (set in `migration-lite.ts:106-110`):

- `journal_mode = OFF`
- `synchronous = OFF`
- `locking_mode = EXCLUSIVE`
- `foreign_keys = OFF`

The entire initial sync runs inside a single `BEGIN EXCLUSIVE` transaction. Indexes are correctly deferred until after all data is loaded. These settings are already good.

**What's NOT set**: `cache_size`, `mmap_size`, `page_size`, `temp_store`.

## Bottleneck Breakdown

For a multi-hour sync (500M+ rows), the work splits roughly into:

| Phase             | What happens                                  | Serialization             |
| ----------------- | --------------------------------------------- | ------------------------- |
| PG COPY streaming | Network I/O from PostgreSQL                   | Async, per-table parallel |
| TSV parsing       | Byte-by-byte scanning in `TsvParser.parse()`  | Sync, per-chunk           |
| Type conversion   | `pgParser(text)` then `liteValue()` per value | Sync, per-value           |
| SQLite inserts    | 50-row batched INSERTs via `better-sqlite3`   | Sync, blocks event loop   |
| Index creation    | Sequential `CREATE INDEX` per index           | Sync, sequential          |

The code tracks `flushTime` (SQLite insert time) vs `total` -- the gap is PG streaming + parsing. The finding that posting messages to a worker costs ~= the SQLite write itself tells us data transformation/copying is a significant fraction, not just the B-tree operations.

## Recommendations (ordered by impact/effort ratio)

### 1. Set `cache_size` and `mmap_size` before bulk load

**Impact: High | Effort: Trivial (2 lines)**

The default SQLite page cache is **2MB**. For a bulk load of hundreds of millions of rows, this means constant page eviction. The B-tree grows large, and each new insert may need to re-read internal pages that were evicted.

```typescript
// In migration-lite.ts, before running the migration:
db.pragma('cache_size = -524288'); // 512MB (negative = KiB)
db.pragma('mmap_size = 1073741824'); // 1GB memory-mapped I/O
```

This keeps more of the B-tree in memory and lets the OS page cache manage I/O more efficiently. For a multi-GB SQLite file, this alone could be a significant win.

### 2. Set `page_size = 8192` or `16384` before table creation

**Impact: Moderate-High | Effort: Low**

Larger page sizes mean:

- Fewer B-tree levels (shallower tree = fewer page lookups per insert)
- Fewer page splits during sequential inserts
- More efficient I/O (fewer, larger reads/writes)
- Better alignment with modern SSD block sizes

Must be set before any tables are created (which is the case -- it's a fresh DB). In `migration-lite.ts`, before the setup migration runs:

```typescript
db.pragma('page_size = 16384'); // Before any table creation
```

This requires a `VACUUM` to take effect on existing databases, but for initial sync the DB is brand new.

### 3. Increase `INSERT_BATCH_SIZE` to 200-500

**Impact: Moderate | Effort: Low (benchmark + constant change)**

The current batch size of 50 was determined empirically, but the referenced SQLite forum post is from 2021. Each batch incurs:

- A JS-to-native boundary crossing
- Statement execution overhead
- An `Array.slice()` allocation of `batchSize * columnsPerRow` elements

For tables with 10 columns, SQLite supports up to 3276 rows per INSERT (limited by `SQLITE_MAX_VARIABLE_NUMBER = 32766`). Testing with 200-500 could reduce the number of native calls by 4-10x. The tradeoff is larger SQL statements, but SQLite handles these efficiently.

Benchmark at 100, 200, 500, and 1000 to find the new sweet spot for large datasets.

### 4. Eliminate `Array.slice()` allocations in `flush()`

**Impact: Moderate | Effort: Moderate**

Each batch insert does `pendingValues.slice(l, l + valuesPerBatch)`, allocating a new array. For 500M rows / 50 per batch = 10M allocations. Options:

```typescript
// Option A: Use a fixed-size reusable array per batch
const batchBuffer = new Array(valuesPerBatch);
// ... copy values into batchBuffer instead of slicing
```

Alternatively, increasing the batch size (recommendation 3) reduces the number of slices proportionally, making this less critical.

### 5. Specialize per-column type converters for the copy path

**Impact: Moderate | Effort: Moderate**

Currently, each value goes through a generic chain:

```
pgParser(text) → liteValue(val, pgType, jsonFormat)
```

`liteValue()` checks `typeof val`, calls `liteTypeToZqlValueType()` (which does string manipulation on the type string), and branches on the result. For the copy path, column types are known upfront. Create specialized converters:

```typescript
// Instead of generic parsers:
const parsers = columnSpecs.map(c => {
  const pgParse = pgParsers.getTypeParser(c.typeOID);
  return (val: string) => liteValue(pgParse(val), c.dataType, JSON_STRINGIFIED);
});

// Create specialized fast-path converters:
const parsers = columnSpecs.map(c => {
  const pgParse = pgParsers.getTypeParser(c.typeOID);
  const valueType = liteTypeToZqlValueType(c.dataType);

  if (valueType === 'json') {
    return (val: string) => val; // Already stringified
  }
  if (valueType === 'number' || valueType === 'string') {
    return (val: string) => pgParse(val); // Direct passthrough
  }
  if (valueType === 'boolean') {
    return (val: string) => (pgParse(val) ? 1 : 0); // Boolean → int
  }
  // Fallback to generic path for complex types
  return (val: string) => liteValue(pgParse(val), c.dataType, JSON_STRINGIFIED);
});
```

This eliminates per-value type string parsing and branching for the common cases.

### 6. Checkpoint/resume for multi-hour syncs

**Impact: Critical for reliability | Effort: High**

For a 2-hour initial sync, a failure at 95% means restarting from scratch. Since `journal_mode = OFF`, intermediate commits are essentially free. The approach:

1. Commit after each table finishes copying (breaking the single giant transaction into per-table commits)
2. Record completed tables in a `_zero.initialSyncProgress` table
3. On restart, skip already-completed tables and only copy remaining ones
4. The PG snapshot is held by the replication slot, so the consistent view is maintained

This doesn't make the happy path faster, but prevents catastrophic restart scenarios. The per-table commit boundaries also allow SQLite to manage its internal state more efficiently (smaller B-tree modifications per transaction).

One complication: the current architecture runs the entire initial sync as a single `runTransaction()` in `migration-lite.ts`. This would need to be restructured to commit after each table while still maintaining the atomicity guarantee (i.e., either all tables are synced or the replica is dropped).

### 7. Use PostgreSQL COPY binary format

**Impact: High for numeric-heavy schemas | Effort: High**

The current text/TSV format requires:

- PostgreSQL to text-encode every value (expensive for timestamps, UUIDs, numerics)
- TSV parsing byte-by-byte in `TsvParser`
- Text-to-value parsing via `pgParser(text)` per value

Binary format (`COPY ... TO STDOUT WITH (FORMAT binary)`) sends values in PostgreSQL's native binary representation:

- Integers as 4/8-byte big-endian
- Text as length-prefixed UTF-8
- No escaping/unescaping needed

For schemas with many numeric, timestamp, or UUID columns, this could be 2-3x faster for the data transfer + parsing phase. The tradeoff is implementing a binary COPY parser, which is more complex than TSV but well-documented.

### 8. Pipeline PG reads and SQLite writes within a single table

**Impact: Moderate | Effort: Moderate**

Currently, within a single table copy, PG reads and SQLite writes are serialized: the `flush()` call blocks the event loop, preventing new PG chunks from arriving.

An alternative: use `setImmediate` or yield between flushes to allow PG data to arrive into kernel buffers while SQLite writes are queued. Or, split the pipeline so PG data is buffered in a separate async generator while SQLite writes drain from the buffer independently.

This would help when PG network latency is the bottleneck (remote PG instances). For local PG, the current serialization is fine.

### 9. Fix the pendingValues cleanup bug

**Impact: Memory efficiency | Effort: Trivial**

In `initial-sync.ts:535-538`, the cleanup loop only clears `flushedRows` entries but should clear `flushedRows * valuesPerRow`:

```typescript
// Current (clears too few entries):
for (let i = 0; i < flushedRows; i++) {
  pendingValues[i] = undefined as unknown as LiteValueType;
}

// Should be:
const flushedValues = flushedRows * valuesPerRow;
for (let i = 0; i < flushedValues; i++) {
  pendingValues[i] = undefined as unknown as LiteValueType;
}
```

This means parsed values from previous flushes aren't released for GC until overwritten. For large datasets with big JSON/text values, this keeps unnecessary memory pressure between flushes.

## What likely matters most

Since worker message posting cost ~= SQLite write cost, the dominant factor for multi-hour syncs is likely **data volume x per-value processing cost**, not purely I/O. The highest-leverage changes are:

1. **cache_size + mmap_size** (trivial, reduces B-tree re-reading)
2. **Larger batch sizes** (reduces native calls and allocations by 4-10x)
3. **Checkpoint/resume** (reliability win -- prevents restarting 2-hour syncs)
4. **page_size = 16384** (fewer page operations for large B-trees)
5. **Specialized type converters** (eliminates per-value overhead in hot loop)
6. **Binary COPY** (biggest possible win, but highest effort)
