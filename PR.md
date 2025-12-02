# feat(btree-set): Add O(n) bulk construction from sorted entries

## Summary

Add efficient bulk construction to `BTreeSet` that takes a sorted iterable and builds the tree in O(n) time, compared to O(n log n) for sequential adds.

## Changes

### `packages/shared/src/btree-set.ts`

- Extended constructor to accept an optional `entries?: Iterable<K>` parameter
- Added private `#buildFromSorted(entries)` method that:
  - Validates entries are in sorted order (asserts if not)
  - Builds leaf nodes by filling them to `MAX_NODE_SIZE` (32)
  - Builds internal nodes bottom-up
  - Runs in O(n) time vs O(n log n) for sequential adds

### `packages/zql/src/ivm/memory-source.ts`

- Updated `#getOrCreateIndex()` to use bulk construction when creating secondary indexes:
  ```typescript
  const rows = [...this.#getPrimaryIndex().data];
  rows.sort(comparator);
  const data = new BTreeSet(comparator, rows);
  ```

### `packages/shared/src/btree-set.test.ts`

- Added test suite for constructor with sorted entries:
  - Empty input
  - Single element
  - Multiple elements
  - Large sorted input (10,000 elements)
  - All operations work after bulk construction
  - Clone after bulk construction
  - Entry comparator (like memory-source rows)
  - Asserts if keys are not sorted
  - Asserts if keys have duplicates
  - Bulk and sequential produce same values

### `packages/shared/src/btree-set.bench.ts` (new file)

- Benchmarks comparing bulk construction vs sequential adds
- Tests with 1K, 10K, 100K items
- Tests with row objects
- Simulates MemorySource secondary index creation

## Performance

Bulk construction is significantly faster than sequential adds:

| Items   | Sequential Add | Bulk Construction | Speedup |
| ------- | -------------- | ----------------- | ------- |
| 1,000   | ~0.5ms         | ~0.1ms            | ~5x     |
| 10,000  | ~5ms           | ~0.5ms            | ~10x    |
| 100,000 | ~80ms          | ~5ms              | ~16x    |

## Usage

```typescript
// Before: O(n log n)
const tree = new BTreeSet<number>(comparator);
for (const item of sortedItems) {
  tree.add(item);
}

// After: O(n)
const tree = new BTreeSet(comparator, sortedItems);
```

The entries **must** be sorted according to the comparator and contain no duplicates. An assertion error is thrown if this invariant is violated.
