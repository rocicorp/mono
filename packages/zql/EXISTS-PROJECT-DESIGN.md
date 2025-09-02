# EXISTS Query Optimization - ExtractMatchingKeys & SortToRootOrder Design

## Problem Statement

When optimizing EXISTS queries by reordering joins, we get better performance but wrong output structure and ordering.

### Example Scenario

```sql
-- Original query
SELECT * FROM foo WHERE EXISTS(bar WHERE EXISTS(baz))
-- User expects: foo at root, ordered by foo's columns

-- After optimization (if baz is tiny/empty)
-- Execution: baz → bar → foo
-- Problem: Results are in baz order with baz at root!
```

## Solution: Extract and Reorder Pipeline

Use existing Join and Exists operators with reordering, then extract and fix the structure.

## Operator Design

### ExtractMatchingKeys Operator

Extracts the full row data of a target table from a potentially deeply nested tree structure. This is used after join reordering for EXISTS queries to get the root table rows that have matches.

```typescript
/**
 * ExtractMatchingKeys extracts full row data of a target table from a
 * potentially deeply nested tree structure created by reordered joins.
 *
 * Example:
 * Input tree (after baz → bar → foo reordering):
 * {
 *   row: { baz_id: 1, baz_data: 'x' },
 *   relationships: {
 *     bar: () => [{
 *       row: { bar_id: 10, bar_data: 'y' },
 *       relationships: {
 *         foo: () => [{
 *           row: { foo_id: 100, foo_name: 'test', foo_value: 42 },
 *           relationships: {}
 *         }]
 *       }
 *     }]
 *   }
 * }
 *
 * Output (with targetTable='foo', targetPath=['bar', 'foo']):
 * { row: { foo_id: 100, foo_name: 'test', foo_value: 42 }, relationships: {} }
 */
```

#### Key Design Points

- **Extracts full row data**, not just primary keys
- **Handles deduplication** - same row might appear multiple times in cartesian product
- **Traverses relationship path** to find target table deep in the tree
- **Maintains schema** of the target table

#### Interface

```typescript
export interface ExtractMatchingKeysArgs {
  input: Input;
  targetTable: string; // The table whose rows we want
  targetPath: string[]; // Path to traverse to find target table
  targetSchema: SourceSchema; // Schema of the target table
}
```

#### Implementation Details

1. **Fetch**: Traverse the nested tree structure following `targetPath`
2. **Deduplication**: Use primary key to detect and skip duplicate rows
3. **Output**: Emit full row data with empty relationships
4. **Push**: Traverse changes to find affected target rows

### SortToRootOrder Operator

Re-sorts extracted rows to match the original root table's sort order. This is necessary because join reordering changes the natural order of results.

```typescript
/**
 * SortToRootOrder re-sorts extracted rows to match the root table's
 * sort order. After join reordering, results are ordered by the wrong table.
 *
 * Example:
 * Input (after extraction, in baz order):
 * { foo_id: 5, foo_name: 'E' }
 * { foo_id: 2, foo_name: 'B' }
 * { foo_id: 8, foo_name: 'H' }
 *
 * Output (sorted by foo_id asc):
 * { foo_id: 2, foo_name: 'B' }
 * { foo_id: 5, foo_name: 'E' }
 * { foo_id: 8, foo_name: 'H' }
 */
```

#### Key Design Points

- **Buffers and sorts** on initial fetch
- **Push is unsorted** - no need to maintain order for incremental updates
- **Uses query's ORDER BY** - sorts by columns specified in the original query
- **Currently sorts by root table columns** only (our current query limitation)

#### Interface

```typescript
export interface SortToRootOrderArgs {
  input: Input;
  storage: Storage;
  targetSort: Ordering; // The columns to sort by (from query's ORDER BY)
}
```

#### Implementation Details

1. **Fetch**: Buffer all results, sort them, yield in order
2. **Push**: Pass through unsorted (push is inherently unordered)
3. **Storage**: May cache sorted state for optimization (optional)

## Complete Pipeline

```typescript
// Original query: SELECT * FROM foo ORDER BY foo.name WHERE EXISTS(bar WHERE EXISTS(baz))

// Step 1: Reordered joins for optimization (start with smallest table)
const reorderedJoins =
  baz →
  Join(bar, { relationshipName: 'bar' }) →
  Join(foo, { relationshipName: 'foo' });

// Step 2: Apply EXISTS filters (on the reordered tree)
const filtered =
  reorderedJoins →
  Exists('bar') →  // Has bar relationship
  Exists('foo');   // Has foo relationship

// Step 3: Extract full foo rows from the nested structure
const extractedRows = new ExtractMatchingKeys({
  input: filtered,
  targetTable: 'foo',
  targetPath: ['bar', 'foo'],  // Path from baz through bar to foo
  targetSchema: fooSchema,     // Contains all foo columns
});

// Step 4: Remove duplicates (cartesian product may have duplicated rows)
const distinct = new Distinct(
  extractedRows,
  storage,
  ['foo_id']  // Distinct on foo's primary key
);

// Step 5: Sort to match query's ORDER BY
const sorted = new SortToRootOrder({
  input: distinct,
  storage,
  targetSort: [['foo_name', 'asc']],  // From query's ORDER BY
});

// Result: Full foo rows, properly ordered, with correct structure
```

## Benefits

1. **Reuses existing operators** - No need for complex new join types
2. **Simple mental model** - Extract then reshape
3. **Optimization friendly** - Can reorder joins arbitrarily
4. **Full row preservation** - No need for additional joins to get full data

## Push Handling

### ExtractMatchingKeys Push

```typescript
push(change: Change): void {
  // For changes at any level, traverse to find affected target rows
  switch (change.type) {
    case 'add':
    case 'remove':
      // Traverse the tree in change.node to find target table rows
      this.#extractAndPushFromChange(change);
      break;
    case 'edit':
      // Extract both old and new target rows
      this.#extractAndPushEditFromChange(change);
      break;
    case 'child':
      // Follow the child change path
      this.#extractAndPushChildChange(change);
      break;
  }
}
```

### SortToRootOrder Push

```typescript
push(change: Change): void {
  // Push is inherently unsorted - just pass through
  this.#output.push(change);
}
```

## Alternative Optimizations

### Streaming Sort (Future)

If we can prove certain order properties are preserved through joins, we might be able to sort in a streaming fashion without full buffering.

### Lazy Extraction (Future)

Instead of traversing the entire tree eagerly, we could mark nodes with metadata and extract lazily.

## Key Invariants

1. **ExtractMatchingKeys** must handle duplicate rows from cartesian products
2. **SortToRootOrder** only needs to sort on fetch, not maintain order on push
3. **The pipeline assumes flat joins are at the top**, before any regular joins
4. **Full row data is preserved** through extraction, eliminating need for rejoin

## AST Transformation Approach (Alternative)

Instead of handling `flip: true` in the pipeline builder, we can transform the AST to eliminate flip flags by restructuring the query.

### Basic Transformation

When we encounter `flip: true` on an EXISTS condition:

```typescript
// Original AST with flip
{
  table: 'users',
  orderBy: [['id', 'asc']],
  where: {
    type: 'correlatedSubquery',
    op: 'EXISTS',
    flip: true,
    related: {
      subquery: { table: 'orders', ... },
      correlation: { parentField: ['id'], childField: ['userId'] }
    }
  }
}

// Transformed AST (no flip)
{
  table: 'orders',  // Subquery becomes root
  orderBy: [['userId', 'asc']],  // Adjust ordering
  where: {
    type: 'correlatedSubquery',
    op: 'EXISTS',
    related: {
      subquery: { table: 'users', ... },  // Parent becomes subquery
      correlation: { 
        parentField: ['userId'],  // Swap correlation
        childField: ['id'] 
      }
    }
  }
}
// Returns: { ast: transformedAST, pathToOriginalRoot: ['users'] }
```

### Handling AND/OR with Multiple Flipped EXISTS

For complex boolean conditions with multiple flipped EXISTS:

#### AND Case
```typescript
// Multiple flipped EXISTS with AND
// WHERE EXISTS(orders) AND EXISTS(reviews)
// Both with flip: true

// Transform into:
// 1. Create two pipelines, each starting from subquery
// 2. Extract users from each
// 3. INTERSECT the extracted users
// 4. Apply SortToRootOrder
```

#### OR Case
```typescript
// Multiple flipped EXISTS with OR
// WHERE EXISTS(orders) OR EXISTS(reviews)
// Both with flip: true

// Transform into:
// 1. Create two pipelines, each starting from subquery
// 2. Extract users from each
// 3. UNION the extracted users
// 4. Apply SortToRootOrder
```

### Implementation Plan

1. **Phase 1**: Implement AST transformation for single flipped EXISTS
   - `transformFlippedExists(ast: AST): { ast: AST, pathToRoot: string[] }`
   - Handle WHERE conditions moving with parent
   - Return path for later extraction

2. **Phase 2**: Handle complex cases
   - Detect AND/OR with multiple flipped EXISTS
   - Create union/intersect operators for combining results
   - Ensure correct deduplication

3. **Phase 3**: Integration
   - Update pipeline builder to use transformed AST
   - Apply ExtractMatchingKeys using pathToRoot
   - Apply SortToRootOrder at the end

### Benefits of AST Transformation

1. **Cleaner separation**: AST transformation is pure, pipeline building stays simple
2. **Easier testing**: Can test AST transformation independently
3. **Better optimization**: Can analyze and optimize the transformed AST
4. **Type safety**: No type mismatches between Input and FilterInput

## Next Steps

1. ✅ Implement ExtractMatchingKeys operator
2. ✅ Implement SortToRootOrder operator
3. Implement AST transformation for flipped EXISTS
4. Add union/intersect operators for AND/OR cases
5. Create tests for the complete pipeline
6. Add query planner logic to detect when to apply this optimization
7. Add cardinality estimation to decide when reordering is beneficial
