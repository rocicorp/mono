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

#### AND Case - Transform First Approach ✅ IMPLEMENTED

**Strategy:** Transform only the **first** flipped EXISTS, convert others to regular EXISTS.

```typescript
// Original
users WHERE EXISTS(orders WITH flip) AND EXISTS(reviews WITH flip)

// Transformation
orders WHERE EXISTS(users) AND EXISTS(reviews) // reviews converted to regular EXISTS
```

**Benefits:**
- **Query author control:** Put the most selective table first to control optimization
- **Simple implementation:** Reuses existing single-flip transformation logic  
- **Predictable behavior:** First flip always wins
- **Semantic preservation:** Non-flipped conditions stay with their logical parent

**Implementation Logic:**
1. Detect multiple flipped EXISTS in AND condition
2. Transform only the first flipped EXISTS found
3. Split conditions:
   - **Flipped EXISTS:** Others become regular EXISTS at root level
   - **Non-flipped conditions:** Stay with parent (preserve original semantics)
4. Combine all conditions at appropriate levels

```typescript
// Example with mixed conditions:
// Original: users WHERE age > 30 AND EXISTS(orders WITH flip) AND EXISTS(reviews WITH flip)
// Result: orders WHERE EXISTS(users WHERE age > 30) AND EXISTS(reviews)
//         ^         ^                   ^               ^
//         new root  parent EXISTS      age stays       reviews at root
//                   preserves context  with parent     level
```

#### OR Case - Union Approach 📋 PLANNED

**Strategy:** Create separate pipelines for each flipped EXISTS and union results.

```typescript
// Original
users WHERE EXISTS(orders WITH flip) OR EXISTS(reviews WITH flip)

// Transformation Strategy:
// 1. Create Pipeline 1: orders -> ExtractMatchingKeys(users)
// 2. Create Pipeline 2: reviews -> ExtractMatchingKeys(users)  
// 3. Union(Pipeline1, Pipeline2) -> Distinct -> SortToRootOrder
```

**Why Union Approach:**
- **Preserves semantics:** Still returns users (not orders)
- **Maintains performance:** Each branch starts from optimal table
- **Database-like:** How SQL engines handle complex OR conditions
- **Conceptually sound:** OR naturally maps to set union

**Required Components:**
```typescript
// New operators needed:
class Union implements Operator {
  // Combines results from multiple input streams
  constructor(inputs: Input[]) { ... }
}

class Distinct implements Operator {
  // Removes duplicates based on primary key
  constructor(input: Input, primaryKey: string[]) { ... }
}

// Enhanced pipeline builder:
function buildUnionPipeline(
  flippedConditions: CorrelatedSubqueryCondition[],
  originalRoot: AST
): Input {
  const pipelines = flippedConditions.map(condition => 
    buildFlipPipeline(condition, originalRoot)
  );
  
  return Union(pipelines) -> Distinct -> SortToRootOrder;
}
```

**Implementation Phases:**
1. **Phase 2A:** Implement Union and Distinct operators
2. **Phase 2B:** Detect OR with multiple flips in AST transformation
3. **Phase 2C:** Generate union-based pipeline for OR cases
4. **Phase 2D:** Handle mixed OR conditions (flipped + non-flipped)

**Conservative Fallback:**
For now, OR with multiple flips can fall back to no transformation:
```typescript
// In findFlippedExists():
if (condition.type === 'or') {
  const flippedCount = countFlippedExists(condition);
  if (flippedCount <= 1) {
    // Safe to transform single flip in OR
    return findFirstFlip(condition);
  }
  // Skip transformation for multiple flips - use regular pipeline
  return null;
}
```

### Implementation Plan

1. **Phase 1**: ✅ COMPLETE - AST transformation for single flipped EXISTS
   - ✅ `transformFlippedExists(ast: AST): { ast: AST, pathToRoot: string[] }`
   - ✅ Handle WHERE conditions moving with parent
   - ✅ Return path for later extraction
   - ✅ AND with multiple flips: Transform first, convert others to regular EXISTS

2. **Phase 2**: 📋 IN PROGRESS - OR handling with Union approach
   - **Phase 2A**: 🔄 NEXT - Implement Union operator
   - **Phase 2B**: Implement Distinct operator  
   - **Phase 2C**: Detect OR with multiple flips in AST transformation
   - **Phase 2D**: Generate union-based pipeline for OR cases
   - **Phase 2E**: Handle mixed OR conditions (flipped + non-flipped)

3. **Phase 3**: Integration and optimization
   - Update pipeline builder to use transformed AST
   - Apply ExtractMatchingKeys using pathToRoot
   - Apply SortToRootOrder at the end
   - Add query planner logic to detect when to apply optimization
   - Add cardinality estimation for join reordering decisions

### Benefits of AST Transformation

1. **Cleaner separation**: AST transformation is pure, pipeline building stays simple
2. **Easier testing**: Can test AST transformation independently
3. **Better optimization**: Can analyze and optimize the transformed AST
4. **Type safety**: No type mismatches between Input and FilterInput

## Current Status & Next Steps

### Completed ✅
1. ✅ ExtractMatchingKeys operator - Extracts target table rows from nested structures
2. ✅ SortToRootOrder operator - Restores original sort order after extraction
3. ✅ AST transformation for single flipped EXISTS - Core transformation logic
4. ✅ AND with multiple flips - "Transform first, convert others" approach
5. ✅ Comprehensive test coverage - All transformation scenarios tested
6. ✅ Nested flipped EXISTS - Handles complex nested cases with hierarchy preservation

### In Progress 🔄
- **Phase 2A**: Implement Union operator for OR case handling

### Upcoming 📋
1. **Phase 2B**: Implement Distinct operator for deduplication
2. **Phase 2C**: Extend AST transformation to detect and handle OR cases
3. **Phase 2D**: Integration - Update pipeline builder to use transformed ASTs
4. **Phase 2E**: End-to-end testing of complete flip optimization pipeline
5. **Phase 3**: Query planner integration and cardinality-based optimization decisions
