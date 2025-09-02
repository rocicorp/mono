# EXISTS Query Optimization Project

## Initial Goal

The project aims to address performance issues with `whereExists` queries in the incremental view maintenance (IVM) engine. The current implementation has two main components:

1. **Join Node** - Creates the relationship between tables
2. **Exists Filter Operator** - Filters results based on existence

### The Core Problem: Join Order

The key issue is determining the optimal join order - specifically, which table should be the outer loop (first in the pipeline). Ideally, the most constrained or smallest table should be processed first for better performance.

### Current Challenge

The existing join structure creates a tree output, which is problematic when re-ordering joins because:

- Changing join order changes the output structure
- Tree structure makes it difficult to optimize join ordering dynamically

### Proposed Solution: Flat Joins

The idea is to create a new type of join called `flatJoin` that:

- Creates a cartesian product rather than a tree structure
- Is used specifically for exists operations (since they don't need to appear in final output)
- Can be reordered without affecting the output structure

#### Row Format

The flat join will produce rows in the following format:

```typescript
{
  // All fields from the root table
  ...rootTableFields,

  // Joined data nested under aliases (not in the tree structure)
  [flatJoinAlias]: { ...joinedRow },
  [anotherAlias]: { ...anotherJoinedRow }
}
```

For example:

```typescript
{
  id: '1',
  name: 'Alice',
  value: 10,
  existsJoin1: { id: 'j1', data: 'joined1' },
  existsJoin2: { id: 'j2', data: 'joined2' }
}
```

The processing pipeline would be:

1. Process all flat joins first (can be reordered for optimization)
2. Apply a new `distinct` operator to collapse duplicates
   - Distinct only on the primary keys of the root table
   - Preserves the first occurrence of each unique combination
   - Ignores the nested join data when determining uniqueness
3. Continue with normal tree-structured joins for the rest of the pipeline

## What Has Been Implemented

### 1. Distinct Operator (`src/ivm/distinct.ts`)

Created a new operator that:

- **Deduplicates rows** based on specified keys (typically primary keys)
- **Maintains state** using the storage mechanism to track seen rows
- **Handles all change types**:
  - `add`: Only passes through if key hasn't been seen
  - `remove`: Removes from state and passes through if exists
  - `edit`: Handles key changes by converting to remove+add when needed
  - `child`: Passes through if parent row is in the distinct set
- **Supports composite keys** for complex deduplication scenarios
- **Integrates seamlessly** with the existing IVM pipeline architecture

### 2. Comprehensive Test Suite (`src/ivm/distinct.test.ts`)

Implemented tests covering:

- Filtering duplicate rows during fetch operations
- Handling incremental changes (add/remove/edit)
- Composite key deduplication
- State cleanup and restoration
- Mock input operator to simulate duplicate rows (since MemorySource enforces uniqueness)

### 3. Integration Points

The distinct operator follows the established patterns in the codebase:

- Implements the `Operator` interface
- Uses the standard `Input`/`Output` pattern for pipeline composition
- Leverages the `Storage` interface for state persistence
- Follows the same fetch/push/cleanup lifecycle as other operators

## Repository Understanding

### Architecture Overview

The repository implements an incremental view maintenance (IVM) engine with a novel approach:

1. **Data Flow Graph**: Constructs a standard data flow graph for query processing
2. **Upqueries**: Unique feature allowing operators to query upstream for state
   - `fetch`: Path for upqueries to answer queries from scratch
   - `push`: Path for incremental updates when base data changes
3. **Minimal State**: Operators maintain minimal state by leveraging upqueries

### Key Components

#### Core Operators (`src/ivm/`)

- **Source operators**: `MemorySource`, base data providers
- **Transform operators**: `Filter`, `Take`, `Skip`, `Join`
- **State management**: `Storage` interface with `MemoryStorage` implementation
- **Change propagation**: Structured change types (add/remove/edit/child)

#### Query System (`src/query/`)

- AST-based query definition
- Query compilation to operator pipelines
- Expression evaluation and filtering

#### Testing Infrastructure

- Comprehensive test utilities (`Snitch` for debugging, `MockInput` for testing)
- Property-based testing with fast-check
- Extensive test coverage for edge cases

### Design Patterns

1. **Operator Pattern**: Each operator is both an Input and Output
2. **Streaming**: Uses generator-based streams for efficient data processing
3. **Schema-driven**: Operations guided by `SourceSchema` with sorting and comparison
4. **Type Safety**: Strong TypeScript types throughout
5. **Monorepo Structure**: Part of larger system with shared utilities

### Performance Considerations

- Lazy evaluation with generators
- Efficient sorting using BTree structures
- Index management in MemorySource
- Careful state management to minimize memory usage

### Next Steps for the Project

With the distinct operator complete, the next phases would be:

1. Implement the `flatJoin` operator for cartesian products
2. Modify the exists query compilation to use flatJoin + distinct
3. Implement join reordering logic based on cardinality estimates
4. Performance testing and optimization
5. Integration with the broader query planning system
