# Planner Debug Delegate Implementation Summary

## What Was Implemented

A flexible debug tracing system for the query planner that allows developers to observe and analyze planning decisions in real-time.

## Key Components

### 1. **PlannerDebugDelegate Interface** (`planner-debug-delegate.ts`)

- Defines optional callback methods for key planning events
- Provides detailed type information for all events
- Includes `ConnectionCostDetail` type with branch pattern breakdown

### 2. **Connection Cost Details** (`planner-connection.ts`)

- Added `getBranchCostDetails()` method to PlannerConnection
- Exposes per-branch-pattern cost information
- Useful for understanding OR query optimization

### 3. **Join Flip Tracking** (`planner-graph.ts`)

- Modified `pinAndMaybeFlipJoins()` to return array of flipped joins
- Updated `traverseAndPin()` to track which joins were flipped
- Provides insight into which direction joins are executed

### 4. **Delegate Integration** (`planner-graph.ts`)

- Modified `plan()` method to accept optional `PlannerDebugDelegate`
- Added delegate calls at 6 key decision points:
  - `onAttemptStart`: When a planning attempt begins
  - `onConnectionCosts`: When connection costs are evaluated
  - `onConnectionPinned`: When a connection is selected and pinned
  - `onAttemptComplete`: When an attempt finishes successfully
  - `onBestPlanFound`: When a new best plan is discovered
  - `onAttemptFailed`: When an attempt fails

### 5. **Example and Documentation**

- Example test showing delegate usage (`planner-debug-delegate.test.ts`)
- Comprehensive documentation (`DEBUG_DELEGATE.md`)
- Use case examples for debugging, performance analysis, and visualization

## Events Traced

The delegate captures the complete planning timeline:

```
Attempt 0: Starting with posts (cost: 100)
  Pinned: posts (cost: 100) (flipped)
  Attempt 0 complete: cost=1000000, flipped=1/1 joins
  *** New best plan found at attempt 0: cost=1000000

Attempt 1: Starting with users (cost: 10000)
  Pinned: users (cost: 10000)
  Attempt 1 complete: cost=1000000, flipped=0/1 joins
```

## Information Available

### Connection Cost Details

- **Total cost**: Aggregate cost across all branch patterns
- **Branch costs**: Map of branch pattern path → individual cost
- **Branch constraints**: Map of branch pattern → constraint applied
- **Connection metadata**: Table name, filters, ordering

### Join Information

- **Flipped joins**: Array of joins that were flipped when a connection was pinned
- **Join direction**: Whether join was flipped (child→parent) or left (parent→child)

### Plan Summary

- Total connections and pinned count
- Total joins and flipped count
- Attempt number and final cost

## Use Cases

### 1. **Debugging**

Understand why the planner chose a specific execution plan:

```typescript
const delegate = {
  onBestPlanFound: (attempt, cost) =>
    console.log(`Best plan: attempt ${attempt}, cost ${cost}`),
};
```

### 2. **Visualization**

Build interactive visualizations of the planning process by collecting all events.

### 3. **Performance Analysis**

Record metrics to identify optimization opportunities:

```typescript
const metrics = {attempts: 0, bestCost: Infinity};
const delegate = {
  onAttemptStart: () => metrics.attempts++,
  onBestPlanFound: (_, cost) => (metrics.bestCost = cost),
};
```

### 4. **Testing**

Verify planner behavior in automated tests:

```typescript
const trace: string[] = [];
const delegate = {
  onConnectionPinned: (conn, _, flipped) =>
    trace.push(`${conn.table}${flipped.length ? ':flipped' : ''}`),
};
// Assert on trace array
```

## API Design Principles

1. **Optional Everything**: All delegate methods are optional - implement only what you need
2. **Rich Data**: Each event provides detailed context for analysis
3. **Non-Intrusive**: Delegate is completely optional, no impact on existing code
4. **Backward Compatible**: Old `debug` parameter still works
5. **Type Safe**: Full TypeScript types for all delegate methods and data structures

## Files Modified

- `packages/zql/src/planner/planner-debug-delegate.ts` (new)
- `packages/zql/src/planner/planner-debug-delegate.test.ts` (new)
- `packages/zql/src/planner/DEBUG_DELEGATE.md` (new)
- `packages/zql/src/planner/planner-graph.ts` (modified)
- `packages/zql/src/planner/planner-connection.ts` (modified)

## Testing

Run the example:

```bash
npm test -- planner-debug-delegate.test.ts
```

Type checking:

```bash
npm run check-types -- --filter=zql
```
