# Query Planner for WHERE EXISTS

This directory contains a cost-based query planner that optimizes `WHERE EXISTS` (correlated subquery) statements by choosing optimal join execution strategies.

## Table of Contents

- [Overview](#overview)
- [Core Concepts](#core-concepts)
- [Node Types](#node-types)
- [Planning Algorithm](#planning-algorithm)
- [Key Flows](#key-flows)
- [Developer Guide](#developer-guide)
- [Examples](#examples)

## Overview

### Purpose

The planner transforms queries with `EXISTS`/`NOT EXISTS` subqueries into optimized execution plans by deciding:

1. **Join direction**: Should the parent or child table be scanned first?
2. **Cost estimation**: What's the expected cost of each plan?
3. **Constraint propagation**: How do join predicates constrain table scans?

### Example Transformation

```typescript
// Input query
builder.users.whereExists('posts', p => p.where('published', true)).limit(10);

// Planner decides between:
// Plan A (semi-join): Scan users → For each user, check if posts exist
// Plan B (flipped):   Scan posts → For each post, fetch matching user
```

The planner evaluates both strategies and selects the one with the lowest estimated cost.

## Core Concepts

### 1. Dual-State Pattern

Every planner node separates **immutable structure** from **mutable planning state**:

- **Immutable structure** (set at construction):
  - Node connections (parent, child, output)
  - Table names, filters, ordering
  - Join correlation constraints

- **Mutable planning state** (changes during plan search):
  - Join type (semi vs flipped)
  - Fan-out/fan-in type (FO/UFO, FI/UFI)
  - Accumulated constraints
  - Limits

The `reset()` method on each node clears mutable state for replanning without rebuilding the graph structure.

### 2. Join Flipping

A join can execute in two directions:

- **Semi-join** (default): Parent is outer loop, child is inner

  ```
  for each parent row:
    if EXISTS (matching child row):
      emit parent row
  ```

- **Flipped join**: Child is outer loop, parent is inner
  ```
  for each child row:
    fetch matching parent row(s)
    emit results
  ```

**Key constraints:**

- `NOT EXISTS` joins cannot be flipped (marked `flippable: false`)
- Flipping a join makes both parent and child unlimited (removes LIMIT propagation)

### 3. Constraint Propagation

Constraints represent join predicates that narrow table scans:

```typescript
// Example: posts.userId = users.id
type PlannerConstraint = Record<string, undefined>;
// e.g., {userId: undefined} or {id: undefined}
```

Constraints flow **backwards** through the graph from terminus to connections:

- Semi-join: Forwards constraints from output to parent, sends child constraint to child
- Flipped join: Translates output constraints to child, merges constraints to parent
- Connection: Applies constraints to cost model for index selection

### 4. Cost Estimation

Cost flows **forward** through the graph from connections to terminus:

```typescript
type CostEstimate = {
  startupCost: number; // One-time setup (e.g., sorting)
  scanEst: number; // Estimated rows scanned
  cost: number; // Total cumulative cost
  returnedRows: number; // Rows output
  selectivity: number; // Fraction of input rows passing filters
  limit: number | undefined;
};
```

Cost estimation considers:

- Number of rows scanned in each table
- Filter selectivity (fraction of rows passing predicates)
- Join selectivity (fraction of parent rows with matching children)
- Limit propagation (early termination)

### 5. Branch Patterns

Branch patterns (`number[]`) uniquely identify paths through OR branches:

```typescript
// Example: (EXISTS posts) OR (EXISTS comments)
//
//      users
//        |
//       FO ────────┐
//      /  \        │
//     J1  J2       │  Branch patterns:
//      \  /        │  J1: [0]
//       FI         │  J2: [1]
//        |         │
//     terminus     │
//                  │
// Nested ORs:      │
// ((A OR B) AND (C OR D))
//                  │
// Results in patterns like:
// [0,0], [0,1], [1,0], [1,1]
```

Branch patterns allow connections to maintain separate constraints and costs for each OR path.

## Node Types

### PlannerSource (`planner-source.ts`)

Factory for creating connections to a table. Each source represents one table in the query.

**Key methods:**

- `connect()`: Creates a PlannerConnection for this table

### PlannerConnection (`planner-connection.ts`)

Represents a table scan with filters and ordering.

**Immutable:**

- Table name, filters, ordering
- Base constraints (from parent correlation)
- Base limit (from query structure)
- Selectivity (fraction of rows passing filters)

**Mutable:**

- Accumulated constraints from joins
- Current limit (can be cleared by flipped joins)

**Key methods:**

- `propagateConstraints()`: Receives constraints from parent nodes
- `estimateCost()`: Computes scan cost using cost model
- `unlimit()`: Removes limit when join is flipped
- `reset()`: Clears mutable state

**Cost model:**

```typescript
type ConnectionCostModel = (
  table: string,
  sort: Ordering,
  filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
) => {
  startupCost: number;
  rows: number;
};
```

### PlannerJoin (`planner-join.ts`)

Represents a join between parent and child data streams, corresponding to an `EXISTS` or `NOT EXISTS` check.

**Immutable:**

- Parent and child nodes
- Parent/child constraints (correlation predicates)
- Flippability (`NOT EXISTS` cannot flip)
- Plan ID (for tracking in AST)

**Mutable:**

- Join type: `'semi'` or `'flipped'`

**Key methods:**

- `flip()`: Converts semi-join to flipped join
- `propagateUnlimit()`: Removes limits from child when flipped
- `propagateConstraints()`: Routes constraints based on join type
- `estimateCost()`: Computes join cost

**Semi-join costing:**

```typescript
// Parent drives execution
cost = parent.cost + parent.scanEst * (child.startupCost + child.scanEst);
returnedRows = parent.returnedRows * child.selectivity;
```

**Flipped join costing:**

```typescript
// Child drives execution
cost = child.cost + child.scanEst * (parent.startupCost + parent.scanEst);
returnedRows = parent.returnedRows * child.returnedRows * child.selectivity;
```

### PlannerFanOut (`planner-fan-out.ts`)

Splits execution flow for OR branches.

**Types:**

- `FO` (Fan-Out): All branches share constraints and branch pattern
- `UFO` (Union Fan-Out): Each branch gets unique branch pattern

**Conversion rule:** FO → UFO when any join between FO and its corresponding FanIn is flipped.

**Behavior:**

- `FO`: Forwards constraints to input with single branch pattern `[0, ...]`
- `UFO`: Would assign unique patterns, but constraint propagation doesn't differentiate

### PlannerFanIn (`planner-fan-in.ts`)

Merges execution flow from OR branches.

**Types:**

- `FI` (Fan-In): All inputs use same branch pattern (single fetch)
- `UFI` (Union Fan-In): Each input gets unique branch pattern (multiple fetches)

**Conversion rule:** FI → UFI when any join between FanOut and FanIn is flipped.

**FI cost (max across branches):**

```typescript
returnedRows = max(input.returnedRows for input in inputs)
cost = max(input.cost for input in inputs)
selectivity = 1 - ∏(1 - input.selectivity)  // OR probability
```

**UFI cost (sum across branches):**

```typescript
returnedRows = Σ(input.returnedRows for input in inputs)
cost = Σ(input.cost for input in inputs)
selectivity = 1 - ∏(1 - input.selectivity)  // OR probability
```

### PlannerTerminus (`planner-terminus.ts`)

Final output node where planning begins.

**Key methods:**

- `propagateConstraints()`: Initiates backward constraint flow with empty branch pattern
- `estimateCost()`: Initiates forward cost flow with selectivity = 1

### PlannerGraph (`planner-graph.ts`)

Container managing all nodes and orchestrating the planning process.

**Key collections:**

- `connections[]`: All table scans
- `joins[]`: All join nodes
- `fanOuts[]`, `fanIns[]`: All fan nodes
- `#sources`: Map of table name → PlannerSource

**Key methods:**

- `plan()`: Main planning algorithm (exhaustive enumeration)
- `propagateConstraints()`: Triggers backward constraint flow
- `getTotalCost()`: Computes total plan cost
- `resetPlanningState()`: Resets all nodes for replanning
- `capturePlanningSnapshot()` / `restorePlanningSnapshot()`: Save/restore plan state

## Planning Algorithm

The planner uses **exhaustive enumeration** of join flip patterns:

```typescript
// For n flippable joins, evaluate 2^n plans
for pattern in 0...(2^n - 1):
  1. Reset all nodes to initial state
  2. Apply flip pattern (bit i = flip join i)
  3. Derive FO/UFO and FI/UFI from flip pattern
  4. Propagate unlimiting for flipped joins
  5. Propagate constraints backwards through graph
  6. Estimate cost forwards through graph
  7. Track best plan

// Restore best plan
```

### Complexity Limits

- **Max flippable joins**: 9 (configurable via `MAX_FLIPPABLE_JOINS`)
- At 9 joins: 512 plans evaluated
- Safety check logs warning and falls back to unoptimized query if limit exceeded

### Planning Steps in Detail

1. **Build graph structure** (`planner-builder.ts`):

   ```typescript
   buildPlanGraph(ast, model, isRoot) → Plans
   ```

   - Creates sources for each table
   - Builds connections, joins, fan nodes
   - Assigns plan IDs to joins

2. **Enumerate flip patterns** (`PlannerGraph.plan()`):
   - For each pattern (bitmask of which joins are flipped):
     - Reset planning state
     - Apply flips
     - Convert FO/FI to UFO/UFI as needed
     - Propagate unlimiting
     - Propagate constraints
     - Estimate cost
     - Track best

3. **Restore best plan**:
   - Apply best flip pattern
   - Propagate constraints
   - Ready for execution

4. **Apply to AST** (`applyPlansToAST()`):
   - Mark flipped joins in AST with `flip: true`
   - Recurse into related subqueries

## Key Flows

### Constraint Propagation Flow

Constraints flow **backwards** from terminus to connections:

```
terminus
   ↓ constraint=undefined, branchPattern=[]
 join (semi)
   ↓ forwards output constraint to parent
   ↓ sends childConstraint to child
connection
   ↓ applies constraint to cost model
```

For flipped joins:

```
 join (flipped)
   ↓ translates output constraint to child space
   ↓ merges output + parentConstraint for parent
```

For fan nodes:

```
FI/UFI
   ↓ propagates to all inputs
   ↓ FI: same pattern [0, ...] for all
   ↓ UFI: unique patterns [i, ...] for each input

FO/UFO
   ↓ forwards to input (single stream)
```

### Cost Estimation Flow

Costs flow **forward** from connections to terminus:

```
connection
   ↑ base cost from cost model
 join
   ↑ combines parent + child costs
   ↑ computes selectivity
FI/UFI
   ↑ FI: max of inputs
   ↑ UFI: sum of inputs
terminus
   ↑ final total cost
```

### Unlimiting Flow

When a join is flipped, limits are removed to allow full scans:

```
join.flip()
   ↓ calls propagateUnlimit()
   ↓
   ├→ parent.propagateUnlimitFromFlippedJoin()
   │    ↓ connection: clears limit
   │    ↓ semi-join: continues to its parent
   │    ↓ flipped join: stops (already unlimited)
   │    ↓ fan nodes: propagates to all inputs
   │
   └→ child.propagateUnlimitFromFlippedJoin()
        ↓ (same rules as parent)
```

**Why unlimit?** In a flipped join, the child becomes the outer loop and must produce all rows, not just enough to satisfy the parent's limit.

## Developer Guide

### Entry Points

**For query execution:**

```typescript
import {planQuery} from './planner-builder.ts';

const optimizedAST = planQuery(ast, costModel, planDebugger);
// Returns AST with `flip: true` on flipped joins
```

**For testing/analysis:**

```typescript
import {buildPlanGraph} from './planner-builder.ts';
import {AccumulatorDebugger} from './planner-debug.ts';

const plans = buildPlanGraph(ast, costModel, true);
const debugger = new AccumulatorDebugger();
plans.plan.plan(debugger);
console.log(debugger.format());
```

### Cost Model Interface

Implement `ConnectionCostModel` to provide cost estimates:

```typescript
type ConnectionCostModel = (
  table: string,
  sort: Ordering,
  filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
) => {
  startupCost: number; // One-time cost (e.g., sorting)
  rows: number; // Estimated rows returned
};
```

**Guidelines:**

- Use database statistics (row counts, indexes, selectivity)
- Account for constraints (join predicates enable index usage)
- Consider sort order (may require sorting or use index)
- Return conservative estimates when statistics unavailable

**Example (SQLite):**
See `packages/zqlite/src/sqlite-cost-model.ts` for a production implementation using `sqlite_stat1`.

### Debugging

**Enable debug logging:**

```typescript
import {AccumulatorDebugger} from './planner-debug.ts';

const debugger = new AccumulatorDebugger();
planQuery(ast, costModel, debugger);

// Print formatted output
console.log(debugger.format());

// Access specific events
const attempts = debugger.getEvents('attempt-start');
const costs = debugger.getEvents('node-cost');
```

**Debug events:**

- `attempt-start`: New flip pattern being evaluated
- `node-constraint`: Constraint propagated to a node
- `node-cost`: Cost computed for a node
- `constraints-propagated`: All constraints set for an attempt
- `plan-complete`: Plan evaluation succeeded
- `plan-failed`: Plan evaluation failed
- `best-plan-selected`: Best plan chosen

### Testing Patterns

**Test graph construction:**

```typescript
const plans = buildPlanGraph(ast, simpleCostModel, true);
expect(plans.plan.connections).toHaveLength(2);
expect(plans.plan.joins).toHaveLength(1);
```

**Test planning:**

```typescript
plans.plan.plan();
expect(plans.plan.joins[0].type).toBe('flipped');
```

**Test with different costs:**

```typescript
const cheapParentModel = (table, sort, filters, constraint) => {
  if (table === 'parent') return {startupCost: 0, rows: 10};
  return {startupCost: 0, rows: 1000};
};

plans.plan.plan();
// Expect parent chosen first (cheap)
```

### Common Pitfalls

1. **Forgetting to reset between planning attempts**
   - The planning loop handles this, but manual testing needs `resetPlanningState()`

2. **Modifying immutable structure during planning**
   - Graph structure is built once, only mutable state changes

3. **Not handling undefined constraints**
   - When FO → UFO, non-flipped branches may have `constraint: undefined`

4. **Incorrect constraint translation in flipped joins**
   - Must translate from parent space to child space using index-based mapping

5. **Forgetting to propagate unlimiting**
   - Flipped joins must unlimit both parent and child chains

## Examples

### Example 1: Simple EXISTS

```typescript
// Query
builder.users.whereExists('posts').limit(10)

// Graph structure
[users connection] → [join] → [terminus]
                      ↓
              [posts connection]

// Semi-join plan:
// - Scan users (limit 10)
// - For each user, check if posts exist (limit 1)
// Cost: 10 users × 1 post check

// Flipped plan:
// - Scan posts (no limit - unlimited by flip)
// - For each post, fetch user
// Cost: 1000 posts × 1 user fetch
```

### Example 2: AND with Multiple EXISTS

```typescript
// Query
builder.users
  .whereExists('posts', p => p.where('published', true))
  .whereExists('comments')
  .limit(10)

// Graph structure
[users] → [join1] → [join2] → [terminus]
           ↓         ↓
         [posts]   [comments]

// Planner considers 4 patterns (2^2 joins):
// 00: both semi     (scan users first)
// 01: J1 semi, J2 flipped
// 10: J1 flipped, J2 semi
// 11: both flipped  (scan posts or comments first)
```

### Example 3: OR Creates Fan Nodes

```typescript
// Query
builder.users.where(({or, exists}) =>
  or(
    exists('posts'),
    exists('comments')
  )
)

// Graph structure
              FO
             /  \
     [posts J] [comments J]
             \  /
              FI
              |
          [terminus]

// If both joins are semi (not flipped):
// - FO type: FO (single fetch)
// - FI type: FI (max cost of branches)

// If any join is flipped:
// - FO type: UFO (needs unique branch patterns)
// - FI type: UFI (sum cost of branches)
```

### Example 4: Nested Subqueries

```typescript
// Query (ZQL RELATED clause)
builder.issues.related('assignee', a =>
  a.whereExists('labels', l => l.where('name', 'urgent')),
);

// Creates separate plan graphs:
// - Main plan: issues query
// - Subplan: assignee query with EXISTS labels
// Plans are optimized independently, then composed
```

## Related Files

- `SELECTIVITY_PLAN.md`: Design document for semi-join selectivity improvements
- `packages/zqlite/src/sqlite-cost-model.ts`: Production cost model for SQLite
- `packages/zql/src/query/joins/`: Runtime join implementations (SemiJoin, FlippedJoin)

## Further Reading

- PostgreSQL cost model: [compute_semi_anti_join_factors](https://doxygen.postgresql.org/costsize_8c.html)
- SQLite query planner: [Query Planning](https://sqlite.org/optoverview.html)
- Classic paper: Selinger et al., "Access Path Selection in a Relational Database" (1979)
