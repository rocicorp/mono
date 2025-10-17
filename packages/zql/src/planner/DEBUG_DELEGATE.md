# Planner Debug Delegate

The planner debug delegate allows you to trace and visualize planning decisions made by the query planner.

## Overview

The planner uses a multi-start greedy search algorithm to find the optimal execution plan. During planning, it:

1. Tries different starting connections (tables)
2. Selects connections in order of estimated cost
3. Flips joins when beneficial (choosing which table to scan first)
4. Evaluates the total cost of each complete plan
5. Selects the plan with the lowest cost

The debug delegate provides hooks at each decision point, allowing you to:

- **Trace** planning decisions for debugging
- **Visualize** the planning process
- **Analyze** why certain plans were chosen
- **Record** planning metrics for performance analysis

## Usage

### Basic Example

```typescript
import {buildPlanGraph} from './planner-builder.ts';
import type {PlannerDebugDelegate} from './planner-debug-delegate.ts';

// Create a delegate to trace planning
const delegate: PlannerDebugDelegate = {
  onAttemptStart(attempt, connection, cost) {
    console.log(
      `Attempt ${attempt}: Starting with ${connection.table} (cost: ${cost})`,
    );
  },

  onConnectionPinned(connection, cost, flippedJoins) {
    const flipped = flippedJoins.length > 0 ? ' (FLIPPED)' : '';
    console.log(`  Pinned ${connection.table}${flipped}`);
  },

  onBestPlanFound(attempt, cost) {
    console.log(`*** Best plan found! Cost: ${cost}`);
  },
};

// Use the delegate when planning
const {plan} = buildPlanGraph(ast, costModel);
plan.plan(false, delegate);
```

### Visualizing Connection Costs with Branch Patterns

For queries with OR conditions, connections may have multiple branch patterns. The delegate provides detailed cost breakdowns:

```typescript
const delegate: PlannerDebugDelegate = {
  onConnectionCosts(step, costs) {
    console.log(`Step ${step} - Available connections:`);
    for (const {connection, totalCost, branchCosts} of costs) {
      console.log(`  ${connection.table}: total=${totalCost}`);

      // Show per-branch costs (for OR queries)
      if (branchCosts.size > 1) {
        for (const [pattern, cost] of branchCosts) {
          console.log(`    branch[${pattern}]: ${cost}`);
        }
      }
    }
  },
};
```

## Delegate Methods

All methods are optional. Implement only the events you want to observe.

### onAttemptStart(attempt, connection, cost)

Called at the start of each planning attempt.

- `attempt`: Zero-based attempt number
- `connection`: The connection being tried as the root
- `cost`: Initial cost of this connection

### onConnectionCosts(step, costs)

Called after estimating connection costs during greedy selection.

- `step`: Step number within the current attempt (0 = initial, 1+ = greedy steps)
- `costs`: Array of `ConnectionCostDetail` objects with:
  - `connection`: The connection
  - `totalCost`: Total cost across all branch patterns
  - `branchCosts`: Map of branch pattern (e.g., "0,1") to individual cost
  - `branchConstraints`: Map of branch pattern to the constraint used

### onConnectionPinned(connection, cost, flippedJoins)

Called when a connection is successfully pinned.

- `connection`: The connection that was pinned
- `cost`: The cost at the time it was pinned
- `flippedJoins`: Array of joins that were flipped as a result

### onAttemptComplete(attempt, totalCost, summary)

Called when a planning attempt completes successfully.

- `attempt`: Zero-based attempt number
- `totalCost`: Total cost of the completed plan
- `summary`: Plan summary with connection/join counts

### onBestPlanFound(attempt, cost, summary)

Called when a new best plan is found.

- `attempt`: Zero-based attempt number that produced this plan
- `cost`: Cost of the new best plan
- `summary`: Plan summary

### onAttemptFailed(attempt, reason)

Called when a planning attempt fails.

- `attempt`: Zero-based attempt number
- `reason`: Description of why the attempt failed

## Use Cases

### 1. Debugging Query Plans

Understand why the planner chose a particular execution order:

```typescript
const trace: string[] = [];
const delegate: PlannerDebugDelegate = {
  onAttemptStart: (attempt, conn, cost) =>
    trace.push(`[${attempt}] Start: ${conn.table} (${cost})`),
  onConnectionPinned: (conn, cost, flipped) =>
    trace.push(`  Pin: ${conn.table} ${flipped.length > 0 ? '(flipped)' : ''}`),
  onBestPlanFound: (attempt, cost) =>
    trace.push(`*** Best: attempt ${attempt}, cost ${cost}`),
};
```

### 2. Performance Analysis

Record planning metrics to identify optimization opportunities:

```typescript
let totalAttempts = 0;
let bestCost = Infinity;
let bestAttempt = -1;

const delegate: PlannerDebugDelegate = {
  onAttemptStart: () => totalAttempts++,
  onBestPlanFound: (attempt, cost) => {
    bestCost = cost;
    bestAttempt = attempt;
  },
};

// After planning
console.log(
  `Tried ${totalAttempts} attempts, best was #${bestAttempt} with cost ${bestCost}`,
);
```

### 3. Visualization

Build a visual representation of the planning process:

```typescript
interface PlanStep {
  attempt: number;
  step: number;
  action: 'pin' | 'cost' | 'complete';
  data: unknown;
}

const steps: PlanStep[] = [];

const delegate: PlannerDebugDelegate = {
  onConnectionPinned: (conn, cost, flipped) =>
    steps.push({
      attempt,
      step,
      action: 'pin',
      data: {conn: conn.table, cost, flipped: flipped.length},
    }),
  onConnectionCosts: (step, costs) =>
    steps.push({
      attempt,
      step,
      action: 'cost',
      data: costs.map(c => ({table: c.connection.table, cost: c.totalCost})),
    }),
  onAttemptComplete: (attempt, cost, summary) =>
    steps.push({attempt, step: -1, action: 'complete', data: {cost, summary}}),
};

// Use 'steps' to render a visualization
```

## Understanding Branch Patterns

For queries with OR conditions (FanOut/FanIn structures), connections can have multiple branch patterns:

```sql
-- Query: track WHERE EXISTS(album) OR EXISTS(genre)
```

In this case, the `track` connection might show:

- Branch pattern `"0"`: Cost for the EXISTS(album) path
- Branch pattern `"1"`: Cost for the EXISTS(genre) path
- Total cost: Sum of both branch costs

The delegate's `onConnectionCosts` provides detailed branch-level information to help you understand how OR queries affect planning.

## Integration with Existing Debug Output

The delegate can be used alongside the existing `debug` parameter:

```typescript
// Both console logging AND delegate tracing
plan.plan(true, myDelegate);

// Only delegate tracing (no console output)
plan.plan(false, myDelegate);
```

The `debug` parameter is deprecated in favor of the delegate for more flexible debugging.
