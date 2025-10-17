# Planner

SQLite plans single table queries. Our planner plans the joins.

Note: It may eventually plan single table queries as it becomes necessary on the frontend and/or we decide to replace SQLite with a different store.

## Architecture: Structure vs. Planning State

The planner uses a **dual-state design pattern** to separate concerns and enable fast planning:

### Graph Structure (Immutable)

Built once by `planner-builder.ts` and never modified during planning:

- **Nodes**: Sources, Connections, Joins, FanOut/FanIn, Terminus
- **Edges**: How nodes connect to each other (parent/child relationships)
- **Configuration**: Cost models, filters, orderings, constraints definitions

Think of this as the "blueprint" of the query - the structural relationships that don't change.

### Planning State (Mutable)

Modified during `PlannerGraph.plan()` as we search for the optimal execution plan:

- **Pinned flags**: Which connections have been locked into the plan
- **Join types**: Whether joins are 'left' (original) or 'flipped' (reversed)
- **Accumulated constraints**: What constraints have propagated from parent joins

Think of this as the "current attempt" - the state that changes as we explore different plans.

### Why This Separation?

1. **Performance**: Mutating state in-place is much faster than copying entire graph structures
2. **Multi-start search**: Can `resetPlanningState()` and try different starting connections
3. **Backtracking**: Can `capturePlanningSnapshot()` and `restorePlanningSnapshot()` when attempts fail
4. **Clarity**: Makes it obvious what changes during planning vs. what's fixed structure

This pattern is common in query optimizers (see Postgres, Apache Calcite, etc.) where the search space is large and performance matters.

## Graph

The planner creates a graph that represents the pipeline. This graph consists of nodes that are relevant to planning joins:

1. **Connection** - Represents a table scan with filters and ordering
2. **Join** - Combines two data streams (parent and child)
3. **FanOut** - Distributes a single stream to multiple branches (used in OR conditions)
4. **FanIn** - Merges multiple branches back into a single stream
5. **Terminus** - The final output node where constraint propagation begins

Note: `PlannerSource` exists as a factory for creating connections but is not itself a graph node.

**Example graph:**

```ts
issue
  .where(
    ({or, exists}) => or(
      exists('parent_issue', q => q.where('id', ?)),
      exists('parent_issue', q => q.where('id', ?)),
    )
  )
```

```mermaid
flowchart TD
    S(["Source"]) --> C1["Connection1"]
    S-->C2["Conneciton2"]
    C1-->FO1["Fan Out"]
    FO1-->J1{"Join"}
    FO1-->J2{"Join"}
    C2-->J1
    C2-->J2
    J1-->FI1["Fan In"]
    J2-->FI1
    C1@{ shape: dbl-circ}
    C2@{ shape: dbl-circ }
```

### FanOut/FanIn Type Conversion

FanOut and FanIn nodes have two variants that affect how branches are handled:

**FanIn Types:**

- **FI (Normal FanIn)**: All branches share the same branch pattern `[0, ...]`. Used when branches are correlated (no flipped joins between FO and FI).
- **UFI (Union FanIn)**: Each branch gets a unique pattern `[0, ...]`, `[1, ...]`, etc. Required when joins are flipped, making branches independent.

**FanOut Types:**

- **FO (Normal FanOut)**: Standard distribution to branches.
- **UFO (Uncorrelated FanOut)**: Marks that downstream branches are independent.

**Conversion Trigger:**

When a join is flipped between a FanOut and its corresponding FanIn, both must convert:

- FO → UFO
- FI → UFI

This happens automatically during the planning phase via `checkAndConvertFOFI()` after join flipping.

**Why This Matters:**

UFI changes cost semantics. Consider `(A OR B) AND (C OR D)`:

- With FI: Evaluates as a single correlated operation
- With UFI: Each branch is independent, causing exponential cost growth if chained

The conversion ensures accurate cost modeling when joins are reordered.

## Plan Shape

```ts
// planner-builder.ts
export type Plans = {
  plan: PlannerGraph;
  subPlans: {[key: string]: Plans};
};
```

Because a query can be composed of sub-queries, a query plan can be composed of sub-plans. Concretely, `related` calls get their own query plans.

```ts
issue
  .related('owner', q => ...)
  .related('comments', q => ...);
```

The above query would result in a plan that is composed of 3 plans:

- Plan for the top level issue query
- Plan for the nested owner query
- Plan for the nested comments query

If there is more nesting, or more sibling related calls, there are more plans. There is a tree of plans.

`exists` calls do not create separate plans. All `exists` are planned together (that's the whole point of the planner!) as `exists` are inner & semi-joins so they are what need planning.

```ts
issue
  .exists('owner', q => ...)
  .exists('comments', q => ...)
```

The above query would result in a single plan that is not composed of any other plans. This is also the case if more exists were present.

## Degrees of Freedom

The planner can adjust two sets of knobs when creating a plan:

1. The flipping, or not, of joins
2. The ordering of `AND` conditions

The planner currently only leverages (1).

## Join Flipping

The planner creates a graph of the query as the query is written (i.e., ordering of logical terms is not changed). It then loops through these steps:

1. **Connection cost estimation** - Calculate costs for all unpinned connections
2. **Connection selection** - Pick the lowest-cost connection
3. **Join flipping & pinning** - Traverse downstream, flip joins if needed, pin all joins on the path
4. **FO/FI type conversion** - Convert FO→UFO and FI→UFI if any join was flipped between them
5. **Constraint propagation** - Send constraints up the graph from the terminus
6. **Implicit connection pinning** - Connections receiving constraints from pinned joins become pinned

This repeats until all connections have been pinned (selected or forced by pinned joins).

## Cost Estimation

The cost of a connection is the estimated number of rows that will be scanned by that connections as well as any additional post-processing run against the connection to return those rows.

Examples:

```sql
SELECT * FROM issue;
```

The cost of the above would be the size of the issue table.

```sql
SELECT * FROM issue WHERE creator_id = ?;
```

The cost of the above would be the average number of rows per creator.

```sql
SELECT * FROM issue ORDER BY unindexed_column;
```

The cost of the above would be the size of the issue table + the cost to create a temp b-tree that contains all rows. This is one of those post processing steps referred to earlier.

`planner-connection.ts` takes a function that can return a cost, allowing different cost models to be applied. E.g., SQLite's or our own.

Limits are never provided to the cost estimator as we can never know how many rows will be filtered out before fulfilling a limit.

### Cost Estimation with Branch Patterns

The `estimateCost()` method accepts an optional branch pattern parameter:

- **`estimateCost(undefined)`**: Returns the sum of costs across all branches. Used by `getUnpinnedConnectionCosts()` to rank connections for selection.
- **`estimateCost([0, 1, 2])`**: Returns the cost for a specific branch pattern. Used during constraint propagation when joins estimate their costs.

Branch patterns flow through the graph during both constraint propagation and cost estimation:

- **FanIn (FI)**: Passes `[0, ...]` to all branches (correlated)
- **Union FanIn (UFI)**: Passes `[i, ...]` with unique index per branch (independent)
- **Joins**: Pass through the branch pattern unchanged

**Caching Strategy:**

To avoid redundant cost model invocations, connections cache costs at two levels:

1. **Total cost cache**: The sum of all branch costs (when `branchPattern === undefined`)
2. **Per-constraint cache**: A map from branch pattern key (`"0,1"`) to computed cost

Both caches are invalidated when constraints change during propagation.

## Connection Selection

The lowest cost N connections are chosen as starting points for the planning algorithm. Each one will generate a unique plan and the plan with the lowest total cost will be the one selected.

## Join flipping & pinning

Once a connection is selected, we follow its outputs to the first join. That join, and all joins it outputs to, is the pinned.

`source -> connection1 -> join1 -> join2 <- connection2`

When "connection1" is chosen, both `join1` and `join2` will become pinned. This is because flipping any join on the path to the connection would cause that connection to no longer be run in the chosen position.
In other words, if a later step flips `join2` then `connection2` would become the outer loop to `connection1`, invalidating our plan that put `connection` in the outer loop.

If `connection1` was the child input to `join1` then `join1` is flipped. We follow `join1`'s output and apply the same logic:

- if `join1` is the child input of `join2`, flip and pin `join2` otherwise only pin `join2`.

## Constraint Propagation

After flipping and pinning, constraints are propagated up the graph from the final / "view" node.

node -propagate_constraints-> node -propagate_constraints-> node ...

- a pinned and not-flipped join will send constraints to its child
- a pinned and not-flipped join will forward constraints it received to its parent
- a pinned and flipped join will send undefined constraints to its child
- a pinned and flipped join will send a merger of constraints to its parent
- a non-pinned and non-flipped join will forward constraints to its parent
- a not pinned and flipped join is an error

See `planner-join.ts` for more detail.

### Branch Patterns

Branch patterns are arrays of numbers that uniquely identify paths through the query graph, particularly when OR conditions create multiple branches.

**How They Work:**

1. **Terminus starts with `[]`**: The empty pattern at the root
2. **FanIn adds a prefix**:
   - **FI**: Adds `[0, ...]` to all branches (correlated access)
   - **UFI**: Adds `[i, ...]` with unique `i` per branch (independent access)
3. **Other nodes pass through unchanged**: Joins, FanOuts, Connections preserve the pattern

**Example: OR Query**

```ts
track.where(({or, exists}) => or(exists('album'), exists('invoiceLines')));
```

If the FanIn is converted to UFI (because a join flipped), constraint propagation generates two patterns:

- Branch 0 (album path): `[0]`
- Branch 1 (invoiceLines path): `[1]`

**Why Branch Patterns Matter:**

1. **Constraint Tracking**: Connections map constraints by branch pattern key (`"0"`, `"1"`, `"0,1"`)
2. **Cost Estimation**: Each branch can have different costs based on its constraints
3. **Exponential Cost Growth**: Chained UFIs create cartesian products:
   - `(A OR B)` → 2 branches
   - `(A OR B) AND (C OR D)` → 4 branches (2×2)
   - Three ORs → 8 branches (2×2×2)

This is why the planner tries to avoid flipping joins in OR regions when possible.

## Connection Pinning

If a pinned join feeds a constraint directly to an unpinned connection, that connection becomes pinned. This is because that connection's order in the plan has becomed fixed by the pinning of the join feeding it.

```
c1 --> join
c2 --/
```

If `c1` is chosen, pinning join, then `c2` must go next.

```
c1  c2  c3
 |  |   |
 \  /   |
 join   |
   |    |
    \  /
    join
```

If `c1` is chosen, both joins are pinned. Since both joins are pinned, no more choices are available to the planner. Both `c2` and `c3` become pinned.

## Repeat: Cost Estimation

Once constraints have been propagated to connections, they can update their costs. In simple terms: selecting a join to put in the outer loop reveals constraints that will be available to later joins.
Costs are updated to reflect the new constraints.

---

## NOT EXISTS Handling

`NOT EXISTS` cannot be flipped at the moment. This is handled by marking them as "unflippable" and throwing if we try to flip. The planner catches the throw and moves on to pick a different connection which may not incur a flip of the `NOT EXISTS` join.
