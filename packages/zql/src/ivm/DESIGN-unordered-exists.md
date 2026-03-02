# Design: Remove Ordering Requirement from EXISTS Subqueries

## Problem

`whereExists` subqueries currently get `orderBy(pk)` appended by `completeOrdering()`. This ordering flows through the IVM pipeline and into server-side SQL generation:

```sql
-- Current: ORDER BY forces SQLite to sort, even when issueID index would be faster
SELECT id, issueID, body FROM comments WHERE issueID = ? ORDER BY id ASC

-- Desired: no ORDER BY, SQLite uses issueID index directly
SELECT id, issueID, body FROM comments WHERE issueID = ?
```

For EXISTS, ordering is semantically irrelevant — we only need to know if ≥1 matching row exists. The `ORDER BY` can force SQLite into suboptimal query plans, sorting millions of rows unnecessarily when a selective index on the join/correlation key would return results immediately.

## Current Architecture

### How ordering gets into EXISTS subqueries

```
User: q.whereExists('comments')
  → query-impl.ts creates CorrelatedSubqueryCondition {op: 'EXISTS', related: {subquery: {table: 'comments'}}}
  → buildPipeline() calls completeOrdering()
  → completeOrdering() recursively visits all subqueries, including those in WHERE conditions
  → completeOrderingInCondition() adds PK to child's orderBy: {orderBy: [['id', 'asc']]}
```

**File**: `packages/zql/src/query/complete-ordering.ts` lines 46-64

```typescript
function completeOrderingInCondition(condition, getPrimaryKey) {
  if (condition.type === 'correlatedSubquery') {
    return {
      ...condition,
      related: {
        ...condition.related,
        subquery: completeOrdering(condition.related.subquery, getPrimaryKey),
        //        ^^^^^^^^^^^^^^^^ adds PK ordering to EXISTS child
      },
    };
  }
}
```

### How the EXISTS child pipeline is built

**File**: `packages/zql/src/builder/builder.ts` lines 255-354

```
buildPipelineInternal(existsChildAST)
  → source.connect(must(ast.orderBy), ...)    // orderBy = [['id', 'asc']]
  → [Skip]
  → [WHERE filters]
  → Take(limit=3, partitionKey=childField)     // ← requires ordering
  → [nested related subqueries]
```

The child pipeline is wrapped in a `Join`, and the `Exists` filter operator checks the relationship size.

### Why Take requires ordering

**File**: `packages/zql/src/ivm/take.ts`

Take maintains a **bound** — the last row in sort order that it has accepted. This bound is used to:

1. **Accept/reject new rows on push**: `compareRows(change.node.row, takeState.bound) >= 0` → reject (line 271)
2. **Find replacement rows on remove**: Fetches with `reverse: true` starting at the bound (line 294-311)
3. **Handle edits that change sort position**: Compares old/new position relative to bound (lines 443-683)

All of this logic is meaningless without a defined sort order. Take asserts this in its constructor:

```typescript
assertOrderingIncludesPK(input.getSchema().sort, input.getSchema().primaryKey);
```

### Why ordering hurts EXISTS performance

On the server side, `TableSource.#fetch()` generates SQL via `buildSelectQuery()`:

**File**: `packages/zqlite/src/query-builder.ts` line 52

```typescript
return sql`${query} ${orderByToSQL(order, !!reverse)}`;
```

This always appends `ORDER BY`. For EXISTS children with a correlation key constraint like `WHERE issueID = ?`, SQLite's query planner now must:

1. Use the `issueID` index to find matching rows
2. Sort those rows by `id` (PK) to satisfy ORDER BY
3. Return the first 3

Without ORDER BY, SQLite would:

1. Use the `issueID` index
2. Return the first 3 matching rows directly (no sort)

On tables with millions of rows and many matches per correlation key, the sort in step 2 can be extremely expensive.

### What Exists actually needs

**File**: `packages/zql/src/ivm/exists.ts`

The Exists operator is simple — it checks if a relationship is non-empty:

```typescript
*#fetchExists(node: Node): Generator<'yield', boolean> {
  // While it seems like this should be able to fetch just 1 node
  // to check for exists, we can't because Take does not support
  // early return during initial fetch.
  return (yield* this.#fetchSize(node)) > 0;
}

*#fetchSize(node: Node): Generator<'yield', number> {
  let size = 0;
  for (const n of relationship()) {
    if (n === 'yield') { yield 'yield'; }
    else { size++; }
  }
  return size;
}
```

It iterates the child relationship (bounded by Take to at most 3 rows) and counts. No ordering needed.

## Design

### New `Cap` Operator

Replace `Take` with a new `Cap` operator for EXISTS child pipelines. Cap is a simple count-based limiter that does not require ordering.

#### State

Per partition (keyed by partition key values, same as Take):

```typescript
type CapState = {
  size: number;
  // JSON-serialized PKs of rows currently in scope
  pks: string[];
};
```

For EXISTS with limit=3, each partition stores at most 3 PKs. Memory footprint is negligible.

#### Constructor

```typescript
class Cap implements Operator {
  constructor(
    input: Input,
    storage: Storage,
    limit: number,
    partitionKey?: PartitionKey,
  ) {
    // NO assertOrderingIncludesPK — ordering not required
  }
}
```

#### Fetch

**Initial fetch** (no state for this partition):

- Read up to `limit` rows from input
- Store their PKs in `CapState`
- Yield each row

**Subsequent fetch** (has state):

- Read from input
- Yield only rows whose PK is in the stored set
- Stop after yielding all in-scope rows

**Unpartitioned fetch with partition key** (nested sub-query case — same as Take's maxBound path at line 128-149):

- Iterate input rows
- For each row, look up its partition's CapState
- Yield if the row's PK is in that partition's stored set

#### Push

**`add`**:

- Look up partition state. If no state, drop (partition not hydrated).
- If `size < limit`: add PK to set, `size++`, forward the add to output.
- If `size === limit`: drop. Cap is full, and without ordering there's no "better" row to swap in.

**`remove`**:

- Look up partition state. If no state, drop.
- If PK is not in set: drop (row was never in scope).
- If PK is in stored set:
  1. Remove PK from set, `size--`
  2. **Refill**: fetch from input with constraint, find first row NOT in PK set
  3. If replacement found: add replacement PK to set, `size++`
     - Forward remove(old) to output
     - Forward add(replacement) to output
  4. If no replacement: forward remove(old) to output

**Why refill is necessary**: Losing all capped rows doesn't mean NOT EXISTS — there may be more matching rows in the source beyond what Cap was tracking. Without refill, Cap would incorrectly report an empty relationship after `limit` removes even when matching rows still exist.

**Refill performance**: Cap's set has at most `limit-1` entries (one just removed). The refill fetch iterates the source (with constraint) and skips rows in the set. With `limit=3` and 2 entries in set, we skip at most 2 rows before finding a replacement. Very fast.

**No `#rowHiddenFromFetch` needed**: The replacement is added to the PK set AFTER the remove is forwarded to output. During remove processing, if downstream re-fetches through Cap, the replacement is not in the set and won't be yielded. After the add is forwarded, the replacement IS in the set and will be yielded. This naturally maintains consistency without hiding rows.

**`edit`**:

- Look up partition state. If no state, drop.
- If old row's PK is in stored set: forward the edit. (PK doesn't change in edits — edits that change correlation keys are split into remove+add by the source.)
- If not in set: drop.

**`child`**:

- Look up partition state. If no state, drop.
- If the change's row PK is in stored set: forward.
- If not in set: drop.

#### Fetch — Counted Early-Stop

Cap's subsequent fetch (after initial hydration) uses a **counted early-stop** strategy:

```typescript
*fetch(req) {
  const capState = this.#getState(req);
  if (!capState) {
    yield* this.#initialFetch(req);
    return;
  }

  let remaining = capState.size;
  if (remaining === 0) return;

  for (const node of this.#input.fetch(req)) {
    if (node === 'yield') { yield node; continue; }
    if (this.#isInScope(capState, node.row)) {
      yield node;
      remaining--;
      if (remaining === 0) return; // ← early stop once all in-scope rows yielded
    }
  }
}
```

This is O(position_of_last_in_scope_row) rather than O(source_size). Since in-scope rows were typically the first N returned during initial fetch, they tend to appear early in subsequent scans too.

#### Key Differences from Take

| Aspect                   | Take                                            | Cap                                               |
| ------------------------ | ----------------------------------------------- | ------------------------------------------------- |
| Requires ordering        | Yes                                             | No                                                |
| Bound tracking           | Yes (row comparison)                            | No (PK set membership)                            |
| Refill on remove         | Yes (ordered fetch for next row)                | Yes (unordered fetch for any row not in set)      |
| Reverse fetch            | Yes                                             | No                                                |
| `#rowHiddenFromFetch`    | Yes                                             | No (natural ordering of set updates handles this) |
| Edit handling            | Complex (6 sub-cases based on old/new vs bound) | Simple (in set → forward, else drop)              |
| State size per partition | 2 values (size + bound row)                     | size + up to N PKs                                |

### Changes to `completeOrdering`

**File**: `packages/zql/src/query/complete-ordering.ts`

In `completeOrderingInCondition`, for `correlatedSubquery` conditions, do NOT add PK ordering to the child's `orderBy`. Instead, leave it as-is (likely `undefined`).

```typescript
if (condition.type === 'correlatedSubquery') {
  return {
    ...condition,
    related: {
      ...condition.related,
      // Still recursively process nested related/where INSIDE the child,
      // but don't add ordering to the child's own orderBy
      subquery: completeOrderingInSubquery(
        condition.related.subquery,
        getPrimaryKey,
      ),
    },
  };
}
```

Where `completeOrderingInSubquery` processes nested structures but does NOT call `addPrimaryKeys` on the top-level child:

```typescript
function completeOrderingInSubquery(ast, getPrimaryKey) {
  return {
    ...ast,
    // Don't touch ast.orderBy — leave it undefined for EXISTS children
    ...(ast.related
      ? {
          related: ast.related.map(r => ({
            ...r,
            subquery: completeOrdering(r.subquery, getPrimaryKey), // nested subqueries still get full ordering
          })),
        }
      : undefined),
    ...(ast.where
      ? {
          where: completeOrderingInCondition(ast.where, getPrimaryKey),
        }
      : undefined),
  };
}
```

### Changes to `buildPipelineInternal`

**File**: `packages/zql/src/builder/builder.ts`

Add an `unordered?: boolean` parameter to `buildPipelineInternal`.

When building EXISTS child pipelines (`applyCorrelatedSubQuery` for `fromCondition=true`, lines 308-328), pass `unordered: true` to the recursive `buildPipelineInternal` call.

When `unordered` is true:

1. **Source connection**: Pass PK-only ordering to `source.connect()`. Sources need _some_ ordering for internal data structures (MemorySource uses BTreeSet). PK index always exists. But mark the connection to skip ORDER BY in SQL (see source changes below).

```typescript
const ordering = unordered
  ? source.tableSchema.primaryKey.map(k => [k, 'asc'] as const)
  : must(ast.orderBy);

const conn = source.connect(
  ordering,
  ast.where,
  splitEditKeys,
  delegate.debug,
  unordered,
);
```

2. **Use Cap instead of Take**: When `ast.limit !== undefined`:

```typescript
if (ast.limit !== undefined) {
  if (unordered) {
    const cap = new Cap(
      end,
      delegate.createStorage(capName),
      ast.limit,
      partitionKey,
    );
    // ...
  } else {
    const take = new Take(
      end,
      delegate.createStorage(takeName),
      ast.limit,
      partitionKey,
    );
    // ...
  }
}
```

For the **flipped EXISTS** path (`applyFilterWithFlips`, lines 445-472), the child `buildPipelineInternal` call should also receive `unordered: true`.

### Source Changes

#### `Source` interface (`packages/zql/src/ivm/source.ts`)

Add optional parameter to `connect()`:

```typescript
connect(
  sort: Ordering,
  filters?: Condition,
  splitEditKeys?: Set<string>,
  debug?: DebugDelegate,
  skipOrderByInSQL?: boolean,  // NEW
): SourceInput;
```

#### `TableSource` (`packages/zqlite/src/table-source.ts`)

Store `skipOrderByInSQL` on the connection. In `#requestToSQL`, when the flag is set, call `buildSelectQuery` without ordering.

#### `buildSelectQuery` (`packages/zqlite/src/query-builder.ts`)

Make `order` parameter optional. When undefined/empty, omit the `ORDER BY` clause:

```typescript
export function buildSelectQuery(
  tableName: string,
  columns: Record<string, SchemaValue>,
  constraint: Constraint | undefined,
  filters: NoSubqueryCondition | undefined,
  order: Ordering | undefined, // now optional
  reverse: boolean | undefined,
  start: Start | undefined,
) {
  // ... existing query building ...

  if (order && order.length > 0) {
    return sql`${query} ${orderByToSQL(order, !!reverse)}`;
  }
  return query;
}
```

#### `MemorySource` (`packages/zql/src/ivm/memory-source.ts`)

No behavioral changes needed. When `skipOrderByInSQL` is passed, MemorySource ignores it (it doesn't generate SQL). It still uses the PK ordering for its BTreeSet, which is already available as the primary index.

### Changes to Exists Operator

**File**: `packages/zql/src/ivm/exists.ts`

Update the comment at line 247-250. With Cap replacing Take, Cap supports early return during initial fetch (no `downstreamEarlyReturn` assert). However, since we keep limit=3, `#fetchSize` still counts up to 3 rows, which is fine.

```typescript
*#fetchExists(node: Node): Generator<'yield', boolean> {
  return (yield* this.#fetchSize(node)) > 0;
}
```

## Data Flow: Before and After

### Before (Current)

```
EXISTS child SQL: SELECT * FROM comments WHERE issueID = ? ORDER BY id ASC
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^   ^^^^^^^^^^^^^^
                  Uses issueID index                       Forces sort by PK

IVM Pipeline:     Source(comments, sort=[id,asc])
                    → Take(limit=3, partitionKey=[issueID])
                       tracks bound, does ordered refetches
```

### After (Proposed)

```
EXISTS child SQL: SELECT * FROM comments WHERE issueID = ?
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                  Uses issueID index, returns first 3 rows, no sort

IVM Pipeline:     Source(comments, sort=[id,asc], skipOrderByInSQL=true)
                    → Cap(limit=3, partitionKey=[issueID])
                       tracks PK set, no ordering assumptions
```

## Edge Cases

### 1. Remove with refill — source has more rows

- Cap has rows A, B, C (size=3). Source also has D, E.
- Remove A:
  1. Remove A from set → {B, C}, size=2
  2. Refill: fetch from source, skip B and C, find D. Add D → {B, C, D}, size=3
  3. Forward remove(A), then forward add(D)
- Exists sees: child remove (size 3→3 via refill), child add. No transition. ✓

### 2. Remove drains to 0 — source is truly empty

- Cap has row A (size=1). Source has no other matching rows.
- Remove A:
  1. Remove A from set → {}, size=0
  2. Refill: fetch from source, no rows found. No replacement.
  3. Forward remove(A)
- Exists sees relationship size go to 0, transitions EXISTS→NOT EXISTS ✓

### 3. Add when cap is full

- Cap has A, B, C (size=3)
- Add D → dropped (cap full)
- This is correct. D exists in source but isn't tracked by cap.
- If A is later removed, refill will find D (or any other non-tracked row). ✓

### 4. Remove, refill, then remove the refill

- Cap has A, B, C. Source also has D.
- Remove A → refill finds D. Cap = {B, C, D}.
- Remove D → refill searches source, skips B and C. If source has E, finds E. Cap = {B, C, E}.
- If source has no E, no refill. Cap = {B, C}, size=2. EXISTS still true. ✓

### 3. Edit that changes a non-correlation, non-PK column

- Cap has row A (id=1, issueID=5, body="old")
- Edit: A changes body to "new"
- PK (id=1) is in cap's set → forward edit ✓

### 4. Edit that changes the correlation key

- This is split into remove + add by the source (correlation key is in `splitEditKeys`)
- Cap receives remove(old) + add(new) separately
- Remove: old PK is in set → remove, forward
- Add: new partition may or may not have room → handled per-partition ✓

### 7. Cap's fetch during push processing — consistency

When Exists re-fetches during push (e.g., to check size after a child change):

- Join calls Cap's fetch with constraint matching the parent's join key
- Cap reads from source (which includes the overlay for the in-progress push)
- Cap yields only rows whose PK is in its stored set
- Consistency is maintained by the ordering of set updates vs output pushes:
  - During remove(A) processing: set = {B, C}, size=2 (A removed, replacement D not yet added)
  - During add(D) processing: set = {B, C, D}, size=3 (D now in set)
  - Downstream never sees D before being told about it

### 6. Flipped EXISTS

The child pipeline for flipped EXISTS is built the same way. Cap applies identically — FlippedJoin doesn't depend on child ordering.

### 7. Nested sub-queries (unpartitioned fetch)

When Cap has a partition key but receives a fetch without a matching constraint:

- Must iterate all source rows and check each row's partition state
- This is the same pattern as Take's `maxBound` path (take.ts lines 128-149)
- Performance: potentially slow but this case is rare (nested sub-queries only)

## Implementation Order

1. **`Cap` operator** (`packages/zql/src/ivm/cap.ts`) — new file, with tests
2. **`completeOrdering`** changes — skip EXISTS children
3. **Source interface + TableSource + query-builder** — `skipOrderByInSQL` support
4. **`builder.ts`** — wire Cap into EXISTS pipeline building
5. **`exists.ts`** — update comment
6. **Integration testing** — verify existing tests pass, add new tests

## Files to Modify

| File                                               | Change                                           |
| -------------------------------------------------- | ------------------------------------------------ |
| `packages/zql/src/ivm/cap.ts`                      | **NEW** — Cap operator                           |
| `packages/zql/src/ivm/cap.test.ts`                 | **NEW** — Cap tests                              |
| `packages/zql/src/query/complete-ordering.ts`      | Skip ordering for EXISTS children                |
| `packages/zql/src/query/complete-ordering.test.ts` | Update/add tests                                 |
| `packages/zql/src/builder/builder.ts`              | Use Cap for EXISTS children, pass unordered flag |
| `packages/zql/src/ivm/source.ts`                   | Add `skipOrderByInSQL` to `connect()`            |
| `packages/zqlite/src/table-source.ts`              | Honor `skipOrderByInSQL` flag                    |
| `packages/zqlite/src/query-builder.ts`             | Handle optional ordering                         |
| `packages/zql/src/ivm/memory-source.ts`            | Accept `skipOrderByInSQL` param (no-op)          |
| `packages/zql/src/ivm/exists.ts`                   | Update early-return comment                      |

## Verification

1. `npm --workspace=zql run test` — all IVM and builder tests
2. `npm --workspace=zqlite run test` — TableSource and query builder tests
3. `npm --workspace=zero-cache run test` — server-side pipeline tests
4. Key test files to verify:
   - `exists.push.test.ts` — EXISTS push behavior
   - `exists.flip.push.test.ts` — flipped EXISTS
   - `builder.test.ts` — pipeline construction
5. New tests:
   - Cap operator unit tests (fetch, push add/remove/edit/child, partitioned)
   - `completeOrdering` test for EXISTS children not getting ordering
6. EXPLAIN QUERY PLAN verification: EXISTS child queries should no longer show "USE TEMP B-TREE FOR ORDER BY"

## Follow-ups

- **Verify FlippedJoin ordering assumptions with unordered EXISTS children.** FlippedJoin has a hard dependency on ordering — it uses a k-way merge over parent iterators (lines 199-220) and binary search for removed child re-insertion (lines 150-156). The design removes ordering from the _child_ pipeline, and FlippedJoin depends on _parent_ ordering, so this should be safe. But trace through `applyFilterWithFlips` in builder.ts to confirm the unordered pipeline always feeds into FlippedJoin as the child input, never the parent.

- **Audit of IVM operator ordering assumptions.** Full audit of which operators assume ordered input:
  - **Hard dependency**: Take (bound tracking, `compareRows`, reverse fetches — replaced by Cap), Skip (bound + comparator — not used in EXISTS children).
  - **Soft/partial**: Join (split-push overlay optimization, ~line 274 — not correctness), FlippedJoin (hard on _parent_ ordering, child can be unordered), JoinUtils (overlay positioning), UnionFanIn (k-way merge — not used in EXISTS children).
  - **No dependency**: Filter (stateless predicate), Exists (pure counting), FanOut (pass-through), ViewApplyChange (`compareRows` only for equality), MemorySource (internal BTree, no upstream requirement).
  - **Conclusion**: The EXISTS child pipeline (Source → Filter → Cap → nested joins) is safe. Skip would break if present but EXISTS children don't use offset. Join/FlippedJoin are safe as long as unordered input is the child side.
