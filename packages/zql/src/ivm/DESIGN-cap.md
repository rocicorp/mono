# Cap Operator

## Motivation

EXISTS subqueries only need to know whether matching rows exist, not their order. Yet the standard pipeline uses Take, which requires ordered input — forcing `ORDER BY` in SQL. On large tables this is expensive: SQLite must sort matching rows even when a covering index on the correlation key could return results immediately.

Cap replaces Take in EXISTS child pipelines, removing the ordering requirement and the `ORDER BY` from generated SQL.

## Why Take Needs Ordering (and Cap Doesn't)

Take maintains a **bound** — the last row in sort order — to decide whether incoming rows belong in the window. On remove, it fetches the next row *in sort order* as a replacement. All of this requires a total ordering.

Cap doesn't track a bound. It tracks a **set of primary keys** of the rows currently in scope. Since EXISTS only cares about count, any N matching rows are equally valid — there's no "better" row to prefer. This eliminates the need for ordering entirely.

## State

Per partition (keyed the same way as Take):

```typescript
type CapState = {
  size: number;
  pks: string[];  // JSON-serialized PKs of in-scope rows
};
```

With EXISTS limit=3, each partition stores at most 3 PKs.

## Fetch

**Initial fetch** (no state): Read up to `limit` rows from input, store their PKs, yield each.

**Subsequent fetch** (has state): Issue one PK-constrained point lookup per tracked row. Each fetch uses the deserialized PK as the constraint, so the source does an index seek rather than scanning the partition. With `limit=3`, this is 3 fast lookups regardless of partition size.

**Unpartitioned with partition key** (nested sub-query case): Iterate all input rows, look up each row's partition state, yield if PK is in that partition's set. Same pattern as Take's maxBound path. PK-based lookups aren't possible here because we don't know which partitions to query.

## Push

**add**: If `size < limit`, add PK to set and forward. If full, drop — without ordering, there's no reason to swap.

**remove**: If PK not in set, drop. If PK is in set:
1. Remove PK, decrement size
2. **Refill**: fetch from input with partition constraint, find first row NOT in the PK set
3. If replacement found: add its PK, forward `remove(old)` then `add(replacement)`
4. If no replacement: forward `remove(old)` — size genuinely decreased

Refill is necessary because Cap may be tracking only N of many matching rows. Without it, removing all N tracked rows would incorrectly signal NOT EXISTS when matching rows still exist in the source.

Refill is fast: the set has at most `limit-1` entries (one just removed), so we skip at most `limit-1` rows before finding a replacement.

**edit**: If old PK is in set, update the PK if it changed and forward. Otherwise drop. (Edits that change the correlation key are split into remove+add by the source via `splitEditKeys`.)

**child**: If PK is in set, forward. Otherwise drop.

## Why This Is Safe

The ordering invariant in the pipeline exists so that operators can make decisions based on row position within a sorted window. Cap makes no positional decisions — only set membership checks. The operators downstream of Cap in an EXISTS pipeline (Join, Exists) don't depend on child ordering either:

- **Exists** counts child rows — order irrelevant
- **Join/FlippedJoin** depend on *parent* ordering, not child ordering

The only operators with hard ordering dependencies (Take, Skip) are not present downstream of Cap.

## Consistency During Push

The core invariant is: **fetch yields exactly the rows whose PKs are in the stored set**. The PK set is the single source of truth and is always updated atomically before forwarding to output. Any mid-push re-fetch by downstream sees a complete, self-consistent snapshot.

Walk through each push type:

- **add**: PK added to set, then forwarded. Mid-push re-fetch sees the new row — correct, the add is in flight.
- **remove (no refill)**: PK removed from set, then forwarded. Mid-push re-fetch does not see the removed row — correct.
- **edit**: PK updated in set (if changed), then forwarded. Mid-push re-fetch sees the new PK — correct.
- **child**: No state change. Forwarded if PK in set.
- **remove (with refill)**: Old PK removed, replacement PK added to set, state stored — *then* remove forwarded, *then* add forwarded. If downstream re-fetches after receiving the remove but before receiving the add, it sees the replacement "early". This is safe because EXISTS only counts: the count reflects the final state (e.g., size stays at 3 through a remove+refill), avoiding a spurious intermediate size drop.

No `#rowHiddenFromFetch` mechanism is needed. Take needs it because downstream operators track row *positions* within a sorted window — seeing a replacement at the wrong position before it's announced would corrupt their state. Cap has no position concept; downstream only counts rows, so early visibility of the replacement is harmless.
