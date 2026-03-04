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

```typescript
// cap.ts — subsequent fetch
for (const pk of capState.pks) {
  const constraint = deserializePKToConstraint(pk, this.#primaryKey);
  for (const inputNode of this.#input.fetch({constraint})) {
    if (inputNode === 'yield') { yield inputNode; continue; }
    yield inputNode;
  }
}
```

**Unpartitioned with partition key** (nested sub-query case): This path is hit when Cap has a partition key but the fetch constraint doesn't match it. For example:

```typescript
issue.whereExists('comments', q => q.related('author'))
```

The EXISTS child pipeline is:

```
Source(comments)
  → Cap(limit=3, partitionKey=[issueID])
  → Join(parentKey=[authorID], childKey=[id], child=Source(author))
```

When an author changes, the `comments → author` Join fetches its parent side (through Cap) with constraint `{authorID: 42}`. But Cap partitions by `issueID`, not `authorID` — the constraint doesn't match the partition key. Cap can't do PK lookups because it doesn't know which `issueID` partitions contain comments with `authorID = 42`.

Instead, Cap passes the fetch through to the source (which handles the `{authorID: 42}` constraint — so we're scanning matching comments, not the whole table). For each comment returned, Cap looks up that comment's `issueID` partition state and yields the comment only if its PK is in that partition's set.

## Push

**add**: If `size < limit`, add PK to set and forward. If full, drop — without ordering, there's no reason to swap.

**remove**: If PK not in set, drop. If PK is in set:
1. Remove PK, decrement size
2. **Refill**: fetch from input with partition constraint, find first row NOT in the PK set
3. If replacement found: add its PK, forward `remove(old)` then `add(replacement)`
4. If no replacement: forward `remove(old)` — size genuinely decreased

Refill is necessary because Cap may be tracking only N of many matching rows. Without it, removing all N tracked rows would incorrectly signal NOT EXISTS when matching rows still exist in the source.

Refill is fast: the set has at most `limit-1` entries (one just removed), so we skip at most `limit-1` rows before finding a replacement.

```typescript
// cap.ts — remove with refill
const pks = [...capState.pks];
pks.splice(pkIndex, 1);
const newSize = capState.size - 1;

// Find first row in source NOT already in our set
const pkSet = new Set(pks);
let replacement: Node | undefined;
for (const node of this.#input.fetch({constraint})) {
  if (node === 'yield') { yield node; continue; }
  const nodePK = serializePK(node.row, this.#primaryKey);
  if (!pkSet.has(nodePK)) {
    replacement = node;
    break;
  }
}

if (replacement) {
  // Store state WITHOUT replacement — hide it during remove forward
  this.#storage.set(capStateKey, {size: newSize, pks});
  yield* this.#output.push(change, this);           // forward remove
  // Now reveal replacement
  pks.push(serializePK(replacement.row, this.#primaryKey));
  this.#storage.set(capStateKey, {size: newSize + 1, pks});
  yield* this.#output.push({type: 'add', node: replacement}, this);
} else {
  this.#storage.set(capStateKey, {size: newSize, pks});
  yield* this.#output.push(change, this);           // forward remove
}
```

**edit**: If old PK is in set, update the PK if it changed and forward. Otherwise drop. (Edits that change the correlation key are split into remove+add by the source via `splitEditKeys`.)

**child**: If PK is in set, forward. Otherwise drop.

## Why This Is Safe

The ordering invariant in the pipeline exists so that operators can make decisions based on row position within a sorted window. Cap makes no positional decisions — only set membership checks. The operators downstream of Cap in an EXISTS pipeline (Join, Exists) don't depend on child ordering either:

- **Exists** counts child rows — order irrelevant
- **Join/FlippedJoin** depend on *parent* ordering, not child ordering

Within Cap's subgraph (the EXISTS child pipeline), no operator depends on Cap's output ordering. Cap feeds into a Join as the child input, and from there changes flow as opaque `child` type changes on the parent pipeline. Operators with hard ordering dependencies (Take, Skip) may exist on the parent side but they operate on parent ordering, not child ordering.

## Consistency During Push

The core invariant is: **fetch yields exactly the rows whose PKs are in the stored set**. The PK set is the single source of truth and is always updated atomically before forwarding to output. Any mid-push re-fetch by downstream sees a complete, self-consistent snapshot.

Walk through each push type:

- **add**: PK added to set, then forwarded. Mid-push re-fetch sees the new row — correct, the add is in flight.
- **remove (no refill)**: PK removed from set, then forwarded. Mid-push re-fetch does not see the removed row — correct.
- **edit**: PK updated in set (if changed), then forwarded. Mid-push re-fetch sees the new PK — correct.
- **child**: No state change. Forwarded if PK in set.
- **remove (with refill)**: Old PK removed from set, state stored *without* replacement, remove forwarded. Then replacement PK added to set, state updated, add forwarded. This matches Take's convention: during the remove forward, a mid-push re-fetch sees only the remaining rows (e.g., `{B, C}`), not the replacement. The replacement becomes visible only when its add is forwarded.

No `#rowHiddenFromFetch` field is needed. Take uses one because its fetch scans by bound — the replacement row is in the source and would appear in a bound-based scan, so it must be explicitly skipped. Cap's fetch does PK-based lookups from the stored set, so simply deferring the storage update is equivalent to hiding — if the replacement PK isn't in storage, fetch won't look it up.

```typescript
// Take hides via a field checked during fetch:
this.#rowHiddenFromFetch = row;
yield* this.#output.push(removeChange, this);  // fetch skips the hidden row
this.#rowHiddenFromFetch = undefined;

// Cap hides by deferring the storage update:
this.#storage.set(capStateKey, {size: newSize, pks});       // replacement NOT in pks
yield* this.#output.push(change, this);                     // fetch won't look up replacement
pks.push(replacementPK);
this.#storage.set(capStateKey, {size: newSize + 1, pks});   // replacement now in pks
yield* this.#output.push({type: 'add', node: replacement}, this);
```

## Known Limitation: Unpartitioned Fetch Fan-Out

The unpartitioned fetch path (see Fetch section above) scans all source rows matching the fetch constraint and filters by PK set membership. This is problematic when the constraint has high fan-out — e.g., a bot that has commented on every issue. When that bot's author row changes, the `{authorID: botId}` scan touches millions of comment rows just to find the handful in Cap's tracked sets.

In practice, the pipeline timeout mechanism handles this: if a push exceeds the time budget, the pipeline is destroyed and re-hydrated from scratch.
