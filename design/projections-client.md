# Projection support: client design

**Status:** Exploratory. No implementation yet. Companion doc: `projections-cvr.md`.

## Problem

With projections, the client's row store may hold a row with only some columns. A query that needs columns the row doesn't have must not return that row locally — partial rows would render with `undefined` fields and confuse the user.

We need a way to say: "this query needs cols `{a,b,c}`; only return rows that have all three."

## Proposed approach: source-level "has all needed keys" filter

Each query's IVM pipeline wraps its source-scan with a predicate:

```
∀ col ∈ Q.required-cols : col ∈ row's own keys
```

Where `Q.required-cols` is the **server-select** for `Q` (the cols the server's pipeline materializes and sends on the wire) — see CVR doc, "Server vs client selects." The client-select (the typed-view's row shape) is purely TS-API shaping and is a subset of server-select; it doesn't enter the source-filter predicate.

The client AST only references server-selected cols by construction — anything reachable from the typed query builder is in server-select. Server-side filtering inside the custom query's TS impl is opaque to the client; the client just receives the rows the server emitted and reactively responds.

Rows that fail the predicate are invisible to this query. When the server `put`s a row to add a missing key (a "widening" merge — see CVR doc for `put` semantics), the predicate flips false → true and the source emits ADD downstream. Symmetrically, narrowing flips true → false → REMOVE.

This is just `Filter`, reusing existing IVM machinery. The IVM layer never has to know about projection — it sees rows that satisfy a filter, just like always.

**Optimization:** when no projected query is registered (every query's projection is `'*'`), the filter is trivially true and can be elided. Pre-projection workloads pay zero overhead.

## Prerequisites

### P1. Distinguish "key absent" from "key present and null"

Today both look like `row.col === undefined` in JS. The filter must use `'col' in row`, not value comparison.

This means:

1. **Wire protocol.** Server serializes `{id:1}` vs `{id:1,col:null}` distinctly. JSON does this naturally — but we must audit any normalization that omits `undefined` or coerces `omit ↔ null`.
2. **Storage.** IndexedDB structured-clones objects, preserving key presence end-to-end. Should be fine; worth verifying.
3. **EDIT diff representation.** Need an op meaning "this key was removed" (for narrowing) distinct from "set to null."

### P2. Narrowing op (or escape hatch)

Two options for representing narrowing on the wire:

- **P2a. Extend the EDIT op** to carry "removed keys."
- **P2b. Model narrowing as `del + re-add at smaller projection`**, since narrowing is rare (last query referencing a col goes away).

P2b is the pragmatic escape hatch and avoids a protocol change.

### P3. Mutator API change

Optimistic mutators today do full-row writes (`tx.set(key, row)`). That's wrong with projection: it would either clobber other queries' columns, or write `undefined`-keyed rows that corrupt the presence semantics.

We need a `tx.patch(key, partialRow)` (or similarly-shaped API) that merges keys without dropping unspecified ones. This is a real client-API change.

## What this gives us

- **No per-row metadata.** The "what cols does this row have" question is answered by `'col' in row`.
- **Reuses existing IVM filter machinery.** No new operator types; widening/narrowing are just predicate flips that emit ADD/REMOVE.
- **Local-first behavior preserved.** A new query whose required cols happen to be present on local rows returns immediately. Rows that need widening pop in as the server sends widening puts — same timing model as today's "pending hydration."

## Divergence axes

See the CVR doc's `Divergence axes` section for the full table. Client-side angles on each:

- **Sync drift** (silent). The presence-of-key model is robust to drift IF widening puts and narrowing ops are reliably delivered. A dropped widening put leaves the client unable to evaluate queries that need the missing col, with no easy recovery short of resync. A `colsHash` on the wire would let the client detect divergence and request repair.
- **Query-def drift** (loud). If the custom query's server impl returns rows whose col set differs from the client's typed view, the source filter rejects every row and the query appears empty. Easy to diagnose; type system catches the structural case at build time.
- **Mutator-def drift** (silent, no built-in detection). Optimistic mutations against partial rows can compute different results than the server's authoritative mutator. Patch-style `tx.update` limits the surface area but doesn't eliminate it when the mutator's logic depends on values the client can't see. This is the only divergence axis with no automatic detection — divergence telemetry on the optimistic-vs-confirm diff is the realistic mitigation.

## Alternatives considered

### B1. Track column metadata per row

Each row in the client store carries `{cols: Set<string>}`. Local query filters `row` if `query.required-cols ⊄ row.cols`.

- Pro: self-describing rows; explicit.
- Con: per-row metadata in IndexedDB, even for the common case where most rows have the same column set.
- **Discarded:** the source-level filter solves the same problem with no extra storage, by reusing the IVM Filter machinery. The "cols" set is already implicit in the row's key presence.

### B2. Refuse to serve projected queries locally until server hydrates

A projected query must round-trip to the server before returning anything client-side.

- Pro: trivially correct.
- Con: defeats local-first behavior, which is most of why people use Zero.
- **Discarded.**

### B3. Derive "what cols this row should have" from subscribed queries

Client knows its subscriptions and their projections; compute the expected col set per row from "queries that contain this row."

- Pro: no per-row metadata; symmetric to server-side approach.
- Con: requires knowing which rows each query contains, which is itself the IVM result we're trying to compute. Circular.
- **Discarded.**

## Open questions

- **Mutator API shape.** What does `patch` look like in user code? Auto-detect partial vs full from row shape, or a separate API? (Zero's existing `tx.update` is already patch-style — may need only a doc clarification, not new API.)
- **Audit of `undefined`-erasure paths.** Where in the wire/storage stack do we currently normalize `undefined` → omit or omit → `null`? Each is a place that could break key-presence semantics.
- **Join-col leakage.** Join cols are required for client-side join evaluation; if they aren't user-selected, do they leak into user-visible row shape? Probably trim before exposing.
- **Initial-hydration UX for projected queries.** A client offline when a new query is added needs cols that aren't local. Query is empty until reconnect. Real UX regression for offline-first; worth taking a position.
- **Interaction with the row-set signature.** Confirm that the client-side row signature (if any) treats projection the same way the server does — identity-based, projection applied after.
