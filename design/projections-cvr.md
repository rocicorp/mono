# Projection support: CVR design

**Status:** Exploratory. No implementation yet. Companion doc: `projections-client.md`.

## Problem

Zero today syncs full rows. We want to support projections (selecting a subset of columns). With projections, two queries can sync the same row but with different column views. When a query stops syncing, we should drop the columns it had contributed.

The CVR is what tracks "what the client has" so the server doesn't re-sync known rows. With projections, "what the client has" gains a second axis: which columns of that row.

## Today

`RowRecord` stores:

```ts
{
  id: RowID,
  rowVersion: string,
  refCounts: Record<queryHash, number> | null,  // null = tombstone
}
```

The `refCounts` map already enumerates which queries reference the row. `QueryRecord` carries the AST (projection lives there once supported).

**Query model.** All queries are custom queries: the client gets a typed AST view, the server runs an opaque TypeScript implementation that enforces row-level and col-level permissions internally. The legacy declarative-query + separate-permissions system is being removed. Permissions are not a separate layer in this design — they are whatever the custom query's server impl chooses to return.

**Surface API.** Selection is positional on the typed builder: `builder.issue('id', 'title')`. No-arg form (`builder.issue()`) selects all cols (full row, today's behavior).

## Proposed approach: derive column union from refCounts

Don't add per-row column tracking to the CVR. Derive the row's effective column set at write time from `refCounts`, by looking up each referencing query's projected column set:

```
row.cols  =  ⋃ { Q.projection : Q ∈ row.refCounts.keys }
```

Where `Q.projection` is the query's selected cols plus client-side join-cols and order-by cols (see "Projection = explicitly selected cols only" below).

`RowRecord` is unchanged. `QueryRecord` gains a cached `projection: Set<string>` derived from the AST (cached so we don't re-walk the AST on every row write).

### Server vs client selects

There are two distinct selects in play, and they have different jobs:

- **Server-select** (the custom query's ZQL selection): defines what the server pipeline materializes and what goes on the wire. This is what `Q.projection` refers to throughout this doc — the CVR's col-union derivation and per-row tracking are over server-select.
- **Client-select** (the typed-view selection): purely a TypeScript-API concern. Shapes the row type the user code sees. Subset of server-select.

Server-select can be wider than client-select (e.g., the server selects extra cols the client uses for IVM filter/join evaluation but doesn't expose to user code). The client's source filter (see client doc) checks for server-select cols, since those are what the local IVM pipeline operates on.

### Server pipeline ends in a Project op

The server pipeline that produces RowChanges for the CVR ends in a `Project` operator that strips cols outside the query's server-select. This is where the projection rule physically materializes — the IVM operators upstream of `Project` can reference any col the underlying ZQL touches; downstream of `Project` only server-select cols exist.

`Project` only emits an EDIT downstream when the projected col-set's values changed. Edits to non-projected cols become no-ops at the Project boundary, suppressing wire churn for irrelevant changes.

### AST representation

Selection lives at the table-reference node in the AST, alongside the table name. Nested relationships (`related('comments', c => c('id', 'body'))`) carry their own selection at the related node. The cached `Q.projection` on `QueryRecord` is therefore not a flat `Set<string>` but a tree of `(table, cols)` mirroring the AST shape — flatten to `(table, col)` pairs for the col-union derivation if a flat representation is more convenient at the row level.

### Pipeline keying

Pipelines are keyed by `(query, args)`. User identity is an arg, so per-user variations (admins see `email`, others don't) are naturally distinct pipelines — different args, different keys. No special "per-user projection" mechanism needed.

### CVR write path gains a second axis

Today the CVR detects per-row changes by comparing `rowVersion`. With projections it also compares column unions. With key-merge `put` semantics (next subsection), a single op covers any combination of value-change and widening:

| change                       | wire op                                                       |
| ---------------------------- | ------------------------------------------------------------- |
| no change                    | nothing                                                       |
| value change OR cols widened | `put` carrying changed-or-new keys (merged into existing row) |
| cols narrowed (row stays)    | `remove-keys` carrying the keys to drop                       |
| row removed entirely         | `del`                                                         |

Coalescing: a single CVR write transaction that both widens cols AND changes values produces ONE `put` carrying both.

### Put semantics: key-merge, not key-replace

Widening on the wire is expressed as a `put` containing only the new keys. The client must apply `put` as a **key-merge** — keys present in the put are added/updated, keys absent are left intact. A `put {id, c}` against existing `{id, a, b}` yields `{id, a, b, c}`, not `{id, c}`.

Widening puts must be idempotent: replaying the same put (reconnect, retry) produces the same row.

Narrowing uses a dedicated **remove-keys op**: a wire op that names the keys to drop from the row. JSON can't express "set this key to undefined" (undefined doesn't serialize), so a distinct op is required regardless. The remove-keys op is cheaper than `del + re-add` at runtime and avoids the UI-flicker risk of split transactions.

### Projection = explicitly selected cols only

A query's projection is exactly its selected cols, plus client-side join-cols (needed to evaluate joins) and order-by cols (needed to maintain order). Nothing is auto-included.

The client AST only references projected cols by construction — the typed query builder makes it impossible to reference a col you didn't select. Any server-side filtering happens inside the custom query's TypeScript implementation and is opaque to the CVR: the CVR sees only the rows the custom query returned.

This is also where row-level and column-level permissions live. There is no separate permissions layer; custom queries enforce permissions by selecting only the cols (and returning only the rows) the user is authorized to see.

### Row-set signature

Per existing design, signatures hash the pipeline's RowChange stream (identity-based). Projection does NOT enter the signature — it's applied after signature computation. This preserves cross-query dedup.

A pipeline's projection doesn't change during its life, so per-pipeline signatures are stable across the projection axis.

## Divergence axes

Three independent ways the system can disagree with itself once projections are in play. Each has a different detection story and a different mitigation:

| axis              | what diverges                                                                                                                           | failure mode                                                                                      | mitigation                                                                                                                                                                                                                                                |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Sync drift        | server's belief about the client's col-set vs the client's actual local state                                                           | silent — query mysteriously returns nothing or wrong values                                       | reliable wire delivery + server-derived col-union from query hashes is normally sufficient; no `colsHash`. Bug-class detection (server-side derivation bug, client merge bug, schema-migration race) is forfeit — surfaces as user-reported empty queries |
| Query-def drift   | custom query's server impl returns rows whose col set differs from the client's typed view                                              | **loud** — source filter rejects every row, query is empty                                        | type system catches structural mismatches at build time; runtime empty-result is easy to diagnose                                                                                                                                                         |
| Mutator-def drift | optimistic mutator computes a different result than the server's authoritative mutator (because client can't read cols server can read) | **silent** — optimistic UI flickers to authoritative result on confirm; user notices but no error | shared types between client/server mutators where possible; divergence telemetry on optimistic-vs-confirm differ                                                                                                                                          |

Only sync drift is in this design's territory. The other two are contract problems shared by all client/server-split systems. Worth being explicit because the failure-mode column is the one that matters operationally — silent failures are the worst kind, and mutator-def drift is the only one of the three that's silent AND has no built-in detection.

## Backward compatibility

The wire protocol changes (put-merge semantics, narrowing op). Existing CVRs and client stores assume full-row sync. On rollout: bump the protocol version; clients on the new protocol resync from scratch.

Pre-projection queries (no `select` clause specified) are treated as `projection = '*'` — full row, same behavior as today. Existing query records can be migrated as such with no semantic change.

## Alternatives considered

### A1. Per-row column set in the CVR

Add `cols: Set<string>` directly to `RowRecord`.

- Pro: self-contained per row; query-record drift can't make col tracking inconsistent.
- Con: storage bloat per row.
- Con: removing a query still requires walking other refs to know what's still referenced (unless paired with A2).
- **Discarded:** the `refCounts` map already gives us the index we need to derive the union. Materializing it is redundant.

### A2. Per-column refcounts on the row

`colRefCounts: Record<colName, number>` on `RowRecord`.

- Pro: column-removal-on-query-removal becomes O(cols) decrement-and-check.
- Con: heaviest storage of the three.
- **Discarded unless** profiling shows the union recompute is a hot-path cost. Falling back to this is straightforward.

### A3. Query-scoped client store (don't union at all)

Server tracks `(queryId, rowKey) → projected row`, sends per-query views, client materializes the union locally.

- Pro: cleanest model; CVR never unions.
- Con: forces client store to be `(query, row)`-keyed, which is a much larger Replicache change.
- **Discarded:** client-store change is too invasive for the value.

## Open questions

- **Cost of unioning on write.** The `refCounts` map is small in practice but worth measuring when many queries fan in to the same row.
- **Schema/projection migration.** If a query's projection set changes between versions, rows in the CVR are at the old projection. Handled by the existing query-version invalidation? Worth verifying — particularly relevant given we're forfeiting `colsHash` detection.
- **Mutator-def divergence detection.** Silent failure mode with no built-in detection (see Divergence axes). Followup: telemetry on optimistic-vs-confirm diff, or required logging in the mutator framework.

## Decided (formerly open)

- **Pipeline keying:** `(query, args)`. User identity is an arg.
- **Narrowing op:** dedicated `remove-keys` wire op (not `del + re-add`).
- **AST representation:** selection sits at the table-reference node, nested relationships carry their own selection.
- **`colsHash`:** not added. Reliable wire delivery + query-hash-keyed projection derivation is sufficient for normal operation. Bug-detection scenarios are accepted as out-of-scope.
- **Server pipeline:** ends in a `Project` op that strips non-server-select cols.
- **Two selects:** server-select drives the wire and the CVR; client-select shapes the TS API.
