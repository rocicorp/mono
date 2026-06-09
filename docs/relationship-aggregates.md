# Relationship aggregates in Zero

Status: implemented (foundational feature + sync path; optimism done for the
invertible relationship functions — see Delivery plan) · Audience: Zero
engineering

## Summary

Add aggregates to ZQL — `count`, `sum`, `avg`, `min`, `max` — in two forms:
as a reduction of a **relationship** (a scalar per parent row) and as a
**top-level** reduction of a whole query (a single scalar result):

```ts
z.query.issue.related('comments', c => c.count())  // relationship: issue.comments: number
z.query.issue.where('open', true).count()          // top-level: number
```

Aggregates are incrementally maintained (IVM), available both in
client-materialized queries and in server-side SQL execution, and — the part
that needed real design — relationship aggregates can be **synced to a subscribed
client without syncing the underlying rows**, with optimistic updates for the
invertible functions.

Grouped query-level aggregates (`groupBy`) are out of scope here (Open questions).

## Motivation

UIs constantly need a scalar derived from a relationship: a comment count on a
badge, a cart total, an average rating, "last active" as `max(updatedAt)`. Today
an app must either sync the whole collection and reduce it on the client (fine
for small relationships, wasteful for large ones), or stand up a bespoke
server endpoint outside the reactive query system. Relationship aggregates make
this a first-class, reactive query whose cost can be made independent of the
collection size.

## Developer API

`count`/`sum`/`avg`/`min`/`max` are reducers on the related sub-query; the
relationship value becomes the scalar instead of a row array.

```ts
issue.related('comments', c => c.count())                       // number
issue.related('comments', c => c.sum('points'))                 // number | null
issue.related('comments', c => c.avg('score'))                  // number | null
issue.related('comments', c => c.max('createdAt'))              // <field type> | null
issue.related('comments', c => c.where('approved', true).count()) // filtered
issue.related('labels',   l => l.count())                       // junction: counts edges
issue.related('labels',   l => l.sum('weight'))                 // junction: over the destination
issue.related('labels',   l => l.where('color','red').sum('weight')) // junction + filter
```

The same reducers apply at the **top level** of a query, where the whole query
result becomes the scalar (an ungrouped aggregate over the matching rows):

```ts
z.query.issue.count()                       // number
z.query.issue.where('open', true).count()   // number
z.query.issue.sum('points')                 // number | null
z.query.issue.max('createdAt')              // <field type> | null
```

- **Result types:** `count → number`; `sum`/`avg → number | null`;
  `min`/`max → <field type> | null` (the field's own type — `max` of a string
  column is a `string`). A top-level aggregate makes the *query's* result that
  scalar; a relationship aggregate makes the *relationship field* that scalar.
- **`where`** filters which rows are aggregated (relationship or top-level).
- **`min`/`max` return the extreme *value*,** not the row that achieves it.
  "Give me the latest comment" is `argMax`, expressed as
  `related('comments', c => c.orderBy('createdAt','desc').one())`.

Grouped query-level aggregates (`groupBy(...)` → one row per group) are a
separate, future feature; see Open questions.

## Semantics

Match SQL exactly so client (IVM) and server (SQL) never disagree:

- `count(*)` of an empty group is `0`; `sum`/`avg`/`min`/`max` of an empty (or
  all-null) group is `null`.
- null field values are ignored by `sum`/`avg`/`min`/`max`.
- `avg` is `sum / count(non-null)` computed on read (never stored as a rounded
  average).

## Architecture

### The Aggregate operator (IVM)

A single reducing operator sits at the end of a related-sub-query pipeline —
where a normal `related` would emit child rows — and collapses each parent's
children into one synthetic row `{ …groupKey, value }`, where `groupKey` is the
correlation key (e.g. `issueID`). Carrying the group key lets the existing join
route changes to the right parent unchanged. Per-group state is
`{count, sum, nonNull, extreme}`, and the value is derived per function.

Two maintenance strategies share one operator:

- **Invertible** (`count`/`sum`/`avg`): add/remove/edit adjust running numbers in
  O(1); no re-read.
- **Non-invertible** (`min`/`max`): add and removal of a *non-extreme* value are
  O(1); removing or editing away the *current extreme* is not invertible, so the
  operator re-reads the group from its **local input** and recomputes. (Same
  "re-read on the boundary" shape used by bounded `limit`/`Take`.)

An update is emitted only when the derived value actually changes — which is why
`count` is untouched by an edit while `sum` is not.

The same operator serves a **top-level** aggregate with an *empty* group key: a
single global group reducing all of the query's rows, wired at the root of the
pipeline (after `where`) rather than under a join. Its one synthetic row is
projected to the query's scalar result by the view; `where` still applies,
`orderBy`/`limit`/`related` do not.

### Junction (many-to-many) aggregates

For a many-to-many relationship (`issue.labels` via the `issueLabel` junction):

- **`count`** is a shortcut — counting `labels` equals counting `issueLabel`
  edges — so it collapses to a single-hop `count` over the junction table and
  never touches the destination.
- **`sum`/`avg`/`min`/`max`** need a field on the *destination* (`label`), one
  hop past the junction. A small `LiftField` operator sits between the
  `junction → Join(destination)` pipeline and the Aggregate, projecting
  `destination.<field>` onto the junction row as a synthetic column (translating
  a destination change — which arrives as a CHILD change on the junction row —
  into an edit of that column). That turns the two-hop problem back into the flat
  single-hop the Aggregate already handles, including the `min`/`max` re-read.
  The z2s surface compiles the same as an aggregate over the junction join;
  synced read reuses the synthetic-source path (keyed by the junction's parent
  correlation). Optimism is *not* applied to junction aggregates (the field
  lives past the junction) — they're server-authoritative, like `min`/`max`.

A **`where` on the destination** is supported (e.g.
`issue.related('labels', l => l.where('color', 'red').sum('points'))`):

- For `sum`/`avg`/`min`/`max` the predicate rides along on the destination
  subquery, so it's just a `Filter` on the destination before the lift/aggregate
  — a label outside the filter contributes `null` (ignored), exactly like an
  empty group.
- For `count`, which never visits the destination, the predicate becomes an
  `EXISTS` on the junction row (keep only edges whose destination matches), so
  the count still collapses to a single hop and never materializes the
  destination.

Both work identically across materialization, z2s (the filter is an `AND` /
`EXISTS` in the generated SQL), and synced read (server-authoritative; a
filtered aggregate gets no optimism — the predicate can't be evaluated without
the destination). Bounding the destination (`limit`/`start`) and nesting it
(`related`) are not supported (`limit`/`orderBy` are rejected for any junction).

### Two execution surfaces

- **Client IVM materialization** — the operator runs in the client pipeline and
  the relationship materializes as a bare scalar.
- **Server SQL (z2s)** — compiles to a correlated scalar subquery
  (`SELECT COUNT(*) / SUM / AVG / MIN / MAX FROM (…correlated subquery…)`) for
  custom-query / server-side execution. No rows materialized.

Both consume the same AST, so the API and semantics are identical across them.

## Sync design

The interesting case: a **subscribed** aggregate query that updates reactively on
the client *without* syncing the child rows.

### What gets synced — a synthetic aggregate table

The server computes the aggregate over its replica and syncs a **synthetic
table**, one row per parent: `{ …groupKey, payload }`. Because the operator
consumes its child rows, only these synthetic rows reach the wire — the children
never sync.

| function | payload | client shows | optimistic delta |
| --- | --- | --- | --- |
| `count` | `{value}` | `value` | `±1` |
| `sum` | `{value}` | `value` | `± row[field]` |
| `avg` | `{sum, count}` | `sum / count` | adjust both |
| `min`/`max` | `{value}` | `value` | — |

### Modes

| mode | rows synced | optimistic | functions |
| --- | --- | --- | --- |
| client-computed | all child rows | ✅ | any |
| synced + invertible-delta | none (only the mutated row) | ✅ | `count`/`sum`/`avg` |
| synced opaque | none | ❌ (server round-trip) | `min`/`max` |

### Identity — `aggregate:<queryID>:<alias>`

The synthetic table name folds in the query so differently-filtered aggregates
of the same relationship never collide. `queryID` is the **client query hash**,
which both sides already share: the client computes it to subscribe; the server
stores it as the query-record id (the view-syncer passes that id straight to
`buildPipeline`, so the synthetic name matches what the client derives). It is
independent of any server-side AST transform, so client and server agree on the
name with no negotiation. Rows are already refcounted by this id, so synthetic
rows are garbage-collected with their query for free.

The separator is **`:`, not `/`** — deliberately. The client persists every
synced row under a Replicache key `e/<table>/<pk>` and recovers the table by
splitting on the first `/`; a `/` inside the table name would break that. Real
table names are SQL identifiers and never contain `:`, so there is no collision.

A **top-level** (ungrouped) aggregate has no relationship alias, so its table is
`aggregate:<queryID>` (no trailing segment) — distinct from relationship names,
which always carry a `:<alias>`. That difference is also how the client decides,
from the name alone, that it can provision the source: top-level shape is fixed.
It has no group key either, but the CVR keys every row and forbids empty keys, so
the single global row carries one synthetic constant-valued key column
(`AGGREGATE_KEY_COLUMN`); the row is `{ key, value }`. The materialized-view path
ignores the key (it projects only `value`).

### Versioning — replica state version, stamped server-side

The CVR uses a per-row version as a dirty check (skip-if-equal; emit only on a
strictly greater version). Synthetic rows have no replicated row version, so the
server stamps each with the **current replica state version** as it streams them
(the operator stays pure — it also runs on the client). State version is
monotonic, which is exactly what the dedup requires. (Deriving the version from
child row versions is wrong: it is non-monotonic under deletion and would
suppress a required update.)

### Wire

Synthetic rows ride the existing row-patch protocol as ordinary rows in a table
named `aggregate:<queryID>:<alias>`. The CVR's row store, refcounting, GC, and
persistence are table-agnostic, so no new patch type or channel is required. On
the client, the poke handler routes these rows through with identity name
mapping (the synthetic columns have no client↔server mapping).

### Optimism

- **`count`/`sum`/`avg`** are invertible, so an optimistic update needs only the
  *delta* from the row being mutated, never the rest of the collection:
  `displayed = server base ± local mutation deltas`. An insert carries its own
  row; a delete/edit needs that *one* old row (loaded, or supplied by the
  mutator), not the set. The local deltas are reconciled when the server's
  authoritative value pokes back (rebase). `avg` syncs its `{sum, count}`
  components so the ratio can absorb a delta.
- **`min`/`max`** are server-authoritative. Recomputing the new extreme after a
  boundary delete requires the other rows, which by design aren't on the client,
  so the value updates only when the server's recompute syncs back. (This is a
  fundamental property of "don't sync the rows," not an implementation gap.)

## Test coverage

The feature is exercised end to end:

- the operator for all five functions, including the `min`/`max` boundary
  re-read, add/remove/edit, and SQL-faithful null/empty handling (unit tests);
- the client query API → materialized view for `count`/`sum`/`avg`/`min`/`max`,
  `where`-filtered aggregates, **junction** (many-to-many) aggregates for all
  five functions — including a `where` on the destination (an `EXISTS` for
  `count`, a destination filter for the rest) — and **top-level** aggregates
  (`z.query.issue.count()` → a reactive scalar, with incremental updates);
- result-type inference (`count → number`, `sum`/`avg → number | null`,
  `min`/`max → field type | null`) for both relationship and top-level forms;
- z2s SQL compilation and **execution against PostgreSQL** for all five,
  including junction aggregates (filtered and unfiltered);
- the synced path: an IVM-boundary simulation (the server computes, only the
  aggregate rows cross the boundary, the client renders them while holding zero
  child rows, and an incremental change propagates as an update); the
  view-syncer pipeline-driver streaming the synthetic rows (and *not* the child
  rows); the client read path (poke routing + source provisioning, top-level and
  relationship, including a filtered junction read); and the optimistic-delta
  layer for the invertible relationship functions — including `where`-filtered
  children via a compiled per-row predicate (`crud-impl`,
  `aggregate-optimistic-delta`).

The sync-protocol integration (server streaming, client source provisioning, the
optimistic delta layer) has since landed — see chunks 8–10 of the Delivery plan
below.

## Delivery plan (reviewable chunks)

Foundational feature (each independently green):

1. ✅ **AST + protocol + format** — the `aggregate` descriptor on a correlated
   sub-query, protocol-version bump and fingerprints, the `format` marker.
2. ✅ **Aggregate operator** — the IVM operator + unit tests (all five functions).
3. ✅ **Builder + view** — pipeline wiring, the bare-scalar view projection, and a
   pipeline-level end-to-end test.
4. ✅ **Query API + types** — `count`/`sum`/`avg`/`min`/`max`, result-type
   inference, query-level tests.
5. ✅ **Top-level aggregates** — empty-group-key operator mode, root `aggregate`
   on the AST, builder root wiring, scalar result type/view, z2s scalar SQL.
6. ✅ **Server SQL (z2s)** — `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` compilation + output
   snapshots + Postgres execution tests (relationship and top-level).
7. ✅ **Server guard** — historically rejected synced aggregate queries with a
   clear error so the foundational chunks could ship before the sync path landed.
   Narrowed to *top-level* once relationship emit landed (chunk 8), then **removed
   entirely** once top-level emit landed (chunk 8b). No guard remains.

Sync path:

8. ✅ **Server emit (relationship)** — the view-syncer's pipeline-driver streams
   the Aggregate operator's synthetic rows. The synthetic schema carries
   `isAggregate: true` (`SourceSchema`); the `Streamer` keys those rows by the
   operator's own primary key (the group key) and stamps the current replica
   state version (they have no replicated `_0_version`), rather than looking the
   table up in the replica schema. `pipeline-driver.test.ts` asserts `addQuery`
   streams `aggregate:<queryID>:<alias>` rows (stamped `_0_version`) and **not**
   the child (comment) rows.
8b. ✅ **Top-level (ungrouped) sync** — `z.query.issue.count()` over the wire.
   The operator gives the single global row a synthetic constant key column (so
   it has a non-empty CVR key); the builder routes it to `aggregate:<queryID>`
   (compute mode) and, on the synced client, short-circuits to read that one row
   from the synthetic source (`aggregatesFromSource`) without building or syncing
   the underlying table. The guard is gone. Covered by `top-level-count-sync.test.ts`
   (IVM-boundary simulation) and a `pipeline-driver.test.ts` server-emit case.
9. ✅ **Client read** — top-level *and* relationship. `zero-client`:
   (a) sets `aggregatesFromSource` on `ZeroContext`; (b) routes all
   `aggregate:`-prefixed rows through the poke handler (identity mapping; key =
   the row's non-`value`/non-`_0_version` columns) instead of dropping them; and
   (c) provisions the synthetic `MemorySource` three ways, matching what each
   case can know: top-level from the name alone (fixed shape) in
   `getSource`; relationship from the AST (correlation-key columns + types) via
   the builder's `getAggregateSource` delegate hook when a query materializes;
   and, for either kind on reload (rows replayed before the query
   re-materializes), inferred from the row's own shape in `applyDiffs`. The
   operator's schema derivation is shared (`aggregateSourceSchema`) so client and
   server agree. Covered by `context.test.ts` (top-level + relationship end to
   end), `zero-poke-handler.test.ts`, and the IVM-boundary `*-count-sync.test.ts`.
   `min`/`max` ride the same path (the value just isn't optimistic — see 10).
10. 🟡 **Invertible-delta optimism** — *done for relationship `count`, `sum`,
    `avg`.* When a child row is locally inserted/deleted/updated, the CRUD
    executor (`crud-impl.ts`) writes the delta to the aggregate's synthetic
    Replicache row, so the value updates instantly: `count` ±1, `sum` ±field
    (null-aware), `avg` adjusts its components and recomputes the ratio. Because
    the write is an ordinary Replicache write inside the mutator's transaction,
    Replicache's existing rebase reconciles it for free: a pending mutation's
    delta is replayed while pending and absorbed into the base once the server's
    authoritative value pokes back — no double-count (the delta is applied
    exactly once per *pending* mutation, on top of the confirmed base). Driven by
    a child-table→aggregate registry on `IVMSourceBranch`, populated from the AST
    when an eligible query materializes (`builder.ts` → `getAggregateSource`).
    For `avg` the operator emits `{sum, count}` components alongside `value` (you
    can't move an average from the final value alone); `aggregateRowKey` and the
    reload inference treat those as payload, not key. Eligibility: **direct
    (non-junction) relationship `count`/`sum`/`avg`** — the invertible functions.
    A **`where` on the child is supported**: the builder compiles it to a per-row
    predicate (`createPredicate`) and registers it with the delta, so a child
    entering or leaving the filtered set on an insert/delete/update deltas
    correctly — a non-matching row is treated as absent, which `nextAggregateRow`
    folds into the same ±1 / ±field math. Covered by `crud-impl.test.ts` and
    `aggregate-optimistic-delta.test.ts`. **Deferred** (still correct, just
    updated on the server poke rather than optimistically): `min`/`max` (non-
    invertible); a child `where` containing a **correlated subquery** (can't be
    judged from a single child row — `transformFilters` flags it and we fall back
    to server-authoritative); **junction** aggregates (the field/predicate live
    past the junction, which an edge mutation doesn't carry); and top-level /
    filtered scalars. (Edge: optimistically deleting the last `sum` contributor
    shows `0` until the server reconciles it to `null`, since `sum` — unlike
    `avg` — doesn't carry a count.)

Junction (many-to-many) aggregates:

11. ✅ **Junction aggregates** — all five functions over a many-to-many
    relationship (see *Junction aggregates* above). `count` collapses to a
    single-hop count over the junction edges; `sum`/`avg`/`min`/`max` lift the
    destination field onto the junction row (`LiftField`) so the Aggregate stays
    a flat single-hop. A `where` on the destination is supported — a destination
    `Filter` for `sum`/`avg`/`min`/`max`, an `EXISTS` on the junction edge for
    `count`. Works across materialization, z2s (Postgres-verified), and synced
    read (server-authoritative; no optimism, since the field/predicate live past
    the junction). `LiftField` is unit-tested (incl. its CHILD path);
    materialized, z2s PG, and synced reads are covered including the filtered
    cases. Bounding (`limit`/`start`) and nesting (`related`) the destination are
    not supported (`limit`/`orderBy` are rejected for any junction).

## Open questions / future work

- **`HAVING`** — filtering the parent *by* an aggregate value
  (`issue.where(commentCount > 5)`): an `Aggregate → Filter` on the parent, plus
  an API surface.
- **`GROUP BY`** — *grouped* query-level aggregates (e.g. count of issues by
  status → one row per group). The *ungrouped* top-level case is supported; the
  grouped case adds a new result shape (the result set is the synthetic group
  rows) and a `groupBy` API.
- **`DISTINCT`** aggregates — require per-value reference counts.
- **`sum` precision** — `Math.sumPrecise` gives an exact batch recompute (e.g.
  for the seed / a precise mode); it does not make the O(1) incremental path
  exact, which is inherent to running-sum maintenance under deletion.
