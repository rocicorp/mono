# 005: Query families

- **Status:** Proposed
- **Date:** 2026-09-22
- **Packages:** `zero-protocol` (ast), `zql` (builder, take, join,
  connection-index), `zqlite` (table-source), `zero-cache` (pipeline-driver,
  view-syncer, workers/syncer)
- **Builds on:** [004: Cross-client-group query dedupe](004_cross_client_group_query_dedupe.md),
  [003: Per-pipeline reset](003_per_pipeline_reset.md),
  [001: Correlated predicate pushdown](001_correlated_predicate_pushdown.md),
  and Rindle design 310, "Parameterized query families"
  (`designs/310-PARAMETERIZED-QUERY-FAMILIES-DESIGN.md` in the Rindle repo)

## Summary

004 shares a pipeline between client groups only when they run the same
transformed query, literals included. Its first risk says why that may not
be enough: a query filtered by user never dedups across users. The common
shape in a real app is one standing query per user. The queries have the
same template and differ only in a literal.

A query family groups queries that are identical except for the literals of
root-level equality conditions. A family runs as one pipeline, partitioned by
the columns of those conditions. Each client group subscribes to one
partition, which is called a binding. Per write, the work is one push into
the family plus the work for the bindings the write affects. It does not
grow with the number of bindings. Rindle shipped this design and measured a
flat curve: on its worst shape, write throughput at 2,048 bindings went from
54 to 2,388 writes/s, 97% of the one-binding rate.

The engine half is small in Zero. Zero already builds every `.related()`
subquery as a pipeline partitioned by its correlation columns. A family root
is the same build, one level up, with the holed columns as the partition
key. The builder, `Take`, and `Join` already handle that case. The new
pieces are a membership gate, bind, unbind, and routing by binding.

The cross-group half is not small, and it is 004. Every client group has its
own snapshot, sources, and pipelines, so there is nothing shared to put a
family into. Families come after 004 step 2. After that, they replace the
registry key of 004 step 3. Exact dedup becomes a special case, where two
subscribers have the same binding.

The gate is data. Step F0 adds the family key to the telemetry that 004
already reads, and reports how much families would group on real
workloads.

## Problem

### One standing query per user

These queries run once per client group, and each client group supplies a
different literal:

| Query                        | Shape                                                 | Literal per group   |
| ---------------------------- | ----------------------------------------------------- | ------------------- |
| zbugs `userPref(key)`        | `userPref.where('key', k).where('userID', sub).one()` | `sub`               |
| zbugs `user(id)`             | `user.where('id', id).one()`                          | `id`                |
| zbugs `comment(id)`          | `comment.where('id', id).related(...).one()`          | `id`                |
| zbugs `emojiChange(subject)` | `emoji.where('subjectID', s).related('creator')`      | `s`                 |
| zbugs `commentsPage`, page 1 | `comment.where('issueID', i).related(...).limit(n)`   | `i`                 |
| internal `lmids`             | `clients.where('clientGroupID', cg)`                  | the client group ID |
| internal `mutationResults`   | `mutations.where('clientGroupID', cg)`                | the client group ID |

Under 004's key, `(clientSchemaKey, transformationHash)`, each row of this
table is one pipeline per distinct literal. On a worker with V client
groups, that is up to V pipelines per template, and every one of them sees
every write to its tables.

Per write, for V such pipelines on one worker:

| Write                                                      | Today                                                                                                   | After 004                                              | After families                                                                                 |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ | ---------------------------------------------------------------------------------------------- |
| Root table, e.g. a `clients` row                           | V drivers each scan the diff and check the row against their own connections                            | One diff scan, then V connections on the shared source | One push, one membership check                                                                 |
| Child of a `.related()`, e.g. an emoji under `comment(id)` | V join child pushes, each with a constrained parent fetch unless 001 pushdown filtered it at the source | One diff scan, then V join child pushes                | One push, one lookup in the join's partition map, and parent fetches only for bound partitions |

The internal queries are the extreme case. Every mutation from every client
writes a `clients` row and a `mutations` row. Under 004 those are V
pipelines with one subscriber each. Under families they are two pipelines
per worker.

### Why exact dedup does not cover it

`transformAndHashQuery` binds auth data into the AST as literals, and a
custom-query transform usually adds the user. Two users' copies of
`userPref('theme')` hash differently, so 004 never shares them. Families
share them whenever the per-user part is a root-level equality. They do not
help when the per-user part is anywhere else. See
[What does not group](#what-does-not-group).

## Prior art: Rindle 310

Rindle is a Rust IVM engine with the same operator model as ZQL. Its design
310 shipped on 2026-09-02, behind a runtime flag that defaults to on.

- **Identity.** `FamilyKey` is the canonical AST with a hole in place of
  every eligible literal. The extracted literal tuple is the binding. Two
  queries are family-mates when their keys are equal.
- **Eligibility.** A hole goes only at the right-hand side of a root-level
  `=` conjunct, with a column on the left and a scalar, non-null literal on
  the right. Every eligible conjunct gets a hole. The engine does not guess
  which literal is "the parameter".
- **Engine.** One pipeline per family. The root connection filters by
  membership in the bound set instead of the holed conditions. The root
  `Take` partitions by the holed columns. An edit that changes a holed
  column splits into a remove and an add. No other operator needs to know
  about partitions, because root partitions are disjoint.
- **Lifecycle.** Bind does a fetch constrained to the new partition. Unbind
  drains the partition's rows through the pipeline as synthetic removes, so
  every operator runs its normal cleanup, then evicts the root `Take`
  partition.
- **Delivery.** The drain reads the root row of each change to find its
  binding and routes it to that binding's subscribers. Each subscriber gets
  exactly the frames the standalone query would have produced.
- **Formation.** A family forms at its first member. Rindle first built
  "promote a singleton to a family and migrate its subscribers", then
  removed it before merge. A family of one costs a membership test and a
  partitioned `Take`. The migration path was the least-tested code in the
  design.

Rindle's benchmark on chinook, one album query per artist with tracks
nested, writes/s:

| Bindings | Singletons | Family |
| -------- | ---------- | ------ |
| 1        | 2,428      | 2,461  |
| 128      | 740        | 2,361  |
| 512      | 221        | 2,240  |
| 2,048    | 54         | 2,388  |

These are Rindle numbers, not Zero numbers. The shape of the curve is the
point. Families cost nothing on shapes that were already flat and flatten
every shape that bent.

What Rindle had that Zero does not: one engine per process. Its V singletons
already shared per-table sources, so a family only merged V connections into
one. Zero's client groups share nothing below the row cache.

## Goals and non-goals

Goals:

- One pipeline per `(clientSchemaKey, familyKey)` per sync worker, where
  per-write work for eligible shapes does not grow with the number of
  bindings.
- Per-binding equivalence. Each subscriber receives exactly the row changes,
  and so the per-query occurrence counts, that the standalone pipeline for
  its concrete query would have produced. The CVR, the protocol, and the
  client do not change.
- A query that is not eligible runs exactly as it would under 004. It is a
  family with no holes.
- Ship in flagged steps, with a kill switch.

Non-goals:

- Holes inside subqueries. See [Future work](#future-work).
- Holes on anything but `=` with a root column on the left: no `IN`, no
  ranges, nothing under `OR` or `NOT`.
- Cross-worker sharing. Same as 004.
- Families in `zero-client`. The client runs many queries from one template
  too, for example a `useQuery` per list item. That is a separate design.
- Query covering. `query-covering.ts` shadow-logs containment within a
  client group. That is a different form of sharing.

## Design

### Family identity

The key is computed from the transformed AST, the same AST 004 hashes, and
before scalar subqueries are resolved.

1. **Normalize** with `normalizeAST`. It flattens and sorts conjuncts, so
   conjunct order does not split families. Rindle has no normalization and
   lives with that.
2. **Walk the root `where`**: a single `simple` condition, or the direct
   `simple` children of a top-level `and`.
3. **Hole a conjunct** when all of these are true:
   - `op` is `=`.
   - `left` is a column and `right` is a literal.
   - The literal is not `null` and not an array.
   - No other eligible conjunct has the same column.
4. **Key** = hash of `clientSchemaKey` plus the normalized AST with each
   holed `right` replaced by `{hole: <column>}`.
5. **Binding** = the holed literals, in normalized order.

The same-column rule exists because `cmpCondition`
(`packages/zero-protocol/src/ast.ts:689`) sorts simple conditions by column,
then operator, then literal. For distinct columns, the order of the holes
does not depend on the literals. For two `=` conjuncts on one column, it
does. That case is a contradiction or a duplicate, so excluding it costs
nothing.

A query with no holes is still a family: its key is its normalized AST and
it has exactly one binding, the empty tuple. The registry treats all
queries the same way.

The binding key must match the value equality that the `=` predicate, the
`Take` partition, and the `Join` partition map use. The binding key uses
`canonicalKey` (`packages/zql/src/ivm/join-utils.ts:272`), which the `Join`
partition map already uses. A property test checks that two literals give
the same binding exactly when the `=` predicate treats them as equal.

The extraction is pure AST work. It goes beside `normalizeAST` in
`zero-protocol`, because both `zql` and `zero-cache` need it.

### Eligibility

A query is excluded from families in v1, and runs as a family with no
holes, when:

- It has `start`. A paging cursor is per-subscriber resume state, not a
  parameter.
- It has a scalar subquery. `#resolveScalarSubqueries` bakes the resolved
  value into the AST and keeps a companion pipeline that resets the whole
  driver when that value changes. See [Future work](#future-work).

Everything outside the holes is part of the key: subquery literals, the
`related` structure, `orderBy`, `limit`, `one`, and permission conditions.
A difference anywhere else is a different family.

### The family pipeline

`buildFamilyPipeline` sits beside `buildPipeline` in
`packages/zql/src/builder/builder.ts`.

```text
buildFamilyPipeline(concreteAst, holes, bindings, delegate, costModel)
  ast = completeOrdering(concreteAst)
  ast = pushDownCorrelatedPredicates(ast, excluding facts from holes)
  ast = planQuery(ast, costModel)            with the holed conjuncts present
                                             (pushdown and planning run in
                                             the order buildPipeline's flags pick)
  stripped = remove holed conjuncts from ast.where
  return buildPipelineInternal(stripped, ..., partitionKey = holeColumns)
         with a membership gate on the root connection
```

**`partitionKey` at the root.** `applyCorrelatedSubQuery` already calls
`buildPipelineInternal` with `partitionKey = correlation.childField` for
every subquery. A family root is the child side of a `.related()` join
whose parent is the set of bindings. Passing the holed columns as the root
`partitionKey` turns on machinery that already exists:

| Rindle 310 requirement                    | Existing Zero code                                                                                                                                                                                                        |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Split an edit that changes a holed column | `splitEditKeys` starts from `partitionKey` (`builder.ts:320`). The source splits the edit into a remove and an add.                                                                                                       |
| Partitioned root `Take`                   | `new Take(..., partitionKey)` (`builder.ts:428`)                                                                                                                                                                          |
| Every root fetch is constrained           | `Take.fetch` asserts that the constraint contains the partition key (`take.ts:112`)                                                                                                                                       |
| A child write fetches only bound parents  | `.related()` joins get `parentPartitionKey`. `Join.#partitionMap` maps a child join key to the parent partitions that hold it and fetches each one constrained (`join.ts:255`). An unmapped key returns before any fetch. |
| Take bound for a partition                | `TakeGate` and `Join.#processParentNode` pass the partition constraint to `getBound`                                                                                                                                      |
| Flipped EXISTS under a partition          | `applyFilterWithFlips(..., parentPartitionKey)`                                                                                                                                                                           |
| Hydrate one partition                     | `top.fetch({constraint: binding})` reaches `Take.#initialFetch` for that partition                                                                                                                                        |

Rindle's decision D4 does not apply. Rindle's partitioned `Take` deleted a
partition's state when it drained to empty, so a later add to a bound
partition was dropped. Zero's `Take` never deletes state.

**The membership gate.** The root connection loses the holed conditions, so
without a gate the pipeline would process writes for every value of the
holed columns. The gate is a filter directly above the root connection. It
passes a row when `canonicalKey(row, holeColumns)` is in the bound set.
Fetches for a binding are constrained to that binding, so they pass. A push
for an unbound value stops at the gate.

The root connection also registers a dynamic constraint in
`ConnectionIndex` (`packages/zql/src/ivm/connection-index.ts`):
`holeColumns[0] IN <bound values>`. Today the index only answers "can any
connection accept this row?", from constraints fixed at connect time. It
gains `addValue` and `removeValue`. When no other connection on the table
accepts the row, `TableSource.genPush` then skips the write entirely.

The bound set and the index change only between pushes, under the same
mutual exclusion that 004 uses to keep hydration away from the shared
advance. On bind, add to the index, then to the set. On unbind, remove from
the set, then from the index. At every instant the index accepts a superset
of what the gate accepts. That is Rindle's ordering rule, and the index is
allowed to be conservative.

**No unconstrained root fetch.** For a family with `limit`, the `Take`
assertion already enforces this. For a family without `limit`, the gate's
`fetch` gets the same assertion. `hydrateInternal`, which fetches with an
empty request, is never called on a family.

### The planner

Plan the concrete AST of the first binding, then remove the holes. Do not
plan the stripped template. Without the holed conjuncts, the root looks
unfiltered, and the planner will flip joins to drive from the subqueries.

One plan for all bindings is correct in the sense that matters: every
binding gets the plan it would have gotten on its own.
`createSQLiteCostModel` inlines the literal into the SQL and asks SQLite for
its row estimate. The replica has no `sqlite_stat4`, so SQLite estimates
`col = literal` from `sqlite_stat1`, which gives the average rows per
distinct value and does not depend on the literal. The join fanout
estimate also comes from stat1. If the replica ever collects stat4,
per-literal plans could differ, and a family would pin the first binding's
plan. That trade-off would then be explicit. It is not a correctness issue.

### 001 pushdown and holes

001 copies a root `=` on a correlation column into the child subquery. With
a hole on that column, the copy puts the binding's literal inside a
subquery. The template then depends on the binding, and the pipeline is
wrong for every other binding.

v1 excludes facts that come from holes from the pushdown. The family key is
computed before `buildPipeline` runs the pushdown, so the key does not
change. What is lost is the push-time filtering 001 added. What replaces it:

- For `.related()`, `Join.#partitionMap` rejects a child write whose join
  key has no bound parent before any fetch.
- For EXISTS, the child write fetches its parent constrained on the
  correlation. The root gate rejects unbound parents.

A later step can push a hole down as a membership gate instead of a
literal. The child connection gets a gate over the renamed column, fed by
the projection of the bound set onto that column. This is sound for the
same reason 001 is: every child row that joins a bound parent has a bound
value in that column. The gate may accept extra rows, and the join removes
them.

### Bind, late join, and unbind

**Bind** a new binding on a family instance:

```text
under the advancer's exclusion, at shared version V
  connectionIndex.addValue(binding); bindings.add(binding)
  rows = Streamer(top.fetch({constraint: binding}))    rows with occurrence counts
  register the subscriber as pending at V               004's join protocol
```

The fetch creates the partition's `Take` state and indexes its parents in
each `Join`. Its output through the `Streamer` is the binding's footprint in
exactly 004's sense: rows, with occurrence counts per query. The subscriber
reconciles its CVR against it through 004's group pass.

**Late join** on a binding that is already bound: the same constrained
fetch, without the two `add` calls. It reads existing state and writes
nothing. `Join` re-indexes parents during a fetch (`join.ts:142`), and that
is idempotent.

**Unbind** when the binding's last subscriber leaves, after 004's TTL
handling. This is Rindle's decision D5:

```text
under the advancer's exclusion
  bindings.remove(binding)
  roots = top.fetch({constraint: binding}), root rows only
  for each root row: push a REMOVE at the gate's output, discard what comes out
  take.evictPartition(binding)             new; only when the family has a limit
  connectionIndex.removeValue(binding)
```

The removes enter below the gate, so the gate cannot reject them. `Take`
tries to refill after each remove, and the refill fetch goes through the
gate, which now rejects the partition. The partition drains to size 0.
`Join` unindexes each removed parent. `evictPartition` then deletes the
empty state. The output is discarded, because nobody subscribes to the
binding anymore.

**Child partition state is a leak that already exists.** A `.related()`
subquery with a `limit`, for example
`album.related('tracks', t => t.limit(10))`, keeps `Take` state per
correlation value. Zero never evicts it when the last parent with that
value leaves. The orphaned state stays in storage, and every push to that
correlation value keeps updating it. Today this is bounded by the lifetime
of one client group's query. A family lives as long as any of its bindings,
so the orphans pile up. Rindle evicts child partitions when a parent is
removed, with a guard for parents that share a correlation value. Zero
should do the same. Until then, the instance rebuild under
[Instances](#instances-and-the-hydration-stall) bounds the leak.

### Routing

Every change at the top of a pipeline carries a root row. `ADD`, `REMOVE`,
and `EDIT` carry it in their node. `CHILD` carries the parent node. An
`EDIT` never crosses a partition, because the source splits those edits. So
the binding is a column read:

```diff
 Streamer.#streamChanges(key, schema, changes)
   for each change
+    binding = canonicalKey(change.node.row, holeColumns)   top level only
     ... unchanged recursion into relationships ...
-    yield RowChange{queryID, table, rowKey, row}
+    yield RowChange{pipelineKey, binding, table, rowKey, row}
```

004's envelope becomes `(pipelineKey, binding)`, and the fan-out after the
`Streamer` looks up the subscribers of that pair:

```text
subscribers[(pipelineKey, binding)] = [(clientGroupID, queryID), ...]
```

Two query IDs in one client group can map to the same binding, just as 004
allows two query IDs to share a transformation hash.

### Registry: families in 004's design

Families change 004 step 3 and step 4. Steps 1 and 2 are unchanged.

| 004 concept                                 | With families                                                                           |
| ------------------------------------------- | --------------------------------------------------------------------------------------- |
| Key `(clientSchemaKey, transformationHash)` | Key `(clientSchemaKey, familyKey)`, plus a binding per subscriber                       |
| Subscribers per pipeline                    | Subscribers per `(instance, binding)`                                                   |
| Footprint per pipeline                      | Footprint per binding, from the constrained fetch above                                 |
| Hydration on a leased connection            | New instances only. See the next section.                                               |
| Diff buffer per client group                | Unchanged. It receives only the changes for that group's bindings.                      |
| Group version barrier                       | Unchanged. A bind is a join pinned at a version.                                        |
| Internal queries, one subscriber each       | `lmids` and `mutationResults` are two families per worker, one binding per client group |

The group pass in 004 skips a client group with an empty buffer. With
families, a write reaches only the groups whose bindings it touches. That is
where families pay off in Zero: CVR work goes from every group on the worker
to the groups the write affects. 004 measured CVR flush at about the same
cost as advance.

### Instances and the hydration stall

A bind writes into shared operator state, so it must run between advances,
under the exclusion. Today a slow hydration only delays its own client
group. On a shared worker, a slow inline bind delays every client group on
the worker. Rindle accepts this because its binds take microseconds to
milliseconds. Zero's hydrations can take seconds.

Hydrating a new partition while the shared advance runs is not safe.
Partitions are disjoint at the root, but not below it. Two root rows in
different partitions can share a correlation value, so they share the child
`Take` state for that value and an entry in the `Join` partition map. A
concurrent hydration would race the advance on that state.

So a family on a worker has one or more **instances**. Each instance is a
pipeline over a disjoint subset of the family's bindings.

- **The first binding** creates the family's first instance through 004's
  path: build on a leased connection at V0, hydrate, replay the change log
  to the shared head, and register. That is exactly what a new pipeline
  costs under 004. Rindle measured a family of one at the same cost as a
  singleton.
- **A later binding goes inline** into the primary instance under the
  exclusion when the family's own bind times say it is cheap. Every bind is
  timed. When the family's p95 bind time is under a budget, new bindings go
  inline. This is measured, not estimated by the planner.
- **Otherwise the binding gets a new instance** through the leased path,
  like the first binding.

Per-write cost is proportional to the number of instances, not the number
of bindings. An inline bind cannot be aborted halfway, because partial
state would be left in shared operators. If an inline bind overruns, it
still finishes. The family's timing history then sends later bindings to
new instances.

**Rebuild** merges instances and clears orphaned child state. Build a new
instance with the union of the bindings on a leased connection, replay it to
the shared head, and swap it in at a version boundary under the exclusion.
The swap moves subscribers between two pipelines that are at the same
version. By per-binding equivalence, their future streams are the same. This
is the migration that Rindle removed because it was the least-tested path.
It stays out of v1. The instance count per family and the storage size per
instance are metrics, so we can see whether it is needed.

### Before sharing: families inside one client group

Families also work inside one `PipelineDriver`, with no 004 at all.
`#pipelines` is keyed by query ID, and a family table would sit next to it.
Bind is inline, because the driver owns its snapshot. This helps a client
group that runs many queries from one template, for example a `useQuery` per
comment or per emoji. It also tests everything except the cross-group
parts: the builder, the gate, bind and unbind, routing, and CVR identity.
Whether it pays for itself on its own is a question for F0.

### Across workers

A family is per worker, like every pipeline under 004. Each worker holds its
own instances over the bindings of its own client groups. Per write, the
fleet does one push per worker that holds the family. Rindle's benchmark
(§12) measured that splitting one family across workers does not raise
throughput. The per-write work is already constant. Sharding is only for
memory.

## What does not group

Families help only when the per-user part of a query is a root-level
equality. zbugs shows how often it is somewhere else:

| Query                       | Why it does not share across users                                                                                                |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `issueDetail`               | `related('viewState', q => q.where('userID', sub))` and `related('notificationState', ...)`. The user is a literal in a subquery. |
| `issuePreloadV2`            | The same `viewState` overlay                                                                                                      |
| `issueListV2`, `issueList`  | The same `viewState` overlay in `buildListQuery`. A scalar `project` EXISTS. `start` on every page after the first.               |
| `labelsOrderByName`         | A scalar `project` EXISTS                                                                                                         |
| `commentsPage`, later pages | `start`                                                                                                                           |

These are zbugs' heaviest queries. In v1, each of them is one family per
user, and families share them only across that user's tabs and devices.
That is no better than 004 for them. The `.related()` overlay with the
user's ID is how Zero apps attach per-user state to shared rows. That makes
subquery holes the most important follow-on, and F0 measures how much they
would add.

Legacy permissions behave the same way. A rule that binds `authData.sub` in
a root-level `=` becomes a hole. A rule that puts it under `OR` or inside an
EXISTS makes a family per user.

## Risks

- **The grouping rate may be low.** See the previous section. F0 measures it
  before anything else is built.

- **Inline binds stall the worker.** A bind that takes longer than its
  family's history predicted holds every client group's advance for that
  long. The budget is per family and measured, but the first slow bind of a
  new pattern still costs one stall.

- **Blast radius.** One query that throws during a push fails its own
  client group today. A family instance that throws fails every binding on
  it. 003 resets one pipeline. Under families, that is one instance, and the
  instance rehydrates every binding. Rehydrating per binding, as Rindle's
  §11.4 suggests, makes recovery incremental.

- **Budgets.** 003 gives each pipeline its own advancement budget, based on
  its hydration time. A family does work for many bindings, so a budget per
  pipeline would throttle a family at exactly the scale it exists for.
  Rindle charges each change's cost to the binding it reaches, and charges
  shared work to a small per-family meter. Over budget, it drops one
  binding, not the family.

- **Orphaned child state.** Described under unbind. It exists today. Long
  family lifetimes make it grow.

- **A new edit-split trigger.** Edits that change a holed column now split
  at the source, for every connection on that table. This is the same
  mechanism `.related()` parent keys already use. The differential tests
  cover it directly.

- **The plan is fixed per family.** Correct today, because the replica has
  no stat4. If stat4 arrives, per-literal plans could diverge from the
  family's plan.

## Sequencing

| Step | Change                                                                                                                                                                                                                                                                                                              | Prerequisite    | Flag  |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- | ----- |
| F0   | Family key in `computePipelineDedupStats` (`packages/zero-cache/src/workers/syncer.ts:93`). Log `familyDedupFactor` next to `dedupFactor` in `shared-advance-eligibility`, and the same factor within each client group. Also log what the factor would be with subquery holes and with scalar subqueries resolved. | None            | None  |
| F1   | Family key extraction. `buildFamilyPipeline`, the gate, dynamic `ConnectionIndex` values, bind, unbind, `Take.evictPartition`. Nothing calls them.                                                                                                                                                                  | None            | None  |
| F2   | Families inside one `PipelineDriver`, with routing in the `Streamer`.                                                                                                                                                                                                                                               | F1              | new   |
| F3   | 004 steps 1 and 2: read-only derivation, then a shared snapshot pair and advance per worker.                                                                                                                                                                                                                        | 004             | 004's |
| F4   | 004 step 3 with family keys: the registry, inline and leased binds, instances.                                                                                                                                                                                                                                      | F2, F3, F0 data | new   |
| F5   | Per-binding budgets, child partition eviction, rebuild if the metrics call for it.                                                                                                                                                                                                                                  | F4              | new   |

F0 is a few days and decides the rest. If the family dedup factor is close
to 004's exact dedup factor, families add nothing, and 004 goes ahead alone.
If the factor is high only with subquery holes, the next design is subquery
holes, not F1.

F1 and F2 do not depend on 004, and F2 can ship alone if F0 shows many
family-mates within client groups.

## Testing

- **Family key.** Two queries are family-mates exactly when they are equal
  except for the holes. Conjunct order does not matter. A second `=` on the
  same column prevents holes on that column. `null`, arrays, `start`, and
  scalar subqueries are not holed. The internal `lmids` and
  `mutationResults` queries of two client groups are family-mates.
  Substituting the binding back into the template gives the original
  normalized AST.
- **Binding equality.** Two literals give the same binding exactly when the
  `=` predicate treats them as equal.
- **Engine differential.** One family pipeline and k singleton pipelines on
  the same sources, driven by the same write log. After every push, each
  binding's stream from the family equals its singleton's stream. The
  comparison is on the change stream and on exact occurrence counts per row,
  not on the final row set. 004's refcount requirement depends on this.
  Run on `MemorySource` and on zqlite `TableSource`. Cases, each a named
  test:
  - bind and unbind interleaved with writes, including a rebind after
    unbind;
  - an edit that changes a holed column, with both values bound, one bound,
    and neither, and with `null` changing to a bound value;
  - `limit` and `one` at the root, with displacement and refill;
  - `.related()` with and without `limit`, EXISTS, NOT EXISTS, and a
    flipped EXISTS;
  - two root rows in different partitions that share a child correlation
    value;
  - a hole on a correlation column, to check that 001 pushdown does not put
    the literal in the child;
  - zero bindings: every push stops at the gate or at the index.
- **Unbind leaves no root state.** Bind and unbind many times. The root
  `Take` storage and the `Join` partition maps return to their starting
  size. A second test pins the orphaned child `Take` state until F5 fixes
  it.
- **Constrained fetches only.** An unconstrained fetch on a family root
  throws, with and without `limit`.
- **Fuzz.** Add a template-and-bindings mode to the chinook fuzz backbone
  push lane (`packages/zql-integration-tests/src/chinook/`). Chinook has
  the same artist, album, and track shape as Rindle's benchmark.
- **Cross-group (F4).** 004's tests with family keys: two client groups
  with different bindings, two with the same binding, a late joiner with
  exact counts, and a bind as a join pinned at a version under the group
  barrier.
- **Benchmark.** Extend the advance bench from #6233 with a sweep over the
  number of bindings for the four shapes in Rindle's §12: a filter on a
  key, a `Take` on a key, a `.related()` child, and an EXISTS child.

## Future work

- **Holes in `.related()` subqueries.** This is the `viewState` shape.
  `issue.where('id', ?).related('viewState', q => q.where('userID', ?))`
  would have the binding `(id, userID)`. Root rows would be shared by every
  binding with the same `id`. Child rows would be routed by their own
  `userID` column. Rindle rejected subquery holes, but its apps did not
  depend on this idiom. It is a separate design, because the child
  partition key must gain the holed column. With `.one()` on `viewState`,
  a `Take` partitioned only by `issueID` would keep one row across all
  users, which is wrong. EXISTS subqueries change which root rows qualify
  for each binding, so they stay excluded.
- **Scalar subqueries as holes.** Compute the key on the resolved AST, where
  the scalar has become a root `=`. A change in the scalar's value then
  moves the affected subscribers to another binding, instead of resetting
  the driver.
- **Holes pushed down as gates.** See
  [001 pushdown and holes](#001-pushdown-and-holes).
- **Client-side families** in `zero-client`.

## Open questions

- **Inline bind budget.** What p95 bind time is cheap enough to hold every
  client group's advance on the worker for? Does the budget scale with the
  number of client groups on the worker?
- **Rebuild.** Is instance count or orphaned state large enough in practice
  to justify the swap? Rindle removed its version of that path.
- **Unbind grace period.** 004 unsubscribes after TTL. Should a family keep
  a binding bound for a short time after that, to avoid a rebind when a
  user comes back? Rindle's families idle-sweep like its other
  materializations.
- **Budget attribution.** Charge per binding touched, as Rindle does, or
  per instance, as 003 would by default?
- **Where the gate lives.** A filter operator above the connection is the
  smallest change. Putting the check in the connection's predicate would
  also narrow the source's overlay during a push. Is that worth the extra
  coupling?
