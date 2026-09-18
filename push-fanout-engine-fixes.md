# Engine fixes for unbounded push fan-out through `related` chains

Status: proposal, not started. See `homeview-runaway-push.md` for the full
Margins analysis and replica measurements.

## What triggered this

### The update

Margins ran a backfill that edits `catalog.work_covers` in batches of 500 rows
per transaction, walking `cover_id` in ascending order:

```sql
UPDATE catalog.work_covers c
SET background_color_hex = v.hex
FROM (
  SELECT unnest(ARRAY['00000022-…', '00000091-…', /* …500 cover_ids */]::uuid[]) AS cover_id,
         unnest(ARRAY['#031b11', '#fdfdfd', /* …500 hex values */]::text[]) AS hex
) v
WHERE c.cover_id = v.cover_id
  AND c.background_color_hex IS NULL
RETURNING c.cover_id
```

`background_color_hex` is not a column in the Zero schema, but that doesn't
matter. Every replicated row change becomes an EDIT pushed through every
pipeline that reads `work_covers` (`pipeline-driver.ts`, the
`makeSourceChangeEdit` path in advance). The edit doesn't change a join key,
so it passes through to every subscribed `covers` / `preferredCover` join.

### The query

`homeViewQuery` (Margins, `packages/home/zero-schema/src/queries.ts`) is
registered by every connected user. It is rooted at one user and pinned by a
literal equality:

```ts
zql.profiles.where("user_id", "=", userId).one()
  .related("ownReadthroughs", rt => rt            // profiles.user_id → readthroughs.user_id
    .where("status", "=", "in_progress")
    .related("preferredCover")                    // readthroughs.preferred_cover_id → work_covers.cover_id
    .related("work", w => w                       // readthroughs.work_id → works.work_id
      .related("covers", c => c.where("is_primary", "=", true))))
  .related("ownLastFinishedReadthrough", rt => rt
    .where("status", "=", "finished").orderBy("end_date", "desc").limit(1)
    .related("work", w => w
      .related("covers", …)
      .related("relatedItems", i => i            // works.work_id → work_related_items.work_id
        .related("relatedWork", rw => rw          // work_related_items.related_work_id → works.work_id
          .related("covers", …)))))
  .related("ownWantToRead", w => w.limit(12)      // profiles.user_id → want_to_read.user_id
    .related("wantToReadSeries", s => s           // want_to_read.series_id → series
      .related("works", e => e.limit(3)           // series → work_series
        .related("work", w => w.related("covers", …)))))
```

### Why it blows up

On a child change, `Join.#pushChildChange` (`packages/zql/src/ivm/join.ts`)
fetches the parents constrained **only by the join key**. The root's
`user_id = X` never reaches the intermediate tables, so a cover edit for work
`W` fetches `readthroughs WHERE work_id = W` for **every user**. Rows that
don't belong to the viewer are dropped only at the root join. With a
partitioned `limit`, `Take.fetch` scans the whole input and does a per-row
take-state lookup (`take.ts`, the `maxBound` branch).

On the Margins replica (56M readthroughs, 443k profiles, 2.76M works), a
primary-cover edit on one of the most-read works costs, per client group:

- 60k–113k readthroughs scanned, twice (the in-progress and finished paths);
- 0.3M–4.0M readthrough row visits through
  `relatedItems → relatedWork → covers`;
- up to 5.5k rows through `preferredCover`, and up to 4.5k rows through
  `want_to_read WHERE series_id = S`.

`homeView` hydrates in milliseconds, so the push cost far exceeds the reset
budget and the driver throws `ResetPipelinesSignal`. That happens in every
client group at once, and again for each batch that contains a popular work's
cover.

The user-level workaround is to add a redundant `.where("user_id", "=", userId)`
to each user-owned subquery. The two options below make that unnecessary.

## Option 1: derive filters from literal pins through correlations

**Idea.** If a parent has a top-level AND conjunct `parentField = <literal>`,
and a `related` or `whereExists` correlates on `parentField → childField`, add
`childField = <literal>` to the child's conditions. Then apply the same rule
recursively to the child's own subqueries.

**Why it's safe.** The join only matches rows where
`child.childField = parent.parentField = literal`, so the added filter can't
remove a row that would have matched.

**Why it helps.** Hydration is already bounded, because the child fetch
carries the parent's value. Only push fetches parents by the child's join key
alone. With the derived filter in the source connection's SQL, the push fetch
becomes `work_id = ? AND user_id = ?`. On the Margins replica, SQLite picks
`idx_readthroughs_user_id_work_id` for that, and
`idx_want_to_read_user_id_series_id` for the want-to-read path (checked with
`EXPLAIN QUERY PLAN`). For `homeView`:

| Path | Before | After |
|---|---|---|
| `ownReadthroughs.work.covers` | all readthroughs of W | point lookup |
| `ownLastFinishedReadthrough.work.covers` | all finished readthroughs of W + sort | point lookup |
| `…relatedItems.relatedWork.covers` | up to 4M row visits | at most about 233 related items × 3 lookups |
| `…preferredCover` | all readthroughs with that cover | point lookup |
| `wantToReadSeries.works.work.covers` | all want_to_read rows for S | point lookup |

**Where.** An AST rewrite in `packages/zql/src/builder/builder.ts`, next to
`bindStaticParameters` or at the start of `buildPipelineInternal`. It must run
after static parameters are bound, so that `authData`-derived literals count
as pins. As far as I can tell nothing does this today:
`planner/planner-connection.ts` `propagateConstraints` propagates constraints
for cost estimation only and doesn't rewrite filters.

**Details to work out.**

- Only propagate from top-level AND conjuncts that use `=` with a non-null
  literal. `IN (literals)` could propagate as `IN`. Skip anything under `OR`
  or `NOT`.
- A compound correlation should propagate per column. Each pinned
  `parentField[i]` pins `childField[i]`.
- The chain continues only through columns that are pinned. With
  `readthroughs.work_id → works`, `work_id` isn't pinned, so propagation stops
  there. That's fine, because the bound is needed at the readthroughs level.
- Flipped joins (`whereExists` chosen by the planner to run child-first):
  confirm the added conjunct doesn't change planner behavior in a bad way. It
  should only make the child more selective.
- Compare with how permissions are applied. Permission rules often have
  exactly this `ownerID = authData.sub` shape on the root and the children.
- Tests:
  - Row equality between hydrating with and without the rewrite (the
    `zql-integration-tests` / fuzz harness).
  - A pipeline-driver test in the style of
    `pipeline-driver.runaway-push.test.ts`: a root pinned by user, many
    other users' children sharing a join key, and a grandchild edit. Assert
    that the push does no work proportional to the other users.

**Limitation.** It only helps when a literal equality pins the chain. A root
like `works.where("popularity", ">", …)`, or a chain that pins only unrelated
columns, gets nothing. Option 2 covers those.

## Option 2: joins remember which parents are live

**Idea.** Change `Join` so a child change re-fetches only the parents that
could be in the output, not `fetch({constraint: childKey})` across the whole
parent input. Keep a map in each join's storage from child join key to the set
of parent primary keys that have left the join:

- rows that go out through `fetch` (hydration, and refetches from downstream
  Take or Exists);
- rows that go out through the parent-side push path (a parent ADD).

On a child change for key `k`, look up `k`. If it isn't in the map, stop. If
it is, fetch each recorded parent by primary key and push only those.

**Why it's sound.** Any parent row in the downstream output must have left
this join through fetch or a parent push, so the recorded set is always a
superset of the relevant parents. A set that only grows is safe: extra entries
waste work but never produce a wrong result. Remove an entry on a parent
REMOVE, and optionally when downstream drops the row. That second case needs a
signal from downstream and is the hard part.

**Why it's more general.** Fan-out is bounded by what the view has seen, not
by the table size. It doesn't need a literal pin, so it works for any root.

**Costs and risks.**

- Memory and storage writes proportional to the parent rows the join has
  emitted, in each client group's operator storage (`DatabaseStorage`). For
  big views that's comparable to the view itself.
- Hydration gets slower, because every fetched parent row is recorded.
- Correctness with `Take` refetches (a new bound after a removal), `Exists`
  size checks, flipped joins and `FanOut`/`FanIn` branches all needs careful
  review. It's a core IVM change.
- Re-fetching by primary key through a partitioned `Take` goes through the
  `maxBound` branch. That's cheap by PK, but it needs checking.
- An alternative: record only child keys, not parent primary keys. That's
  cheaper and prunes irrelevant keys, but a relevant key still scans all
  parents with it (a popular work the viewer is reading still scans all 113k
  readthroughs). So parent primary keys are needed to fix the Margins case.

## Recommendation

Do Option 1 first. It's a small, semantics-preserving rewrite in the builder
that removes the Margins failure without any query changes. Treat Option 2 as
the longer-term general fix for queries without a literal pin.

(Separately, and not covered here: rehydrating only the expensive query,
instead of resetting the whole client group, would cap the blast radius of
any shape these options miss. It fits with the hydration cost model work on
`mlaw/y-slow`.)
