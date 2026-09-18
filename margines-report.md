# Margins Zero Query Performance Investigation

## Summary

The initial artifact audit found one high-risk relationship lookup that strongly
matched the reported pipeline-advancement timeouts. The customer has since
deployed the composite relationship fix. Production follow-up shows that the
original scan is no longer visible, but 500-row backfill transactions still
cause a rehydration stampede across hundreds of client groups.

The relationship from `catalog.work_isbn13s` to `catalog.work_covers` joins on `cover_id`, but the supplied replica has no index on `catalog.work_isbn13s.cover_id`. When a cover row changes, Zero's IVM pipeline performs the reverse lookup from the changed child row to matching parent ISBN rows. SQLite must scan the entire `catalog.work_isbn13s` table for that lookup.

On the supplied replica:

- `catalog.work_isbn13s` has 11,795,283 rows.
- The table occupies approximately 1.44 GiB.
- Its indexes cover `isbn13` and `work_id`, but not `cover_id`.
- `EXPLAIN QUERY PLAN` reports `SCAN catalog.work_isbn13s` for a `cover_id` lookup.
- A single no-match lookup took approximately 1.4–2.3 seconds locally.

A batch of 500 `catalog.work_covers` changes can therefore cause roughly 720 GiB of logical table scanning per affected pipeline. At the measured cached lookup time, 500 independent scans are on the order of 12 minutes. Multiple active query pipelines can amplify this further.

This edge explains the behavior before the relationship correction. The live
post-deployment failure has a different immediate mechanism, described below.

## Production follow-up after the relationship fix

Production logs for stack `xvav0bi8hbwv988s` were examined from 2026-09-17
21:10 through 21:35 UTC (17:10 through 17:35 America/New_York). All four
view-syncer pods were on ReplicaSet `dd4d5649d` for the full window.

The deployed relationship correction appears to have removed the original
pathological lookup:

- No `catalog.work_isbn13s` SQL appeared in the slow-query or slow-row logs
  from 21:15 through 21:25 UTC.
- The generated live `catalog.work_covers` reads include
  `background_color_hex`, which confirms that the view-syncers loaded the new
  schema generation rather than continuing to use the supplied query bundle.
- Advancement now processes tens of cover changes in tens of milliseconds for
  many client groups. This is incompatible with the previous measured cost of
  1.4–2.3 seconds for each unindexed `work_isbn13s.cover_id` lookup.

However, every 500-row transaction still causes a reset storm. Between 21:19
and 21:21 UTC, the view-syncers logged:

- 5,732 pipeline resets across approximately 344 client groups
- 3,410 resets because advancement was projected to exceed hydration time
- 2,298 resets because advancement exceeded its timeout
- 24 resets while processing the current change
- 47.8 resets per second on average

The raw messages directly name the backfill batch size. Representative events
include resets at 21 of 500 changes after 429 ms, 50 of 500 after 95 ms, and 11
of 500 after 319 ms. As clients fall behind during rehydration, later attempts
contain more than one transaction; the same interval includes advancement sets
of 1,000–6,081 changes.

This creates a positive-feedback loop:

```text
500 cover changes
  -> hundreds of client groups choose reset
  -> every group rehydrates its full query set
  -> large library queries saturate the view-syncers
  -> groups fall behind additional backfill commits
  -> the next advancement contains more changes and resets again
```

Across the 25-minute window, the logs contain 48,368 reset-triggered full-query
rehydrations affecting 683 client groups. The hydration analyzer observed
1,701,533 individual query hydrations and 10,018 query-set batches whose wall
time exceeded five seconds.

This is now primarily a fan-out and scheduling problem, not another obviously
missing index. For example:

- A batch starting at 21:20:19 UTC hydrated 39 queries. Their summed processing
  time was 12.5 seconds, but the batch took 175 seconds wall-clock. Only 7.2%
  of wall time was measured query processing.
- A batch starting at 21:20:03 UTC hydrated 55 queries. Their summed processing
  time was 6.1 seconds, but the batch took 157 seconds wall-clock.
- The slowest batch in the window took 180 seconds wall-clock while recording
  only 10.7 seconds of query processing.

The expensive rehydrations are dominated by the intentionally broad background
queries:

| Query | Largest observed hydration | Largest result |
|---|---:|---:|
| `libraryWorks` | 12.5 s | 52,250 rows |
| `libraryReadthroughs` | 7.3 s | 26,598 rows |

During 21:15–21:25 UTC, the stampede produced 546,607 slow warnings for the
indexed point lookup `catalog.works(work_id)`, 119,145 for
`catalog.work_covers(work_id, is_primary)`, and 88,163 for
`catalog.work_contributors(work_id)`. These are not evidence that those
individual point lookups lack indexes. They are evidence that full graph
rehydration repeatedly executes enormous numbers of normally cheap lookups
while the workers are saturated.

There are still query-shape optimization opportunities. The large library
roots read and sort all rows for a user:

```sql
SELECT ... FROM "userspace.works"
WHERE "user_id" = ?
ORDER BY "position", "user_id", "work_id";

SELECT ... FROM "userspace.readthroughs"
WHERE "user_id" = ?
ORDER BY "touched_at" DESC, "readthrough_id";
```

The supplied replica has `user_id` indexes, but not indexes that also satisfy
these complete orderings. Live warnings reached 97.6 seconds for the first
shape and 72.3 seconds for the second under saturation. Matching composite
indexes may remove temporary sorts, but they cannot eliminate the cost of
returning tens of thousands of root and related rows. The broad subscriptions
remain the larger scaling issue.

### Revised operational recommendation

Pause or heavily throttle this backfill before making another index change.
Use much smaller transactions with pacing between commits and watch whether
pipeline resets stop accumulating. The goal is to let every client group
finish advancement before the next transaction rather than allowing multiple
500-row commits to stack up.

For a durable fix, avoid forcing every active client group to rehydrate the
full `libraryWorks` and `libraryReadthroughs` graphs for a cover-metadata
backfill. Options include reducing the full-library background subscriptions,
windowing them, or excluding backfilled metadata from synced query shapes when
clients do not require it. Composite root indexes can be evaluated separately,
but they are a secondary optimization rather than the explanation for the
current reset storm.

## High-risk relationship

The relationship is defined in `packages/catalog/zero-schema/src/schema.ts`:

```ts
export const workISBN13sRelationships = relationships(
  workISBN13sTable,
  ({ one }) => ({
    cover: one({
      sourceField: ["cover_id"],
      destField: ["cover_id"],
      destSchema: workCoversTable,
    }),
  }),
);
```

This traversal is used by the offer-query helper in `packages/catalog/zero-schema/src/queries.ts`:

```ts
const offerRows = () =>
  zql.ingram_stock
    .related("streetDate")
    .related("edition", (edition) => edition.related("cover"))
    .related("workCovers", (cover) => cover.where("is_primary", true));
```

The affected named queries are:

- `offersByWorkId`
- `offerByISBN13`
- `offersByISBN13s`

`workByISBN13` also traverses this relationship, but its root is constrained by the indexed `isbn13` primary key. The nested traversal used by the offer queries does not have that same static root constraint when a cover change propagates in reverse.

### Why child changes cause the reverse lookup

During incremental maintenance, Zero handles a child change by constructing a constraint from the child key to the parent key and fetching matching parent rows. In the current monorepo implementation this is in `packages/zql/src/ivm/join.ts`:

```ts
const constraint = buildJoinConstraint(
  childRow,
  this.#childKey,
  this.#parentKey,
);
if (constraint) {
  for (const parentNode of this.#parent.fetch({constraint})) {
    // propagate the child change
  }
}
```

For this relationship, a changed cover produces a parent constraint equivalent to:

```sql
SELECT ...
FROM "catalog.work_isbn13s"
WHERE cover_id = ?;
```

The supplied replica produces this plan:

```text
SCAN catalog.work_isbn13s
```

## Recommended fix

### Preferred: use the existing composite foreign-key shape

The schema comments say that the cover belongs to the same work through the composite `(work_id, cover_id)` foreign key. Expressing that complete key in the Zero relationship avoids the scan using indexes already present in the supplied replica:

```ts
cover: one({
  sourceField: ["work_id", "cover_id"],
  destField: ["work_id", "cover_id"],
  destSchema: workCoversTable,
})
```

The resulting lookups plan as:

```text
SEARCH catalog.work_isbn13s
  USING INDEX catalog.idx_work_isbn13s_work_id (work_id=?)

SEARCH catalog.work_covers
  USING COVERING INDEX catalog.work_covers_work_id_cover_id_key
  (work_id=? AND cover_id=?)
```

The `work_id` index narrows the parent side to approximately five rows on average, after which SQLite filters on `cover_id`.

### Alternative: add a `cover_id` index

An upstream index also fixes the existing single-column relationship and protects older query shapes:

```sql
CREATE INDEX CONCURRENTLY idx_work_isbn13s_cover_id
ON catalog.work_isbn13s (cover_id)
WHERE cover_id IS NOT NULL;
```

After the index reaches the replica, verify that this query uses `SEARCH`, not `SCAN`:

```sql
EXPLAIN QUERY PLAN
SELECT 1
FROM "catalog.work_isbn13s"
WHERE cover_id = ?;
```

Using both fixes may be appropriate if older deployed clients or retained query transformations can continue using the single-column relationship.

## Other relationship-index findings

The audit examined 174 relationship declarations across the supplied source bundle. For relationship tables present in the supplied replica, every destination-side join key had a usable index.

The following additional source-side lookups do not have dedicated usable indexes:

| Source lookup | Replica rows | Assessment |
|---|---:|---|
| `userspace.club_meetings(work_id)` | 0 | Future scaling risk |
| `userspace.club_book_suggestions(work_id)` | 0 | Future scaling risk |
| `userspace.club_book_suggestions(user_id)` | 0 | Future scaling risk |
| `userspace.poll_options(proposed_work_id)` | 0 | Future scaling risk |
| `userspace.showcase(work1_id)` through `work5_id` | 11,665 | Low current risk |

The book-club relationships are traversed by `myClubs`, which is a background subscription. These indexes should be added before those tables become large:

```sql
CREATE INDEX CONCURRENTLY idx_club_meetings_work_id
ON userspace.club_meetings (work_id);

CREATE INDEX CONCURRENTLY idx_club_book_suggestions_work_id
ON userspace.club_book_suggestions (work_id);

CREATE INDEX CONCURRENTLY idx_club_book_suggestions_user_id
ON userspace.club_book_suggestions (user_id);

CREATE INDEX CONCURRENTLY idx_poll_options_proposed_work_id
ON userspace.poll_options (proposed_work_id);
```

The five `showcase` work relationships can scan the 3.3 MiB `userspace.showcase` table when driven solely by a catalog-work change. However, the current `profileContentShowcase` query statically constrains `user_id`, and `user_id` is the table's primary key. SQLite can use that primary-key index when both constraints are present, so these edges are not a likely explanation for the reported timeout.

The five composite showcase-to-readthrough relationships were also flagged by a strict full-key audit, but they use constraints such as `(user_id, work1_id)`. The existing unique `user_id` primary-key index reduces these lookups to one showcase row, so they are not pathological.

## Replica/query version mismatch

The artifacts are not from the same schema generation:

- Query bundle export: September 17, 2026
- Replica's last recorded replication event: August 11, 2026
- Query bundle version: `@rocicorp/zero` `1.10.0-canary.21`

Several tables referenced by the current query bundle do not exist in the supplied replica, including:

- `catalog.ingram_stock`
- `catalog.ingram_street_dates`
- `userspace.sbv_searches`
- `userspace.sbv_search_results`
- Current commerce tables such as `userspace.orders`, `order_line_items`, `order_shipments`, and `cart_items`

Consequently, their live replica indexes could not be validated. A current replica or the current PostgreSQL DDL is needed for a complete audit.

Indexes that should be checked explicitly in a current replica include:

- `userspace.sbv_search_results(work_id)` for the result-to-catalog-work reverse edge
- A user/time index for the recent-search root, such as `userspace.sbv_searches(user_id, created_at)`
- `catalog.ingram_stock(work_id)` for work offer lookups
- `userspace.order_line_items(order_id)`
- `userspace.order_shipments(order_id)`
- `userspace.order_events(order_id)`
- `userspace.cart_items(user_id)`

Of these, `userspace.sbv_search_results(work_id)` deserves particular attention. Its declared primary key is `(search_id, position)`, while its `work` relationship uses `work_id`. Without a separate `work_id` index, catalog-work changes can cause reverse scans of the entire result table for active `sbvSearchView` queries.

## Identifying the failing batch

The supplied replica's `_zero.changeLog2` table is empty, so it does not preserve the table composition of the failing 500-row batches.

For a reproduction, temporarily set IVM sampling to every request and retain slow-row warnings. Current Zero exposes the relevant logging settings as:

```text
--log-ivm-sampling 1
--log-slow-row-threshold 2
```

The table source emits slow-fetch messages containing the generated SQL, including the diagnostic text `Are you missing an index?`. That should identify both the changed-table traversal and the exact scanning lookup.

The first table to check in replication logs is `catalog.work_covers`. A 500-row cover update batch, especially from a cover metadata or dimensions backfill, fits the observed failure and the measured unindexed reverse lookup.

## Conclusion

The composite `(work_id, cover_id)` relationship correction addressed the
original `work_isbn13s.cover_id` reverse scan. The post-deployment logs contain
no slow `work_isbn13s` lookups, so adding another index there is not the next
action for the current incident.

The active failure is a reset-and-rehydrate feedback loop. Every 500-row cover
transaction reaches hundreds of client groups, most pipelines decide that
rehydration is cheaper than incremental advancement, and the broad
`libraryWorks` and `libraryReadthroughs` subscriptions then saturate all four
view-syncers. While they rehydrate, more backfill commits accumulate and make
the next advancement larger.

The immediate mitigation is to pause or substantially throttle the backfill.
The durable work is to reduce the size and fan-out of the full-library query
shapes, or to keep this metadata backfill out of those synced shapes. The
remaining confirmed missing source indexes are currently low-impact because
their tables are empty or small. The newer Search-by-Vibes, Ingram, and
commerce tables still require a current replica for a complete independent
index audit.
