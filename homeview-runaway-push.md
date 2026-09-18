# Margins `homeViewQuery`: runaway push on `work_covers` updates

The question: is `homeViewQuery`
(`margins-zero-queries/packages/home/zero-schema/src/queries.ts`) vulnerable to
runaway push when a backfill like this runs:

```sql
UPDATE catalog.work_covers c SET background_color_hex = v.hex
FROM (SELECT unnest(ARRAY[...500 cover_ids...]::uuid[]) AS cover_id,
             unnest(ARRAY[...500 hex values...]::text[]) AS hex) v
WHERE c.cover_id = v.cover_id AND c.background_color_hex IS NULL
```

This analysis assumes the earlier `work_isbn13s.cover` fix, which joins on
`(work_id, cover_id)`, is already in place.

**Answer: yes, it is vulnerable.** The indexes are fine. The problem is that a
`work_covers` change is pushed up to parent readthroughs of **every user**, not
just the viewer.

## Why

When a child row changes, `Join.#pushChildChange`
(`packages/zql/src/ivm/join.ts`) fetches the parents constrained **only by the
join key**. The `profiles.user_id = X` at the root does not reach down to the
readthroughs, so the fetch is `readthroughs WHERE work_id = W`, across all
users. The rows that don't belong to the viewer are dropped only at the very
top. For `ownLastFinishedReadthrough`, the `limit(1)` is partitioned by
`user_id`, so `Take.fetch` scans the whole input and does a take-state lookup
per row (`packages/zql/src/ivm/take.ts:136-155`).

It also doesn't help that `background_color_hex` isn't in the Zero schema.
Every replicated row change becomes an EDIT pushed through every pipeline that
reads `work_covers` (`pipeline-driver.ts:1089-1113`).

## The push paths for one cover edit

| Path in `homeView` | What the push fetches | Bounded to the viewer? |
|---|---|---|
| `ownReadthroughs.work.covers` (primary only) | `readthroughs WHERE work_id=W AND status='in_progress'`, then one `profiles` lookup per match | ❌ |
| `ownLastFinishedReadthrough.work.covers` | `readthroughs WHERE work_id=W AND status='finished'…`, sorted in a temp B-tree | ❌ |
| `ownLastFinishedReadthrough.work.relatedItems.relatedWork.covers` | `work_related_items WHERE related_work_id=W`, then, for each anchor work A, all finished readthroughs of A | ❌❌ (two hops) |
| `…preferredCover` (every cover, not just primary) | `readthroughs WHERE preferred_cover_id=C` | ❌ |
| `wantToReadSeries.works.work.covers` | `want_to_read WHERE series_id=S` (up to about 4.5k rows per series) | ❌ |
| `wantToReadReadthrough.work.covers` | has `.where("user_id","=",userId)`, so it uses `idx_readthroughs_user_id_work_id` | ✅ |

## How big the fan-out is

These are replica row counts, per client group, per cover.

- **A random 500-cover batch** (the first 500 by `cover_id`) is mild. 64 of
  the covers are primary, and the batch costs about 3.4k row visits.
- **The tail is severe.** A primary cover of the most-read works costs:
  - 60k–113k readthroughs scanned, twice (the in-progress and finished paths).
  - 1k–4k `profiles` lookups.
  - **0.3M–4.0M readthrough row visits** through the related-items path. Work
    `d3ffdab7…` has 233 related anchors and costs 3.99M.

Top works by readthrough count:

| work_id | readthroughs | in progress | related anchors | related-path readthrough visits | preferred_cover rows (primary) |
|---|---|---|---|---|---|
| 4e28c97b… | 113,256 | 3,534 | 123 | 3,293,580 | 666 |
| 642b4bed… | 105,188 | 2,716 | 15 | 733,858 | 102 |
| bfce543f… | 86,660 | 2,151 | 17 | 687,063 | 49 |
| d3ffdab7… | 85,946 | 4,332 | 233 | 3,992,219 | 523 |
| 6d0854cf… | 60,449 | 1,940 | 197 | 3,506,005 | 204 |

The batches walk `cover_id` in order across 2.76M works, so they will hit
those popular primary covers. When that happens, the cost is seconds of push
work per client group. Every connected user has `homeView` registered, so that
cost repeats in every client group.

## Why it ends in a runaway

Each client group's reset budget is its own hydration time. `homeView`
hydrates cheaply because it is scoped to one user. So the push cost goes over
budget, the driver throws `ResetPipelinesSignal`, and every client group
rehydrates at the same time. Any later transaction that touches a popular
primary cover triggers it again.

## Fix

Add the redundant `.where("user_id", "=", userId)` to the subqueries that
don't have it, as they already did on `wantToReadReadthrough`:

```ts
.related("ownReadthroughs", rt => rt.where("user_id", "=", userId).where("status", "=", "in_progress")…)
.related("ownLastFinishedReadthrough", rt => rt.where("user_id", "=", userId).where("status", "=", "finished")…)
.related("ownWantToRead", w => w.where("user_id", "=", userId)…)
```

The results don't change, because the predicate matches the parent
correlation. The push fetches become point lookups. `EXPLAIN QUERY PLAN`
confirms:

- `work_id=? AND user_id=?` uses `idx_readthroughs_user_id_work_id`.
- `preferred_cover_id=? AND user_id=?` uses the same index.
- `series_id=? AND user_id=?` uses `idx_want_to_read_user_id_series_id`.

With the fix, the related-items path costs one lookup per related item (at
most about 230) instead of millions of row visits.

## Caveats

- The replica (`margins-base.db`) is a snapshot from Aug 12 and doesn't have
  `background_color_hex` or `width_px`, so the counts are approximate.
- These are row-visit counts from SQL. The change was not replayed through a
  real pipeline.
- Only `homeView` was checked. The 43 background queries (for example
  `userReadthroughsQuery` with `.work.covers`) probably have the same shape and
  are worth checking the same way.
