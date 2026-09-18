# Margins: query fixes for runaway push fan-out

This covers all 98 registered Margins server queries (bundle commit
`368a1d433f`), checked against their replica (`margins-base.db`). The pattern
is the one found in `homeView` (see `homeview-runaway-push.md`):

> A subquery is reachable only through a parent pinned by `col = literal`, and
> the correlation carries that column down. But the subquery doesn't restate
> the pin. When a row changes further down, Zero's join fetches the subquery's
> table by the join key alone: **every user's rows, not just the pinned
> parent's**. The unwanted rows are dropped only at the pinned root.

The fix is the same everywhere: restate the pin on the subquery. The results
don't change, because the join already guarantees the column equals the pin.
But the push fetch now goes through an index that starts with the pinned
column.

Measured on `homeView` with the replica: one primary-cover edit for a popular
work took **235 s** without the pin and **12 ms** with it.

## How the queries were checked

The `fanout` workload in `packages/zql-benchmarks/src/replica-advance-perf.ts`
does four things:

1. Builds every registered server query's AST. Background queries get the
   arguments the app actually sends; the rest get placeholder arguments.
2. Carries literal pins down each correlation, and flags every subquery that
   could inherit a pin, doesn't state it, and has subqueries of its own (the
   edges a push arrives through).
3. Sizes each flagged edge from the replica: `sqlite_stat1` for the average
   rows fetched, a `GROUP BY` for the hottest key, and the best index once the
   pin is added.
4. Separately lists push fetches that have **no usable index at all**.

```bash
pnpm --filter zql-benchmarks advance:perf -- --workload fanout \
  --replica ~/workspace/zero/investigate/margins-base.db \
  --queries-dir ~/workspace/zero/investigate/margins-zero-queries [--json]
```

"Hottest key" below is the most rows one push fetches for a single join key
value (for example, the most readthroughs any one work has). That is the cost
of the worst single change, per registration of the query.

**Apply every fix to both the client and the server body.** Most of these
queries keep a client twin "byte for byte" with its server body, and some have
equivalence tests. The added predicate is harmless on the device.

## Priority summary

| # | Query | Registered | What triggers the fan-out | Hottest key before → after |
|---|---|---|---|---|
| 1 | `homeStories` | **background (every client)** | a story posted by a popular author | 440,241 follow edges → 1 |
| 2 | `homeView` | **background (every client)** | a `work_covers` / `works` edit | 113,256 readthroughs (+ 3.3M through related items) → 1 |
| 3 | `myClubs`, `myClubsList` | **background (every client)** | a `works` / `profiles` edit | **full table scan** (needs indexes, see §B) |
| 4 | `connectionsView` | per profile screen | profile edit or follow/unfollow involving a popular account | 440,241 → 1 |
| 5 | `profileView`, `profileContentMonthView` | per profile visited | a `work_covers` / `works` edit | 113,256 → 1; `showcase` full scan → 1 |
| 6 | `inboxActorView` | per notification actor | a `work_covers` / `works` edit | 113,256 → 1 |
| 7 | `workById` | per work opened (accumulates; 218 seen in one client group) | a tag, work, cover or contributor edit | 469,574 / 5,076 / 3,413 → 1 |
| 8 | `libraryListView` | Library tab | a catalog series or contributor edit | 13,583 / 10,045 → 1 |
| 9 | `profileContentShowcase` | per profile | a `work_covers` edit | 9,435 → the owner's readthroughs |
| 10 | `libraryWorksPage` | Library tab | a `work_covers` edit | 9,435 → the owner's readthroughs |
| 11 | `clubView`, `clubPreview` | per club | a `works` / `profiles` edit | **full table scan** → the club's rows |
| 12 | `sbvSearchView` | per search | a `works` / `work_covers` edit | all users' results containing the work → 1 |
| 13 | catalog `*ById` stragglers | per screen | a `works` edit | ≤ 151 → 1 (low) |

The rows fetched cost roughly 60–70 µs each on this replica. That's the rate
measured for the `homeView` repro, which spent almost all its time in SQLite
stepping rows. So 440k rows is about 30 s of push work **per client group
that has the query registered**.

---

## A. Queries to change

### 1. `homeStories` — `packages/story/zero-schema/src/queries.ts:235`

When author A posts a story, the `authorStories` join fetches
`social_connections WHERE target_user_id = A`, which is **every follower of
A**: 440,241 for the house account. That happens in every connected client
group, and all but one row is thrown away at the root. This is a background
query, and stories are posted all the time, so it is probably costing them
already.

```ts
export const homeStoriesQuery = ({ userId, now }: StoryQueryArgs) =>
  zql.profiles
    .where("user_id", "=", userId)
    .related("ownStories", (story) =>
      story
        .where((eb) => withinVisibilityWindow(eb, now))
        .orderBy("created_at", "desc")
        .related("viewerStoryViews", (view) =>
          view.where("user_id", "=", userId),
        ),
    )
    .related("followingEdges", (edge) =>
      edge
        // FIX: restate the root pin. A story insert pushes through
        // authorStories (target_user_id); without this the fetch is every
        // follower of the author, not the viewer's one edge.
        .where("user_id", "=", userId)
        .where("social_connection_type", "=", "following")
        .where((eb) =>
          eb.or(
            eb.cmp("target_privacy_level", "IS NOT", "friends_only"),
            eb.cmp("is_reciprocal_follow", "=", true),
          ),
        )
        .related("authorStories", (story) =>
          story
            .where("is_private", "=", false)
            .where("user_privacy_level", "IS NOT", "only_me")
            .where((eb) =>
              withinVisibilityWindow(eb, now - STORY_WINDOW_ANCHOR_MILLIS),
            )
            .orderBy("created_at", "desc")
            .related("viewerStoryViews", (view) =>
              view.where("user_id", "=", userId),
            ),
        ),
    );
```

The pinned fetch `(user_id, target_user_id)` uses the table's primary key.

### 2. `homeView` — `packages/home/zero-schema/src/queries.ts:346`

Full analysis and repro: `homeview-runaway-push.md`. Add the pin to the three
user-owned branches. The rest of the query is unchanged.

```ts
    .related("ownReadthroughs", (readthrough) =>
      readthrough
        .where("user_id", "=", userId) // FIX
        .where("status", "=", "in_progress")
        // …unchanged…
    )
    .related("ownLastFinishedReadthrough", (readthrough) =>
      readthrough
        .where("user_id", "=", userId) // FIX
        .where("status", "=", "finished")
        // …unchanged…
    )
    .related("ownWantToRead", (wantToRead) =>
      wantToRead
        .where("user_id", "=", userId) // FIX
        .orderBy("created_at", "desc")
        // …unchanged…
    )
```

| Push edge | Hottest key before | After (index) |
|---|---|---|
| `ownReadthroughs.work` / `ownLastFinishedReadthrough.work` | 113,256 | 1 (`idx_readthroughs_user_id_work_id`) |
| `…relatedItems.relatedWork` → anchor readthroughs | 3.3M row visits for one cover (measured: 235 s) | ≤ 233 lookups |
| `…preferredCover` | 9,435 | the viewer's readthroughs (`idx_readthroughs_user_id`) |
| `ownWantToRead.wantToReadSeries` | 4,480 | 1 (`idx_want_to_read_user_id_series_id`) |

### 4. `connectionsView` — `packages/social-connections/zero-schema/src/queries.ts:597`

The doc comment says "the correlation supplies the root `target_user_id =
target` filter". That's true for hydration but not for push. In
`followingEdges`, the `followedProfile`, `edgesToTarget` and `edgesFromTarget`
edges (and the server guard's `exists` subqueries) are keyed on
`target_user_id`. So when a popular account edits its profile, or anyone
follows or unfollows it, the push fetches **all of its followers** (440,241).
The partitioned `limit` then does a take-state lookup for each one.
`followerEdges` is the mirror image, with a much smaller hottest key (432),
but pin it too.

```ts
function connectionsViewBase(caller, target, leg, followersLimit, followingLimit) {
  const withBranches = profileBase(caller, target)
    .related("followerEdges", (q) => {
      const shaped = q
        // FIX: restate the correlation's pin (profiles.user_id = target).
        .where("target_user_id", "=", target)
        .where("social_connection_type", "=", "following")
        .where((eb) => listDisplayFilter(eb, "follower"))
        .orderBy("created_at", "desc")
        .orderBy("user_id", "asc")
        .related("followerProfile")
        .related("edgesToUser", (e) =>
          e
            .where("user_id", "=", caller)
            .where("social_connection_type", "=", "following"),
        )
        .related("edgesFromUser", (e) =>
          e
            .where("target_user_id", "=", caller)
            .where("social_connection_type", "=", "following"),
        )
        .limit(clampConnectionsListLimit(followersLimit));
      return leg === "server"
        ? shaped.where((eb) => listServerGuard(eb, caller, target, "follower"))
        : shaped;
    })
    .related("followingEdges", (q) => {
      const shaped = q
        // FIX: without this, a change to any account the target follows
        // fetches that account's entire follower list.
        .where("user_id", "=", target)
        .where("social_connection_type", "=", "following")
        .where((eb) => listDisplayFilter(eb, "followed"))
        .orderBy("created_at", "desc")
        .orderBy("target_user_id", "asc")
        .related("followedProfile")
        .related("edgesToTarget", (e) =>
          e
            .where("user_id", "=", caller)
            .where("social_connection_type", "=", "following"),
        )
        .related("edgesFromTarget", (e) =>
          e
            .where("target_user_id", "=", caller)
            .where("social_connection_type", "=", "following"),
        )
        .limit(clampConnectionsListLimit(followingLimit));
      return leg === "server"
        ? shaped.where((eb) => listServerGuard(eb, caller, target, "followed"))
        : shaped;
    });
  // …unchanged…
}
```

`followersOfQuery` / `followingOfQuery` are rooted directly at
`social_connections` with these predicates, so they are already fine.

### 5. `profileView` and `profileContentMonthView` — `packages/social-connections/zero-schema/src/profile-content-server-queries.ts:400` and `:714`

These have `homeView`'s shape, rooted at the profile being viewed and
registered for each profile opened. `dataTierVisibleForOwner(…, input.userId)`
doesn't count as a pin, because its `user_id` test sits inside an `OR`. Add a
top-level `.where("user_id", "=", input.userId)` to every user-owned branch:

```ts
    .related("profileShowcase", (showcase) =>
      showcase
        .where("user_id", "=", input.userId) // FIX: showcase has no index on
        //   work1_id…work5_id, so today every works/covers edit makes this
        //   push a FULL SCAN of userspace.showcase (11,665 rows), ×5 edges.
        .related("showcaseWork1", /* …unchanged… */)
        // …showcaseWork2–5 unchanged…
        .related("showcaseReadthroughs1", (readthrough) =>
          readthrough
            .where("user_id", "=", input.userId) // FIX (and 2–5)
            .where("preferred_cover_id", "IS NOT", null)
            .where((eb) =>
              showcaseCoverReadthroughVisible(asShowcaseCoverBuilder(eb), caller),
            )
            .related("preferredCover"),
        ),
        // …showcaseReadthroughs2–5: same one-line fix…
    )
    .related("profileReadthroughs", (readthrough) =>
      readthrough
        .where("user_id", "=", input.userId) // FIX: work edge, 113,256 → 1
        .where((eb) => eb.or(/* …month window, unchanged… */))
        .where((eb) =>
          dataTierVisibleForOwner(asDataTierBuilder(eb), caller, input.userId),
        )
        .orderBy("touched_at", "desc")
        .related("work", /* …unchanged… */)
        .related("preferredCover")
        .related("siblingReadthroughs", (sibling) =>
          sibling
            .where("user_id", "=", input.userId) // FIX: preferredCover edge
            .where("preferred_cover_id", "IS NOT", null)
            .where((eb) =>
              dataTierVisibleForOwner(asDataTierBuilder(eb), caller, input.userId),
            )
            .related("preferredCover"),
        ),
    )
    .related("profileReadingSessions", (session) =>
      session
        // …unchanged; its `readthrough` hop already pins user_id…
        .related("readthrough", (readthrough) =>
          readthrough
            .where("user_id", "=", input.userId)
            // …
            .related("siblingReadthroughs", (sibling) =>
              sibling
                .where("user_id", "=", input.userId) // FIX
                .where("preferred_cover_id", "IS NOT", null)
                // …unchanged…
            ),
        ),
    )
    .related("profileWantToRead", (item) =>
      item
        .where("user_id", "=", input.userId) // FIX: series edge 4,480 → 1
        .where((eb) =>
          dataTierVisibleForOwner(asDataTierBuilder(eb), caller, input.userId),
        )
        // …unchanged…
    )
    .related("profileFavorites", (item) =>
      item
        .where("user_id", "=", input.userId) // FIX: work 5,477 / series 1,372
        //   / contributor 843 → 1 (idx_favorites_user_id_*)
        .where((eb) =>
          dataTierVisibleForOwner(asDataTierBuilder(eb), caller, input.userId),
        )
        // …unchanged…
    );
```

`profileContentMonthViewServerQuery` has the same `profileReadthroughs` and
`profileReadingSessions` branches. Apply the same three lines there
(`profileReadthroughs`, and both `siblingReadthroughs`).

### 6. `inboxActorView` — `packages/notifications/zero-schema/src/inbox-actor-view-queries.ts:111`

The app registers this once for each notification actor, so one client group
can hold many. Each `.one()` branch is a `limit(1)` partitioned by `user_id`,
so a `works` or `work_covers` push scans every user's readthroughs of the work
(113,256), exactly like `homeView`'s `ownLastFinishedReadthrough`.

```ts
    .related("actorCurrentlyReading", (readthrough) => {
      const shaped = readthrough
        .where("user_id", "=", actorUserId) // FIX
        .where("status", "=", "in_progress")
        // …unchanged…
    })
    .related("actorRecentlyFinished", (readthrough) => {
      const shaped = readthrough
        .where("user_id", "=", actorUserId) // FIX
        .where("status", "=", "finished")
        // …unchanged…
    })
    .related("actorFavoriteWork", (favorite) => {
      const shaped = favorite
        .where("user_id", "=", actorUserId) // FIX: 5,477 → 1
        .where("work_id", "IS NOT", null)
        // …unchanged…
    });
```

### 7. `workById` — `packages/catalog/zero-schema/src/queries.ts:170`

Rooted at one work, and it **accumulates**: the source notes 218
registrations in one client group. Every branch that leaves the work loses the
`work_id` pin:

| Branch | Push from | Hottest key before |
|---|---|---|
| `tags` → `name` | a `tag_names` edit | 469,574 `work_tags` rows |
| `relatedItems` → `relatedWork` → `covers` | a `works` / primary-cover edit (the backfill) | 5,076 `work_related_items` rows |
| `contributors` → `contributor` / names / pictures | a contributor edit (e.g. a `num_works` recount) | 3,413 `work_contributors` rows |
| `workSeries` → `series` / titles / `coverEntries` | a series edit | 639 `work_series` rows |

```ts
  workById: defineQuery(WorkByIdArgs, ({ args }) =>
    zql.works
      .where("work_id", "=", args)
      .related("titles")
      .related("descriptions")
      .related("covers", (cover) =>
        cover.orderBy("is_primary", "desc").limit(1),
      )
      .related("contributors", (q) =>
        q
          .where("work_id", "=", args) // FIX
          .orderBy("position", "asc")
          .related("contributorNames")
          .related("contributorProfilePictures", (p) =>
            p.where("is_primary", true),
          )
          .related("contributor"),
      )
      .related("workSeries", (q) =>
        q
          .where("work_id", "=", args) // FIX
          .related("seriesTitles")
          .related("series")
          .related("coverEntries", /* …unchanged… */),
      )
      .related("tags", (q) =>
        q
          .where("work_id", "=", args) // FIX: 469,574 → 1
          .orderBy("position", "asc")
          .related("name"),
      )
      .related("goodreadsEditions", (q) =>
        q.orderBy("goodreads_edition_id", "asc").limit(1),
      )
      .related("relatedItems", (q) =>
        q
          .where("work_id", "=", args) // FIX: 5,076 → 1
          .orderBy("position", "asc")
          .limit(30)
          .related("relatedWork", /* …unchanged… */),
      )
      // …remaining branches unchanged…
  ),
```

### 8. `libraryListView` — `packages/library/zero-schema/src/queries.ts:550`

A `catalog.series` edit (or a cover/work push that comes up through
`worksPreview`) fetches **every user's** `user_series` rows for that series
(13,583). A contributor edit fetches every user's `user_contributors` rows
(10,045).

```ts
export const libraryListViewQuery = (userId: string) =>
  zql.profiles
    .where("user_id", "=", userId)
    .related("listUserSeries", (userSeries) =>
      userSeries
        .where("user_id", "=", userId) // FIX
        .orderBy("position", "asc")
        .related("catalogSeries", /* …unchanged… */),
    )
    .related("listUserContributors", (userContributors) =>
      userContributors
        .where("user_id", "=", userId) // FIX
        .orderBy("position", "asc")
        .related("catalogContributor"),
    )
    .one();
```

### 9. `profileContentShowcase` — `packages/social-connections/zero-schema/src/profile-content-server-queries.ts:279`

`showcaseReadthroughs1–5` correlate on `(user_id, workN_id)` from a root pinned
by `user_id = target`. A cover edit fetches every readthrough that prefers that
cover (9,435).

```ts
    .related("showcaseReadthroughs1", (readthrough) =>
      readthrough
        .where("user_id", "=", target) // FIX (same for 2–5)
        .where("preferred_cover_id", "IS NOT", null)
        .where((eb) => showcaseCoverReadthroughVisible(eb, caller))
        .related("preferredCover"),
    )
```

The pinned fetch uses `idx_readthroughs_user_id`, so its cost is the owner's
readthrough count. An index on `readthroughs(user_id, preferred_cover_id)`
would make it exact; the same applies to #10 and to the `preferredCover`
edges in #2, #5 and #6.

### 10. `libraryWorksPage` — `packages/library/zero-schema/src/queries.ts:431`

```ts
    .related("readthroughs", (readthrough) =>
      readthrough
        .where("user_id", "=", userId) // FIX: preferredCover edge, 9,435 →
        //   the user's readthroughs
        .orderBy("touched_at", "desc")
        .related("preferredCover"),
    )
```

### 11. `clubView` and `clubPreview` — `packages/book-clubs/zero-schema/src/queries.ts:341` and `:364`

The club tables are empty in this replica, but their indexes show that today
these pushes are **full table scans**:

- `club_meetings` has no index on `work_id`.
- `club_book_suggestions` has no index starting with `work_id` or `user_id`.

So in each open `clubView`, a `works` edit scans `club_meetings` and
`club_book_suggestions`, and a profile edit scans `club_book_suggestions`.
Pinning `club_id` puts every one of those fetches on an existing index
(`idx_club_meetings_club`, `book_suggestions_club_work_user_unique`,
`members_club_user_unique`).

```ts
export function clubViewQuery(caller: string, clubId: string) {
  return zql.clubs
    .where("club_id", "=", clubId)
    .where((eb) => eb.exists("members", (q) => q.where("user_id", "=", caller)))
    .related("members", (m) =>
      m
        .where("club_id", "=", clubId) // FIX
        .related("profile")
        .related("readthroughs", /* …unchanged… */),
    )
    .related("suggestions", (s) =>
      s
        .where("club_id", "=", clubId) // FIX: scan → (club_id, work_id) prefix
        .related("profile")
        .related("work", /* …unchanged… */)
        .orderBy("created_at", "desc")
        .limit(25),
    )
    .related("meetings", (m) =>
      m
        .where("club_id", "=", clubId) // FIX: scan → idx_club_meetings_club
        .related("polls", /* …unchanged… */)
        .related("work", /* …unchanged… */),
    )
    .one();
}

export function clubPreviewQuery(clubId: string) {
  return zql.clubs
    .where("club_id", "=", clubId)
    .related("members", (m) =>
      m
        .where("club_id", "=", clubId) // FIX
        .limit(PREVIEW_AVATAR_LIMIT)
        .related("profile"),
    )
    .related("meetings", (m) =>
      m
        .where("club_id", "=", clubId) // FIX
        .related("work", /* …unchanged… */),
    )
    .one();
}
```

`meetings.polls.options.work` still scans `poll_options` on a `works` edit.
That node isn't pinned, so no query edit fixes it; it needs the index in §B.

### 12. `sbvSearchView` — `packages/search-by-vibes/zero-schema/src/queries.ts:73`

The `sbv_*` tables aren't in this replica, so this one is unmeasured.
`results` is `limit(48)` partitioned by `search_id`. A `works` edit (including
primary-cover pushes through `work.covers`) fetches `sbv_search_results WHERE
work_id = W`, which is **every user's search result for that book**, each with
a take-state lookup.

```ts
export const sbvSearchViewQuery = (searchId: string, userId?: string) => {
  const base = zql.sbv_searches.where("search_id", "=", searchId);
  const scoped =
    userId === undefined ? base : base.where("user_id", "=", userId);
  return scoped.one().related("results", (result) =>
    result
      .where("search_id", "=", searchId) // FIX
      .orderBy("position", "asc")
      .limit(48)
      .related("work", /* …unchanged… */),
  );
};
```

Check that `sbv_search_results` has an index starting with `search_id` (the
primary key is likely `(search_id, position)`). Without one, the unpinned
fetch is also a full scan.

### 13. Low-impact catalog stragglers

These are the same one-line fix with small hottest keys. Worth doing for
consistency, but not urgent.

| Query | Branch | Add | Hottest key before |
|---|---|---|---|
| `contributorById`, `contributorPreviewById`, `contributorSummaryById` | `works` | `.where("contributor_id", "=", args)` | 151 |
| `seriesById`, `seriesPreviewById` | `works` | `.where("series_id", "=", args)` | 18 |
| `offerByISBN13` | `edition` (built in the shared `offerRows()` helper, which never sees the ISBN) | pass the ISBN into `offerRows` and add `.where("isbn13", "=", isbn)` to `edition`, or leave it | 86 |
| `meetingPolls` | `exists("meeting")` | `.where("meeting_id", "=", meetingId)` | small (per club) |

---

## B. Index gaps no query edit can fix

These push fetches have **no usable index**, so SQLite scans the whole table.
Their nodes aren't pinned by a literal, so no query edit reaches them. They
need Postgres indexes (Zero replicates plain, non-partial, non-expression
indexes):

| Table (column) | Scanned by | Triggered by |
|---|---|---|
| `club_meetings (work_id)` | `myClubs`, `myClubsList` (**background**), `clubView` before the fix | any `works` edit, including primary-cover pushes |
| `club_book_suggestions (work_id)` | `myClubs` (**background**) | any `works` / primary-cover edit |
| `club_book_suggestions (user_id)` | `myClubs` (**background**) | any `profiles` edit |
| `poll_options (proposed_work_id)` | `myClubs` (**background**), `clubView` | any `works` / primary-cover edit |

These tables are empty today, so nothing is scanned yet. But `myClubs` is
registered by every client. Once clubs are used, each works edit (the cover
backfill included) and each profile edit will scan these tables in every
connected client group. This is the same failure as the original
`work_isbn13s.cover` join, so fix it before the feature grows:

```sql
CREATE INDEX idx_club_meetings_work ON userspace.club_meetings (work_id);
CREATE INDEX idx_club_book_suggestions_work ON userspace.club_book_suggestions (work_id);
CREATE INDEX idx_club_book_suggestions_user ON userspace.club_book_suggestions (user_id);
CREATE INDEX idx_poll_options_proposed_work ON userspace.poll_options (proposed_work_id);
```

---

## C. Flagged but not worth changing

- **`libraryWorkGraphs`** (`listItems.list`, `recommendations.source`): the
  fan-out stays inside the owner's own lists and sources. Pinning `user_id`
  doesn't shrink it (no `(user_id, list_id)` index), and it can't reach other
  users' rows.
- **`profileView` `profileReadingSessions.readthrough`**: the fetch is the
  sessions of one readthrough, which belong to one user. The pin changes
  nothing.
- **`myOrder`** (`shipments.items`): the push fetch is by the shipments
  primary key.

## Caveats

- The replica is a snapshot from Aug 12. Book clubs, orders and search-by-vibes
  had no data yet, so their fixes come from the schema and indexes, not from
  measured counts.
- The hottest key is a single join hop. Chains multiply: `homeView`'s
  related-items path measured **3.3M row visits** for one cover, against a
  single-hop number of 113k.
- Only the server bodies were analyzed, because they are what zero-cache runs.
  The client twins have the same shape; apply the fixes to both (see the
  first section).
- Placeholder arguments (`"arg"`, a record of `arg:<field>`, and 10 for
  limits) stood in for everything except the background queries. They only
  have to be literals for the pin analysis, which is all it needs.
