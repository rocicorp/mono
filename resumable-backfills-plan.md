# Resumable Backfills — Design and Implementation Plan

**Status: R0-R7 implemented** in `packages/zero-cache`, including the soak's
chaos **C15**; R8 (the rollout flip and the dashboard row, which lives in the
controller repo) is not. See §3.10 for R0's measured result and §13 for what
was built differently.
Companion to
[`sqlite-change-log-plan.md`](./sqlite-change-log-plan.md) (`P§`) and
[`sqlite-change-log-backfill-cookies-plan.md`](./sqlite-change-log-backfill-cookies-plan.md)
(`C§`). It adds slices **R0–R8**. R4 shares a change-log schema bump with
whatever `P§` bump is next in line; nothing else interleaves.

Every claim marked **[verified]** was read out of the tree at `e5dfd061f`
(#6507). Everything else is proposal.

The design brief this responds to is "RMv2: Resumable Backfills" (ordered
backfill streams, subscribers declare progress marks, catchup replays from the
log or restarts the backfill). This plan keeps the first two and drops the
third: **there is no log-replay path**. §3.1 and Appendix A say why.

---

## 0. The governing premise

A backfill is a *run*: one snapshot, one ordered pass over a table's rows,
one completion. Today a run is anonymous, unordered, and private to the
replication-manager that started it. A subscriber that moves between
replication-managers mid-run can therefore be completed early (rows missing),
never completed (the new manager already finished), or fed rows from two
snapshots with a hole between them. Those are the brief's Scenarios A, B, C.

Everything below follows from three rules:

> **1. Rows are ordered by the row key, in Postgres, and only Postgres ever
> compares keys.** A subscriber records the last key it applied; the change
> source asks Postgres what comes after it. No collation is reproduced in
> SQLite or JavaScript.
>
> **2. A run is explicit and a subscriber either follows it or does not.** A
> run announces itself; a subscriber that has seen the announcement and every
> message since has everything the run sent from the announced point. Only a
> following subscriber advances its mark or honors the run's completion.
>
> **3. Backfill transaction versions are replica-local.** The version a
> replication-manager stamps on a backfill transaction orders that manager's
> stream. It means nothing to another manager, so no subscriber ever hands one
> back to a change-streamer.

Rule 1 replaces `COLLATE "C"` and the log's new columns. Rule 2 replaces
"ignore redundant messages" with a decision the subscriber can make without
comparing keys. Rule 3 closes the gap that makes cross-manager bounces fail
today, which the brief did not name.

---

## 1. Current state

### 1.1 A backfill run is unordered and anonymous

| what | site **[verified]** |
| --- | --- |
| COPY has no `ORDER BY`; the SELECT is column list + row filter only | `pg/initial-sync.ts:786-826` (`makeDownloadStatements`) |
| a run is a snapshot from a temporary slot, then one COPY | `pg/backfill-stream.ts` (`createSnapshotTransaction`, `stream`) |
| messages carry `relation`, `columns`, snapshot `watermark`, `rowValues`, `status`; nothing identifies the run | `protocol/current/data.ts:272-325` |
| one run at a time, picked at random from the required set | `common/backfill-manager.ts` (`#checkAndStartBackfill`) |
| a run's transactions are minted at the stream's last watermark plus one minor | `common/backfill-manager.ts:263` |
| a row-key change on the running table cancels the run; the retry must snapshot after it | `common/backfill-manager.ts` (`case 'update'`, `beginTxFor`) |
| requests enter only at `startStream`; the upstream direction is status-only | `change-source/change-source.ts:34-37`, `protocol/current/upstream.ts` |
| custom change sources reject backfill requests outright | `custom/change-source.ts:143-148` |

### 1.2 The subscriber cannot tell a needed message from a redundant one

| what | site **[verified]** |
| --- | --- |
| a backfilled row is skipped only if a delete postdates the snapshot or every backfilling column has a newer replicated version | `replicator/change-processor.ts:911-926` |
| per-column versions are recorded only while the column is in the table's `backfilling` set | `replicator/change-processor.ts:846-863`, `replicator/schema/change-log.ts:63-68` |
| `backfill-completed` clears the columns and bumps every row version in the table, unconditionally | `replicator/change-processor.ts:949-966` |
| in-flight backfills per replica are keyed by upstream identity in `_zero.backfilling` | `replicator/schema/backfilling.ts:62-70` |
| a replica can reconstruct its outstanding requests | `replicator/schema/backfilling.ts:194` (`readBackfillRequests`) |

The consequence for Scenario A: once a column's backfill has completed on a
subscriber, a later `backfill` message for it from an older snapshot passes the
guard at `change-processor.ts:921` (no per-column version is recorded any
more) and overwrites newer values. The redundant `backfill-completed` then
resets the whole table for IVM. "Ignore redundant messages" is not a
one-liner today; it needs the state this plan adds.

### 1.3 Backfill transaction versions collide across managers

| what | site **[verified]** |
| --- | --- |
| a subscriber resumes at its replica's full `stateVersion`, minor included | `replicator/incremental-sync.ts:95-108`, `replicator/schema/replication-state.ts` |
| PG catchup resumes *after* the first entry whose watermark equals the subscriber's, else `WatermarkTooOld` (or auto-reset for a backup subscriber) | `change-streamer/storer.ts:940-975` |
| SQLite catchup requires a `commit` row at exactly the subscriber's watermark | `change-streamer/sqlite-change-log-reader.ts` (`SQLITE_CHANGE_LOG_BOUNDARY_SQL`) |
| live sends are filtered by `watermark > subscriber.watermark` | `change-streamer/subscriber.ts:150` |
| a state version is `major` (upstream LSN) plus optional `minor` | `types/state-version.ts` |

Two managers under the same upstream watermark `M` both mint `M.1, M.2, …`
for different rows. A subscriber whose last commit was manager A's `M.3`
subscribing to manager B at `M.3` either lands on B's own `M.3` and silently
skips B's `M.1..M.3`, or finds no `M.3` and is told `WatermarkTooOld`, which
`IncrementalSyncer` answers with a full replica restore. During a backfill on a
quiet upstream most commits are backfill commits, so the second outcome is the
common one: **today a cross-manager bounce mid-backfill is usually a restore.**
Marks alone do not fix this.

### 1.4 The cookie jar already carries the identity half

| what | site **[verified]** |
| --- | --- |
| one fold, three interpreters (PG storer, SQLite writer, replica tracker) | `replicator/change-log-cookies.ts` (`cookieOps`), `storer.ts:1044`, `change-log-stream-writer.ts:158-162`, `replicator/schema/backfilling.ts` |
| the initializer folds a replica-derived set forward over the log's schema changes in `(replicaVersion, head]` | `change-streamer/change-log-initializer.ts:298-315`, `readSchemaChanges` at `:456-482` |
| the range read is tag-filtered and capped at `MAX_FOLD_SCAN_ROWS = 10_000` rows | `change-log-initializer.ts:147` |
| schema-change tags are a closed list | `protocol/current/schema-change-tags.ts` |
| change-log schema is at v4 | `replicator/change-log-db.ts:81` |
| replica schema is at v17 (`_zero.backfilling` was v17) | `common/replica-schema.ts:344-353` |

### 1.5 Transport and compatibility surfaces

| what | site **[verified]** |
| --- | --- |
| the subscriber context travels as URL query parameters on the WebSocket upgrade | `change-streamer-http.ts:265-282`, `getParams` at `:305-318` |
| unknown query parameters are ignored by the server | `types/url-params.ts` (lookup by name) |
| a client newer than the server is rejected; the server supports clients back to v4 | `change-streamer-http.ts:35`, `:284-302` |
| messages can be gated per subscriber protocol version | `subscriber.ts:355-360` (`supportsMessage`) |
| the stream is parsed in passthrough mode, so unknown fields are ignored by older peers | `change-streamer.ts` (protocol history comment) |
| there is no client→server message path on the changes socket | `change-streamer-http.ts` (`streamOutStringified` only) |
| `PROTOCOL_VERSION = 6` | `change-streamer.ts:113` |

### 1.6 Key types and key nullability

| what | site **[verified]** |
| --- | --- |
| a synced table's key is a unique index whose columns are all `NOT NULL` | `db/lite-tables.ts:292-305` |
| timestamps decode to fractional milliseconds; numerics decode lossily | `db/pg-copy-binary.ts:335-344`, `decodeNumeric` |
| int2/int4/int8, text/varchar, uuid, bool round-trip exactly | `db/pg-copy-binary.ts` (`makeBinaryDecoder`) |
| collation determinism is already reported per column | `pg/schema/published.ts:50` |

---

## 2. Design

### 2.1 Ordered, resumable runs (change source)

The backfill SELECT gains `ORDER BY <rowKey columns>` in each column's own
collation, and, when resuming, `WHERE (<rowKey columns>) > (<mark literals>)`
ANDed with the publication row filter. Row-value comparison with a same-order
`ORDER BY` is index-optimizable on the key's btree in Postgres, and the
comparison uses the index's collation because the WHERE names the columns.

`COPY (query) TO STDOUT` accepts no bind parameters, so the mark is inlined as
literals. Resume is offered only for key types whose text form is exact and
whose literal is trivially safe to inline:

| type | literal |
| --- | --- |
| int2, int4, int8 | validated `^-?[0-9]+$`, bare |
| text, varchar | `E'…'`, with `\` and `'` escaped |
| uuid | validated canonical form, `'…'::uuid` |
| bool | `true` / `false` |

**[measured, R0]** The literal must be a *constant* expression. The originally
proposed `convert_from(decode('<hex>', 'hex'), 'UTF8')` is not: `convert_from`
is `STABLE`, so Postgres renders the predicate as a `Filter` rather than an
`Index Cond`, and every resume scans the index from the beginning instead of
seeking. An `E'…'` literal produces a true index seek and has the same meaning
regardless of `standard_conforming_strings`. `backfill-resume.pg.test.ts`
asserts `Index Cond` and the absence of a `Sort` for int, text and uuid keys.

Any other key type, and any key with a non-deterministic collation, is
**not resumable**: the change source never attaches a `lastKey`, so no
subscriber ever holds a mark, so every run starts from the beginning. That is
today's behavior, kept for the cases that need it.

**A key that is expensive to order is also not resumable (§3.10).** Ordering
is offered only when the heap's physical order already matches the key order,
which is what makes the ordered COPY nearly as fast as today's.

A **mark** is `string[]`: the Postgres text form of the row key values, in
`relation.rowKey.columns` order. The change source computes it from the
decoded last row of each message (it knows the types); the subscriber stores
and returns it opaquely; equality is on `JSON.stringify(mark)`.

### 2.2 Explicit runs (protocol v7)

New data message, emitted as the first message of every run and re-emitted
to cover a late subscriber (§2.5):

```ts
{
  tag: 'backfill-started',
  relation, columns,          // as on `backfill`
  watermark: string,          // the run's snapshot
  runID: string,              // random, unique across managers
  resumeFrom: string[] | null // the mark this run (or this announcement) covers from
}
```

`backfill` gains `runID` and `lastKey: string[] | undefined` (undefined when
the key is not resumable). `backfill-completed` gains `runID`. The `begin`
message of a backfill transaction gains `backfill: true` (today the only
marker is `skipAck`, which is not a statement of kind).

Semantics: **a subscriber that has processed `backfill-started(R, m)` and
every subsequent message of the stream holds every row of run `R` whose key
sorts after `m`** (all rows when `m` is null). This is what makes following
decidable without comparing keys: the stream is gap-free from any point the
subscriber has, so seeing the announcement is seeing the run.

New upstream message, change-streamer → change-source:

```ts
['backfill-request', {
  table, columns,             // a BackfillRequest, folded to the log head
  mark: string[] | null,
  markWatermark: string | null,
  runID: string | null,       // the run the subscriber is following, if any
}]
```

`BackfillRequest` (already passed at `startStream`) gains optional
`resumeFrom`, `resumeFromWatermark`, and `minSnapshot` (§2.6).

`PROTOCOL_VERSION` 6 → 7. `Subscriber.supportsMessage` drops
`backfill-started` for v6 subscribers; they ignore the new fields and behave
exactly as today. A v7 subscriber cannot reach a v6 server at all (the path
version is rejected), so there is no new-client/old-server case.

### 2.3 Subscriber state and rules (replicator)

`_zero.backfilling` (replica schema v18) gains four nullable columns:
`mark TEXT`, `markWatermark TEXT`, `runID TEXT`, `minSnapshot TEXT`. The
first three are *subscriber state* and are excluded from the cookie set the
initializer compares. The fourth is a cookie (§2.6).

Let `B(T)` be the columns of `T` currently in `_zero.backfilling`. Rules, in
`ChangeProcessor`:

| message | rule |
| --- | --- |
| `backfill-started(R, resumeFrom)` | for each column in `columns ∩ B(T)`: if `resumeFrom` is null, or equals the column's `mark`, or the column's `runID` is already `R`: set `runID = R`. Otherwise set `runID = NULL`. Never touch `mark`. |
| `backfill(R, rows, lastKey)` | apply rows for `columns ∩ B(T)` only (the `DO UPDATE SET` lists only those). If empty, skip the message. For each applied column with `runID = R` and `lastKey` present: `mark = lastKey`, `markWatermark = watermark`. |
| `backfill-completed(R)` | `C' = columns ∩ B(T)` with `runID = R`. If empty, ignore. Else clear `C'` from `column_metadata.backfill` and `_zero.backfilling`, and bump the table's row versions once. |
| key-changing `update` on `T` (any `rowKey` column differs between `key` and `new`) | for every row of `T` in `_zero.backfilling`: `mark = NULL`, `markWatermark = NULL`, `minSnapshot = <tx version>`. Keep `runID`. |

Keeping `runID` on a key change is deliberate: the manager does not cancel a
run whose rows were all sent before the change (the cancel check runs at the
next data message, `beginTxFor`), so its completion is valid and a following
subscriber must honor it. Only the *mark* is unsafe after a key change (§3.6),
and the mark is what is cleared.

Declaration: `readBackfillDeclarations(db)` returns one entry per table with
in-flight columns, `mark`/`markWatermark`/`runID` set to the columns' common
value when all columns agree and null otherwise. It is sent in the subscribe
context as the `backfills` query parameter (JSON).

### 2.4 Replica-local backfill versions (replicator + change-streamer)

The replicator subscribes at the **major** of its state version. When it
applies a transaction whose `begin` carries `backfill: true`, it commits at a
local version: the incoming version if that is above the replica's current
state version, else the current major with the current minor plus one. The
existing guarantee that a completion transaction's version is at least the
snapshot watermark is preserved, because the local version is never below the
incoming one.

Consequences: PG and SQLite catchup find the subscriber's boundary (the major
is a real upstream commit); manager B's `M.k` transactions pass the live
filter and are applied; a same-manager reconnect re-delivers `M.1..M.k`,
which the following subscriber re-applies idempotently. The `_0_version` of
backfilled rows is still the snapshot watermark, unchanged.

### 2.5 The manager (change source)

`#requiredBackfills` entries become
`{request, minWatermark, resumeFrom, pendingDeclarations}`; `minWatermark`
moves here from the running state so a key change on a table whose run is not
active is not forgotten. Running state gains `runID`, `startMark`, `lastMark`
(last *pushed* key).

On `backfill-request` for table `T` with `(mark m, markWatermark s, runID r)`:

1. If `m` is not null and `s < entry.minWatermark`: `m = null`.
2. If `T` is not required: add it from the declaration (Scenario B). Its
   mark is always dropped — the manager has no key-change memory for a table
   it finished (§3.7).
3. If a run `R` for `T` is active:
   - `r == R`: the subscriber is following; nothing to do.
   - else ask Postgres `rowsExist(T, m, lastMark)` — are there rows with key in
     `(m, lastMark]` under the row filter (with `m` null, the range is
     everything up to `lastMark`, and the answer is yes if any row exists):
     - **yes**: cancel `R`, set `resumeFrom = m`, start a new run. Its first
       message is `backfill-started(R', m)`.
     - **no**: re-emit `backfill-started(R, m)` on the running stream, without
       restarting the COPY. The subscriber's mark equals `m`, so it starts
       following `R`; existing followers are unaffected.
4. If no run is active: `resumeFrom = m` if the entry has none yet, else
   queue `m`; when the run starts, drain the queue through step 3.

Restarts only move the start mark downward, so they terminate. `rowsExist` is
one index-seeking query per declaration and is the only key comparison
outside the COPY itself.

On a key-changing `update` for any table: record `minWatermark = tx version`
on its entry if required, drop its queued marks, and cancel a running run as
today. The run restarts with `resumeFrom = null`.

The `beginTxFor` snapshot check (`msg.watermark < minWatermark` → cancel)
extends to `backfill-started`.

### 2.6 Cookie fold extension: `minSnapshot`

A key-changing `update` on table `T` yields a new cookie op
`{op: 'invalidate-marks', table}`; each interpreter stamps its own
transaction version. Interpreters:

| store | action |
| --- | --- |
| SQLite log `_zero.changeLogBackfilling` | `UPDATE … SET minSnapshot = ? WHERE schema = ? AND table = ?` (no-op if no row) |
| replica `_zero.backfilling` | the same, plus `mark = NULL, markWatermark = NULL` (§2.3) |
| PG `cdc.backfilling` | **no-op**; PG is being retired and this column is an annotation, not a transition (§3.5) |

`backfillRequestsFrom(cookies)` carries `minSnapshot` (max over the table's
columns) into the initial `BackfillRequest`s, which seeds the manager's
`minWatermark` at every `startStream`. The initializer's canonical rendering
excludes the column so the three-store comparison is unchanged.

Detecting the key change is the manager's existing test (`key !== null` and
some `relation.rowKey.columns` value differs), applied at the writer's
`append`, the tracker's `apply`, and the storer's queue only when the tag is
`update` and `key` is non-null. Replica-identity-full tables pay a
per-update comparison of key columns; that is the cost the manager already
pays.

### 2.7 The change-streamer

On `subscribe(ctx)` with a non-empty `ctx.backfills`, after the catchup head is
pinned:

1. If there is no SQLite log (`sqliteChangeLogMode = off`, or `not-ready`):
   drop the declarations with one warning. Resume needs the log.
2. Read the log's schema changes and `backfill-started` messages over
   `(ctx.watermark, head]` — the initializer's `readSchemaChanges`, moved to a
   shared module and given a second tag list, with the same
   `MAX_FOLD_SCAN_ROWS` behavior (over the cap: drop declarations, warn).
3. Fold each declaration's *identity* forward with `foldCookies`' rename/drop
   ops. Do **not** drop on `complete-backfill`: a completion the subscriber
   has not followed is not its completion (Scenario B).
4. For each declared table: if the last `backfill-started` for it in the
   range has `resumeFrom` null or equal to the declared mark, the subscriber
   will follow that run from catchup; forward nothing for it.
5. For each declared table not in the log's cookie jar: `mark = null`
   (Scenario B; §3.7).
6. Forward the rest as `backfill-request` on the change stream's upstream
   sink. Remember the forwarded declarations on the `Subscriber`; on every
   stream (re)connect, re-forward the declarations of all connected
   subscribers after `startStream`.

At `startStream`, the initial requests also carry `resumeFrom` from the
replication-manager's **own replica** (`_zero.backfilling.mark`, folded over
`(replicaVersion, head]` and voided by `minSnapshot`), so a manager restart
resumes where its backup replicator left off rather than from zero. The backup
replicator then declares the same mark, matches `resumeFrom` by equality, and
follows with no redundant rows.

The subscribe query string can carry a few hundred bytes per declared table.
Node's default 16KB request-header limit is raised to 64KB on the
change-streamer's HTTP server, and the encoding drops table metadata when the
key is not resumable. If real deployments exceed that, the escape hatch is a
first client frame on the socket, which also enables re-declaration after
catchup; not built now.

---

## 3. Decisions

### 3.1 No log replay; restart from the mark instead

The brief's "Catchup Changes" section stores per-message key ranges in the
log so a subscriber behind the log can be replayed from its mark. Not taken:

- With the default one-minute retention (`sqliteChangeLogRetentionMs`) plus
  the backup floor, an in-flight run's transactions are purged long before
  the run ends. Making replay useful means retaining them until completion,
  which is a second copy of the table in the log for the run's duration.
- It is the only place a key comparison would happen outside Postgres, and
  therefore the only reason for `COLLATE "C"`.
- The one case replay serves well — a subscriber restoring from a backup
  taken mid-run — is already served by the existing purge floors: the backup
  watermark pins everything above it, and the restored replica carries the
  backup replicator's `runID`, so the subscriber follows through catchup with
  no replay machinery (§7, case F).

Restarting from the mark costs a fresh snapshot and re-sending `(m, L]` to
subscribers that already have it. That is bounded and rare (§7).

### 3.2 Native collation, comparisons only in Postgres

`ORDER BY k COLLATE "C"` cannot use a text key's index unless the column is
already `COLLATE "C"`: the COPY would sort the whole table before its first
row, and the resume WHERE could not seek. Ordering in the column's native
collation keeps the index and is a total, snapshot-stable order, which is all
resume needs. Non-deterministic collations are excluded not because
comparison is ambiguous — a unique index under such a collation enforces
uniqueness by the same equality — but to keep the first cut's allowlist
conservative.

The remaining risk is a collation *version* change between two runs (a glibc
or ICU upgrade). That already corrupts Postgres's own btree indexes, and
Postgres warns about it; nothing here makes it worse.

### 3.3 Following by announcement, not by key comparison

The subscriber has to answer "may I honor this completion?" without ordering
keys. The run announcement makes that an equality: the subscriber's mark
equals the announced `resumeFrom`, or the announcement covers everyone, or the
subscriber was already following. Every other formulation examined
(sequence numbers, start marks alone, lists of covered marks) either fails for
a subscriber that joins mid-run or needs the manager to route a per-subscriber
answer; re-announcing on the broadcast stream is both simpler and ordered
with the run's messages.

### 3.4 Marks are subscriber state, not cookies

A mark changes on every backfill transaction. Putting it in the cookie jar
would make the three-store comparator fold backfill transactions too, and
would put a per-transaction cookie write on the PG storer. Marks live in each
replica's `_zero.backfilling` and travel in the subscribe context. The
replication-manager reads its own replica's marks at `startStream` because
its backup replicator is a subscriber like any other.

### 3.5 `minSnapshot` is a cookie, in two of three stores

Whether a mark is still valid depends on key changes the subscriber has not
processed, which can predate the current manager session. That needs a
durable, per-table "earliest valid snapshot", folded like the other cookies
and seeded into the manager on every `startStream`. It is added to the
SQLite log's cookie table and the replica, and excluded from the canonical
rendering, so PG's `cdc.backfilling` needs no migration. In
`sqliteChangeLogMode = off` there is no jar to hold it, and resume is inert
anyway (§2.7 step 1).

### 3.6 A key change voids marks, not runs

Why a mark is unsafe after a key change on its table: a row whose key moves
from above the mark to below it is never sent by the resumed run (it resumes
above the mark) and was never sent by the original (it was above the mark
then), and the replicated update that moved it may omit an unchanged TOASTed
value of the backfilling column. That is the hazard the manager's existing
cancel already guards against; marks inherit the guard through `minSnapshot`.
A run whose rows were all sent before the change has no such row and completes
normally, so `runID` survives the change.

### 3.7 Scenario B starts from zero

A subscriber declaring a table the manager has already finished has a mark
from another manager's run and the current manager has no `minSnapshot` for a
table it finished (the cookie row is gone). Rather than scan the log range
for key changes on that table, the mark is dropped and the run starts from
the beginning. Scenario B requires two managers at very different points in
the same backfill; the cost is one full backfill of one table, which every
other subscriber ignores via the column guard.

### 3.8 Restart policy: rows-exist, not min-mark

Computing the minimum of several marks needs an ordering. "Are there rows in
`(m, lastMark]`" needs only the table's own index and answers the actual
question — does this subscriber need rows the run has passed — including the
empty-range case for free. Each restart lowers the start; a burst of
declarations at the same mark (a fleet restoring from one backup) produces at
most one restart because the rest match the re-announcement by equality.

### 3.9 Custom change sources

Custom sources reject backfill requests today, so they see no `backfill-request`
upstream message either; the upstream schema is a union and the custom
source's sink ignores anything but status. A custom source that later
supports backfill implements ordering and `lastKey` or leaves `lastKey`
undefined and gets restart-from-zero behavior. Protocol version bump per
`protocol/current` conventions.

### 3.10 Ordering is gated on heap correlation **[decided, R0]**

Ordering a backfill COPY by the row key means walking the key's index in order
and fetching each row from the heap. The index is sorted; the *heap* is not.
When the two orders agree the fetches are sequential and ordering is nearly
free. When they do not — a random uuid or nanoid key — each of N rows becomes
a scattered visit into the same pages, with no readahead and repeated visits
per page, and throughput collapses.

Measured with `backfill-resume.bench.pg.ts`, 1M rows (~155MB), warm cache,
PG 17; ordered COPY as a fraction of unordered:

| key shape | correlation | ordered / unordered |
| --- | --- | --- |
| int8, inserted in order | 1.0 | 82% |
| int8, lightly shuffled | 0.99995 | 51% |
| int8, more shuffled | 0.995 | 32% |
| int8, heavily shuffled | 0.70 | 22% |
| uuid, random | ~0 | 18% |
| text, random (nanoid-like) | ~0 | 17% |
| uuid, random, `CLUSTER`ed | 1.0 | 80% |

Two of §5.1's proposed mitigations do not hold. Postgres does **not** choose a
sort over the index scan on its own — it picks the index scan in every case
above. Forcing the sort (`enable_indexscan = off`, raised `work_mem`) lifts the
random-uuid case to ~55% but leaves the random-*text* case at ~19%, because
sorting a million collated strings costs more than the random heap access it
avoids. `CLUSTER` does hold.

So a run is ordered — and therefore resumable — only when
`pg_stats.correlation` for the leading row key column is at least
`MIN_KEY_CORRELATION` (0.9999). Correlation saturates near 1, so the threshold
has to be tight to hold ordering within ~2x of an unordered COPY. In practice
this admits append-mostly tables with a monotonic key, and `CLUSTER`ed tables.

A table that fails the gate takes exactly the §7 case I path: no `lastKey`, no
marks, `backfill-started(R, null)` on every run, and today's unordered COPY at
today's cost. It still gets the column guard, the following rule, and
replica-local backfill versions, which §9 already notes are unconditional
correctness fixes. The decision is per run, and a table may pass in one
manager and fail in another; the protocol already covers that, because a
subscriber holding a stale mark is announced to with `resumeFrom = null` and
follows from zero.

A never-analyzed table has no `pg_stats` row; that is treated as failing the
gate, which keeps today's behavior for a table whose cost is unknown.

---

## 4. Invariants

1. **Order.** A run's rows are emitted in the row key's Postgres order, and a
   resumed run's first row sorts strictly after its `resumeFrom`.
2. **Following.** A subscriber holds `runID = R` for a column only if it has
   processed `backfill-started(R, m)` with `m` null or equal to its mark at
   the time, and every stream message since. (Same-manager re-delivery
   re-establishes the same `R`.)
3. **Marks advance only while following.** A persisted `(mark, markWatermark,
   runID)` means: every row of run `runID` with key ≤ `mark`, as of snapshot
   `markWatermark`, has been applied, subject to later replication.
4. **Completion needs following.** `backfill-completed(R)` clears a column
   only where `runID = R`.
5. **A key change voids marks.** After a key-changing update at version `v`
   on `T`, no mark on `T` with `markWatermark < v` is used to resume; the
   manager, the log, and every replica agree on this through `minSnapshot`.
6. **Versions are local.** No subscriber sends a minor version to a
   change-streamer; every backfill transaction commits at a version above the
   replica's current state version.
7. **Restarts move backward and terminate.** A run is restarted only to a
   mark strictly below its start (rows exist between), and marks are bounded
   below.
8. **The column guard.** A `backfill` row never writes a column that is not
   in the replica's backfilling set; a completion never bumps a table for
   which no column completed.
9. **No key comparison outside Postgres.** The change-streamer and replicator
   compare marks by string equality only.

---

## 5. Costs, accepted

### 5.1 Index-ordered COPY **[measured, R0]**

An ordered scan over a random-uuid key is random heap I/O where today's COPY
is sequential, and it costs 5-6x. The numbers are in §3.10. Rather than pay
that, ordering is gated on heap correlation, so the cost accepted here is
bounded at roughly 1.2-2x on the tables that are ordered at all, and zero on
the tables that are not.

All measurements are warm-cache. On a table larger than RAM the ordered index
scan degrades much further (the scattered fetches become real seeks) while the
unordered seq scan degrades gracefully, so the gate matters more, not less, at
scale.

### 5.2 One `rowsExist` query per forwarded declaration

Index seek, bounded by the number of subscribers that connect during a run.

### 5.3 A replica migration (v18) and a change-log schema bump (v5)

v18 is four nullable columns; no `minSafeVersion` (an older zero-cache ignores
them). v5 adds `minSnapshot` to the log's cookie table and a partial index
over the schema-change and `backfill-started` tags. A change-log schema bump
reseeds every log in the fleet at once (`schema-mismatch`); R4 lands with the
next `P§` bump rather than alone.

### 5.4 A per-update key check on replica-identity-full tables

Already paid by the manager; now paid by the writer, tracker, and storer too,
only when `key !== null`.

### 5.5 Redundant rows on restart

`(m, L]` is re-sent to every subscriber; the column guard and following rule
make it harmless. Bounded by §3.8.

### 5.6 Larger subscribe request

See §2.7. Measured in R6; the header limit is raised in the same slice.

---

## 6. Slices

Dependencies: R0 → R1 → {R2, R3, R4} → R5 → R6 → R7 → R8. R2, R3, R4 are
independent of one another.

### 6.1 Slice R0 — Measure ordered COPY, build the literal helper

- `pg/backfill-resume.ts` (new): `isResumableColumn`/`isResumableKey`,
  `keyLiteral`, `orderByRowKey`, `resumeWhere`, `rowsExist`, `textKey` /
  `markOfLastRow` (mark from a decoded row), `getKeyCollations`,
  `getKeyCorrelation` / `isCheaplyOrderable`, `publicationRowFilter`.
- `makeDownloadStatements` gains `order?: {by, after?}` (`DownloadOrder`).
  `after` applies to the row and byte totals as well, so a resumed run reports
  the progress of what remains.
- Bench: `backfill-resume.bench.pg.ts` (new) — int, uuid and random-text key
  fixtures, unordered vs ordered vs forced-sort vs resumed-at-50%, draining
  the COPY without decoding so the numbers isolate Postgres's scan.

  ```bash
  ZERO_BACKFILL_RESUME_BENCH_ROWS=1000000 \
    pnpm --filter zero-cache run bench:pg backfill-resume.bench
  ```

- Tests: literal injection cases (quotes, backslashes, unicode), int/uuid/bool
  validation, multi-column keys, row filter ANDed, non-deterministic collation
  excluded, correlation gate, `rowsExist` bounds, and `EXPLAIN` showing an
  `Index Cond` (not a `Filter`) and no `Sort` for the resume WHERE on int,
  text and uuid keys.

**Gate result: failed as proposed, resolved by gating.** Ordered COPY reached
17-18% of today's throughput on the random uuid and text fixtures, against the
proposed >= 50%. Rather than redraw the plan around sorting cost — a forced
sort rescues uuid to ~55% but random text only to ~19% — ordering is gated on
heap correlation (§3.10), so tables that would pay the 5-6x are simply not
ordered and keep today's behavior. R1-R8 proceed unchanged.

### 6.2 Slice R1 — Protocol v7

- `data.ts`: `backfillStartedSchema`; `runID` on `backfill` and
  `backfill-completed`; `lastKey` on `backfill`; `backfill: true` on
  `begin`. A `backfillTags` list beside `schemaChangeTags`.
- `upstream.ts`: `backfillRequestSchema` gains `resumeFrom`,
  `resumeFromWatermark`, `minSnapshot`; `changeSourceUpstreamSchema` becomes
  a union with `['backfill-request', …]`.
- `change-streamer.ts`: `PROTOCOL_VERSION = 7`; `SubscriberContext.backfills`.
- `change-streamer-http.ts`: parse/encode `backfills`.
- `subscriber.ts`: `supportsMessage('backfill-started')` → v7+.
- `custom/change-source.ts`: ignore non-status upstream messages.
- Tests: passthrough parse of v7 messages by the v6 schema; the http
  round-trip of `backfills`; version gating.

### 6.3 Slice R2 — Ordered, resumable stream (change source)

- `streamBackfill` yields `backfill-started` first, `lastKey` on every
  `backfill` for resumable keys, `runID` throughout; honors
  `bf.resumeFrom`.
- `validateSchema` additionally checks the key's resumability and that
  `resumeFrom` has one value per key column; otherwise resume from null.
- Tests (`backfill-stream.pg.test.ts`): ordering by int, text (default
  collation), uuid, composite keys; resume at an interior key yields exactly
  the suffix; resume with a row filter; a non-resumable key yields no
  `lastKey`; `backfill-started` precedes rows.

### 6.4 Slice R3 — Subscriber rules and declaration (replicator)

- `replica-schema.ts` v18: the four columns.
- `BackfillingTracker`: `setFollowing`, `advanceMark`, `invalidateMarks`,
  `readBackfillDeclarations`; `readReplicaCookies` selects `minSnapshot` and
  excludes the other three.
- `ChangeProcessor`: rules of §2.3; the column guard in `processBackfill`
  and `processBackfillCompleted`; key-change detection in `processUpdate`
  (reuse the manager's test as a shared helper); local versioning of
  `backfill: true` transactions (§2.4).
- `incremental-sync.ts`: subscribe at the major; send `backfills`.
- Tests (`change-processor.test.ts`, new `backfill-rules.test.ts`): each rule
  row of §2.3 as a table-driven test; the Scenario A overwrite from §1.2 is
  the regression test for the guard; re-delivery of a run is idempotent;
  local versions are monotone across `M.3` then `M.1`; migration fresh and
  upgrade.

### 6.5 Slice R4 — `minSnapshot` in the cookie fold

- `change-log-cookies.ts`: `invalidate-marks` op; `markOps(update)`;
  `foldCookies` handles it; `backfillRequestsFrom` carries `minSnapshot`;
  canonical rendering excludes it.
- `change-log-db.ts`: schema v5 — the column and the partial tag index.
- `ChangeLogStreamWriter.append`, `BackfillingTracker.apply`,
  `Storer.#processQueue`: call the fold for key-changing updates.
- `change-log-initializer.ts`: `readSchemaChanges` moves to
  `change-log-range.ts` with a tag-list parameter; comparator unchanged.
- Tests: the fold (three interpreters agree, per `C§`), reseed carries
  `minSnapshot` from the replica, `readBackfillRequests` output.

### 6.6 Slice R5 — The manager

- `RequiredBackfill` and running-state changes of §2.5; `onBackfillRequest`;
  the `rowsExist` dependency injected beside `BackfillStreamer`; pending
  declaration queue; re-announcement on the running stream; extended
  `beginTxFor` check; `minWatermark` per required entry.
- `pg/change-source.ts`: route upstream `backfill-request` to the manager;
  provide `rowsExist` from `backfill-resume.ts`.
- Kill switch: `backfillResume: 'on' | 'off'` (hidden; §9). Off means
  every mark is treated as null and no re-announcements are emitted.
- Tests (`backfill-manager.test.ts` with fakes): following no-op; covered
  → re-announce; behind → restart from mark; queued marks drained at start;
  key change drops queued marks and restarts from null; Scenario B adds a
  request with null mark; restarts terminate (property test over random
  mark orders).

### 6.7 Slice R6 — The change-streamer

- `subscribe`: the steps of §2.7; declarations remembered per `Subscriber`;
  re-forward on reconnect in the `startStream` loop; the manager's own
  marks from the replica at `startStream`.
- HTTP server `maxHeaderSize`.
- Tests (`change-streamer-service.pg.test.ts`): a declaration is folded over
  a rename in `(w, head]`; a `backfill-started` in range suppresses
  forwarding; a non-cookie table's mark is dropped; reconnect re-forwards;
  the manager restart resumes from the replica's mark.

### 6.8 Slice R7 — End-to-end scenarios

Two change-streamers over one upstream, in `change-source.backfill.pg.test.ts`
or a new `backfill-resume.pg.test.ts`. Each row of §7 becomes a test:
the subscriber's final table contents equal upstream's, and the completion
count is exactly one per column.

Soak: the harness on `mlaw/soak` (`apps/zbugs/scripts/rmv2-soak/`) runs one
replication-manager. Adding a second manager is its own slice; until then, the
harness gets chaos **C15**: start a backfill, restart the replication-manager
mid-run, and assert the run resumes from the replica's mark (log line) and that
no subscriber is demoted or restored. **[built; see §13.11]**

### 6.9 Slice R8 — Rollout

`backfillResume` default `off` → `on` per stack via god config, after R7 has
run against the soak. Dashboard panels per §10.

---

## 7. Scenario walk-throughs

`A` and `B` are replication-managers; `S` a serving subscriber; `m` its mark.

| # | scenario | what happens |
| --- | --- | --- |
| A | `S` finished on A; bounces to B mid-run | `S` declares nothing for the table. B's rows hit the column guard and are skipped; B's completion finds no following column and is ignored. No version bump. |
| B | `S` at `m` from A's run; B already finished | `S` declares `(m, r_A)`; the table is not in B's cookie jar; the change-streamer drops the mark and forwards; the manager adds the request and runs from zero. Every other subscriber ignores it (guard). `S` follows via `backfill-started(R, null)` and completes. |
| C | both in progress, `S` at `m_A`, B's run at `L` | `S` subscribes at its major; catchup delivers B's `M.k` transactions; `S` is not following (never saw B's announcement, or its `resumeFrom` ≠ `m_A`). Declaration forwarded. If rows exist in `(m_A, L]`: B restarts from `m_A`, `S` matches by equality and follows. Else B re-announces with `resumeFrom = m_A`; `S` follows from there. Either way `S` ends with every row. |
| D | same-manager reconnect mid-run | `S` declares `(m, R)`; `backfill-started(R)` is in `(w, head]` or `r == R` at the manager; nothing forwarded or a no-op. Re-delivered `M.1..M.k` are re-applied idempotently under local versions. |
| E | manager restart mid-run | Initial requests carry the replica's mark; the run resumes there; the backup replicator declares the same mark and matches by equality. No redundant rows. |
| F | `S` restores from a backup taken mid-run | The backup carries `(m, R)`. Catchup from the backup watermark is pinned by the reservation and delivers the rest of `R` (the run's announcement is in range or `S` already holds `R`). `S` follows and completes. No restart. |
| G | key change on `T` at `v` while `S` is at `s < v` | If `S` processed `v`: its mark is null and it declares null. If not: `minSnapshot ≥ v` in the jar seeds the manager, which drops the mark. Either way the next run starts from zero for `S`. Followers whose rows were all sent before `v` still complete. |
| H | ten subscribers restore from one backup during a run | The first declaration may restart the run at the backup's mark; the other nine match the announcement by equality. One restart. |
| I | non-resumable key (timestamp, numeric) | No `lastKey`, no marks; every run announces `resumeFrom = null`; behavior is today's plus the column guard and following rule. |
| J | `sqliteChangeLogMode = off` | Declarations dropped with a warning; today's behavior plus the guard. |

---

## 8. Code structure

| file | change |
| --- | --- |
| `change-source/pg/backfill-resume.ts` | new: key literals, resume WHERE, `rowsExist`, `textKey` |
| `change-source/pg/backfill-stream.ts` | ordering, resume, `backfill-started`, `lastKey`, `runID` |
| `change-source/pg/initial-sync.ts` | `makeDownloadStatements` options |
| `change-source/common/backfill-manager.ts` | §2.5 |
| `change-source/pg/change-source.ts` | upstream routing, `rowsExist` provider |
| `change-source/protocol/current/{data,upstream}.ts` | §2.2 |
| `change-source/protocol/current/schema-change-tags.ts` | `backfillTags` |
| `change-streamer/{change-streamer,change-streamer-http,subscriber,change-streamer-service}.ts` | §2.7, versioning |
| `change-streamer/change-log-range.ts` | new: `readSchemaChanges` generalized |
| `replicator/change-log-cookies.ts` | `invalidate-marks`, `minSnapshot` |
| `replicator/change-log-db.ts` | schema v5 |
| `replicator/change-log-stream-writer.ts` | fold on key-changing updates |
| `replicator/schema/backfilling.ts` | four columns, tracker methods, declarations |
| `replicator/change-processor.ts` | §2.3, §2.4 |
| `replicator/incremental-sync.ts` | major watermark, `backfills` |
| `change-source/common/replica-schema.ts` | v18 |
| `config/zero-config.ts` | `backfillResume` |

---

## 9. Configuration

| option | default | notes |
| --- | --- | --- |
| `backfillResume` | `off` | hidden; `on` enables marks, `rowsExist`, re-announcements. `off` keeps the column guard, following rule, and local versions, which are unconditional correctness fixes. |
| `backfillResumeMinCorrelation` | `0.9999` | hidden; the §3.10 gate. `0` orders every resumable key regardless of cost; `1` effectively disables ordering. |

No new retention or purge settings: nothing here is retained in the log
beyond today's rules.

---

## 10. Observability

Counters and histograms under `replication`:

| metric | labels |
| --- | --- |
| `backfill.runs` | `start` = `zero` / `resumed` |
| `backfill.restarts` | `reason` = `declaration` / `key-change` / `schema` |
| `backfill.reannouncements` | — |
| `backfill.declarations` | `outcome` = `following` / `forwarded` / `dropped-no-log` / `dropped-fold-cap` / `mark-dropped-scenario-b` |
| `backfill.rows_skipped_by_guard` | — |
| `backfill.completions_ignored` | — (a following-rule ignore; nonzero is expected only in Scenarios A/B) |
| `backfill.rows_exist_duration` | histogram |

Log lines: run start with `runID`, `resumeFrom`, snapshot; every restart with
the declaring subscriber; every `minSnapshot` write with the table and
version.

Dashboard: one row on the existing rollout dashboard
(`controller/grafana/src/sqlite-change-log-dashboard.ts`): runs by start
kind, restarts, declarations by outcome.

---

## 11. Open sign-offs

1. ~~**R0's throughput gate factor.**~~ **Resolved:** the gate failed at the
   proposed ≥ 50% (17-18% on random keys), and ordering is now gated on heap
   correlation instead (§3.10). What remains open is the threshold itself,
   `MIN_KEY_CORRELATION = 0.9999`, which admits append-mostly and `CLUSTER`ed
   tables and little else.
2. **Landing R4's change-log schema bump** with the next `P§` bump versus
   alone. A fleet-wide reseed either way.
3. **Whether `backfillResume` should ship `on`** once R7 passes, or stay a
   per-stack opt-in for a release. **Deferred until C15 has run against the
   soak** (user, 2026-09-09); the harness runs it `on` so the run produces the
   evidence.
4. **Second replication-manager in the soak harness** (a follow-on slice to
   `mlaw/soak`), which is the only place Scenarios A-C run against real
   processes. C15 covers Scenario E there; B, C, F and H still do not run
   against real processes.
5. **The subscribe-request size bound** (§2.7). If any real schema has more
   than a few hundred simultaneously backfilling tables, build the
   first-frame declaration instead of raising the header limit.

---

## 12. Standard validation

Per `P§:12`, plus:

```bash
pnpm --filter zero-cache run test backfill-resume.pg.test --coverage
pnpm --filter zero-cache run test backfill-stream.pg.test
pnpm --filter zero-cache run test backfill-manager.test --coverage
pnpm --filter zero-cache run test change-processor.test
pnpm --filter zero-cache run test change-log-cookies.test --coverage
pnpm --filter zero-cache run test change-log-db.test
pnpm --filter zero-cache run test replica-schema.test
pnpm --filter zero-cache run test change-streamer-service.pg.test
pnpm --filter zero-cache run test change-source.backfill.pg.test
```

R0 and R2 publish the exact benchmark command and before/after results in
the PR. R3 and R4 run both fresh-schema and upgrade migration tests, and a
rollback test against v17 / change-log v4.

---

## 13. Implementation notes

Where the code differs from the plan above, and why.

### 13.1 The resume literal must be constant (R0)

`convert_from(decode(...), 'UTF8')` is `STABLE`, so Postgres renders the
resume predicate as a `Filter` rather than an `Index Cond` and scans the index
from the beginning on every resume. An `E'…'` literal is constant and produces
a real seek. See §2.1.

### 13.2 Ordering is gated on heap correlation (R0)

§3.10. The gate is the difference between the plan as written and a plan that
pays 5-6x on the tables most Zero applications have.

### 13.3 `backfill-started` is a third kind of change (R1)

It is neither a data change (no rows) nor a schema change (no DDL), so it is
its own category — `backfillControlSchema` / `isBackfillControl` — rather than
being folded into either. That keeps `isSchemaChange` gating the cookie fold
exactly as before, and makes the new tag a compile error at every exhaustive
switch over changes.

`runID` is **optional** on `backfill` and `backfill-completed`, not required.
Change-log entries written before this change are replayed verbatim during
catchup and carry none; a completion without one completes unconditionally,
which is what a subscriber did before. The same rule covers a change source
that never adopts run announcements.

### 13.4 The run is announced after the snapshot's first query (R2)

`streamBackfill` yields `backfill-started` after computing the run's row and
byte totals rather than before. Yielding suspends the generator until the
manager has reserved the change stream and pushed the message; doing that
between opening the snapshot transaction and issuing its first query leaves
the transaction idle long enough for the pool to close it, which surfaces as
`relation "…" does not exist` and a run that retries with backoff forever.
The announcement still precedes every row of the run, which is all its meaning
requires.

### 13.5 The purge floor is the major (R3)

Not in the plan, and necessary for §2.4 to work. A subscriber resumes at the
**major** of its state version, but its ACK advances through the run's backfill
transactions, so purging to the minimum ACK deletes the last upstream commit —
the very entry the subscriber will name on its next reconnect — and answers it
with `WatermarkTooOld`, which `IncrementalSyncer` answers with a full restore.
On a quiet upstream during a long backfill that is every reconnect.

`#getCleanupFloor()` now floors the purge watermark at the major of the minimum
ACK. What that retains beyond the ACK is exactly the backfill transactions
since the last upstream commit, which is exactly what such a reconnect
replays. (The "client is behind backup" log line still compares the unfloored
minimum, so it does not become chatty.)

Related: the major is taken by splitting the string, not by decoding and
re-encoding. A LexiVersion can be non-canonical — `"101"` and `"01"` both
decode to 1 — and a custom change source's watermarks are its own to choose, so
round-tripping one changes it. `majorVersionOf()` in `types/state-version.ts`.

### 13.6 Declarations lower the start; they never raise it (R5)

§2.5 step 4 queues marks declared while no run is active and drains them
"through step 3" when one starts. Step 3 needs a running run's `lastMark` to
compare against, which does not exist yet at that point, so the queue is
replaced by a rule that needs no ordering at all:

- a declared mark **equal** to the entry's pending start changes nothing —
  which is the common case, since a fleet restoring from one backup declares
  one mark, and a restarted manager seeds the pending start from its own
  replica's;
- anything else drops the start to the beginning, which covers both.

A declaration never *raises* the start from the beginning to a mark. It cannot:
the manager has one subscriber's word for it, and a run that skipped ahead
would leave every subscriber without a mark short. Resuming across a manager
restart comes from the initial request instead (§13.7), where the manager's own
replica is the authority.

### 13.7 The manager's own marks arrive with the initial requests (R6)

`withResumeMarks()` in `change-log-initializer.ts` attaches the
replication-manager's own replica's marks to the `BackfillRequest`s of every
`startStream`, dropping any whose `markWatermark` predates the table's
`minSnapshot`. This is what makes Scenario E resume rather than restart. The
marks ride on `InitializationParameters.marks`, which only the replica-derived
source populates — marks are subscriber state, not cookies, so no change log
carries them, and the three-store comparison is unaffected.

### 13.8 What R7 covers, and what it does not

`backfill-resume.pg.test.ts` runs a real Postgres change source (manager,
ordered COPY, run announcements) against a real replica through
`ChangeProcessor`, and asserts on every case that the replica's rows equal
upstream's and that every column completes exactly once:

- a backfill that runs to completion;
- **Scenario E**, a manager restart interrupted between transactions, resuming
  from the replica's mark;
- **Scenario A**, a stale run replayed after the column has completed and been
  updated, which the column guard drops and whose completion is ignored.

The remaining §7 rows need two replication-managers streaming from one
upstream at once, which is §11.4's open item: two change sources on one shard
contend for the replication slot, so it is a harness build rather than a test.
Until it exists, Scenarios B, C, F and H are covered at the unit level —
`backfill-manager.test.ts` for the manager's decisions, `backfill-rules.test.ts`
for the subscriber's — and not end to end.

### 13.9 The declaration seam had a defect and no test (R6)

`#resolveBackfillDeclarations` resolved a declared table's current identity as
`renames.get(key) ?? null` and read `null` as "dropped in the interval".
`foldIdentities` is seeded lazily, though: a table that was neither renamed nor
dropped has no entry at all and maps to itself. So every declaration for an
unrenamed table -- the common case -- was counted `dropped-table` and
discarded, which left the whole forward path inert: Scenarios B and C never
reached the change source, and no run ever restarted from a mark or
re-announced.

§6.7 listed five tests for this function and none of them had been written; the
unit tests either side of the seam (`backfill-manager.test.ts` for the
manager's decisions, `backfill-rules.test.ts` for the subscriber's) both bypass
it, and R7's end-to-end cases do not exercise the forward path. The five now
exist in `change-streamer-service.pg.test.ts`, driven through `subscribe()`
against a real inline-written change log and asserted on what reaches the
change source's ack channel.

### 13.10 §10's log lines, and who declared a restart

The metrics of §10 were emitted; its log lines were not. Three now are: the run
start (`backfillRun`, with `runID`, `resumeFrom`, snapshot and start kind), the
forwarded declarations (`backfillDeclarations`), and the `minSnapshot` write a
row key change causes. Each carries a structured field rather than prose,
because the soak harness reads fields.

"every restart with the declaring subscriber" needed one protocol addition: the
`backfill-request` message gains an optional `subscriberID`. No decision reads
it -- it is attribution only -- so a sender that omits it is answered exactly as
before, and the protocol stays at v0/v7. The manager puts it on its log context
in `onBackfillRequest`, so every decision that method logs, restarts included,
names the subscriber that caused it.

### 13.11 What C15 does, and why it brings its own table

C15 is in `apps/zbugs/scripts/rmv2-soak/chaos.ts`, in `DEFAULT_CHAOS`. It
creates a fixture table, waits for the run to announce itself and for the RM's
own replica to record a mark, SIGTERMs the RM mid-run, restarts it, and asserts
that the next announcement's start kind is `resumed`, that nobody was demoted
to PG or sent back to a litestream restore, and that every replica ends with
every row. The verdict is `c15Findings`, a pure function unit-tested the way
`c9ResourceFindings` is, so each way it can fail is a case rather than a run.

Two deviations from §6.8 as written:

- **It adds the column to its own table, not to `issue`.** The zbugs `issue`
  table's ids are `change-log-traffic-<run>-<n>`, whose text order and insertion
  order diverge as soon as the sequence crosses a decade, so it never clears
  §3.10's correlation gate and its runs carry no marks. C15's fixture is
  `(id int4 PRIMARY KEY)` filled in key order and `ANALYZE`d, so correlation is
  1; the column is then added with a constant `DEFAULT`, which is metadata-only
  in PG11+.
- **The harness runs `ZERO_CHANGE_STREAMER_BACKFILL_RESUME=on`.** The flag ships
  `off`, and the soak is where the §11.3 decision gets its evidence. The
  correlation gate still applies, so the zbugs tables keep today's unordered
  behavior either way.

  **The prefix is not optional, and getting it wrong is silent.** The option is
  nested under `changeStreamer`, so `ZERO_BACKFILL_RESUME` is simply ignored:
  the process starts, the flag stays `off`, every run comes out unordered, and
  nothing says so. R8's rollout goes through god config, which is the same class
  of hand-written environment, so the variable to set is
  `ZERO_CHANGE_STREAMER_BACKFILL_RESUME` (as with
  `ZERO_CHANGE_STREAMER_SQLITE_CHANGE_LOG_MODE`). C15 cost two soak runs to this
  before it was found, which is why its "no mark" finding now names an unordered
  run as one of the two causes rather than blaming the fixture.

A first draft created the table *with* its rows and expected the change source
to backfill it. It does not, and should not: rows inserted after `CREATE TABLE`
are in the WAL and replicate as ordinary inserts. A backfill carries exactly
what the WAL does not — the values a column already had when it was published —
so `ALTER TABLE … ADD COLUMN`, which §6.8 named, is the only way to trigger one.
The run C15 measures therefore starts from a *settled* table, and its progress
is the mark plus the count of rows that have the new column, not the row count.

`mlaw/soak` was rebased onto main for this, dropping its three tip commits
(`batch writes`, `stop re-encoding json`, `guarded SQLite-only change log
mode`), which had landed as #6478, #6489 and #6488. What remains is
`apps/zbugs/**` only, and `mlaw/incremental-backfill` is stacked on it.

### 13.12 R8

`backfillResume` and `backfillResumeMinCorrelation` exist and default to `off`
and `0.9999`. The metrics of §10 are emitted. The dashboard row lives in the
controller repo and is not part of this change.

---

## Appendix A — Log replay and `COLLATE "C"`: considered, not taken

The brief adds columns to the change log so that the change-streamer can
replay `backfill` messages from a subscriber's mark, and orders the COPY with
`COLLATE "C"` so that SQLite can find the message containing that mark.

The two are one decision: replay needs a key comparison in the log, and a
comparison in SQLite needs an order SQLite can reproduce, which is bytewise
order, which is `COLLATE "C"`. Removing replay removes the comparison,
which removes the collation clause, which restores index use on the COPY.

What replay would have bought is a catchup for a subscriber whose needed rows
are still in the log. §3.1 lists why that is rarely true under the current
purge policy, and §7 case F shows the one common instance is already served
by following through catchup. What it would have cost is a full copy of the
table in the log for the run's duration, and the sort.

## Appendix B — Why the announcement carries a mark and not a sequence

A run's messages could carry `seq`, and a subscriber could follow only from
`seq = 0`. That works for a subscriber present at the start and fails for
every late joiner: it either never follows (never completes) or must be told
by the manager that it is covered. Once the manager must speak, the cheapest
thing it can say on a broadcast stream is "anyone whose mark is `m` is covered
from here" — which is `backfill-started(R, m)` re-emitted. Sequence numbers
add nothing on top.
