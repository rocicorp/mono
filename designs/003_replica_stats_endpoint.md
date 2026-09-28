# 003: `/plannerz`, replica statistics and query plans for LLMs

- **Status:** Phase 1 (`GET /plannerz`) shipped in
  [#6639](https://github.com/rocicorp/mono/pull/6639) on 2026-09-24. Phases 2
  and 3 are not built.
- **Date:** 2026-09-22 (updated 2026-09-28 to match the code)
- **Packages:** `zero-cache` (admin endpoint), `zqlite` (stat4 decoding)

## Goal

A developer gives an LLM two things: their codebase and an admin URL on their
zero-cache. The LLM tells them how to rewrite ZQL queries, and which Postgres
indexes to add, so the queries run well **on the plan Zero actually picks**.

Constraint: the endpoint must not return row data. Schema, sizes, and
statistics are allowed. Values from the database are not.

## What exists today

| Piece                                                                      | Where                                                                                                                                                                                                                       | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| -------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Admin HTTP endpoints `/statz`, `/plannerz`, `/heapz`, `/profz`, `/profrmz` | `server/runner/zero-dispatcher.ts`                                                                                                                                                                                          | Basic auth, password optional in dev. `/heapz` requires the admin password (`isAdminPasswordValid`). `/statz`, `/plannerz`, `/profz` and `/profrmz` also accept the operator password (`getOperatorAccess`, `--operator-password`), since they return no application data. Profiles served for the operator password have regular expression sources redacted, since those can hold `LIKE` patterns from client queries. `/statz` and `/plannerz` open `config.replica.file` read-only. `/statz` reads the WAL size from the WAL files, not from `PRAGMA wal_checkpoint`, which runs a checkpoint. |
| `analyze-query`                                                            | `services/view-syncer/inspect-handler.ts` → `services/analyze.ts`                                                                                                                                                           | Websocket inspector only, admin password only. Needs a connected client (uses the CVR `clientSchema`). **Runs** the query (capped at `MAX_ANALYZE_ROWS` = 1000 per table). Returns planner events (`joinPlans`), SQLite plans, and read counts per SQL. Can also return `syncedRows`/`vendedRows`, which are row data.                                                                                                                                                                                                                                                                             |
| Planner cost model                                                         | `zqlite/src/sqlite-cost-model.ts`, `sqlite-stat-fanout.ts`                                                                                                                                                                  | Costs come from SQLite `scanstatus` estimates. Join fanout comes from stat4, else stat1, else the default of 3.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| Stats collection                                                           | `db/migration-lite.ts` (after migrations), `workers/replicator.ts` (replica setup, `optimize = 0x10002`), `replicator/change-processor.ts` (after a schema change), `zqlite/src/db.ts` (`close()` on a writable connection) | Only `PRAGMA optimize` runs. The replicator's connections set `analysis_limit = 1000` (`getPragmaConfig` in `workers/replicator.ts`); elsewhere `optimize` sets its own temporary limit. There is no full `ANALYZE` anywhere.                                                                                                                                                                                                                                                                                                                                                                      |

## Finding: the replica has no stat4 and only approximate stat1

Checked 2026-09-22 with the bundled `@rocicorp/zero-sqlite3` (SQLite 3.54.0).
The test table had a unique `email` column and a skewed `status` column (90%
`open`, 10% `closed`), both indexed.

| How stats were gathered                   | `t_status` in stat1                 | Rows in stat4 |
| ----------------------------------------- | ----------------------------------- | ------------- |
| `ANALYZE` (no limit)                      | `20000 10000` (exact)               | 170           |
| `analysis_limit=1000` + `ANALYZE`         | `20000 1001`                        | 0             |
| `analysis_limit=1000` + `PRAGMA optimize` | `20000 1001`                        | 0             |
| `PRAGMA optimize`, 300k rows              | `300000 2001` (true value is 30000) | 0             |

What this means:

1. **Every path Zero uses leaves stat4 empty.** The stat4 branch of
   `SQLiteStatFanout` never runs in production. Fanout always comes from stat1
   (which counts NULLs) or the default.
2. **Per-key averages in stat1 are too low on skewed columns**, by 10× in this
   test. The planner and SQLite both see low-cardinality columns as more
   selective than they are.
3. For this endpoint: stat4 redaction doesn't matter for replicas today, but it
   is still needed. A user can run `ANALYZE` by hand, and we may change how
   stats are gathered (see F1).
4. An LLM that reads raw stat1 inherits the same errors. The response must say
   the stats are approximate (§1.3) so the LLM doesn't treat them as exact.

This may matter more than the endpoint itself. See follow-up F1.

## Design

One new admin endpoint family, `/plannerz`, on the zero-dispatcher, next to
`/statz`. It uses the same basic-auth check and the same "open the replica
read-only, then close it" pattern.

Access differs by route. `GET /plannerz` returns no application data, so like
`/statz` it accepts the operator password as well as the admin password
(`getOperatorAccess`). `POST /plannerz/analyze` requires the admin password
(§2.3).

We chose a separate path over a new `/statz` group, because the second route
takes a POST body and returns a different kind of output.

**No table scans by default.** Phase 1 reads only the catalog, `sqlite_stat*`,
and config, so its cost doesn't depend on table size. Phase 2 is plan-only by
default and reads no user table either; running the query is opt-in (§2.2).

Column facts that stat1 doesn't have, such as the NULL fraction, true distinct
counts, or the most common value's frequency, are out of scope. Computing them
means reading a whole table or index, in the main process, on an admin request.
Better statistics come from F1, off the serving path.

### Phase 1: `GET /plannerz` returns a static stats bundle

JSON by default (`?pretty` indents it, as `/statz` does). Everything in it comes
from the catalog, `sqlite_stat*`, and config. No user tables are scanned.

```jsonc
{
  "about": { ... },               // §1.4, a fixed glossary for a cold LLM
  "server": {
    "zeroVersion": "…",
    "sqliteVersion": "3.54.0",
    "replicaWatermark": "…",       // getReplicationState; absent before initial sync
    "generatedAt": "…",
    "planner": {                   // config flags that change plans
      "enableQueryPlanner": true,
      "enableCorrelatedPredicatePushdown": true,
      "enablePlannerAwarePushdown": true
    }
  },
  "statsQuality": {
    "stat1Present": true,
    "stat4Rows": 0,
    "method": "PRAGMA optimize under an analysis_limit. …",  // fixed text, §1.3
    "tablesWithoutStats": ["…"]
  },
  "tables": [{
    "name": "issue",
    "columns": [{
      "name": "…", "dataType": "…", "nullable": true,
      "zqlType": "…",              // null for a column clients cannot see
      "backfilling": true          // only while the column is backfilling
    }],
    "primaryKey": ["id"],
    "estimatedRows": 20000,        // first number in the table's stat1 row
    "syncable": true,              // has a primary key or unique index, and a ZQL-typed column
    "indexes": [{
      "name": "…",
      "columns": [{"name": "projectID", "dir": "ASC"}, …],
      "unique": false,
      "partial": false,
      "stat1": {
        "raw": "20000 1001 3",
        "rows": 20000,
        "avgRowsPerPrefix": [1001, 3],   // key prefix of length 1, 2, …
        "flags": []                      // e.g. "unordered", "noskipscan", "sz=N"
      },
      "stat4": {
        // A digest, not the raw histogram; omitted when there is no stat4.
        "samples": 52,
        "sampleKinds": ["integer", "text", "integer"],  // + the rowid
        "perPrefix": [                                   // per key prefix depth
          {"maxRowsPerKey": 4500, "medianRowsPerKey": 4500, "estimatedDistinctKeys": 2}
        ]
      },
      "stat4Samples": [            // only with ?stat4=full (§1.1b)
        {"nEq": […], "nLt": […], "nDLt": […], "sample": ["integer", "text", "integer"]}
      ]
    }]
  }]
}
```

#### 1.1 Sources

- Tables and columns: `listTables` and `computeZqlSpecsFromLiteSpecs` from
  `db/lite-tables.ts`, with backfilling columns left out of the ZQL specs.
  Don't list Zero's internal tables (`_zero.*`, change log, and so on).
- Indexes: `listIndexes` from `db/lite-tables.ts`. It also returns the indexes
  Zero creates itself, and the ones SQLite creates for primary keys.
- `listTables` reads `_zero.tableMetadata` for backfill status, which does not
  exist until initial sync creates it. Fall back to listing without it, so a
  fresh replica reports its schema instead of failing the request.
- Stats: plain `SELECT` from `sqlite_stat1` and `sqlite_stat4`. Handle the
  case where these tables don't exist yet.

#### 1.1b Size

stat4 holds a few hundred samples per index, which serialized to 170KB of JSON
for a two-table schema in testing, or roughly 40k tokens. So the default
response carries a **digest** per index instead: for each key prefix depth, the
rows behind the most common sampled key, the median, and a lower bound on the
distinct keys. That is the skew an LLM would compute from the raw samples
anyway, and it brought the same schema to 8.5KB.

`?stat4=full` adds the raw (redacted) histogram for a deep dive.

The digest must not depend on the order the samples arrive in. `nLt` and `nDLt`
are text columns, so `ORDER BY nlt` in SQL sorts them as strings: "1049" sorts
below "20". An early version read the last row's `nDLt` as the distinct count
and was off by 8x because of this.

#### 1.2 Privacy rules

These are enforced in code and pinned by tests.

- **stat4 `sample` values are never returned.** Each sample becomes a list of
  kinds (`null | integer | real | text | blob | unknown`), one per key column
  plus the trailing rowid, decoded from the record header by
  `decodeSampleKinds` in `zqlite/src/sqlite-stat4-sample.ts`. The fanout code
  uses the same decoder (`isSampleNull`). The numeric arrays
  (`nEq`/`nLt`/`nDLt`) are returned. They show skew ("one key covers 40% of the
  rows") but not which key it is.
- There is no option to return sample values. `?stat4=full` returns every
  sample, but still only their kinds. Add a values option only if users ask
  for it.
- What is returned: table and column names, index definitions, row count
  estimates, and distinct-value averages. These are served to the operator
  password too, which is for people who operate zero-cache but may not read
  its data. Schema and statistics are fine for them; values are not.

#### 1.3 How approximate the stats are

The server can't tell when stats were gathered or with which limit, but the
code path is known. `statsQuality.method` is fixed text: an explanation that
the stats are sampled estimates, or, when stat4 has rows, that a full `ANALYZE`
ran at some point. It doesn't compare against `count(*)`, because that scans
the whole table.

#### 1.4 `about`: a short glossary

A cold LLM will misread these stats without help. `about` is a fixed string
map that covers:

- The stat1 format: rows, then the average rows per distinct key prefix, and
  that NULLs are counted.
- What `nEq`, `nLt`, and `nDLt` mean.
- How the planner uses the stats: scanstatus estimates, fanout from stat4 then
  stat1 then a default of 3, and semi-join vs flipped joins.
- What the user can change:
  - ZQL shape: `whereExists` vs `related`, and ordering that matches an index.
  - `limit`.
  - **Postgres** indexes. The replica copies upstream indexes, so users don't
    add indexes to the replica directly.
  - The planner config flags.
- Caveats: row counts are estimates, the stats can be stale, and what
  `syncable: false` means.
- No pointer to `POST /plannerz/analyze` or to docs. Add one when that route
  ships (Phase 2).

It is about 530 tokens, under the 1.5k budget. It is not snapshot-tested: the
endpoint test only checks `whatThisIs`.

### Phase 2: `POST /plannerz/analyze` returns the plan for one query

**Not built.** Everything in this section is still a proposal. When it ships,
point to it from `about` (§1.4), and drop the test that `about` does not
mention it.

This is where most of the value is. Without it, the LLM has to rebuild our
cost model from raw stats. The planner has known cost gaps (semi-join double
fetch, lookup overcount), so the LLM's guess can differ from what Zero runs.

Request:

```jsonc
{ "ast": { ... } }
```

**Plan-only is the default.** The route plans the query and explains it, but
does not run it. Running it is opt-in with `?execute=true`. Plan-only keeps the
route's cost independent of table size, which is the same rule phase 1 follows:
executing a query with a bad plan can read a whole large table, because the
1000-row cap bounds the rows _returned_, not the rows _read_.

#### 2.1 Plan-only mode (default)

`buildPipeline` runs the planner and creates the sources, but nothing fetches,
so no table is read. What comes back:

- `joinPlans`: every attempt, the connection costs, the selected plan, and the
  flip pattern. This is the planner's own reasoning, from `AccumulatorDebugger`.
- `sqlitePlans`: EXPLAIN QUERY PLAN per generated statement, plus the
  scanstatus row estimate the cost model already computed for it. The cost
  model in `zqlite/src/sqlite-cost-model.ts` builds and prepares this SQL
  today and then throws it away, so this needs a recording hook on it.
- `warnings`, and the permissions-transformed query (`afterPermissions`) when
  permissions apply.

There are **no measured row counts** in this mode, only estimates. The response
says so in a `mode` field, so the LLM doesn't read an estimate as a measurement.

One exception to "reads nothing": `resolveSimpleScalarSubqueries` executes each
`{scalar: true}` subquery while building the pipeline. Those have `limit: 1`, so
the cost is bounded.

#### 2.2 Execute mode (`?execute=true`)

This is the current inspector behavior: it hydrates the query, capped at
`MAX_ANALYZE_ROWS` (1000) rows per table. It adds the measured
`readRowCountsByQuery`, `dbScansByQuery`, `syncedRowCount` and `elapsed`, which
is what you want when the estimates look wrong. The docs and the `about` text
should say it runs the query against the replica.

Guards, in this mode only: one analyze at a time (return 429 when busy), and a
wall-clock timeout.

#### 2.3 Both modes

- **Admin password only.** Unlike `GET /plannerz`, this route checks
  `isAdminPasswordValid`, not `getOperatorAccess`. It runs caller-supplied
  queries against the replica, and what it reports back (whether a scalar
  subquery matched, measured row counts) reveals application data even when
  no rows are returned.
- **Rows are never returned.** `syncedRows`, `vendedRows` and `readRows` are
  deleted from the result unconditionally, so a later change to the
  `analyzeQuery` defaults can't leak them.
- SQL strings are parameterized with `?`, so no values appear in them.
- Audit `joinPlans` constraint payloads and `warnings` for literal values
  before shipping. The caller's own AST literals are fine, since the caller
  sent them.

Implementation notes:

- Execute mode reuses `analyzeQuery` with `syncedRows=false`,
  `vendedRows=false`, `joinPlans=true`. Plan-only needs a sibling function that
  shares the setup (specs, cost model, permissions) and stops after
  `buildPipeline`.
- `clientSchema`: today it comes from the CVR. Over HTTP there's no client
  group, so build it from the replica's `tableSpecs`, which cover every
  syncable table. It needs a small adapter.
- **AST only.** The request body is `{ast}`. Named queries (`{name, args}`) are
  out of scope for now, because `inspectorDelegate.transformCustomQuery` needs
  a `ConnectionContext` for auth. The LLM can get ASTs with the existing
  `transform-query` CLI or `query.ast`. Later, named queries could forward an
  `X-Zero-User-Authorization` header to the API server. See F4.
- Permissions: follow the inspector. Apply legacy permissions if they're
  present. Custom queries already have permissions applied by the API
  transform.
- The planner must run the same way it does in production: honor
  `enableQueryPlanner` and the pushdown flags, as `services/analyze.ts` does.

### Phase 3: packaging for LLMs (optional)

**Not built.**

- A docs page with a prompt snippet like "fetch `$URL/plannerz`, read my
  queries in `src/queries.ts`, then call `/plannerz/analyze` on the three most
  expensive ones".
- Maybe a `zero-cache` MCP tool that wraps both routes. Only worth it if people
  use the HTTP version.
- `?format=md` if JSON turns out to cost too many tokens on large schemas. It
  probably won't.

## Files

Phase 1:

- `packages/zero-cache/src/services/plannerz.ts`: request handler, bundle
  builder, stat1 parsing, stat4 digest, and the `about` text.
- `packages/zero-cache/src/services/plannerz.test.ts`
- `packages/zero-cache/src/server/runner/zero-dispatcher.ts`: the
  `GET /plannerz` route.
- `packages/zqlite/src/sqlite-stat4-sample.ts` (and its test): the stat4
  record-header decoding, shared by the redaction and
  `zqlite/src/sqlite-stat-fanout.ts`.

Phase 2 (not built): a `clientSchema` from `tableSpecs` adapter next to
`services/analyze.ts`, and the `POST /plannerz/analyze` route.

## Tests

Phase 1, in `services/plannerz.test.ts` and
`zqlite/src/sqlite-stat4-sample.test.ts`:

- 401 without a password or with a wrong one, and 200 with the operator
  password. Dev mode without a password is covered by the tests of the shared
  password check (`config/is-admin-password-valid.test.ts`).
- stat1 parsing, including the `unordered` and `sz=` flags.
- The stat4 digest, including that it does not depend on the order the samples
  arrive in.
- Bundle built from a fixture replica: tables, columns and indexes, before any
  stats exist (`statsQuality.stat1Present=false`, `tablesWithoutStats`), and
  after `PRAGMA optimize` (stat1, no stat4).
- A table with no unique key is reported as not syncable. Internal tables are
  left out.
- **Redaction**: the fixture's indexed columns hold a sentinel string
  (`SECRET-do-not-leak`). After a full `ANALYZE`, the bundle has stat4 counts
  and the sentinel appears nowhere in it, with and without `?stat4=full`, and
  nowhere in the HTTP response body.
- stat4 record-header decoding, including multi-byte varints and NULL samples.
- `about` does not mention `/plannerz/analyze`, which is not built.

The fixture replica has no `_zero.tableMetadata`, so every bundle test goes
through the fallback in §1.1. Not covered: a replica that has table metadata
(and so the `backfilling` column flag), and a snapshot of `about`.

Phase 2 (planned):

- Plan-only: a zbugs-style AST returns `joinPlans` and `sqlitePlans` with no
  row fields, and reads no user table. Pin the "reads nothing" part by counting
  reads, for example with a `TableSource` spy or by asserting that the fetch
  path is never entered.
- Execute mode: same AST with `?execute=true` adds measured row counts, and
  still has no row fields.
- The sentinel check on the Phase 2 response.
- 401 for the operator password.

Manual: point Claude at zbugs plus a local `/plannerz` and see whether the
advice is correct.

## Follow-ups

- **F1: stats quality.** This is independent of the endpoint and probably more
  important. Options:
  - (a) A larger `analysis_limit` for the post-initial-sync optimize.
  - (b) A full `ANALYZE` in the background, off the hot path, such as on the
    replication-manager or backup copy, with the `sqlite_stat*` rows shipped
    to the view-syncers. stat1 and stat4 are ordinary writable tables.
  - (c) An admin-triggered `POST /plannerz/reanalyze`.

  (b) and (c) are full scans of every index, so they must not run on a
  view-syncer that is serving clients. (c) is only acceptable if it runs
  against a copy. (a) is bounded by the limit.

  Measure the planner's plan changes on zbugs with full stats before choosing.
  Once stat4 exists, the redaction in §1.2 stops being theoretical.

- F3: an MCP wrapper (phase 3).
- F4: named queries (`{name, args}`) in `/plannerz/analyze`, with user auth
  forwarded to the API server.

## Decisions

1. Name: `/plannerz` (2026-09-22).
2. `/plannerz/analyze` accepts only an AST for now. Named queries are F4.
3. No table or index scans to gather column statistics (see Design).
4. `/plannerz/analyze` is plan-only by default; running the query is opt-in
   with `?execute=true` (2026-09-22).
5. `GET /plannerz` accepts the operator password; `/plannerz/analyze` requires
   the admin password (2026-09-28).
