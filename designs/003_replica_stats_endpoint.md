# 003: `/plannerz`, replica statistics and query plans for LLMs

- **Status:** Proposed
- **Date:** 2026-09-22
- **Packages:** `zero-cache` (admin endpoint), `zqlite` (stat4 decoding)

## Goal

A developer gives an LLM two things: their codebase and an admin URL on their
zero-cache. The LLM tells them how to rewrite ZQL queries, and which Postgres
indexes to add, so the queries run well **on the plan Zero actually picks**.

Constraint: the endpoint must not return row data. Schema, sizes, and
statistics are allowed. Values from the database are not.

## What exists today

| Piece                                                         | Where                                                                                    | Notes                                                                                                                                                                                                                                                                                             |
| ------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Admin HTTP endpoints `/statz`, `/heapz`, `/profz`, `/profrmz` | `server/runner/zero-dispatcher.ts`                                                       | Basic auth via `isAdminPasswordValid` (password optional in dev). `/statz` already opens `config.replica.file` read-only for pragmas.                                                                                                                                                             |
| `analyze-query`                                               | `services/view-syncer/inspect-handler.ts` → `services/analyze.ts`                        | Websocket inspector only. Needs a connected client (uses the CVR `clientSchema`). **Runs** the query (capped at `MAX_ANALYZE_ROWS` = 1000 per table). Returns planner events (`joinPlans`), SQLite plans, and read counts per SQL. Can also return `syncedRows`/`vendedRows`, which are row data. |
| Planner cost model                                            | `zqlite/src/sqlite-cost-model.ts`, `sqlite-stat-fanout.ts`                               | Costs come from SQLite `scanstatus` estimates. Join fanout comes from stat4, else stat1, else the default of 3.                                                                                                                                                                                   |
| Stats collection                                              | `db/migration-lite.ts:159`, `replicator/change-processor.ts:991`, `zqlite/src/db.ts:156` | Only `PRAGMA optimize` runs, always under `analysis_limit` (1000 is set explicitly in `workers/replicator.ts:176`; `optimize` sets its own limit otherwise). There is no full `ANALYZE` anywhere.                                                                                                 |

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
    "replicaWatermark": "…",       // getReplicationState, as /statz does
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
    "method": "PRAGMA optimize with analysis_limit (approximate)",
    "tablesWithoutStats": ["…"]
  },
  "tables": [{
    "name": "issue",
    "columns": [{"name": "…", "type": "…", "nullable": true, "zqlType": "…"}],
    "primaryKey": ["id"],
    "estimatedRows": 20000,        // first number in the table's stat1 row
    "syncable": true,              // present in zqlSpecs (not backfilling or internal)
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
      "stat4": [                          // omitted when empty (the normal case)
        {"nEq": [..], "nLt": [..], "nDLt": [..], "sample": [{"kind": "text"}, …]}
      ]
    }]
  }]
}
```

#### 1.1 Sources

- Tables and columns: `listTables` and `computeZqlSpecs` from `db/lite-tables.ts`.
  Don't list Zero's internal tables (`_zero.*`, change log, and so on).
- Indexes: `listIndexes` from `db/lite-tables.ts`. It also returns the indexes
  Zero creates itself.
- Stats: plain `SELECT` from `sqlite_stat1` and `sqlite_stat4`. Handle the
  case where these tables don't exist yet.

#### 1.2 Privacy rules

These are enforced in code and pinned by tests.

- **stat4 `sample` values are never returned.** Each sample becomes a list of
  `{kind}` per key column (`null | integer | real | text | blob`), decoded from
  the record header. That is the same decoding `#decodeSampleIsNull` already
  does. The numeric arrays (`nEq`/`nLt`/`nDLt`) are returned. They show skew
  ("one key covers 40% of the rows") but not which key it is. The trailing
  rowid in each sample is also dropped.
- There is no raw-samples option in v1. Add one only if users ask for it.
- What is returned: table and column names, index definitions, row count
  estimates, and distinct-value averages. That is expected for an admin
  endpoint.

#### 1.3 How approximate the stats are

The server can't tell when stats were gathered or with which limit, but the
code path is known. Report it as fixed text in `statsQuality.method`. If stat4
has rows, report `"ANALYZE (full)"`. Don't compare against `count(*)`, because
that scans the whole table.

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
- Pointers: the docs URL and `POST /plannerz/analyze` for checking a specific
  query.

Keep it short, under about 1.5k tokens, and snapshot-test it.

### Phase 2: `POST /plannerz/analyze` returns the plan for one query

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

- A docs page with a prompt snippet like "fetch `$URL/plannerz`, read my
  queries in `src/queries.ts`, then call `/plannerz/analyze` on the three most
  expensive ones".
- Maybe a `zero-cache` MCP tool that wraps both routes. Only worth it if people
  use the HTTP version.
- `?format=md` if JSON turns out to cost too many tokens on large schemas. It
  probably won't.

## Files

- `packages/zero-cache/src/services/plannerz.ts`: request handler, bundle
  builder, stat parsing and redaction.
- `packages/zero-cache/src/services/plannerz.test.ts`
- `packages/zero-cache/src/server/runner/zero-dispatcher.ts`: add routes.
- Maybe `zqlite/src/sqlite-stat-fanout.ts`: move the record-header decoding
  into a shared function so the fanout code and the redaction use the same one.
- Phase 2: a `clientSchema` from `tableSpecs` adapter next to `services/analyze.ts`.

## Tests

- 401 without a password or with a wrong one. Allowed in dev mode without a
  password, which matches the other endpoints.
- Bundle built from a fixture replica: tables, indexes, and stat1 parsing,
  including the `unordered` and `sz=` flags.
- **Redaction**: run a full `ANALYZE` on a fixture with sentinel strings
  (`"SECRET-…"`). Check that the response contains stat4 counts and that no
  sentinel appears anywhere in the serialized body. Do the same check on the
  phase 2 response.
- No stats yet (fresh replica, stat tables missing) returns
  `statsQuality.stat1Present=false` and no error.
- Internal tables are left out.
- Phase 2 plan-only: a zbugs-style AST returns `joinPlans` and `sqlitePlans`
  with no row fields, and reads no user table. Pin the "reads nothing" part by
  counting reads, for example with a `TableSource` spy or by asserting that the
  fetch path is never entered.
- Phase 2 execute mode: same AST with `?execute=true` adds measured row counts,
  and still has no row fields.
- Manual eval: point Claude at zbugs plus a local `/plannerz` and see whether
  the advice is correct.

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
