# CVR replay

Replays the client groups recorded in a CVR export against a Zero
deployment, optionally while running a column backfill and synthetic app
writes against the upstream database, and records what clients experience.

The clients speak the sync protocol directly: no local store and no client
IVM. Each one registers the named queries a real client group held, times how
long each takes to hydrate, and counts the pokes it receives. The query server
that zero-cache calls (`ZERO_QUERY_URL`) is not part of this module: point the
deployment at the application's own query server or at a port of it.

Keep workloads, specs and seed scripts derived from a customer's data or code
out of this repository. Pass them in by path.

## 1. Build a workload from a CVR export

```bash
pnpm --filter zero-throughput run replay:prepare -- \
  --cvr-dir <export>/<shard>_0 \
  --out <private-dir>/workload.json \
  --session-names-file <private-dir>/session-names.json
```

The export directory holds `instances.csv.gz`, `desires.csv.gz` and
`queries.csv.gz` (`COPY ... WITH (FORMAT csv, HEADER)` of the CVR tables).
The workload has:

- the groups that were active in the `--live-window-seconds` before the
  snapshot, each with the user it authenticated as (inferred from the query
  arguments) and its session queries in registration order;
- the screen queries (names held by fewer than `--session-threshold` of the
  live groups and not listed in `--session-names-file`), weighted by how many
  groups held them;
- the most common client schema and a few statistics: connects in the last
  hour, the mean session length that implies, and queries per group.

## 2. Run

```bash
pnpm --filter zero-throughput run replay -- \
  --workload <private-dir>/workload.json \
  --cache-url https://<zero-cache> \
  --auth-secret <secret> \
  --pg-url postgresql://...  \
  --groups 110 \
  --backfill-spec <private-dir>/backfill-spec.json \
  --backfill-tables work_titles \
  --backfill-page-size 100 \
  --backfill-schedule 0:600,24:1200,0:600,92:1200,0:600 \
  --backfill-confirm \
  --output results/replay/<name>.json
```

- **Sessions.** `--groups` sessions stay open. Each lasts an exponential time
  with mean `--mean-session-seconds` (default: the workload's estimate), then
  closes and is replaced on another idle device. Sessions are drawn from
  `--devices` devices (default `2 × groups`); a device that synced before
  reconnects with its cookie and the queries it already has, like an app
  with a local store. Screen queries arrive at `--screen-queries-per-minute`
  per session and stay for `--mean-screen-dwell-seconds`.
- **Clock-derived arguments.** By default every query replays with its
  recorded arguments, so a returning device already holds them. Clients that
  derive an argument from the clock register a new query when that value
  changes. `--arg-rewrite name.key=now|minute|hour|day` (repeatable or
  comma-separated) sets that key in the query's object arguments to the
  current time, floored to the unit in UTC. It applies whenever a session
  registers the query, so identical rewrites collapse into one query.
- **Auth.** Tokens are `<auth-secret>:<userID>`, sent the way zero-client
  sends them. zero-cache forwards them to the query server as
  `Authorization: Bearer ...`.
- **Protocol.** `--protocol-version` (default 52) must be one the server
  accepts. Versions before 52 receive JSON pokes, later ones binary chunks;
  both are handled.
- **Backfill.** The spec names a column, its old and new values, and per
  table the keyset column and an optional conflict guard over alias `t`
  (`{{from}}`/`{{to}}` become quoted literals). Each page is one transaction:
  a keyset page selected on the cheap predicate, and an UPDATE that applies
  the guards and skips rows failing the table's CHECK constraints. Triggers
  are suppressed (`SET LOCAL session_replication_role = replica`) unless
  `--backfill-suppress-triggers=false`. The schedule is
  `rowsPerSecond:seconds,...` starting after the ramp; `0` pauses.
  `--backfill-direction reverse` swaps the values to undo a run. Writing to
  a non-local database needs `--backfill-confirm`.
- **App writes.** `--app-writes-spec` is `{"writes": [{"name", "weight",
"sql"}]}`; each statement runs in its own transaction with `$1` bound to the
  user of a random open session, at `--app-writes-per-second`.
- **CloudZero.** With `--cloudzero-api-key` (or `CLOUDZERO_API_KEY`) and
  `--cloudzero-stack-id`, pod CPU, pipelines and lag are sampled into the
  timeline.

## 3. Local target

`--target local` stands up the stack on this machine instead:

1. the harness's Docker Postgres (or `--local-pg-url`);
2. a database (`--local-database`, rebuilt with `--local-reset`) whose tables
   come from the workload's client schema, with an index on every `*_id`
   column and a publication over their schemas;
3. the replayed user IDs in `replay_meta.users`, then each
   `--local-seed-sql` script;
4. the query server, started with `--local-query-server-command` (given
   `PORT` and `QUERY_SECRET`; it must answer `GET /health`);
5. zero-cache from this repository with `--local-num-sync-workers`.

## Output

`--output` gets a JSON result and a `.timeline.csv`. Phases follow the
backfill schedule. For each phase the summary reports:

- **hydration**: time from registering a query to the poke that confirms it;
- **tail p95**: hydration p95 over the last quarter of the phase, where a
  climb shows;
- **p95 slope/min**: the least-squares slope of per-bucket p95;
- **caught up**: time from connecting to the first complete poke;
- **held p95**: time from a backfill page's commit to the poke that carries a
  renamed row to a client holding it;
- **ping p95**: sync-worker round trip.

The timeline adds sessions, pending queries, pokes, backfill rows and page
latency, errors and the harness's own event-loop delay per bucket. If that
delay grows, the harness itself is the bottleneck: split the groups across
several processes.
