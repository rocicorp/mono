# zero-throughput

Phase 1 E2E throughput harness for Zero.

The default run:

1. Starts a dedicated PostgreSQL 16 Docker container on port `6436`.
2. Resets the benchmark table and Zero metadata for app id `zero_throughput`.
3. Deploys allow-read permissions for the benchmark table.
4. Starts `zero-cache` on port `4848`.
5. Runs analyze-query for each distinct live query shape in the selected profile.
6. Starts synthetic Zero clients with live queries for the selected profile.
7. Writes profile-shaped rows to PostgreSQL at a fixed target rate.
8. Writes a JSON result file and prints a short summary.

```bash
pnpm --filter zero-throughput start
```

By default, the JSON result is written to `apps/zero-throughput/results/latest.json`
and zero-cache logs are written to `apps/zero-throughput/results/logs/`. The
query plan analysis is written to the same logs directory as
`<runID>-query-plans.log`. The summary is printed after child services are
stopped so it is the final benchmark output in the terminal.

Useful overrides:

```bash
pnpm --filter zero-throughput start -- \
  --profile feed-append \
  --users 10 \
  --queries-per-user 1 \
  --rows-per-query 100 \
  --write-rate 500 \
  --batch-size 10 \
  --duration-ms 60000 \
  --output results/feed-append-10u-500rps.json
```

Profiles:

| Profile       | Query shape                                                       | Write shape                                   |
| ------------- | ----------------------------------------------------------------- | --------------------------------------------- |
| `feed-append` | Recent append-only events                                         | Insert one event row                          |
| `email`       | Inbox threads, message lists, and unread thread queries           | Insert message and update parent thread       |
| `forum`       | Category/thread/post queries with author and thread relationships | Insert post and update parent thread/category |
| `relational`  | Org/account/activity queries with nested account/contact joins    | Insert activity and update parent account/org |

`queriesPerUser` cycles through the distinct query shapes for each profile, so
setting `--queries-per-user 3` registers the full current mix for `email`,
`forum`, and `relational`.

`writeRate` is measured in logical writes per second. A logical write maps to
one monotonic `seq`; non-feed profiles may touch additional parent rows so their
list and relationship queries observe that `seq`.

Example profile run:

```bash
pnpm --filter zero-throughput start -- \
  --profile relational \
  --users 10 \
  --queries-per-user 3 \
  --rows-per-query 50 \
  --write-rate 250 \
  --duration-ms 60000 \
  --output results/relational-10u-250rps.json
```

Run the recommended parameter sweep. The default sweep covers
`relational,email,forum`, users `50,100,200,400`, rows per query `50`, sync
workers `1,2,4`, and binary-searches the sustainable write rate from 1 to 100
logical writes/s for each point. This keeps the first sweep focused on
read-heavy fanout, profile complexity, and syncer concurrency without exploding
the run count.

```bash
pnpm --filter zero-throughput run sweep -- --dry-run
pnpm --filter zero-throughput run sweep -- \
  --output-dir results/sweeps/read-heavy
```

To also sweep query window size, add `--rows-per-query 25,50,100`.

Sweep output includes:

- `manifest.json` with the exact matrix and git SHA
- `attempts.jsonl` with every benchmark attempt
- `points.jsonl` with one binary-search result per matrix point
- `summary.csv` with the best sustainable write rate per point
- `runs/` containing the normal per-run benchmark JSON outputs
- `logs/` containing zero-cache and query-plan logs for each benchmark run

Use `--limit 1` for a smoke run, `--pg-start false` when PostgreSQL is already
running, and `--verbose-child-logs` to stream each benchmark's full output.

Analyze the exact profile query shapes against a running zero-cache:

```bash
pnpm --filter zero-throughput run analyze -- \
  --zero-cache-url=http://127.0.0.1:4848 \
  --profile relational \
  --query-index 2 \
  --rows-per-query 50 \
  --join-plans
```

Useful profile query diagnostics:

```bash
pnpm --filter zero-throughput run analyze -- --list-profile-queries
pnpm --filter zero-throughput run analyze -- \
  --profile-query relational:activity-list \
  --rows-per-query 50 \
  --print-ast
```

`--print-ast` prints the server-mapped AST, so it can be passed directly to
the underlying analyze-query `--ast` option.

To stream zero-cache logs directly in the terminal:

```bash
pnpm --filter zero-throughput start -- --process-log-mode inherit
```

Use an already-running PostgreSQL or Zero:

```bash
ZERO_THROUGHPUT_PG_START=false \
ZERO_THROUGHPUT_PG_URL=postgresql://user:password@127.0.0.1:6436/postgres \
ZERO_THROUGHPUT_ZERO_START=false \
ZERO_THROUGHPUT_CACHE_URL=http://127.0.0.1:4848 \
pnpm --filter zero-throughput start
```

Run `pnpm --filter zero-throughput start -- --help` for all options.
