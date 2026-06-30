# zero-throughput

Phase 1 E2E throughput harness for Zero.

The default run:

1. Starts a dedicated PostgreSQL 16 Docker container on port `6436`.
2. Resets the benchmark table and Zero metadata for app id `zero_throughput`.
3. Deploys allow-read permissions for the benchmark table.
4. Starts `zero-cache` on port `4848`.
5. Starts synthetic Zero clients with live queries for the selected profile.
6. Writes profile-shaped rows to PostgreSQL at a fixed target rate.
7. Writes a JSON result file and prints a short summary.

```bash
pnpm --filter zero-throughput start
```

By default, the JSON result is written to `apps/zero-throughput/results/latest.json`
and zero-cache logs are written to `apps/zero-throughput/results/logs/`. The
summary is printed after child services are stopped so it is the final benchmark
output in the terminal.

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
