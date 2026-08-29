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

Models:

| Model       | Behavior                                                                 |
| ----------- | ------------------------------------------------------------------------ |
| `hot`       | Existing pathological shape: every write targets every active query set. |
| `realistic` | Clients watch spread-out partitions; writes mix active and cold targets. |

`hot` is the default and preserves the original profile behavior. Use
`--model realistic` to run the same profile query shapes with deterministic
active/cold partitions and write-impact counters in the result summary.

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
  --model realistic \
  --users 10 \
  --queries-per-user 3 \
  --rows-per-query 50 \
  --write-rate 250 \
  --duration-ms 60000 \
  --output results/relational-10u-250rps.json
```

Run the recommended parameter sweep. The default sweep covers
`relational,email,forum`, users `50,100,200,400`, rows per query `50`, sync
workers `1,2,4`, model `hot`, and binary-searches the sustainable write rate from 1 to 100
logical writes/s for each point. This keeps the first sweep focused on
read-heavy fanout, profile complexity, and syncer concurrency without exploding
the run count.

```bash
pnpm --filter zero-throughput run sweep -- --dry-run
pnpm --filter zero-throughput run sweep -- \
  --output-dir results/sweeps/read-heavy
```

To also sweep query window size, add `--rows-per-query 25,50,100`.
To compare hot and realistic workloads for the same matrix, add
`--models hot,realistic`.

Sweep output includes:

- `manifest.json` with the exact matrix and git SHA
- `attempts.jsonl` with every benchmark attempt
- `points.jsonl` with one binary-search result per matrix point
- `summary.csv` with the best sustainable write rate per point
- `runs/` containing the normal per-run benchmark JSON outputs
- `logs/` containing zero-cache and query-plan logs for each benchmark run

Use `--limit 1` for a smoke run, `--pg-url <url>` when PostgreSQL is already
running, and `--verbose-child-logs` to stream each benchmark's full output.

Analyze the exact profile query shapes against a running zero-cache:

```bash
pnpm --filter zero-throughput run analyze -- \
  --zero-cache-url=http://127.0.0.1:4848 \
  --profile relational \
  --model realistic \
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

Realistic runs intentionally include writes that no active client group should
observe. The result JSON still records the existing global seq-lag fields, but
realistic pass/fail uses client-visible lag and connection/initial-sync checks;
write-impact counters report the active-query impact rate.

To stream zero-cache logs directly in the terminal:

```bash
pnpm --filter zero-throughput start -- --process-log-mode inherit
```

## Process Topologies: Single-Node vs. Distributed

### 1. Default Topology (`--topology single`)

By default, the benchmark runs in single-node mode:

- A single `zero-cache` process tree is started on port `4848`.
- The **Replication Manager** (WAL ingestion pipeline), the **Change Streamer**, and the **Syncer parent** all run in this single parent process, sharing a single local SQLite replica file (`replica.db`).
- The syncer forks `--num-sync-workers N` (default: 1) child worker processes for parallel WebSocket client connection handling, but all workers read from the same shared local SQLite replica and receive in-process change notifications.

### 2. Distributed Topology (`--topology distributed`)

Decouples the architecture into independent processes matching production multi-pod clusters:

$$\text{PostgreSQL} \xrightarrow{\text{WAL}} \text{Replication Manager (rm)} \xrightarrow{\text{WS}} \text{View-Syncer Pods (vs-0} \dots \text{vs-(N-1))} \xrightarrow{\text{IVM}} \text{Clients}$$

- **Replication Manager (`rm`)**: Runs as a dedicated process on port `4848` (with dedicated change-streamer on `4849`) with `numSyncWorkers = 0`. It connects to PostgreSQL, ingests the WAL stream, writes to `replica.db-rm`, and streams change events downstream. It serves no client queries directly.
- **View-Syncers (`vs-0` ... `vs-(N-1)`)**: Run as independent server processes on ports `4858`, `4859`, etc., each maintaining its own SQLite replica (`replica.db-vs0`, `replica.db-vs1`). They connect to `rm`'s change streamer over WebSocket and run independent IVM pipelines. Synthetic clients are partitioned across these View-Syncer instances.

### Examples

```bash
# 1. Distributed topology with 2 View-Syncer pods and 2 sync workers each
pnpm --filter zero-throughput start -- \
  --topology distributed \
  --num-view-syncers 2 \
  --num-sync-workers 2 \
  --profile forum \
  --users 20 \
  --write-rate 200

# 2. Profile RM (rm) and primary View-Syncer (vs-0) under heavy write load
#    --profile-rm profiles WAL ingestion & change-stream encoding on rm
#    --profile-vs profiles IVM pipeline advancement & diff calculation on vs-0
pnpm --filter zero-throughput start -- \
  --topology distributed \
  --num-view-syncers 2 \
  --num-sync-workers 2 \
  --profile forum \
  --users 20 \
  --write-rate 500 \
  --duration-ms 15000 \
  --profile-rm \
  --profile-vs \
  --profile-dir results/profiles

# 3. Linear throughput parameter sweeps (prints side-by-side comparison tables)
#    - sweep:write-rates      Single-pod write ingestion ceiling (500-4000 w/s)
#    - sweep:num-view-syncers Multi-pod scaling across 1, 2, and 3 VS pods side-by-side
#    - sweep:users            Active client subscription fanout (5-50 users)
pnpm --filter zero-throughput run sweep:write-rates
pnpm --filter zero-throughput run sweep:num-view-syncers
pnpm --filter zero-throughput run sweep:users
```

## Load Generation: Write Concurrency & Batching

When benchmarking Zero at high target write rates (e.g. 1,000–5,000+ writes/s), the test harness itself can bottleneck on sending writes to PostgreSQL if using default settings.

### The Problem: Single-Connection Serial Bottleneck

By default, the benchmark writer uses a single PostgreSQL connection (`--write-concurrency 1`) inserting individual single rows (`--batch-size 1`):

- Each write executes as a synchronous round-trip to PostgreSQL (`INSERT ... RETURNING seq`).
- Network latency, SQL parsing, and PostgreSQL WAL fsync require $\sim 1\text{--}2\text{ms}$ per transaction.
- Consequently, a single serial client connection maxes out around $\sim 500\text{--}800\text{ writes/s}$, causing the harness to fall behind the target write rate even when Zero's replication pipeline has ample headroom.

### The Solution: Concurrency & Multi-Row Batching

To generate true high-throughput load without client-side starvation, two orthogonal controls are provided:

1. **`--write-concurrency <N>`** (default: `1`):
   Spawns a pool of $N$ concurrent database writer connections. Writes are distributed evenly across the worker pool, allowing the harness to saturate multiple PostgreSQL backend processes simultaneously.

2. **`--batch-size <N>`** (default: `1`):
   Bundles $N$ logical rows into a single multi-row `INSERT` statement or transaction. This amortizes network round-trip overhead, SQL parsing, and transaction commit fsyncs across all rows in the batch.

### High-Throughput Example

```bash
# Push 2,000 logical writes/s using 5 concurrent writer connections in batches of 10 rows
pnpm --filter zero-throughput start -- \
  --profile feed-append \
  --write-rate 2000 \
  --write-concurrency 5 \
  --batch-size 10 \
  --duration-ms 30000
```

## Benchmarking an Already-Running Zero Stack or Database

By default, the benchmark harness operates in self-contained mode: it spins up a local PostgreSQL Docker container on port `6436`, provisions schemas, and launches local `zero-cache` process trees.

You can point the harness at any pre-existing environment—such as a local `pnpm run dev` cluster, Docker Compose stack, or remote staging cluster—by disabling internal service orchestration.

### 1. Individual Benchmark Runs

Simply provide `--cache-url` (or `--cache-urls`) and `--pg-url`. Specifying external URLs automatically infers that the services are already running and disables local process orchestration:

```bash
# Point synthetic clients at an existing Zero cache and PostgreSQL
pnpm --filter zero-throughput start -- \
  --cache-url http://127.0.0.1:4848 \
  --pg-url postgresql://user:password@127.0.0.1:6436/postgres \
  --profile forum \
  --users 20 \
  --write-rate 300 \
  --duration-ms 30000
```

Or configure via environment variables:

```bash
ZERO_THROUGHPUT_CACHE_URL=http://127.0.0.1:4848 \
ZERO_THROUGHPUT_PG_URL=postgresql://user:password@127.0.0.1:6436/postgres \
pnpm --filter zero-throughput start -- --write-rate 300
```

> **Preserving Existing Data**: By default, the harness drops and recreates the benchmark table before each run. To preserve existing database tables and records, pass `--reset false` (or `ZERO_THROUGHPUT_RESET=false`).

### 2. Multi-Pod Addressing vs. Load Balancers

When benchmarking a multi-pod or distributed View-Syncer deployment:

- **Behind a Load Balancer / Ingress**: Provide the single load balancer URL via `--cache-url`. All synthetic client WebSockets connect to that endpoint and let the load balancer distribute traffic across the backend pods.
  ```bash
  pnpm --filter zero-throughput start -- \
    --cache-url https://zero-lb.staging.internal \
    --users 50
  ```
- **Direct Multi-Pod Addressing**: Provide a comma-separated list of URLs via `--cache-urls` (or `--cache-url`). Synthetic clients are evenly round-robin partitioned across the endpoints by client index:
  ```bash
  pnpm --filter zero-throughput start -- \
    --cache-urls http://10.0.1.10:4848,http://10.0.1.11:4848,http://10.0.1.12:4848 \
    --users 60
  ```

### 3. Running Parameter Sweeps Against Existing Clusters

Both binary searches (`sweep`) and linear sweeps (`sweep:write-rates`, `sweep:users`) support external services:

```bash
# Linear write-rate sweep against an existing cluster
pnpm --filter zero-throughput run sweep:write-rates -- \
  --cache-url http://127.0.0.1:4848 \
  --pg-url postgresql://user:password@127.0.0.1:6436/postgres
```

### Options Reference

| CLI Option            | Environment Variable         | Default                 | Description                                                        |
| :-------------------- | :--------------------------- | :---------------------- | :----------------------------------------------------------------- |
| `--cache-url <url>`   | `ZERO_THROUGHPUT_CACHE_URL`  | `http://127.0.0.1:4848` | Primary Zero cache endpoint or load balancer (disables local Zero) |
| `--cache-urls <urls>` | `ZERO_THROUGHPUT_CACHE_URLS` | `undefined`             | Comma-separated View-Syncer URLs for client partitioning           |
| `--pg-url <url>`      | `ZERO_THROUGHPUT_PG_URL`     | `postgresql://...:6436` | Upstream database connection string (disables local Postgres)      |
| `--reset <bool>`      | `ZERO_THROUGHPUT_RESET`      | `true`                  | When `false`, skips dropping/resetting the benchmark table         |

Run `pnpm --filter zero-throughput start -- --help` for all options.
