# Zero-Cache Pipeline & Throughput Benchmark Harness

A multi-process, end-to-end benchmarking harness that simulates the full Zero-Cache pipeline under realistic production loads:

```mermaid
flowchart LR
    LoadGen["Load Generator (PG Pool)"] -->|SQL Writes| PG[("PostgreSQL")]
    PG -->|walsender (WAL)| RM["Replication Manager (RM)"]
    RM -->|WebSocket Stream| VS["View-Syncer (VS)"]
    VS -->|replica.db| SQLite[("SQLite Replica")]
    VS -->|IVM Diffing & Pokes| Clients["Simulated Clients (WebSockets)"]
```

---

## 1. Prerequisites & Quick Setup

### Step 1: Ensure PostgreSQL is Running

Start the local PostgreSQL container via Docker:

```bash
# From repository root or apps/zbugs:
pnpm run db-up
```

### Step 2: Build Zero Packages

Ensure the monorepo packages are built:

```bash
pnpm --filter @rocicorp/zero run build
```

---

## 2. Benchmark Commands

All benchmark scripts are executed from `packages/zero-cache`:

### A. Maximum Ingestion & Capacity Sweep

Sweeps target write rates to find the physical saturation limit of the system:

```bash
# 1-row transactions (1,000 to 5,000 writes/sec)
pnpm --filter zero-cache run bench:capacity --write-rates 1000,2000,3000,4000,5000 --rows-per-tx 1

# Multi-row transactions (10,000 to 40,000 rows/sec with 20 rows/tx)
pnpm --filter zero-cache run bench:capacity --write-rates 10000,20000,30000,40000 --rows-per-tx 20
```

### B. Client Fanout Scaling Sweep

Sweeps client counts (e.g. 5 to 200 clients) under a constant write load (e.g. 200 writes/sec) to identify single-thread View-Syncer saturation:

```bash
pnpm --filter zero-cache run bench:fanout --clients 5,8,10,12,15,20,50,100 --write-rate 200
```

### C. Frame-Rate Pacing & Latency Benchmark

Compares unconstrained poke dispatch vs. 20 FPS (50ms interval) vs. dynamic adaptive pacing:

```bash
pnpm --filter zero-cache run bench:framerate --write-rate 500 --clients 5
```

### D. Single-Point Benchmark Run

Runs a single benchmark scenario with custom parameters:

```bash
pnpm --filter zero-cache run bench:pipeline \
  --write-rate 1000 \
  --rows-per-tx 10 \
  --clients-per-vs 10 \
  --load-duration 6 \
  --drain-timeout 3
```

---

## 3. Command-Line Options Reference

| Flag                             |      Default      | Description                                                    |
| :------------------------------- | :---------------: | :------------------------------------------------------------- |
| `--write-rate`                   |       `200`       | Target write rate (rows/second) for single runs.               |
| `--write-rates`                  |  `500,1000,...`   | Comma-separated list of target rates for capacity sweeps.      |
| `--rows-per-tx`                  |        `1`        | Number of database rows bundled per transaction.               |
| `--clients` / `--clients-per-vs` |        `5`        | Number of concurrent WebSocket clients per View-Syncer.        |
| `--load-duration`                |        `6`        | Duration of active write generation (in seconds).              |
| `--drain-timeout`                |        `3`        | Maximum time allowed to drain backlogged queues post-load.     |
| `--output-dir`                   | `./bench-results` | Directory where summary text and raw JSON results are written. |
| `--profile-rm`                   |      `false`      | Captures V8 CPU profile for the Replication Manager.           |
| `--profile-vs`                   |      `false`      | Captures V8 CPU profile for the View-Syncer.                   |

---

## 4. Understanding Output Metrics

Each benchmark run produces a summary table and a detailed JSON artifact:

```
========================================================================================================================
                        CLIENT FANOUT SCALING SWEEP (1 View-Syncer @ 200 writes/sec)
========================================================================================================================
 Clients | Mode                | Total Pokes | Pokes/Client | IVM Adv (p50/p95) | E2E Lag (avg/p50/p95)    | Status
---------+---------------------+-------------+--------------+-------------------+--------------------------+------------
 5       | Baseline (Uncapped) | 1,990       | 398.0        | 7.5 / 35.0ms      | 31.2 / 31.2 / 31.2ms     | HEALTHY
 15      | Baseline (Uncapped) | 314         | 20.9         | 35.0 / 75.0ms     | 737.3 / 737.3 / 737.3ms  | DEGRADED
 50      | Baseline (Uncapped) | 409         | 8.2          | 35.0 / 75.0ms     | 2,932.9 / 2,932.9ms      | COLLAPSED
========================================================================================================================
```

- **Actual Rate**: Realized write throughput achieved by the load generator.
- **Pokes Rx**: Total reactive mutation messages received across all connected clients.
- **IVM Adv (p50/p95)**: Latency spent inside the View-Syncer evaluating differential query pipelines.
- **E2E Lag (avg/p50/p95)**: Elapsed time from transaction commit in PostgreSQL to WebSocket delivery at the client.
- **Status Classification**:
  - `HEALTHY`: Average lag $< 500\text{ms}$. System keeps up with arrival rate in real-time.
  - `DEGRADED`: Average lag between $500\text{ms}$ and $2,000\text{ms}$. System CPU is near capacity.
  - `COLLAPSED`: Average lag $> 2,000\text{ms}$. Write arrival rate exceeds service rate; queue accumulates monotonically.
