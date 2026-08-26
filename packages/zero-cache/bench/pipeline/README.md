# Zero-Cache Throughput & Pipeline Benchmarks

End-to-end multi-process throughput and latency benchmarking harness for Zero-Cache:

$$\text{Postgres Pool} \xrightarrow{\text{WAL}} \text{Replication Manager} \xrightarrow{\text{WS}} \text{View-Syncer} \xrightarrow{\text{IVM}} \text{Simulated Clients}$$

---

## 1. Quick Setup (30 seconds)

Ensure PostgreSQL and Zero packages are built:

```bash
pnpm run db-up                        # Start PostgreSQL (Docker)
pnpm --filter @rocicorp/zero run build  # Build packages
```

---

## 2. Common Benchmarks

Run from `packages/zero-cache`:

```bash
# 1. Capacity Sweep (Find max write rate ceiling)
pnpm --filter zero-cache run bench:capacity --write-rates 1000,2000,3000,4000
pnpm --filter zero-cache run bench:capacity --write-rates 10000,20000,30000 --rows-per-tx 20

# 2. Client Fanout Sweep (Test 5 to 200 concurrent WebSocket clients)
pnpm --filter zero-cache run bench:fanout --clients 5,10,15,20,50 --write-rate 200

# 3. Poke Coalescing & Frame-Rate Pacing
pnpm --filter zero-cache run bench:framerate --write-rate 500 --clients 5

# 4. Custom Single Run
pnpm --filter zero-cache run bench:pipeline --write-rate 1000 --clients-per-vs 10
```

---

## 3. Key CLI Flags

| Flag                            |        Default         | Description                                      |
| :------------------------------ | :--------------------: | :----------------------------------------------- |
| `--write-rates`                 | `100,250,500,1000,...` | Comma-separated list of target write rates.      |
| `--rows-per-tx`                 |          `1`           | Number of database rows bundled per transaction. |
| `--clients`                     |          `5`           | Connected WebSocket clients per View-Syncer.     |
| `--load-duration`               |          `6`           | Duration of active write generation in seconds.  |
| `--drain-timeout`               |          `3`           | Seconds to wait for queues to drain post-load.   |
| `--output-dir`                  |   `./bench-results`    | Output directory for summaries and JSON results. |
| `--profile-rm` / `--profile-vs` |        `false`         | Capture V8 CPU profiles (`.cpuprofile`).         |

---

## 4. Metric Interpretation

Output tables classify pipeline state by end-to-end serving lag (Postgres commit to client poke):

| Status        |          Lag Threshold          | System State                                                     |
| :------------ | :-----------------------------: | :--------------------------------------------------------------- |
| **HEALTHY**   |        $< 500\text{ms}$         | Real-time streaming; zero queue accumulation.                    |
| **DEGRADED**  | $500\text{ms} - 2,000\text{ms}$ | High CPU load; near service rate saturation.                     |
| **COLLAPSED** |       $> 2,000\text{ms}$        | Saturated; arrival rate exceeds service rate; queue accumulates. |
