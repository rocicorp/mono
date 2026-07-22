### Reading the output

For throughput rows—transactions, changes, payload MB—the displayed value is time per unit.
Invert it to get throughput.

For latency rows:

- First number: median/p50
- Next number: p75
- Parentheses: minimum and maximum
- Second-line number: p99
- The separate p95 row is synthetic; only its first value matters.

### Derived results

Workload                   Approximate throughput    SQLite transaction p50 / p95 / p99
━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1-row high-frequency     28.5k tx/s and changes/s                 31 µs / 53 µs / 84 µs
──────────────────────  ───────────────────────────  ────────────────────────────────────
100-row mixed/schema    787 tx/s, 78.6k changes/s           1.33 ms / 3.01 ms / 4.10 ms
──────────────────────  ───────────────────────────  ────────────────────────────────────
1001-row oversized       336 tx/s, 336k changes/s           3.02 ms / 3.82 ms / 4.04 ms

Important observations:

- Small transactions are commit-bound: roughly 25 µs of the 31 µs median transaction is the
SQLite commit itself.

- Larger logical transactions amortize commit and statement overhead very effectively.
- The mixed workload is deliberately harsh: it includes long-tail payload sizes and real
schema mutations. Its slower per-change rate versus the oversized case is expected.

- The 1001-row case confirms that a transaction larger than the 1000-row purge target still
makes progress.

### Catchup

The catchup scanned approximately 24,072 rows:

- Around 472k rows/s
- 1000-row batch p50: 1.93 ms
- Batch p95: 2.63 ms
- End-to-end: about 51 ms

That supports 1000 rows as a reasonable initial reader batch size on this hardware.
Individual read snapshots remain short.

### Purge

Between-commit purge nearly doubles the cost of the one-row workload:

- Without purge: 35 µs per source transaction
- Purging after every commit: 69 µs
- Throughput falls from about 28.5k to 14.5k tx/s

That is a worst-case schedule and argues strongly for coalescing purge requests instead of
running one after every commit.

Idle purge looks efficient:

- About 470 µs median per purge transaction
- 524 µs p95
- Roughly 2.1 million deleted rows/s
- The oversized oldest transaction completes instead of looping