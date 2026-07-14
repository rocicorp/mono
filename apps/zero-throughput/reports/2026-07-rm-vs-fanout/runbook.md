# Runbook: reproduce the RM → VS fan-out measurements

All commands from the repo root unless noted. On a normal dev box with Docker and
Node 24 you can skip the container workarounds in the appendix.

## 0. Prereqs

- Node 24 (the code uses `using` declarations; Node 22 cannot parse them — CI
  uses 24).
- `pnpm install && pnpm --filter @rocicorp/zero run build`.
- Docker (for the harness's Postgres) — or an external PG 16 (see appendix).

## 1. Reference ceilings (no PG)

```bash
# R1 — pure-IVM ceiling for the relational hot profile (300 views)
pnpm --filter zql-benchmarks run bench:mem zero-throughput-relational

# R2 — RM replica-apply + change-log SQLite ceiling
pnpm --filter zero-cache exec vitest run --config vitest.config.bench.ts \
  sqlite-change-log-ceiling
```

## 2. Metric (1): sustainable hot writes/s vs sync workers

```bash
pnpm --filter zero-throughput run sweep -- \
  --profiles relational --models hot --users 50 --queries-per-user 3 \
  --rows-per-query 50 --sync-workers 1,2,4 \
  --duration-ms 45000 --warmup-ms 15000 --search-steps 6 \
  --write-rate-min 1 --write-rate-max 400 \
  --output-dir results/sweeps/rm-vs-fanout
# read results/sweeps/rm-vs-fanout/summary.csv -> bestWriteRate per syncWorkers row
```

On production hardware raise `--write-rate-max` and `--users` (50,100,200,400)
and use the default 300 s durations for stable knees.

## 3. Metrics (2)/(3): RM→VS fan-out (subscriber-sim)

The sim attaches K protocol-faithful change-stream subscribers to a running
change-streamer and measures delivered changes/s. Drive it against a live harness
run so the replica watermark advances:

```bash
# Terminal 1 — a long feed-append run gives a steady change stream on :4849
pnpm --filter zero-throughput start -- \
  --profile feed-append --users 2 --write-rate 2000 --batch-size 20 \
  --duration-ms 300000 --zero-num-sync-workers 1

# Terminal 2 — fan-out to K FAST subscribers (expect linear per-sub delivery)
node packages/zero-cache/src/scripts/subscriber-sim.ts \
  --change-streamer-uri ws://127.0.0.1:4849/ \
  --replica-file /tmp/zero-throughput-replica.db \
  --subscribers 8 --duration-ms 8000 --warmup-ms 2500

# One SLOW subscriber (the flow-control coupling): expect the fast subscribers to
# collapse from 2000/s to ~48/s at a 2-5 ms per-message delay.
node packages/zero-cache/src/scripts/subscriber-sim.ts \
  --change-streamer-uri ws://127.0.0.1:4849/ \
  --replica-file /tmp/zero-throughput-replica.db \
  --subscribers 4 --slow-subscribers 1 --ack-delay-ms 2 \
  --duration-ms 8000 --warmup-ms 2500
```

Compare the consensus-padding knob by setting
`ZERO_CHANGE_STREAMER_FLOW_CONTROL_CONSENSUS_PADDING_SECONDS=0` (or a negative
value to disable early release) in the harness's environment before launching it,
then re-running the slow-subscriber sim.

`subscriber-sim.ts` flags: `--subscribers K`, `--slow-subscribers N`,
`--ack-delay-ms MS` (per-message delay on the slow ones), `--duration-ms`,
`--warmup-ms`, `--change-streamer-uri`, `--replica-file`, `--change-db`,
`--app-id`.

## 4. Stage / CPU attribution

Run a _saturating_ hot run with CPU profiling and OTLP metrics:

```bash
# a minimal OTLP/http-json sink (any collector works; a ~90-line one is in the
# scratchpad as otlp-sink.mjs) listening on :4318, then:
NODE_OPTIONS="--cpu-prof --cpu-prof-dir=/tmp/prof --cpu-prof-interval=200" \
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
OTEL_METRIC_EXPORT_INTERVAL=3000 \
pnpm --filter zero-throughput start -- \
  --profile relational --model hot --users 50 --queries-per-user 3 \
  --rows-per-query 50 --write-rate 8 --zero-num-sync-workers 2 \
  --duration-ms 45000 --warmup-ms 12000
```

- Map worker pids to `.cpuprofile` files via the zero-cache log
  (`results/logs/*-zero-cache.log`, grep `worker=syncer`); the two biggest
  profiles are the sync workers. Read them in speedscope or aggregate self-time
  by file/function.
- Key OTLP metrics: `zero.sync.advance-time`, `zero.sync.cvr.flush-time`,
  `zero.sync.hydration-time`, `zero.sync.ivm.advance-time`,
  `zero.sync.pipeline-resets` vs `zero.sync.hydration` (the load-shed thrash),
  and `zero.replication.flow_control.*` (RM side — expect ~idle in hot runs).

## Appendix — container without Docker or IPv6 (this session's setup)

If Docker is unavailable, run PG 16 directly and point the harness at it with
`ZERO_THROUGHPUT_PG_START=false`:

```bash
runuser -u postgres -- /usr/lib/postgresql/16/bin/initdb -D <datadir> -U user \
  --pwfile=<pwfile> --auth-host=scram-sha-256 --auth-local=trust
runuser -u postgres -- /usr/lib/postgresql/16/bin/pg_ctl -D <datadir> -o \
  "-p 6436 -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10 \
   -c hot_standby=on -c hot_standby_feedback=on" start
```

Other gotchas seen on the bare container (none require code changes):

- `@rocicorp/zero-sqlite3` needs `node deps/gen-unicode-case.mjs >
src/util/unicode_case_data.h` then `node-gyp rebuild --release --jobs 1`
  (serial, to avoid OOM) if no prebuilt binary downloads.
- `pnpm install --ignore-scripts` links workspace bins without re-running the
  native build; `pnpm config set verify-deps-before-run false` stops per-command
  re-installs.
- No kernel IPv6 → `http-service.ts`'s hardcoded `host: '::'` fails with
  `EAFNOSUPPORT`; a `net.Server.prototype.listen` preload shim rewriting `::` →
  `0.0.0.0` (via `NODE_OPTIONS=--import`) is a local-only workaround.
