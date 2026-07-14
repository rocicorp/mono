# Stage 3: CPU + stage attribution (relational HOT, 50 users, 2 sync workers, 8 wps, SATURATED)
Run FAILED (p99 lag 8.8s, lag slope +0.96) => genuinely saturated, which is what we profile.
Only 1083 changes / 362 txns in 45s (~24 changes/s) overwhelm the syncers.

## Syncer worker CPU profile (self-time, ex-idle ~30%)
| share | area | file |
|--:|---|---|
| 16.3% | SQLite row iteration/exec (`next` 10.3%, `get`, `run`) | zqlite/db.ts |
| 3.5% | IVM source reads | zqlite/table-source.ts |
| 3.5% | IVM advance orchestration | view-syncer/pipeline-driver.ts |
| 3.0% | row-key stringify (`rowIDString`) | zero-cache/types/row-key.ts |
| 3.4% | SQL string building (`formatStandard`) | @databases/sql |
| ~3%  | JSON/bigint serialization | json-custom-numbers, bigint-json, encoding |
| 1.9% | logger withContext (even at error level) | @rocicorp/logger |
| ~2%  | IVM operators (join/take/memory-source) | zql/src/ivm/* |

=> The syncer's active CPU is dominated by SQLite reads/scans during IVM advance
   (each client-group re-reads the replica + changelog for its queries), plus row-key
   construction, SQL-string building, and serialization.

## Stage timings (OTLP histograms, cumulative over run, units=seconds)
| stage | per-op | count | total | note |
|---|--:|--:|--:|---|
| cvr.flush-time | 46 ms | 768 | 35.2 s | flush CVR to Postgres after advance — CO-DOMINANT |
| advance-time (per client-group) | 58 ms | 542 | 31.2 s | advance all queries for a CG after a txn |
| hydration-time | 85 ms | 212 | 17.9 s | (re)materialize a query pipeline |
| ivm.advance-time (per change) | 3 ms | 4038 | 10.2 s | single-change IVM advance |
| cvr.load_duration | 172 ms | 28 | 4.8 s | CVR load |
| view_syncer_lag (e2e) | 5.68 s | 26 | | serving lag at saturation |

## THE thrash: load-shedding fires on nearly every batch
zero.sync.pipeline-resets = 189   vs   zero.sync.hydration = 201
=> At saturation the load-shed heuristic (eac60e7) projects advance cost > hydration and
   RESETS pipelines ~189 times, forcing constant rehydration (85 ms each). The syncer
   thrashes advance<->rehydrate instead of making progress. Amplification: ~24 changes/s in ->
   84,348 CVR rows flushed + 125,882 poke rows out over the run.

## Cost model per logical write (hot, 50 CGs, 3 changes/write)
Each write must, PER affected client-group: re-read changelog + SQLite-scan queries (IVM
advance, SQLite-bound) -> flush CVR to PG (46 ms) -> poke. Work = O(writes x client-groups x
views), serialized within a sync worker, and periodically thrown away by rehydration.

## RM side is idle here
change-streamer produced only ~24 changes/s; flow_control.wait_duration ~0. Confirms (again)
the RM/replication path is NOT the bottleneck for the hot workload — the VS advance+CVR is.
