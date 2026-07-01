# Zmail Initial Sync Lessons

## Executive Summary

Zero initial sync for this zmail workload is dominated by moving and writing a very large logical payload, not by parser overhead or ordinary query planning. The hot path is: acquire a consistent Postgres snapshot, discover the published schema, create SQLite tables, stream every published row through Postgres `COPY`, decode the stream in Node, synchronously insert the rows into one SQLite replica, then build SQLite indexes.

The hard constraint is full materialization. Because all tables and all columns must be present locally, the `email_content` table forces Zero to read and emit about `27GB` of logical content through Postgres `COPY` and then write that content into SQLite. Local binary COPY-to-null already takes `106.475s`, so a strict 2x win from the original `183.5s` baseline, meaning `<=91.8s`, is impossible without changing the source representation, skipping/deferring data, using snapshots, or using a different bulk-load path.

The main measured bottlenecks are:

- Postgres logical COPY of the TOAST-heavy `email_content` payload.
- SQLite base-table writes of the same large content payload.
- SQLite index creation when indexes touch large content values.
- SQLite write serialization and write amplification when attempting to parallelize final-DB writes.

The strongest product-quality finding is index semantics. Zero currently discovers Postgres index columns and uniqueness, but not the index access method. As a result, a Postgres `USING hash (text)` index on `email_content` is translated into a normal SQLite B-tree index on `email_content(text)`. That one translated index costs about `17.7s` locally and is the clearest avoidable local regression. The next product investigation should preserve Postgres access method in index metadata and decide whether non-btree indexes should be replicated to SQLite at all.

Most other tested optimizations were negative or too small. CTID chunking reduces the longest individual copy task but not total throughput. More COPY workers increase contention. Text COPY is worse than binary. Larger batches, larger buffers, and common SQLite pragmas did not materially improve wall time. `64KiB` SQLite pages helped slightly but not enough to change the conclusion. Pre-creating indexes, staged worker DBs, direct concurrent SQLite writes, and a producer/consumer writer queue all lost because they either rewrote the fat payload again, lost the efficient single unsafe SQLite transaction, or added thread/queue overhead.

The likely paths to a larger win are not incremental JS batching changes. They are product or storage-path changes: avoid blindly creating non-btree-derived content indexes, defer or avoid syncing fat content at startup if requirements change, use a lower-level/native SQLite bulk-load path, or reproduce production storage/cache/temp-file conditions to explain the remaining production index gap.

## Background: How Initial Sync Works

The implementation lives mainly in `packages/zero-cache/src/services/change-source/pg/initial-sync.ts`. The zmail benchmark wrapper in `packages/zero-cache/src/db/zmail-initial-sync.bench.pg.ts` calls `initReplica()`, which runs initial sync as the setup migration for a new SQLite replica file.

Initial sync first establishes a consistent upstream read point. In the normal path, it creates a logical replication slot and records the slot's snapshot name and consistent LSN. In shadow mode, it exports a normal read-only snapshot instead. The LSN becomes the initial Zero state version, and all COPY workers import the same snapshot so every table is copied from one consistent database view.

After the snapshot exists, Zero reads the published schema via `getPublicationInfo()`. That query returns published tables, columns, replica identity information, and eligible indexes. Current index metadata includes table, index name, uniqueness, primary-key/replica-identity flags, and indexed columns/directions. It does not include the Postgres access method such as `btree` versus `hash`.

The SQLite replica is prepared before data copy. The migration wrapper enables unsafe bulk-load settings for a new replica: `unsafeMode(true)`, exclusive locking, `journal_mode = OFF`, and `synchronous = OFF`. Initial sync then creates all SQLite tables from the Postgres table specs. Primary keys are not embedded directly in the table definition; Zero relies on separately created unique indexes, including indexes derived from upstream primary keys.

Data copy is performed by a pool of Postgres read-only transactions. Each worker executes a `COPY (SELECT ...) TO STDOUT WITH (FORMAT binary)` stream for a table or CTID chunk. Binary COPY uses native decoders where Zero has them and casts unknown types to text in the SELECT. Text COPY exists as an option but was slower in these measurements.

The Node import path is synchronous at the SQLite sink. COPY chunks enter a `Writable`; fields are parsed, decoded to SQLite-compatible values, stored into a pending array, and flushed when row or byte thresholds are reached. `flush()` runs prepared SQLite insert statements against the one replica connection. The default batch size is `50`; the current experiment also carries remainder rows forward so threshold flushes write full batches and the final flush drains leftovers.

COPY workers improve source-side scheduling but do not make SQLite a parallel writer. All rows still land in the same SQLite database through one sink path. This is why CTID chunking can shorten the largest individual `email_content` COPY task while failing to produce a comparable wall-clock win: the same logical bytes still have to be decoded, bound, inserted, and written.

Indexes are created after COPY by default. `createLiteIndices()` maps each discovered Postgres index through `mapPostgresToLiteIndex()` and emits a SQLite `CREATE INDEX` statement. This post-copy strategy is generally better than maintaining indexes during import for this workload, because pre-creating indexes only shifts index maintenance cost into SQLite insert/flush time.

The benchmark measurements therefore need phase breakdowns, not just total time. Source COPY, Node parse/decode, SQLite flush, and index creation are separate bottleneck candidates. The import profile showed parser time is negligible, decode time is small, SQLite flush dominates no-index import, and large content-value indexes can dominate the index phase.

## Goal And Constraints

- Goal: understand whether Zero initial sync can be made roughly 2x faster for a zmail/email workload with a large TOAST-heavy `email_content` table.
- Correctness constraint: all columns and all tables must be fully materialized. No deferring or excluding `email_content` columns.
- Product constraints so far: no snapshots, no table layout changes, and no compression/raw-TOAST/shadow-column design unless later explicitly chosen.
- Local correctness check: user-visible replica rows must equal `403091`.
- Cleanup check: no `zmail_bench%` schemas, publications, or replication slots should remain after runs.

## Benchmark Hygiene

- Use the existing local zmail database with `TEST_PG_17=postgres://postgres:pass@localhost:5547/zmail` to avoid vitest Testcontainers setup delay.
- Keep `ZMAIL_APP_SUFFIX` short because PostgreSQL identifiers are capped at 63 bytes.
- Use Node `22.23.1`; `22.22.1` is not installed locally.
- Capture per-run artifacts: config, logs, parsed summary, cleanup status, and row count.
- Treat source COPY, client parse/decode, SQLite insert flush, and index creation as separate phases. Single wall-clock numbers are not enough.

## Source COPY Floor

- Local `email_content` binary COPY-to-null measured `27,743,089,096` bytes in `106.475s`, about `248.5 MiB/s`.
- Local text COPY-to-null was slightly slower at `109.195s`.
- Remote PlanetScale binary COPY-to-null was much slower, `778.148s` for about `26.1GB`.
- This means a strict 2x local win from an initial `183.5s` baseline, meaning `<=91.8s`, is impossible with full logical COPY and no source representation change. The source floor alone exceeds that.

## Copy And Chunking Lessons

- CTID chunking works mechanically and reduces the maximum `email_content` copy task from roughly `145-163s` to roughly `24-35s`.
- CTID chunking did not materially improve total local wall time in paired warm runs.
- More COPY workers, including `8` and `10`, mostly increased contention and aggregate task time.
- Text COPY was worse than binary COPY.
- Larger insert batches (`100`, `250`, `500`) did not improve full-run wall time in earlier trials.
- Larger buffer thresholds (`32MiB`, `64MiB`) reduced reported flush frequency but worsened wall time.
- SQLite pragma tuning tried so far (`cache_size=-1048576`, `mmap_size=1073741824`, `temp_store=memory`) did not improve the measured full run.

## Import Instrumentation Added

- Added gated `InitialSyncOptions.importProfile` so normal runs stay on the existing hot path.
- Added zmail benchmark flag `ZMAIL_IMPORT_PROFILE=1`.
- Added import tuning flags for controlled experiments:
  - `ZMAIL_INSERT_BATCH_SIZE`
  - `ZMAIL_MAX_BUFFERED_ROWS`
  - `ZMAIL_BUFFERED_SIZE_THRESHOLD_BYTES`
  - `ZMAIL_SQLITE_CACHE_SIZE`
  - `ZMAIL_SQLITE_MMAP_SIZE`
  - `ZMAIL_SQLITE_TEMP_STORE`
- Profile logging now records, per copy task:
  - elapsed wall time
  - stream wait / outside write callback time
  - parser time
  - decode time
  - pending-value store time
  - flush count, flushed rows, and flushed bytes
  - slice time
  - SQLite batch insert time
  - SQLite single insert time
  - pending-buffer clear time
  - status update time
  - batch and single statement counts

## Profiled No-Index Baseline

- Run: `no_chunk_workers5_no_indexes` with `ZMAIL_IMPORT_PROFILE=1`.
- Result: `403091` user rows, cleanup clean.
- Initial sync total: `128.738s`.
- Flush total: `90.383s`.
- Index total: `0.098s` because indexes were disabled.

### `email_content` Breakdown

| Phase                                | Seconds |  Share |
| ------------------------------------ | ------: | -----: |
| Total task wall                      | 128.709 | 100.0% |
| Stream wait / outside write callback |  34.947 |  27.2% |
| Binary parser                        |   0.106 |   0.1% |
| Decode / string conversion           |   3.391 |   2.6% |
| Value store                          |   0.042 |   0.0% |
| SQLite flush total                   |  90.119 |  70.0% |
| SQLite batch inserts                 |  55.688 |  43.3% |
| SQLite single inserts                |  34.411 |  26.7% |

Additional `email_content` counters:

- Rows: `200000`.
- Input bytes: `27.743GB`.
- Flush calls: `3199`.
- Batch statements: `2669`, covering `133450` rows at batch size `50`.
- Single statements: `66550`.
- Rows per flush averaged only `62.5`, because the 8MiB size threshold cuts buffers just above one batch for the large content rows.

## Import Path Takeaway

- The import profile shows SQLite insertion dominates local no-index wall time for `email_content`.
- Parser overhead is negligible.
- Decode overhead exists but is not the primary cost.
- A likely insert-path optimization is to reduce or eliminate the large number of single-row remainder inserts, but this should wait until the index workload is characterized.

## Insert Remainder Batching Experiment

Change tested:

- Normal threshold flushes now write only full insert batches.
- Remainder rows are retained in the pending buffer and carried into the next flush.
- Final stream flush still writes any leftover rows.
- Per-row byte sizes are tracked so retained `pendingSize` remains accurate.

Profiled no-index run after the change:

- Artifact root: `/var/folders/97/c3gvpw6d46g3nm0y2684_cfm0000gn/T/opencode/zmail-batch-carry-20260701/profile-no-index`.
- Rows: `403091` user rows.
- Cleanup: clean.
- Initial sync total: `127.477s`.
- Flush total: `89.930s`.
- Index total: `0.079s`.

`email_content` profile comparison:

| Metric            |  Before |   After |
| ----------------- | ------: | ------: |
| Task wall s       | 128.709 | 127.449 |
| Flush s           |  90.119 |  89.662 |
| Batch insert s    |  55.688 |  89.646 |
| Single insert s   |  34.411 |   0.000 |
| Batch statements  |    2669 |    4000 |
| Single statements |   66550 |       0 |
| Rows via batch    |  133450 |  200000 |
| Rows via single   |   66550 |       0 |

Actual-index run after the change:

- Artifact root: `/var/folders/97/c3gvpw6d46g3nm0y2684_cfm0000gn/T/opencode/zmail-batch-carry-20260701/actual-index`.
- Rows: `403091` user rows.
- Cleanup: clean.
- Total initial sync: `134.766s`.
- Copy wall, approximated as total minus index: `126.843s`.
- Flush: `87.558s`.
- Index: `7.923s`.
- Actual local `index / copy` ratio: `0.062x`.

Insert batching takeaway:

- The change successfully eliminated `email_content` single-row inserts.
- It did not materially reduce SQLite flush wall time: batch insert time absorbed nearly all of the previous single-insert time.
- This suggests the bottleneck is not statement count alone; it is likely bytes written through SQLite / B-tree work / binding large values / storage effects.
- Carry-over batching is therefore not a meaningful local speedup by itself. Any further insert-path work should target a more direct bulk-load path or lower SQLite write amplification rather than just reducing single-row remainder statements.

## Indexing Gap

- Observed local index times:
  - First local all-index baseline: about `20s`.
  - Warm local all-index repeat: about `8-9s`.
  - Production incident: `16m43s`.
- The local zmail DB is not index-realistic enough to reproduce production as-is.
- The current local Zero-discovered all-index set has only 15 indexes.
- Only two current indexes touch `email_content`, and both are on `email_id`:
  - `email_content_email_id_idx` took about `8.080s` in a warm all-index repeat.
  - `email_content_pkey` on the same key took about `0.119s` after the first index already existed.
- Metadata indexes stayed cheap locally, generally tens to low hundreds of milliseconds.

## Next Step

- Before insert-path work, run the direct SQLite index replay from `zmail-index-repro-plan.md`.
- Create one no-index SQLite base replica, copy it per synthetic variant, and apply synthetic SQLite `CREATE INDEX` statements directly.
- Measure total index time, per-index time, pragmas, DB size before/after, and whether repeated content-table indexes can approach the production `1003s` index phase.

## Direct SQLite Index Replay Results

Artifacts:

- Base replica: `/var/folders/97/c3gvpw6d46g3nm0y2684_cfm0000gn/T/opencode/zmail-index-repro-20260701/base-no-index.db`.
- Full results: `/var/folders/97/c3gvpw6d46g3nm0y2684_cfm0000gn/T/opencode/zmail-index-repro-20260701/index-replay-results.md`.
- JSON results: `/var/folders/97/c3gvpw6d46g3nm0y2684_cfm0000gn/T/opencode/zmail-index-repro-20260701/index-replay-results.json`.

Base replica:

- Built with `ZMAIL_INDEX_MODE=none` and preserved via `ZMAIL_REPLICA_OUTPUT_PATH`.
- Verified user rows: `403091`.
- Base size: about `27.996GB`.
- Replay pragmas after setup: `journal_mode=off`, `synchronous=0`, `temp_store=0`, `cache_size=-16000`, `mmap_size=0`, `threads=8`, `page_size=4096`.

Summary:

| Variant                           | Indexes | Total s | Size Delta GB | Slowest Index s |
| --------------------------------- | ------: | ------: | ------------: | --------------: |
| current zmail indexes             |      15 |  19.597 |         0.050 |           9.622 |
| 5x `email_content(email_id)`      |       5 |  49.039 |         0.021 |          10.253 |
| 10x `email_content(email_id)`     |      10 |  96.987 |         0.042 |           9.861 |
| 25x `email_content(email_id)`     |      25 | 243.617 |         0.105 |          10.050 |
| 5 metadata composites             |       5 |   0.472 |         0.032 |           0.143 |
| 10 content + 15 metadata          |      25 |  98.764 |         0.156 |          10.077 |
| fat-column probe (`text`, `html`) |       2 | 160.013 |        28.035 |         142.339 |

Prod-relative normalization:

- Production copy time was about `35m`, or `2100s`.
- Production index time was `16m43s`, or `1003s`.
- Production `index / copy` ratio was therefore about `0.48x`.
- Local no-index copy was about `128.2s` in the preserved-base run.
- Local current-index ratio was `19.6 / 128.2 = 0.15x`, clearly lighter than production.
- Local `5x email_content(email_id)` ratio was `49.0 / 128.2 = 0.38x`, close to production.
- Local `10x email_content(email_id)` ratio was `97.0 / 128.2 = 0.76x`, heavier than production on a copy-normalized basis.
- Normalized by copy scale alone, production indexing looks equivalent to roughly `6-7` local small-key `email_content` indexes, not roughly `100`.

Index replay takeaways:

- Small-key `email_content(email_id)` indexes scale almost linearly at about `9.7s` per index on this local machine.
- `25x email_content(email_id)` reaches only `243.6s`, about `24%` of the production `1003s` index phase in absolute time, but it is `1.9x` local copy time and therefore much heavier than the production `0.48x` ratio.
- Reaching `1003s` locally by absolute small-key content index count alone would require roughly `100` similar `email_content` indexes, but that is the wrong comparison when production copy took about `35m`.
- Compared to production copy time, a moderately heavier real content-index workload can plausibly explain `16m43s` of indexing.
- Normal `email_metadata` composite indexes are effectively irrelevant to the production-scale gap on this data: 5 took `0.472s`, and 15 in the mixed run added only about `1.8s` over 10 content indexes.
- The fat-column probe proves large content-value indexes can become expensive: `email_content(html)` alone took `142.3s` and added about `28GB` for the two fat indexes. Even so, two fat indexes plus this local environment still do not reproduce `1003s`.
- The local direct replay reproduces the first local all-index baseline (`19.6s`) and reinforces that the previous local zmail index workload was lighter than production.
- Remaining explanations for production are actual missing indexes in the local source DB, production storage/cache/CPU limits, production cold-cache behavior, SQLite temp placement, or production timing including extra work outside the measured local index loop.

## Next Step With Actual Local Indexes

- Add the real indexes that should have existed to the local PostgreSQL zmail DB.
- Rerun full initial sync with `ZMAIL_INDEX_MODE=all` to let Zero discover and create the actual SQLite index set.
- Compare the new local `index / copy` ratio to the production ratio of about `0.48x`.
- If the ratio now matches production, prioritize index strategy work before insert-path micro-optimizations.
- If the ratio remains far below production, continue with environment-sensitive index checks such as cold cache, SQLite temp placement, CPU limits, and production-like storage.

## Actual Local Index Rerun

Artifacts:

- Run root: `/var/folders/97/c3gvpw6d46g3nm0y2684_cfm0000gn/T/opencode/zmail-index-actual-20260701`.
- Experiment: `no_chunk_workers5_index_threads8`.

Current local public index set after adding the missing real indexes:

- `email_address`: `email_address_company_idx`, `email_address_name_idx`, `email_address_pkey`.
- `email_content`: `email_content_pkey` only.
- `email_metadata`: `email_metadata_category_received_idx`, `email_metadata_mailbox_received_idx`, `email_metadata_mailbox_updated_idx`, `email_metadata_message_id_unique`, `email_metadata_pkey`, `email_metadata_read_received_idx`, `email_metadata_received_idx`, `email_metadata_sender_received_idx`, `email_metadata_starred_received_idx`, `email_metadata_thread_received_idx`.

Result:

- User rows: `403091`.
- Cleanup: clean.
- Total initial sync: `136.350s`.
- Copy wall, approximated as total minus index: `127.363s`.
- Flush: `90.354s`.
- Index: `8.988s`.
- Local actual `index / copy` ratio: `0.071x`.
- Production `index / copy` ratio: about `0.48x`.

Slowest actual local indexes:

| Index                                             | Time s |
| ------------------------------------------------- | -----: |
| `email_content_pkey` on `email_content(email_id)` |  8.160 |
| `email_metadata_category_received_idx`            |  0.257 |
| `email_metadata_sender_received_idx`              |  0.133 |
| `email_metadata_read_received_idx`                |  0.079 |
| `email_metadata_starred_received_idx`             |  0.078 |

Actual-index rerun takeaways:

- The new real local indexes are mostly metadata indexes, and they are cheap on this dataset.
- Removing the duplicate non-unique `email_content(email_id)` index makes actual local indexing lighter than the earlier synthetic/current replay baseline.
- The actual local source index workload still does not explain the production `0.48x` index/copy ratio.
- To match production ratio locally, the workload would need several more content-table indexes, fat content-value indexes, or a production-like environment where SQLite index creation is much slower relative to COPY.

## Current Index Baseline With Content Text Hash

Artifacts:

- Profiled run root: `/var/folders/97/c3gvpw6d46g3nm0y2684_cfm0000gn/T/opencode/zmail-index-current-20260701/profile-all-index`.
- Unprofiled run root: `/var/folders/97/c3gvpw6d46g3nm0y2684_cfm0000gn/T/opencode/zmail-index-current-20260701/unprofiled-all-index`.
- Experiment: `no_chunk_workers5_index_threads8`.

New source indexes since the previous actual-index rerun:

- `email_content_text_hash_idx` on `email_content USING hash (text)`.
- `email_metadata_to_email_received_idx` on `email_metadata(to_email, received_at DESC NULLS LAST, id DESC NULLS LAST)`.

Important translation detail:

- Zero's current index discovery does not preserve or filter by PostgreSQL access method.
- The PostgreSQL `hash (text)` index was discovered and emitted as a normal SQLite B-tree index: `CREATE INDEX "email_content_text_hash_idx" ON "email_content" ("text" ASC);`.

Clean unprofiled result:

- User rows: `403091`.
- Cleanup: clean.
- Total initial sync: `153.120s`.
- Copy wall, approximated as total minus index: `128.414s`.
- Flush: `90.494s`.
- Index: `24.706s`.
- Local current `index / copy` ratio: `0.192x`.
- Production `index / copy` ratio: about `0.48x`.

Profiled all-index result:

- User rows: `403091`.
- Cleanup: clean.
- Total initial sync: `152.720s`.
- Copy wall, approximated as total minus index: `127.267s`.
- Flush: `89.925s`.
- Index: `25.453s`.
- Local profiled `index / copy` ratio: `0.200x`.

Current per-index timing from the clean run:

| Index                                                                    | Time s |
| ------------------------------------------------------------------------ | -----: |
| `email_content_text_hash_idx` translated to SQLite `email_content(text)` | 17.688 |
| `email_content_pkey` on `email_content(email_id)`                        |  6.186 |
| `email_metadata_sender_received_idx`                                     |  0.160 |
| `email_metadata_category_received_idx`                                   |  0.128 |
| `email_metadata_read_received_idx`                                       |  0.087 |
| `email_metadata_starred_received_idx`                                    |  0.084 |
| `email_metadata_to_email_received_idx`                                   |  0.079 |

Current profiled import breakdown for `email_content`:

| Phase                                | Seconds |
| ------------------------------------ | ------: |
| Task wall                            | 127.234 |
| Stream wait / outside write callback |  34.071 |
| Decode                               |   3.286 |
| SQLite flush total                   |  89.654 |
| SQLite batch insert                  |  89.639 |
| SQLite single insert                 |   0.000 |

Current-index takeaways:

- The new `email_content_text_hash_idx` materially increased local index time by about `17-18s`.
- The new `email_metadata_to_email_received_idx` remained cheap at about `0.08s`.
- Local current index/copy ratio moved from about `0.06-0.07x` to about `0.19-0.20x`.
- This is closer to production's `0.48x`, but still less than half the production-relative index load.
- If production's `35m` copy time is the right normalizer, current local indexing corresponds to roughly `6.8m` production-scaled index time, not the observed `16m43s`.
- The remaining gap is consistent with additional content-value indexes, much slower production index/storage conditions, cold-cache/temp-file behavior, or work included in production timing that local logs do not include.

## Additional Benchmark Harnesses

Repo-local harnesses now live under `tmp/` so the experiments can be committed and run on another machine without machine-specific paths:

- `tmp/run-zmail-bench.mjs`: wraps the real zmail initial-sync benchmark, captures config/log/summary/cleanup artifacts, and defaults results to `tmp/results/zmail-initial-sync`.
- `tmp/zmail-sqlite-arch.mjs`: disposable SQLite architecture harness for staged DBs, direct concurrent SQLite writes, and content-only producer/consumer experiments. Defaults results to `tmp/results/zmail-sqlite-arch`.
- `tmp/results/` is ignored in `.gitignore` because full runs can create tens of GB of SQLite DB files.

The zmail runner uses the current `node` and `pnpm` by default. Set `ZMAIL_NODE_USING=22.23.1` to wrap commands with `fnm exec --using=22.23.1`.

## Page Size Sweep

Change tested:

- Added `InitialSyncOptions.sqlitePageSize` and zmail env `ZMAIL_SQLITE_PAGE_SIZE`.
- Page size is set before the replica tables are created.
- Tested no-index full materialization with default page size, `16KiB`, `32KiB`, and `64KiB`.

Results:

| Experiment           | Page Size | Total s | Flush s | Index s | User Rows | Cleanup |
| -------------------- | --------: | ------: | ------: | ------: | --------: | :------ |
| `page_default_noidx` |   default | 127.313 |  90.771 |   0.000 |    403091 | clean   |
| `page_16k_noidx`     |     16384 | 128.089 |  90.182 |   0.000 |    403091 | clean   |
| `page_32k_noidx`     |     32768 | 128.089 |  90.418 |   0.000 |    403091 | clean   |
| `page_64k_noidx`     |     65536 | 125.106 |  81.613 |   0.000 |    403091 | clean   |

Page-size takeaways:

- `64KiB` page size was the best no-index run and reduced reported flush time by about `9.2s` versus the default-page run.
- End-to-end no-index improvement was only about `2.2s` (`127.313s` to `125.106s`), so page size is not a major path by itself.
- `16KiB` and `32KiB` did not improve wall time.
- The likely explanation is that larger pages reduce some large-value page/overflow work, but the source COPY floor and remaining SQLite write cost dominate total time.

## Index Policy And Pre-Index Experiments

Changes tested:

- Added `InitialSyncOptions.experimentalIndexExcludeRegex` and zmail env `ZMAIL_INDEX_EXCLUDE_REGEX` for benchmark-only index exclusion.
- Added `InitialSyncOptions.experimentalIndexTiming` and zmail env `ZMAIL_INDEX_TIMING=after-copy|before-copy`.
- Tested skipping the hash-derived `email_content_text_hash_idx`, testing `64KiB` page size with indexes, and pre-creating indexes before COPY.

Results:

| Experiment           | Page Size | Index Timing | Excluded Hash Index | Total s | Flush s | Index s | User Rows | Cleanup |
| -------------------- | --------: | :----------- | :------------------ | ------: | ------: | ------: | --------: | :------ |
| Current all-index    |   default | after-copy   | no                  | 153.120 |  90.494 |  24.706 |    403091 | clean   |
| `skip_hash_allidx`   |   default | after-copy   | yes                 | 135.451 |  90.819 |   8.118 |    403091 | clean   |
| `page64_allidx`      |     65536 | after-copy   | no                  | 154.003 |  90.398 |  25.531 |    403091 | clean   |
| `page64_skip_hash`   |     65536 | after-copy   | yes                 | 134.961 |  90.231 |   7.280 |    403091 | clean   |
| `preindex_allidx`    |   default | before-copy  | no                  | 154.114 | 125.470 |   0.001 |    403091 | clean   |
| `preindex_skip_hash` |   default | before-copy  | yes                 | 138.359 | 105.743 |   0.001 |    403091 | clean   |

Index-policy takeaways:

- The biggest practical win so far is skipping the hash-derived `email_content_text_hash_idx` SQLite index.
- Skipping that one index reduced all-index total from `153.120s` to `135.451s`, an `~17.7s` improvement.
- `64KiB` page size plus skipping the hash-derived index was slightly best at `134.961s`, but the improvement over default-page skip-hash was only `0.490s`.
- `64KiB` page size did not help the full all-index case when the fat `email_content(text)` index was still created.
- Pre-creating indexes before COPY is worse. It makes the post-copy index phase nearly free, but shifts the work into the insert/flush path:
  - all indexes: flush increased to `125.470s`.
  - skip hash: flush increased to `105.743s`.
- Maintaining indexes during import does not avoid enough work to beat post-copy bulk index creation.
- Product implication: Zero should probably carry PostgreSQL index access method through index metadata and deliberately decide whether non-btree indexes, especially `USING hash`, should become SQLite B-tree indexes. The benchmark-only regex is not a product solution.

## Staged Worker DB Architecture

Disposable harness tested:

- Worker threads each copied part of the zmail workload into private temporary SQLite DBs.
- Final DB was built by attaching stage DBs and running `INSERT INTO main.table SELECT ... FROM stage.table`.
- This preserves private SQLite write parallelism but requires a final rewrite/merge of all rows.

Result:

| Experiment             | Workers | Content Chunks | Total s | Stage Import s | Merge s | Index s | User Rows | Cleanup |
| ---------------------- | ------: | -------------: | ------: | -------------: | ------: | ------: | --------: | :------ |
| `staged_w2_c2_noindex` |       2 |              2 | 218.808 |        104.642 | 104.604 |   0.000 |    403091 | clean   |

Staged-worker takeaways:

- Stage import itself was promising at `104.642s`, close to the local source COPY-to-null floor.
- The final SQL merge took another `104.604s`, which makes the architecture much slower than the current `~127s` no-index path.
- `ATTACH` + `INSERT SELECT` is not viable for this workload because it rewrites the fat `email_content` payload into the final DB.
- A staged design would need a fundamentally different final composition strategy that avoids rewriting the fat content; otherwise the merge cost dominates.

## Direct Concurrent SQLite Writes

Disposable harness tested multiple workers writing directly to the final SQLite DB with WAL/WAL2 and `BEGIN CONCURRENT` or `BEGIN IMMEDIATE`.

Results:

| Experiment                               | Workers | Content Chunks | Tx Mode    | Journal | Commit Rows | Total s | Import s | Retries | User Rows |
| ---------------------------------------- | ------: | -------------: | :--------- | :------ | ----------: | ------: | -------: | ------: | --------: |
| `direct_w2_c2_concurrent250_noindex`     |       2 |              2 | CONCURRENT | WAL2    |         250 | 148.963 |  139.684 |      78 |    403091 |
| `direct_w2_c2_concurrent1000_noindex`    |       2 |              2 | CONCURRENT | WAL2    |        1000 | 164.119 |  153.504 |      19 |    403091 |
| `direct_w2_c2_concurrent250_wal_noindex` |       2 |              2 | CONCURRENT | WAL     |         250 | 148.670 |  139.322 |      86 |    403091 |
| `direct_w5_c5_concurrent250_wal_noindex` |       5 |              5 | CONCURRENT | WAL     |         250 | 150.955 |  141.112 |     172 |    403091 |
| `direct_w2_c2_immediate250_noindex`      |       2 |              2 | IMMEDIATE  | WAL2    |         250 | 273.822 |  264.286 |       0 |    403091 |

Direct-concurrent takeaways:

- Direct concurrent writes avoid the staged merge but lose the current unsafe single-transaction import mode.
- `BEGIN CONCURRENT` completed correctly, but was slower than the current no-index path and incurred `SQLITE_BUSY_SNAPSHOT` retries.
- `5` workers and `5` CTID chunks did not help; it increased retry count and stayed slower.
- Larger `1000`-row commits reduced retry count but made wall time worse.
- `BEGIN IMMEDIATE` serialized badly and was much slower.
- Direct concurrent writes are not worth productizing for initial sync on this workload.

## Single Writer Producer/Consumer Queue

Disposable harness tested a middle path:

- PostgreSQL COPY producer on the main thread.
- One SQLite writer worker owning the final DB and one unsafe SQLite transaction.
- Raw COPY chunks transferred through a bounded queue to the writer.

Results:

| Experiment                | Scope            | Total s |    Rows | Verdict                         |
| ------------------------- | ---------------- | ------: | ------: | :------------------------------ |
| `sync_content_seq`        | full content     | 134.016 |  200000 | baseline content-only sync path |
| `queued_content_seq_q256` | full content     |  >900.0 | unknown | timed out                       |
| `sync_content_10k`        | 10k content rows |   9.844 |   10000 | sample baseline                 |
| `queued_content_10k`      | 10k content rows |   9.970 |   10000 | no benefit                      |

Producer/consumer takeaways:

- The 10k-row sample showed no benefit from moving parse/decode/write into a separate writer worker.
- The full queued run timed out after `900s`, which is far worse than the `134.016s` synchronous content-only baseline.
- Transferring huge COPY buffers/values between threads and preserving backpressure appears to add more overhead than it removes.
- This architecture is not promising unless implemented at a lower level with shared/native buffers and a very different binding path.

## Updated Overall Conclusions

- Full logical materialization remains bounded by the source COPY floor and by SQLite writing the huge logical payload.
- The only clear local end-to-end win from the latest experiments is index policy: do not blindly translate PostgreSQL `USING hash (text)` into a SQLite B-tree index on `text`.
- `64KiB` SQLite page size can slightly improve no-index import, but the effect is small and did not rescue all-index performance.
- Pre-creating indexes, staged worker DBs, direct concurrent final-DB writes, and producer/consumer single-writer queueing are all negative for this workload.
- The next product-quality investigation should be index metadata semantics, specifically carrying PostgreSQL access method and deciding whether non-btree indexes should be replicated to SQLite at all.
- Any larger speedup likely requires changing product semantics or source representation: deferring fat content, avoiding syncing fat columns at startup, native/raw SQLite loading, or a production-like environment where current local conclusions should be revalidated.
