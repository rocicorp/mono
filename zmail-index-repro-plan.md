# Zmail SQLite Index Repro Plan

## Goal

Determine whether the production `16m43s` index phase can be reproduced by adding more SQLite index work to the zmail benchmark, even without exact production index DDL.

Primary question: did production have a heavier index workload, or is the local benchmark too favorable because of storage, cache, resources, or environment differences?

## Background

Observed index times:

| Run                            | Index Time |
| ------------------------------ | ---------: |
| first local all-index baseline |       ~20s |
| warm local all-index repeat    |      ~8-9s |
| production incident            |     16m43s |

A 50x+ gap is too large to explain by CTID or COPY behavior. It likely comes from index workload, SQLite temp/storage behavior, cold cache, CPU limits, or production timing including extra work.

## Recommended Approach

Do not mutate the source Postgres zmail DB first.

Instead:

1. Run initial sync with `ZMAIL_INDEX_MODE=none`.
2. Keep the resulting SQLite replica.
3. Copy that SQLite replica for each synthetic index experiment.
4. Apply synthetic SQLite `CREATE INDEX` statements directly.
5. Measure per-index and total index time.

This isolates SQLite index construction, which is the phase we need to explain.

## Why Direct SQLite Replay

Direct SQLite replay avoids coupling unrelated factors:

| Coupled Factor         | Why Avoid                           |
| ---------------------- | ----------------------------------- |
| Postgres DDL changes   | mutates source DB and needs cleanup |
| full initial sync copy | repeats expensive unrelated work    |
| SQLite indexing        | actual target phase                 |

After direct replay identifies a plausible index workload, optionally create matching source Postgres indexes in a throwaway DB and run full initial sync to validate Zero index discovery.

## Experiment Matrix

| Variant                          | Purpose                                                     |
| -------------------------------- | ----------------------------------------------------------- |
| current zmail indexes            | baseline local SQLite index workload                        |
| `5x email_content(email_id)`     | repeated small-key indexes on huge table                    |
| `10x email_content(email_id)`    | scaling slope for content-table scans/sorts                 |
| `25x email_content(email_id)`    | determine whether index count alone can approach production |
| metadata composite indexes       | cost of many normal secondary indexes                       |
| mixed content + metadata indexes | more realistic broad workload                               |
| fat-column probe                 | upper bound only; use only if needed                        |

## Candidate Synthetic Indexes

Small-key content indexes:

```sql
CREATE INDEX idx_content_email_id_001 ON email_content(email_id);
CREATE INDEX idx_content_email_id_002 ON email_content(email_id);
CREATE INDEX idx_content_email_id_003 ON email_content(email_id);
```

Metadata composite indexes:

```sql
CREATE INDEX idx_metadata_mailbox_received_001
  ON email_metadata(mailbox, received_at, id);

CREATE INDEX idx_metadata_thread_received_001
  ON email_metadata(thread_id, received_at, id);

CREATE INDEX idx_metadata_category_received_001
  ON email_metadata(mailbox, category, received_at, id);

CREATE INDEX idx_metadata_read_received_001
  ON email_metadata(mailbox, is_read, received_at);

CREATE INDEX idx_metadata_starred_received_001
  ON email_metadata(mailbox, is_starred, received_at);
```

Fat-column probe, only as an upper bound:

```sql
CREATE INDEX idx_content_text_probe ON email_content(text);
CREATE INDEX idx_content_html_probe ON email_content(html);
```

Do not treat the fat-column probe as representative unless production likely had similar indexes.

## Metrics To Capture

For each variant:

| Metric                        | Purpose                            |
| ----------------------------- | ---------------------------------- |
| total index time              | compare against production `1003s` |
| per-index time                | identify index offenders           |
| SQLite DB path                | storage context                    |
| SQLite file size before/after | write amplification                |
| temp path/settings            | temp spill clues                   |
| SQLite pragmas                | explain behavior                   |
| process elapsed time          | operator cost                      |

Capture these pragmas before each run:

```sql
PRAGMA journal_mode;
PRAGMA synchronous;
PRAGMA temp_store;
PRAGMA cache_size;
PRAGMA mmap_size;
PRAGMA threads;
PRAGMA page_size;
PRAGMA page_count;
PRAGMA freelist_count;
```

## Decision Rules

| Result                                         | Interpretation                                                               |
| ---------------------------------------------- | ---------------------------------------------------------------------------- |
| each content-table index costs ~8-10s          | production would require ~100 similar indexes; index count alone is unlikely |
| index cost grows superlinearly                 | temp/cache/storage pressure is likely relevant                               |
| metadata indexes stay cheap                    | production likely came from huge-table indexes or environment                |
| fat-column indexes explode                     | production could match only if similar fat-column indexes existed            |
| 25x content indexes still far below production | local environment is too favorable                                           |
| synthetic replay reaches production range      | source index count/workload can plausibly explain incident                   |

## Follow-Up

If direct SQLite replay can reproduce production-scale index time:

1. Create matching indexes in a throwaway zmail Postgres DB.
2. Run full initial sync with all indexes.
3. Confirm Zero emits similar per-index timing.
4. Evaluate index dedupe and required-index strategies.

If replay cannot reproduce production-scale index time:

1. Move the test to a production-like AWS host and storage.
2. Match CPU and memory limits.
3. Use production-like cold-cache conditions.
4. Verify SQLite temp files are on the expected SSD path.
5. Compare per-index timing and SQLite pragmas.
