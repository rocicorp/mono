// Benchmarks initial sync against an existing zmail-style PostgreSQL database.

import {copyFileSync, existsSync, mkdirSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {afterAll, describe, expect, test} from 'vitest';
import {createManualBenchmarkRecorder} from '../../../shared/src/bench.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {createLogContext} from '../../../shared/src/logging.ts';
import {must} from '../../../shared/src/must.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {initReplica} from '../services/change-source/common/replica-schema.ts';
import {
  initialSync,
  type InitialSyncOptions,
} from '../services/change-source/pg/initial-sync.ts';
import {dropShard} from '../services/change-source/pg/schema/shard.ts';
import {DbFile} from '../test/lite.ts';
import {connectPgClient} from '../types/pg.ts';
import type {ShardConfig} from '../types/shards.ts';
import {id} from '../types/sql.ts';

const DEFAULT_UPSTREAM_URI = 'postgres://postgres:pass@localhost:5547/zmail';
const TEST_TIMEOUT_MS = 12 * 60 * 60 * 1000;
const APP_ID_PREFIX = 'zmail_bench';

const lc =
  process.env.ZERO_BENCH_LOG === '0'
    ? createSilentLogContext()
    : createLogContext(
        {log: {level: 'info', format: 'text'}},
        {bench: 'zmail-initial-sync'},
      );

const benchmarkRecorder = createManualBenchmarkRecorder();
const replicaFiles: DbFile[] = [];
const upstreamURI = process.env.ZMAIL_PG_URI ?? DEFAULT_UPSTREAM_URI;
const replicaOutputPath = process.env.ZMAIL_REPLICA_OUTPUT_PATH;
const replicaOutputDir = process.env.ZMAIL_REPLICA_OUTPUT_DIR;

function envNumber(name: string, defaultValue: number): number {
  const value = process.env[name];
  if (value === undefined || value === '') {
    return defaultValue;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${name} must be a finite number, got ${value}`);
  }
  return parsed;
}

function envOptionalNumber(name: string): number | undefined {
  const value = process.env[name];
  if (value === undefined || value === '') {
    return undefined;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${name} must be a finite number, got ${value}`);
  }
  return parsed;
}

function envOptionalString(name: string): string | undefined {
  const value = process.env[name];
  if (value === undefined || value === '') {
    return undefined;
  }
  return value;
}

function envBool(name: string, defaultValue = false): boolean {
  const value = process.env[name];
  if (value === undefined || value === '') {
    return defaultValue;
  }
  return value === '1' || value === 'true';
}

function envIndexMode(): InitialSyncOptions['experimentalIndexMode'] {
  const value = process.env.ZMAIL_INDEX_MODE ?? 'all';
  if (
    value !== 'all' &&
    value !== 'required' &&
    value !== 'none' &&
    value !== 'dedupe'
  ) {
    throw new Error(`ZMAIL_INDEX_MODE must be all|required|none|dedupe`);
  }
  return value;
}

function envIndexTiming(): InitialSyncOptions['experimentalIndexTiming'] {
  const value = process.env.ZMAIL_INDEX_TIMING ?? 'after-copy';
  if (value !== 'after-copy' && value !== 'before-copy') {
    throw new Error(`ZMAIL_INDEX_TIMING must be after-copy|before-copy`);
  }
  return value;
}

function envSqliteTempStore(): InitialSyncOptions['sqliteTempStore'] {
  const value = process.env.ZMAIL_SQLITE_TEMP_STORE;
  if (value === undefined || value === '') {
    return undefined;
  }
  if (value !== 'default' && value !== 'file' && value !== 'memory') {
    throw new Error(`ZMAIL_SQLITE_TEMP_STORE must be default|file|memory`);
  }
  return value;
}

function appID(run: number): string {
  const suffix = process.env.ZMAIL_APP_SUFFIX ?? Date.now().toString(36);
  return `${APP_ID_PREFIX}_${suffix}_${run}`;
}

async function cleanupShard(appID: string) {
  const sql = await connectPgClient(lc, upstreamURI, 'zmail-cleanup', {max: 1});
  try {
    await sql.unsafe(dropShard(appID, 0));
    await sql.unsafe(`DROP SCHEMA IF EXISTS ${id(appID)} CASCADE`);
    await sql /*sql*/ `
      SELECT pg_drop_replication_slot(slot_name)
      FROM pg_replication_slots
      WHERE slot_name LIKE ${`${appID.replaceAll('_', '\\_')}_0_%`} ESCAPE '\\'
        AND active = false;
    `;
  } finally {
    await sql.end();
  }
}

async function zmailPayloadMB() {
  const sql = await connectPgClient(lc, upstreamURI, 'zmail-payload', {max: 1});
  try {
    const [row] = await sql<{payloadMB: number}[]> /*sql*/ `
      WITH content_sample AS (
        SELECT octet_length(email_id) +
               octet_length(html) +
               octet_length(text) +
               octet_length(image_urls::text) +
               octet_length(headers::text) AS row_bytes
        FROM public.email_content TABLESAMPLE SYSTEM (0.5)
      ), content_rows AS (
        SELECT count(*)::float8 AS rows FROM public.email_content
      ), metadata AS (
        SELECT COALESCE(SUM(
          octet_length(id) +
          octet_length(mailbox::text) +
          octet_length(category::text) +
          octet_length(thread_id) +
          octet_length(message_id) +
          octet_length(from_email) +
          octet_length(to_email) +
          octet_length(subject) +
          octet_length(preview) +
          octet_length(received_at::text) +
          octet_length(sent_at::text) +
          octet_length(is_read::text) +
          COALESCE(octet_length(read_at::text), 0) +
          octet_length(is_starred::text) +
          COALESCE(octet_length(starred_at::text), 0) +
          octet_length(body_size_bytes::text) +
          octet_length(content_sha256) +
          octet_length(created_at::text) +
          octet_length(updated_at::text)
        ), 0)::float8 AS bytes
        FROM public.email_metadata
      ), address AS (
        SELECT COALESCE(SUM(
          octet_length(email) +
          COALESCE(octet_length(name), 0) +
          COALESCE(octet_length(company), 0) +
          COALESCE(octet_length(job_title), 0) +
          COALESCE(octet_length(location), 0) +
          COALESCE(octet_length(timezone), 0) +
          COALESCE(octet_length(website_url), 0) +
          COALESCE(octet_length(linkedin_url), 0) +
          COALESCE(octet_length(avatar_url), 0) +
          octet_length(created_at::text) +
          octet_length(updated_at::text)
        ), 0)::float8 AS bytes
        FROM public.email_address
      )
      SELECT (
        ((SELECT AVG(row_bytes) FROM content_sample) * (SELECT rows FROM content_rows)) +
        (SELECT bytes FROM metadata) +
        (SELECT bytes FROM address)
      ) / 1000000 AS "payloadMB";
    `;
    return must(row).payloadMB;
  } finally {
    await sql.end();
  }
}

function replicaRowCount(replicaPath: string): number {
  const db = new Database(lc, replicaPath, {readonly: true});
  try {
    let total = 0;
    for (const table of ['email_content', 'email_metadata', 'email_address']) {
      const row = db
        .prepare(`SELECT count(*) AS n FROM ${table}`)
        .get<{n: number}>();
      total += row.n;
    }
    return total;
  } finally {
    db.close();
  }
}

function preserveReplica(replicaPath: string, app: string) {
  const outputPath =
    replicaOutputPath ??
    (replicaOutputDir === undefined
      ? undefined
      : join(replicaOutputDir, `zmail-initial-sync-${app}.db`));
  if (outputPath === undefined) {
    return;
  }
  mkdirSync(dirname(outputPath), {recursive: true});
  for (const suffix of ['', '-wal', '-shm']) {
    if (existsSync(`${replicaPath}${suffix}`)) {
      copyFileSync(`${replicaPath}${suffix}`, `${outputPath}${suffix}`);
    }
  }
  lc.info?.(`zmail preserved replica at ${outputPath}`);
}

afterAll(() => {
  for (const file of replicaFiles) {
    file.delete();
  }
});

describe('zero-cache/zmail initial-sync throughput', () => {
  test('existing zmail payload MB', {timeout: TEST_TIMEOUT_MS}, async () => {
    const warmupReps = envNumber('ZMAIL_WARMUP_REPS', 0);
    const reps = envNumber('ZMAIL_REPS', 1);
    const payloadMB = envNumber('ZMAIL_PAYLOAD_MB', await zmailPayloadMB());
    const expectedRows = envOptionalNumber('ZMAIL_EXPECTED_ROWS');
    const samples: number[] = [];

    const options: InitialSyncOptions = {
      tableCopyWorkers: envNumber('ZMAIL_TABLE_COPY_WORKERS', 5),
      chunkTargetBytes: envNumber('ZMAIL_CHUNK_TARGET_BYTES', 0),
      maxChunksPerTable: envNumber('ZMAIL_MAX_CHUNKS_PER_TABLE', 64),
      indexThreads: envOptionalNumber('ZMAIL_INDEX_THREADS'),
      experimentalIndexMode: envIndexMode(),
      experimentalIndexTiming: envIndexTiming(),
      experimentalIndexExcludeRegex: envOptionalString(
        'ZMAIL_INDEX_EXCLUDE_REGEX',
      ),
      sampleRate: envOptionalNumber('ZMAIL_SAMPLE_RATE'),
      maxRowsPerTable: envOptionalNumber('ZMAIL_MAX_ROWS_PER_TABLE'),
      textCopy: envBool('ZMAIL_TEXT_COPY'),
      importProfile: envBool('ZMAIL_IMPORT_PROFILE'),
      insertBatchSize: envOptionalNumber('ZMAIL_INSERT_BATCH_SIZE'),
      maxBufferedRows: envOptionalNumber('ZMAIL_MAX_BUFFERED_ROWS'),
      bufferedSizeThresholdBytes: envOptionalNumber(
        'ZMAIL_BUFFERED_SIZE_THRESHOLD_BYTES',
      ),
      sqliteCacheSize: envOptionalNumber('ZMAIL_SQLITE_CACHE_SIZE'),
      sqliteMmapSize: envOptionalNumber('ZMAIL_SQLITE_MMAP_SIZE'),
      sqlitePageSize: envOptionalNumber('ZMAIL_SQLITE_PAGE_SIZE'),
      sqliteTempStore: envSqliteTempStore(),
    };

    lc.info?.(`zmail benchmark config`, {
      upstreamURI,
      warmupReps,
      reps,
      payloadMB,
      expectedRows,
      options,
    });

    for (let rep = 0; rep < warmupReps + reps; rep++) {
      const app = appID(rep);
      await cleanupShard(app);
      const replicaDbFile = new DbFile(`zmail-initial-sync-${app}`);
      replicaFiles.push(replicaDbFile);
      const shard: ShardConfig = {appID: app, shardNum: 0, publications: []};

      const start = performance.now();
      try {
        await initReplica(
          lc,
          `zmail-initial-sync-${app}`,
          replicaDbFile.path,
          (log, tx) =>
            initialSync(log, shard, tx, upstreamURI, options, {
              bench: 'zmail-initial-sync',
              rep,
            }),
        );
      } finally {
        await cleanupShard(app).catch(e =>
          lc.warn?.(`zmail cleanup failed for ${app}`, e),
        );
      }
      const elapsed = performance.now() - start;
      const rows = replicaRowCount(replicaDbFile.path);
      if (expectedRows !== undefined) {
        expect(rows).toBe(expectedRows);
      } else {
        expect(rows).toBeGreaterThan(0);
      }
      preserveReplica(replicaDbFile.path, app);
      lc.info?.(
        `zmail initial sync rep ${rep} copied ${rows} rows in ${elapsed.toFixed(3)} ms`,
      );
      lc.info?.(
        `zmail initial sync memory after rep ${rep}`,
        process.memoryUsage(),
      );
      if (rep >= warmupReps) {
        samples.push(elapsed);
      }
    }

    benchmarkRecorder.recordThroughput(
      'zero-cache/zmail initial-sync existing payload MB',
      samples,
      payloadMB,
    );
  });
});
