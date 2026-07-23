/* oxlint-disable no-console */

// Benchmarks the SQLite write ceiling for the replication-manager shape:
// applying rows to the backup replica and appending the raw change stream to a
// SQLite-local change log. Every logical upstream transaction is committed as
// exactly one SQLite transaction.
//
// Deterministic correctness run:
//   SQLITE_CHANGE_LOG_CORRECTNESS=1 pnpm --filter zero-cache run bench sqlite-change-log-ceiling
//
// Go/no-go runs additionally set SQLITE_CHANGE_LOG_GO_NO_GO=1 plus
// SQLITE_CHANGE_LOG_MIN_CHANGES_PER_SECOND,
// SQLITE_CHANGE_LOG_MAX_COMMIT_P95_MS, and
// SQLITE_CHANGE_LOG_MAX_STALL_REGRESSION_PERCENT.

import {spawn, spawnSync, type ChildProcess} from 'node:child_process';
import {existsSync, mkdtempSync, rmSync, statSync} from 'node:fs';
import {arch, cpus, platform, release, tmpdir, totalmem} from 'node:os';
import {delimiter, join} from 'node:path';
import {afterAll, afterEach, describe, expect, test} from 'vitest';
import {createManualBenchmarkRecorder} from '../../../../shared/src/bench.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Statement} from '../../../../zqlite/src/db.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {DbFile} from '../../test/lite.ts';
import {versionToLexi} from '../../types/lexi-version.ts';
import {getPragmaConfig} from '../../workers/replicator.ts';
import {ZERO_VERSION_COLUMN_NAME} from './schema/constants.ts';
import {applyPragmas} from './write-worker-client.ts';

type WriteMode = 'apply' | 'log' | 'combined';
type Workload = 'small-high-frequency' | 'mixed-row-schema' | 'oversized';

type MessageSize = {
  readonly bytes: number;
  readonly weight: number;
};

type BenchCase = {
  readonly workload: Workload;
  readonly mode: WriteMode;
  readonly logicalTxRows: number;
  readonly sqliteTxRows: number;
  readonly totalTransactions: number;
  readonly messageSizes: readonly MessageSize[];
  readonly schemaEveryTransactions: number | undefined;
};

type FileMetrics = {
  readonly dbBytes: number;
  readonly walBytes: number;
  readonly freelistBytes: number;
};

type PurgeResult = {
  readonly elapsedMs: number;
  readonly deletedRows: number;
  readonly deletedThrough: string | undefined;
  readonly moreEligible: boolean;
};

type WriteResult = {
  readonly elapsedMs: number;
  readonly changes: number;
  readonly transactions: number;
  readonly payloadBytes: number;
  readonly commitLatencyMs: readonly number[];
  readonly transactionLatencyMs: readonly number[];
  readonly upstreamLoopStallMs: readonly number[];
  readonly purgeResults: readonly PurgeResult[];
  readonly purgedRows: number;
};

type CatchupResult = {
  readonly elapsedMs: number;
  readonly rows: number;
  readonly batchLatencyMs: readonly number[];
};

type PublishedResult = {
  readonly name: string;
  readonly measurements: Readonly<Record<string, unknown>>;
};

type GoNoGoThresholds = {
  readonly minChangesPerSecond: number;
  readonly maxCommitP95Ms: number;
  readonly maxStallRegressionPercent: number;
};

const SMALL_MESSAGE_SIZES: readonly MessageSize[] = [
  {bytes: 128, weight: 900},
  {bytes: 1024, weight: 90},
  {bytes: 4096, weight: 10},
];

// A deterministic, production-shaped long-tail distribution. The large
// values match the wide-text and large-payload fixtures used by the end-to-end
// replication benchmarks; their low weights keep them in the long tail.
const MIXED_MESSAGE_SIZES: readonly MessageSize[] = [
  {bytes: 128, weight: 700},
  {bytes: 1024, weight: 200},
  {bytes: 4096, weight: 75},
  {bytes: 16_384, weight: 20},
  {bytes: 275_000, weight: 4},
  {bytes: 683_000, weight: 1},
];

const OVERSIZED_MESSAGE_SIZES: readonly MessageSize[] = [
  {bytes: 128, weight: 1},
];

const LITESTREAM_VERSION_RE = /\bv?0\.5\./;
const LITESTREAM_READY_RE = /replicating|initialized db/i;
const BYTES_PER_MB = 1_000_000;
const TEST_TIMEOUT_MS = 3_600_000;
const CORRECTNESS_MODE = booleanFromEnv('SQLITE_CHANGE_LOG_CORRECTNESS', false);
const GO_NO_GO = booleanFromEnv('SQLITE_CHANGE_LOG_GO_NO_GO', false);
const WARMUP_REPS = nonNegativeIntegerFromEnv(
  'SQLITE_CHANGE_LOG_WARMUP_REPS',
  CORRECTNESS_MODE ? 0 : 1,
);
const REPS = integerFromEnv('SQLITE_CHANGE_LOG_REPS', CORRECTNESS_MODE ? 1 : 5);
const TARGET_PAYLOAD_MB = integerFromEnv(
  'SQLITE_CHANGE_LOG_TARGET_PAYLOAD_MB',
  CORRECTNESS_MODE ? 1 : 64,
);
const MIN_CHANGES = integerFromEnv(
  'SQLITE_CHANGE_LOG_MIN_CHANGES',
  CORRECTNESS_MODE ? 24 : 1000,
);
const MAX_CHANGES = integerFromEnv(
  'SQLITE_CHANGE_LOG_MAX_CHANGES',
  CORRECTNESS_MODE ? 2000 : 100_000,
);
const MIXED_TX_ROWS = integerFromEnv(
  'SQLITE_CHANGE_LOG_MIXED_TX_ROWS',
  CORRECTNESS_MODE ? 8 : 100,
);
const READ_BATCH_ROWS = integerFromEnv(
  'SQLITE_CHANGE_LOG_READ_BATCH_ROWS',
  CORRECTNESS_MODE ? 7 : 1000,
);
const PURGE_BATCH_ROWS = integerFromEnv(
  'SQLITE_CHANGE_LOG_PURGE_BATCH_ROWS',
  CORRECTNESS_MODE ? 10 : 1000,
);
const BETWEEN_COMMIT_PURGE_ROWS = integerFromEnv(
  'SQLITE_CHANGE_LOG_BETWEEN_COMMIT_PURGE_ROWS',
  CORRECTNESS_MODE ? 4 : 100,
);
const SCHEMA_EVERY_TRANSACTIONS = integerFromEnv(
  'SQLITE_CHANGE_LOG_SCHEMA_EVERY_TRANSACTIONS',
  CORRECTNESS_MODE ? 2 : 10,
);

const lc = createSilentLogContext();
const benchmarkRecorder = createManualBenchmarkRecorder();
const publishedResults: PublishedResult[] = [];
const goNoGoResults = new Map<string, WriteResult[]>();
const goNoGoThresholds = readGoNoGoThresholds();
const litestreamExecutable = findLitestreamV5();

let cleanup: (() => void)[] = [];

afterEach(() => {
  runCleanup();
});

afterAll(() => {
  console.log(
    JSON.stringify({
      sqliteChangeLogBenchmark: {
        environment: benchmarkEnvironment(),
        configuration: {
          correctnessMode: CORRECTNESS_MODE,
          goNoGo: GO_NO_GO,
          targetPayloadMB: TARGET_PAYLOAD_MB,
          readBatchRows: READ_BATCH_ROWS,
          purgeBatchRows: PURGE_BATCH_ROWS,
          betweenCommitPurgeRows: BETWEEN_COMMIT_PURGE_ROWS,
          thresholds: goNoGoThresholds,
        },
        results: publishedResults,
      },
    }),
  );
});

function runCleanup() {
  for (const fn of cleanup.reverse()) {
    fn();
  }
  cleanup = [];
}

function booleanFromEnv(name: string, fallback: boolean) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') {
    return fallback;
  }
  switch (raw.toLowerCase()) {
    case '1':
    case 'true':
      return true;
    case '0':
    case 'false':
      return false;
    default:
      throw new Error(`${name} must be true, false, 1, or 0; got ${raw}`);
  }
}

function integerFromEnv(name: string, fallback: number) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive safe integer, got ${raw}`);
  }
  return value;
}

function nonNegativeIntegerFromEnv(name: string, fallback: number) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative safe integer, got ${raw}`);
  }
  return value;
}

function positiveNumberFromEnv(name: string) {
  const raw = process.env[name];
  if (!raw) {
    throw new Error(`${name} is required when SQLITE_CHANGE_LOG_GO_NO_GO=1`);
  }
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive number, got ${raw}`);
  }
  return value;
}

function readGoNoGoThresholds(): GoNoGoThresholds | undefined {
  if (!GO_NO_GO) {
    return undefined;
  }
  return {
    minChangesPerSecond: positiveNumberFromEnv(
      'SQLITE_CHANGE_LOG_MIN_CHANGES_PER_SECOND',
    ),
    maxCommitP95Ms: positiveNumberFromEnv(
      'SQLITE_CHANGE_LOG_MAX_COMMIT_P95_MS',
    ),
    maxStallRegressionPercent: positiveNumberFromEnv(
      'SQLITE_CHANGE_LOG_MAX_STALL_REGRESSION_PERCENT',
    ),
  };
}

function writeModesFromEnv(): WriteMode[] {
  const raw = process.env.SQLITE_CHANGE_LOG_MODES;
  if (!raw) {
    return GO_NO_GO ? ['apply', 'combined'] : ['combined'];
  }

  const modes = raw
    .split(',')
    .map(part => part.trim())
    .filter(part => part.length > 0)
    .map(mode => {
      switch (mode) {
        case 'apply':
        case 'log':
        case 'combined':
          return mode;
        default:
          throw new Error(
            `SQLITE_CHANGE_LOG_MODES must contain apply, log, or combined: ${raw}`,
          );
      }
    });

  if (modes.length === 0) {
    throw new Error('SQLITE_CHANGE_LOG_MODES must contain at least one mode');
  }
  const unique = [...new Set(modes)];
  if (GO_NO_GO && (!unique.includes('apply') || !unique.includes('combined'))) {
    throw new Error(
      'Go/no-go runs require both apply and combined write modes to measure regression',
    );
  }
  return unique;
}

function averageMessageBytes(distribution: readonly MessageSize[]) {
  const totalWeight = distribution.reduce((sum, {weight}) => sum + weight, 0);
  return (
    distribution.reduce((sum, {bytes, weight}) => sum + bytes * weight, 0) /
    totalWeight
  );
}

function transactionCount(
  logicalTxRows: number,
  distribution: readonly MessageSize[],
) {
  const targetChanges = Math.floor(
    (TARGET_PAYLOAD_MB * BYTES_PER_MB) / averageMessageBytes(distribution),
  );
  const bounded = Math.min(Math.max(targetChanges, MIN_CHANGES), MAX_CHANGES);
  return Math.max(2, Math.ceil(bounded / logicalTxRows));
}

function makeCases(): BenchCase[] {
  const workloads = [
    {
      workload: 'small-high-frequency',
      logicalTxRows: 1,
      messageSizes: SMALL_MESSAGE_SIZES,
      schemaEveryTransactions: undefined,
    },
    {
      workload: 'mixed-row-schema',
      logicalTxRows: MIXED_TX_ROWS,
      messageSizes: MIXED_MESSAGE_SIZES,
      schemaEveryTransactions: SCHEMA_EVERY_TRANSACTIONS,
    },
    {
      workload: 'oversized',
      logicalTxRows: PURGE_BATCH_ROWS + 1,
      messageSizes: OVERSIZED_MESSAGE_SIZES,
      schemaEveryTransactions: undefined,
    },
  ] as const;

  return writeModesFromEnv().flatMap(mode =>
    workloads.map(
      ({
        workload,
        logicalTxRows,
        messageSizes,
        schemaEveryTransactions,
      }): BenchCase => ({
        workload,
        mode,
        logicalTxRows,
        // This equality is an intentional go/no-go invariant: batching
        // multiple logical transactions into one SQLite commit understates
        // the production commit cost.
        sqliteTxRows: logicalTxRows,
        totalTransactions: transactionCount(logicalTxRows, messageSizes),
        messageSizes,
        schemaEveryTransactions,
      }),
    ),
  );
}

function weightedMessageBytes(
  distribution: readonly MessageSize[],
  changeIndex: number,
) {
  const totalWeight = distribution.reduce((sum, {weight}) => sum + weight, 0);
  // Multiplication by a number coprime to 1000 spreads long-tail values across
  // the stream instead of clustering them at the end.
  let point = (changeIndex * 811) % totalWeight;
  for (const {bytes, weight} of distribution) {
    if (point < weight) {
      return bytes;
    }
    point -= weight;
  }
  throw new Error('invalid message-size distribution');
}

function makePayload(bytes: number) {
  const chunk = 'abcdefghijklmnopqrstuvwxyz0123456789';
  return chunk.repeat(Math.ceil(bytes / chunk.length)).slice(0, bytes);
}

const messageCache = new Map<string, string>();

function changeJSON(kind: 'row' | 'schema', targetBytes: number) {
  const cacheKey = `${kind}:${targetBytes}`;
  const cached = messageCache.get(cacheKey);
  if (cached !== undefined) {
    return cached;
  }

  let json: string;
  if (kind === 'row') {
    const base = {
      tag: 'insert',
      relation: {
        schema: 'public',
        name: 'bench_rows',
        rowKey: {columns: ['id'], type: 'default'},
      },
      new: {id: 0, indexed: 0, payload: ''},
    };
    const payloadBytes = Math.max(0, targetBytes - JSON.stringify(base).length);
    base.new.payload = makePayload(payloadBytes);
    json = JSON.stringify(base);
  } else {
    const base = {
      tag: 'create-table',
      spec: {
        schema: 'public',
        name: 'bench_schema_event',
        columns: {id: {pos: 1, dataType: 'int8', notNull: true}},
        primaryKey: ['id'],
      },
      metadata: {nextEvent: 0},
      payload: '',
    };
    const payloadBytes = Math.max(0, targetBytes - JSON.stringify(base).length);
    base.payload = makePayload(payloadBytes);
    json = JSON.stringify(base);
  }
  messageCache.set(cacheKey, json);
  return json;
}

function setupDB(dbFile: DbFile) {
  const db = new Database(lc, dbFile.path);
  db.pragma('journal_mode = wal');
  db.pragma('synchronous = NORMAL');
  applyPragmas(db, getPragmaConfig('backup'));

  db.exec(/*sql*/ `
    CREATE TABLE "bench_rows" (
      "id" INTEGER PRIMARY KEY,
      "indexed" INTEGER NOT NULL,
      "payload" TEXT NOT NULL,
      "${ZERO_VERSION_COLUMN_NAME}" TEXT NOT NULL
    );
    CREATE INDEX "bench_rows_indexed_idx" ON "bench_rows"("indexed");

    CREATE TABLE "_zero.changeLogStream" (
      "watermark"   TEXT NOT NULL,
      "pos"         INTEGER NOT NULL,
      "change"      TEXT NOT NULL,
      "precommit"   TEXT,
      "writeTimeMs" INTEGER,
      PRIMARY KEY ("watermark", "pos")
    );

    CREATE INDEX "_zero.changeLogStream_writeTimeMs"
      ON "_zero.changeLogStream" ("writeTimeMs", "watermark")
      WHERE "writeTimeMs" IS NOT NULL;

    CREATE TABLE "_zero.replicationState" (
      "stateVersion" TEXT NOT NULL,
      "writeTimeMs" INTEGER,
      "lock" INTEGER PRIMARY KEY DEFAULT 1 CHECK ("lock" = 1)
    );

    INSERT INTO "_zero.replicationState" ("stateVersion", "writeTimeMs")
      VALUES ('00', unixepoch('subsec') * 1000);
    INSERT INTO "_zero.changeLogStream"
      ("watermark", "pos", "change", "precommit", "writeTimeMs")
      VALUES
        ('00', 0, '{"tag":"begin"}', NULL, NULL),
        ('00', 1, '{"tag":"commit"}', '00', unixepoch('subsec') * 1000);
  `);

  return db;
}

function fileSize(path: string) {
  return existsSync(path) ? statSync(path).size : 0;
}

function fileMetrics(db: Database, path: string): FileMetrics {
  const [{page_size: pageSize}] = db.pragma<{page_size: number}>('page_size');
  const [{freelist_count: freelistPages}] = db.pragma<{
    freelist_count: number;
  }>('freelist_count');
  return {
    dbBytes: fileSize(path),
    walBytes: fileSize(`${path}-wal`),
    freelistBytes: pageSize * freelistPages,
  };
}

function prepareStatements(db: Database) {
  return {
    runner: new StatementRunner(db),
    upsertRow: db.prepare(/*sql*/ `
      INSERT OR REPLACE INTO "bench_rows"
        ("id", "indexed", "payload", "${ZERO_VERSION_COLUMN_NAME}")
        VALUES (?, ?, ?, ?)
    `),
    insertLog: db.prepare(/*sql*/ `
      INSERT INTO "_zero.changeLogStream"
        ("watermark", "pos", "change", "precommit", "writeTimeMs")
        VALUES (?, ?, ?, ?, ?)
    `),
    updateWatermark: db.prepare(/*sql*/ `
      UPDATE "_zero.replicationState"
        SET "stateVersion" = ?, "writeTimeMs" = unixepoch('subsec') * 1000
    `),
  };
}

function runCase(
  db: Database,
  c: BenchCase,
  betweenCommitPurgeRows?: number,
): WriteResult {
  if (c.sqliteTxRows !== c.logicalTxRows) {
    throw new Error(
      `sqliteTxRows (${c.sqliteTxRows}) must equal logicalTxRows (${c.logicalTxRows})`,
    );
  }

  const beginJSON = '{"tag":"begin"}';
  const commitJSON = '{"tag":"commit"}';
  const {runner, upsertRow, insertLog, updateWatermark} = prepareStatements(db);
  const writesRows = c.mode === 'apply' || c.mode === 'combined';
  const writesLog = c.mode === 'log' || c.mode === 'combined';
  const commitLatencyMs: number[] = [];
  const transactionLatencyMs: number[] = [];
  const upstreamLoopStallMs: number[] = [];
  const purgeResults: PurgeResult[] = [];
  let payloadBytes = 0;
  let changeID = 1;

  const start = performance.now();
  for (let txIndex = 0; txIndex < c.totalTransactions; txIndex++) {
    const loopStart = performance.now();
    const watermark = versionToLexi(txIndex + 1);
    const isSchemaTransaction =
      c.schemaEveryTransactions !== undefined &&
      txIndex % c.schemaEveryTransactions === 0;
    const transactionStart = performance.now();
    try {
      runner.beginImmediate();
      if (writesLog) {
        insertLogRow(insertLog, watermark, 0, beginJSON, null, null);
      }

      for (let pos = 0; pos < c.logicalTxRows; pos++) {
        const messageBytes = weightedMessageBytes(c.messageSizes, changeID);
        const kind = isSchemaTransaction && pos === 0 ? 'schema' : 'row';
        const json = changeJSON(kind, messageBytes);
        payloadBytes += json.length;

        if (writesRows) {
          if (kind === 'schema') {
            // Exercise a real SQLite schema mutation. The generated identifier
            // is derived only from the numeric loop index.
            db.exec(/*sql*/ `
              CREATE TABLE "bench_schema_event_${txIndex}" (
                "id" INTEGER PRIMARY KEY,
                "${ZERO_VERSION_COLUMN_NAME}" TEXT NOT NULL
              )
            `);
          } else {
            upsertRow.run(
              changeID,
              changeID & 1023,
              makePayload(Math.max(1, messageBytes - 128)),
              watermark,
            );
          }
        }
        if (writesLog) {
          insertLogRow(insertLog, watermark, pos + 1, json, null, null);
        }
        changeID++;
      }

      if (writesLog) {
        insertLogRow(
          insertLog,
          watermark,
          c.logicalTxRows + 1,
          commitJSON,
          watermark,
          Date.now(),
        );
      }
      updateWatermark.run(watermark);
      const commitStart = performance.now();
      runner.commit();
      commitLatencyMs.push(performance.now() - commitStart);
    } catch (e) {
      if (db.inTransaction) {
        runner.rollback();
      }
      throw e;
    }
    transactionLatencyMs.push(performance.now() - transactionStart);

    if (betweenCommitPurgeRows !== undefined && writesLog) {
      purgeResults.push(
        purgeBatch(db, {
          externalFloor: watermark,
          retentionCutoffMs: Number.MAX_SAFE_INTEGER,
          maxRows: betweenCommitPurgeRows,
        }),
      );
    }
    upstreamLoopStallMs.push(performance.now() - loopStart);
  }

  return {
    elapsedMs: performance.now() - start,
    changes: c.totalTransactions * c.logicalTxRows,
    transactions: c.totalTransactions,
    payloadBytes,
    commitLatencyMs,
    transactionLatencyMs,
    upstreamLoopStallMs,
    purgeResults,
    purgedRows: purgeResults.reduce(
      (sum, {deletedRows}) => sum + deletedRows,
      0,
    ),
  };
}

function insertLogRow(
  stmt: Statement,
  watermark: string,
  pos: number,
  change: string,
  precommit: string | null,
  writeTimeMs: number | null,
) {
  stmt.run(watermark, pos, change, precommit, writeTimeMs);
}

function verifyCase(db: Database, c: BenchCase, result: WriteResult) {
  const writesRows = c.mode === 'apply' || c.mode === 'combined';
  const writesLog = c.mode === 'log' || c.mode === 'combined';
  const schemaTransactions =
    c.schemaEveryTransactions === undefined
      ? 0
      : Math.ceil(c.totalTransactions / c.schemaEveryTransactions);
  const expectedRows = writesRows ? result.changes - schemaTransactions : 0;
  const expectedLogRows = writesLog
    ? 2 + result.changes + c.totalTransactions * 2 - result.purgedRows
    : 2;

  expect(
    db.prepare(`SELECT count(*) AS n FROM "bench_rows"`).get<{n: number}>().n,
  ).toBe(expectedRows);
  expect(
    db
      .prepare(`SELECT count(*) AS n FROM "_zero.changeLogStream"`)
      .get<{n: number}>().n,
  ).toBe(expectedLogRows);
  expect(
    db
      .prepare(`SELECT "stateVersion" AS version FROM "_zero.replicationState"`)
      .get<{version: string}>().version,
  ).toBe(versionToLexi(c.totalTransactions));
  expect(
    db
      .prepare(/*sql*/ `
        SELECT count(*) AS n
        FROM sqlite_schema
        WHERE type = 'table' AND name LIKE 'bench_schema_event_%'
      `)
      .get<{n: number}>().n,
  ).toBe(writesRows ? schemaTransactions : 0);
  expect(
    db
      .prepare(`SELECT max("watermark") AS head FROM "_zero.changeLogStream"`)
      .get<{head: string}>().head,
  ).toBe(writesLog ? versionToLexi(c.totalTransactions) : '00');
  assertCompleteTransactions(db);
}

function assertCompleteTransactions(db: Database) {
  const incomplete = db
    .prepare(/*sql*/ `
      SELECT "watermark"
      FROM "_zero.changeLogStream"
      GROUP BY "watermark"
      HAVING min("pos") <> 0
        OR max("pos") <> count(*) - 1
        OR json_extract(max(CASE WHEN "pos" = 0 THEN "change" END), '$.tag') <> 'begin'
        OR json_extract(max(CASE WHEN "precommit" IS NOT NULL THEN "change" END), '$.tag') <> 'commit'
        OR sum(CASE WHEN "precommit" IS NOT NULL THEN 1 ELSE 0 END) <> 1
    `)
    .all();
  expect(incomplete).toEqual([]);
}

function purgeBatch(
  db: Database,
  opts: {
    externalFloor: string;
    retentionCutoffMs: number;
    maxRows: number;
  },
): PurgeResult {
  const runner = new StatementRunner(db);
  const eligibleCommits = db.prepare(/*sql*/ `
    SELECT "watermark"
    FROM "_zero.changeLogStream"
    WHERE "writeTimeMs" IS NOT NULL
      AND "writeTimeMs" < ?
      AND "watermark" <= ?
      AND "watermark" < (
        SELECT "stateVersion" FROM "_zero.replicationState"
      )
    ORDER BY "writeTimeMs", "watermark"
  `);
  const countTransaction = db.prepare(/*sql*/ `
    SELECT count(*) AS "rows"
    FROM "_zero.changeLogStream"
    WHERE "watermark" = ?
  `);
  const deleteThrough = db.prepare(/*sql*/ `
    DELETE FROM "_zero.changeLogStream" WHERE "watermark" <= ?
  `);

  const start = performance.now();
  let deletedRows = 0;
  let deletedThrough: string | undefined;
  let moreEligible = false;
  try {
    runner.beginImmediate();
    const candidates = eligibleCommits.all<{watermark: string}>(
      opts.retentionCutoffMs,
      opts.externalFloor,
    );
    for (const {watermark} of candidates) {
      const rows = countTransaction.get<{rows: number}>(watermark).rows;
      if (deletedRows > 0 && deletedRows + rows > opts.maxRows) {
        moreEligible = true;
        break;
      }
      // Treat maxRows as a soft limit. In particular, the oldest transaction
      // must be removed even when it is larger than the target batch.
      deletedRows += rows;
      deletedThrough = watermark;
    }
    if (deletedThrough !== undefined) {
      deleteThrough.run(deletedThrough);
    }
    if (!moreEligible && candidates.length > 0) {
      moreEligible = deletedThrough !== candidates.at(-1)?.watermark;
    }
    runner.commit();
  } catch (e) {
    if (db.inTransaction) {
      runner.rollback();
    }
    throw e;
  }
  return {
    elapsedMs: performance.now() - start,
    deletedRows,
    deletedThrough,
    moreEligible,
  };
}

function runCatchup(
  dbPath: string,
  fromWatermark: string,
  throughWatermark: string,
  batchSize: number,
): CatchupResult {
  const reader = new Database(lc, dbPath, {readonly: true});
  const runner = new StatementRunner(reader);
  const readBatch = reader.prepare(/*sql*/ `
    SELECT "watermark", "pos", json_extract("change", '$.tag') AS "tag"
    FROM "_zero.changeLogStream"
    WHERE ("watermark" > ? OR ("watermark" = ? AND "pos" > ?))
      AND "watermark" <= ?
    ORDER BY "watermark", "pos"
    LIMIT ?
  `);
  const transactions = new Map<
    string,
    {firstPos: number; lastPos: number; firstTag: string; lastTag: string}
  >();
  const batchLatencyMs: number[] = [];
  let lastWatermark = fromWatermark;
  let lastPos = Number.MAX_SAFE_INTEGER;
  let rowsRead = 0;
  const start = performance.now();
  try {
    while (true) {
      const batchStart = performance.now();
      runner.begin();
      const rows = readBatch.all<{
        watermark: string;
        pos: number;
        tag: string;
      }>(lastWatermark, lastWatermark, lastPos, throughWatermark, batchSize);
      runner.commit();
      batchLatencyMs.push(performance.now() - batchStart);
      if (rows.length === 0) {
        break;
      }

      for (const row of rows) {
        const seen = transactions.get(row.watermark);
        if (seen === undefined) {
          transactions.set(row.watermark, {
            firstPos: row.pos,
            lastPos: row.pos,
            firstTag: row.tag,
            lastTag: row.tag,
          });
        } else {
          seen.lastPos = row.pos;
          seen.lastTag = row.tag;
        }
      }
      rowsRead += rows.length;
      const last = rows.at(-1)!;
      lastWatermark = last.watermark;
      lastPos = last.pos;
      if (rows.length < batchSize) {
        break;
      }
    }
  } finally {
    reader.close();
  }

  for (const transaction of transactions.values()) {
    expect(transaction).toMatchObject({firstPos: 0, firstTag: 'begin'});
    expect(transaction.lastTag).toBe('commit');
    expect(transaction.lastPos).toBeGreaterThan(transaction.firstPos);
  }
  return {
    elapsedMs: performance.now() - start,
    rows: rowsRead,
    batchLatencyMs,
  };
}

function percentile(values: readonly number[], percent: number) {
  if (values.length === 0) {
    return 0;
  }
  const sorted = values.toSorted((a, b) => a - b);
  return sorted[Math.ceil((percent / 100) * sorted.length) - 1]!;
}

function latencyPercentiles(values: readonly number[]) {
  return {
    p50: percentile(values, 50),
    p95: percentile(values, 95),
    p99: percentile(values, 99),
  };
}

function caseName(c: BenchCase) {
  return (
    `workload=${c.workload} mode=${c.mode} ` +
    `logicalTxRows=${c.logicalTxRows} sqliteTxRows=${c.sqliteTxRows}`
  );
}

function recordWriteSamples(
  name: string,
  samples: readonly {write: WriteResult; files: FileMetrics}[],
) {
  benchmarkRecorder.recordThroughputSamples(
    `${name} transactions`,
    samples.map(({write}) => ({
      elapsedMs: write.elapsedMs,
      operations: write.transactions,
    })),
  );
  benchmarkRecorder.recordThroughputSamples(
    `${name} changes`,
    samples.map(({write}) => ({
      elapsedMs: write.elapsedMs,
      operations: write.changes,
    })),
  );
  benchmarkRecorder.recordThroughputSamples(
    `${name} payload MB`,
    samples.map(({write}) => ({
      elapsedMs: write.elapsedMs,
      operations: write.payloadBytes / BYTES_PER_MB,
    })),
  );

  const allCommits = samples.flatMap(({write}) => write.commitLatencyMs);
  const allTransactions = samples.flatMap(
    ({write}) => write.transactionLatencyMs,
  );
  const allUpstreamStalls = samples.flatMap(
    ({write}) => write.upstreamLoopStallMs,
  );
  recordLatency(`${name} commit latency`, allCommits);
  recordLatency(`${name} SQLite transaction latency`, allTransactions);
  recordLatency(`${name} upstream-loop SQLite stall`, allUpstreamStalls);

  publishedResults.push({
    name,
    measurements: {
      transactionsPerSecond: samples.map(
        ({write}) => (write.transactions * 1000) / write.elapsedMs,
      ),
      changesPerSecond: samples.map(
        ({write}) => (write.changes * 1000) / write.elapsedMs,
      ),
      commitLatencyMs: latencyPercentiles(allCommits),
      sqliteTransactionLatencyMs: latencyPercentiles(allTransactions),
      upstreamLoopSQLiteStallMs: latencyPercentiles(allUpstreamStalls),
      dbBytes: samples.map(({files}) => files.dbBytes),
      walBytes: samples.map(({files}) => files.walBytes),
      freelistBytes: samples.map(({files}) => files.freelistBytes),
    },
  });
}

function recordLatency(name: string, values: readonly number[]) {
  benchmarkRecorder.recordThroughputSamples(
    name,
    values.map(elapsedMs => ({elapsedMs, operations: 1})),
  );
  // The shared benchmark table displays p50 and p99. Record p95 explicitly so
  // it is also retained in BMF output without changing the global formatter.
  benchmarkRecorder.recordThroughput(
    `${name} p95`,
    [percentile(values, 95)],
    1,
  );
}

function writeSample(
  c: BenchCase,
  betweenCommitPurgeRows?: number,
): {write: WriteResult; files: FileMetrics} {
  const dbFile = new DbFile('sqlite-change-log-ceiling-bench');
  cleanup.push(() => dbFile.delete());
  const db = setupDB(dbFile);
  try {
    const write = runCase(db, c, betweenCommitPurgeRows);
    verifyCase(db, c, write);
    return {write, files: fileMetrics(db, dbFile.path)};
  } finally {
    db.close();
    runCleanup();
  }
}

function measuredSamples<T>(run: () => T): T[] {
  const samples: T[] = [];
  for (let rep = 0; rep < WARMUP_REPS + REPS; rep++) {
    const sample = run();
    if (rep >= WARMUP_REPS) {
      samples.push(sample);
    }
  }
  return samples;
}

function recordGoNoGoResult(c: BenchCase, samples: readonly WriteResult[]) {
  const key = `${c.workload}:${c.mode}`;
  goNoGoResults.set(key, [...samples]);
}

function assertGoNoGoGates() {
  if (goNoGoThresholds === undefined) {
    return;
  }

  for (const workload of [
    'small-high-frequency',
    'mixed-row-schema',
    'oversized',
  ] satisfies Workload[]) {
    const apply = goNoGoResults.get(`${workload}:apply`);
    const combined = goNoGoResults.get(`${workload}:combined`);
    expect(apply, `missing apply samples for ${workload}`).toBeDefined();
    expect(combined, `missing combined samples for ${workload}`).toBeDefined();

    const changesPerSecond = percentile(
      combined!.map(result => (result.changes * 1000) / result.elapsedMs),
      50,
    );
    const combinedP95 = percentile(
      combined!.flatMap(result => result.commitLatencyMs),
      95,
    );
    const applyStallP50 = percentile(
      apply!.flatMap(result => result.upstreamLoopStallMs),
      50,
    );
    const combinedStallP50 = percentile(
      combined!.flatMap(result => result.upstreamLoopStallMs),
      50,
    );
    const regressionPercent =
      ((combinedStallP50 - applyStallP50) / applyStallP50) * 100;

    expect(changesPerSecond).toBeGreaterThanOrEqual(
      goNoGoThresholds.minChangesPerSecond,
    );
    expect(combinedP95).toBeLessThanOrEqual(goNoGoThresholds.maxCommitP95Ms);
    expect(regressionPercent).toBeLessThanOrEqual(
      goNoGoThresholds.maxStallRegressionPercent,
    );
  }
}

function benchmarkEnvironment() {
  const db = new Database(lc, ':memory:');
  let sqliteVersion: string;
  try {
    sqliteVersion = db
      .prepare('SELECT sqlite_version() AS version')
      .get<{version: string}>().version;
  } finally {
    db.close();
  }
  const relevantEnvironment = Object.fromEntries(
    Object.entries(process.env)
      .filter(([name]) => name.startsWith('SQLITE_CHANGE_LOG_'))
      .toSorted(([a], [b]) => a.localeCompare(b)),
  );
  const benchmarkCommand =
    'pnpm --filter zero-cache run bench sqlite-change-log-ceiling';
  const invocation = [
    ...Object.entries(relevantEnvironment).map(
      ([name, value]) => `${name}=${JSON.stringify(value)}`,
    ),
    benchmarkCommand,
  ].join(' ');
  return {
    benchmarkCommand,
    invocation,
    environment: relevantEnvironment,
    cpu: cpus()[0]?.model ?? 'unknown',
    cpuCount: cpus().length,
    totalMemoryBytes: totalmem(),
    os: `${platform()} ${release()} ${arch()}`,
    node: process.version,
    sqlite: sqliteVersion,
    journalMode: 'wal',
    synchronous: 'NORMAL',
    walAutocheckpoint: 0,
    litestreamExecutable,
  };
}

function findOnPath(executable: string) {
  if (executable.includes('/')) {
    return existsSync(executable) ? executable : undefined;
  }
  for (const directory of (process.env.PATH ?? '').split(delimiter)) {
    const candidate = join(directory, executable);
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  return undefined;
}

function findLitestreamV5() {
  const configured =
    process.env.SQLITE_CHANGE_LOG_LITESTREAM_EXECUTABLE ??
    process.env.ZERO_LITESTREAM_EXECUTABLE_V5 ??
    'litestream';
  const executable = findOnPath(configured);
  if (executable === undefined) {
    return undefined;
  }
  const result = spawnSync(executable, ['version'], {encoding: 'utf8'});
  const version = `${result.stdout}${result.stderr}`;
  return result.status === 0 && LITESTREAM_VERSION_RE.test(version)
    ? executable
    : undefined;
}

async function waitForLitestream(proc: ChildProcess) {
  await new Promise<void>((resolve, reject) => {
    let output = '';
    const timeout = setTimeout(() => {
      reject(new Error(`timed out waiting for Litestream to start: ${output}`));
    }, 10_000);
    const finish = (fn: () => void) => {
      clearTimeout(timeout);
      fn();
    };
    const onData = (chunk: Buffer) => {
      output += chunk.toString();
      if (LITESTREAM_READY_RE.test(output)) {
        finish(resolve);
      }
    };
    proc.stdout?.on('data', onData);
    proc.stderr?.on('data', onData);
    proc.once('error', error => finish(() => reject(error)));
    proc.once('exit', code => {
      if (code !== null) {
        finish(() =>
          reject(
            new Error(
              `Litestream exited with code ${code} before startup: ${output}`,
            ),
          ),
        );
      }
    });
  });
}

async function stopLitestream(proc: ChildProcess) {
  if (proc.exitCode !== null || proc.signalCode !== null) {
    return;
  }
  await new Promise<void>(resolve => {
    const timeout = setTimeout(() => proc.kill('SIGKILL'), 5000);
    proc.once('close', () => {
      clearTimeout(timeout);
      resolve();
    });
    proc.kill('SIGTERM');
  });
}

describe('replicator/sqlite change-log ceiling', () => {
  test('write workload sweep', {timeout: TEST_TIMEOUT_MS}, () => {
    for (const c of makeCases()) {
      const samples = measuredSamples(() => writeSample(c));
      const name = `replicator/sqlite change-log ceiling ${caseName(c)}`;
      recordWriteSamples(name, samples);
      recordGoNoGoResult(
        c,
        samples.map(({write}) => write),
      );
    }
    assertGoNoGoGates();
  });

  test(
    'large catchup scan on a second connection',
    {timeout: TEST_TIMEOUT_MS},
    () => {
      const c = makeCases().find(
        c => c.workload === 'mixed-row-schema' && c.mode === 'combined',
      );
      if (c === undefined) {
        return;
      }

      const samples = measuredSamples(() => {
        const dbFile = new DbFile('sqlite-change-log-catchup-bench');
        cleanup.push(() => dbFile.delete());
        const db = setupDB(dbFile);
        try {
          const write = runCase(db, c);
          verifyCase(db, c, write);
          const catchup = runCatchup(
            dbFile.path,
            '00',
            versionToLexi(c.totalTransactions),
            READ_BATCH_ROWS,
          );
          expect(catchup.rows).toBe(write.changes + write.transactions * 2);
          return {catchup, files: fileMetrics(db, dbFile.path)};
        } finally {
          db.close();
          runCleanup();
        }
      });

      const name =
        'replicator/sqlite change-log large catchup ' +
        `batchRows=${READ_BATCH_ROWS}`;
      benchmarkRecorder.recordThroughputSamples(
        `${name} rows`,
        samples.map(({catchup}) => ({
          elapsedMs: catchup.elapsedMs,
          operations: catchup.rows,
        })),
      );
      const batchLatencies = samples.flatMap(
        ({catchup}) => catchup.batchLatencyMs,
      );
      recordLatency(`${name} batch latency`, batchLatencies);
      recordLatency(
        `${name} end-to-end latency`,
        samples.map(({catchup}) => catchup.elapsedMs),
      );
      publishedResults.push({
        name,
        measurements: {
          rowsPerSecond: samples.map(
            ({catchup}) => (catchup.rows * 1000) / catchup.elapsedMs,
          ),
          batchLatencyMs: latencyPercentiles(batchLatencies),
          endToEndLatencyMs: latencyPercentiles(
            samples.map(({catchup}) => catchup.elapsedMs),
          ),
          dbBytes: samples.map(({files}) => files.dbBytes),
          walBytes: samples.map(({files}) => files.walBytes),
          freelistBytes: samples.map(({files}) => files.freelistBytes),
        },
      });
    },
  );

  test(
    'small purge batches between commits',
    {timeout: TEST_TIMEOUT_MS},
    () => {
      const c = makeCases().find(
        c => c.workload === 'small-high-frequency' && c.mode === 'combined',
      );
      if (c === undefined) {
        return;
      }
      const samples = measuredSamples(() =>
        writeSample(c, BETWEEN_COMMIT_PURGE_ROWS),
      );
      const name =
        'replicator/sqlite change-log between-commit purge ' +
        `batchRows=${BETWEEN_COMMIT_PURGE_ROWS}`;
      recordWriteSamples(name, samples);
      const purges = samples.flatMap(({write}) => write.purgeResults);
      const productivePurges = purges.filter(
        ({deletedRows}) => deletedRows > 0,
      );
      expect(productivePurges.length).toBeGreaterThan(0);
      recordLatency(
        `${name} transaction latency`,
        productivePurges.map(({elapsedMs}) => elapsedMs),
      );
      benchmarkRecorder.recordThroughputSamples(
        `${name} rows`,
        productivePurges.map(({elapsedMs, deletedRows}) => ({
          elapsedMs,
          operations: deletedRows,
        })),
      );
      publishedResults.push({
        name: `${name} purge`,
        measurements: {
          transactionLatencyMs: latencyPercentiles(
            productivePurges.map(({elapsedMs}) => elapsedMs),
          ),
          purgeTransactions: productivePurges.length,
          rowsDeleted: productivePurges.reduce(
            (sum, {deletedRows}) => sum + deletedRows,
            0,
          ),
        },
      });
    },
  );

  test(
    'idle purge drains oversized transactions',
    {timeout: TEST_TIMEOUT_MS},
    () => {
      const c = makeCases().find(
        c => c.workload === 'oversized' && c.mode === 'combined',
      );
      if (c === undefined) {
        return;
      }
      const samples = measuredSamples(() => {
        const dbFile = new DbFile('sqlite-change-log-purge-bench');
        cleanup.push(() => dbFile.delete());
        const db = setupDB(dbFile);
        try {
          const write = runCase(db, c);
          verifyCase(db, c, write);
          const purges: PurgeResult[] = [];
          for (;;) {
            const result = purgeBatch(db, {
              externalFloor: versionToLexi(c.totalTransactions),
              retentionCutoffMs: Number.MAX_SAFE_INTEGER,
              maxRows: PURGE_BATCH_ROWS,
            });
            purges.push(result);
            if (!result.moreEligible) {
              break;
            }
            expect(result.deletedRows).toBeGreaterThan(0);
          }
          expect(
            purges.some(({deletedRows}) => deletedRows > PURGE_BATCH_ROWS),
          ).toBe(true);
          expect(purges.at(-1)?.moreEligible).toBe(false);
          expect(
            db
              .prepare(`SELECT count(*) AS n FROM "_zero.changeLogStream"`)
              .get<{n: number}>().n,
          ).toBe(c.logicalTxRows + 2);
          assertCompleteTransactions(db);
          return {purges, files: fileMetrics(db, dbFile.path)};
        } finally {
          db.close();
          runCleanup();
        }
      });

      const name =
        'replicator/sqlite change-log idle purge ' +
        `batchRows=${PURGE_BATCH_ROWS}`;
      const productivePurges = samples.flatMap(({purges}) =>
        purges.filter(({deletedRows}) => deletedRows > 0),
      );
      recordLatency(
        `${name} transaction latency`,
        productivePurges.map(({elapsedMs}) => elapsedMs),
      );
      benchmarkRecorder.recordThroughputSamples(
        `${name} rows`,
        productivePurges.map(({elapsedMs, deletedRows}) => ({
          elapsedMs,
          operations: deletedRows,
        })),
      );
      publishedResults.push({
        name,
        measurements: {
          transactionLatencyMs: latencyPercentiles(
            productivePurges.map(({elapsedMs}) => elapsedMs),
          ),
          purgeTransactions: productivePurges.length,
          rowsDeleted: productivePurges.reduce(
            (sum, {deletedRows}) => sum + deletedRows,
            0,
          ),
          dbBytes: samples.map(({files}) => files.dbBytes),
          walBytes: samples.map(({files}) => files.walBytes),
          freelistBytes: samples.map(({files}) => files.freelistBytes),
        },
      });
    },
  );

  test.skipIf(litestreamExecutable === undefined)(
    'litestream v5/checkpoint pressure',
    {timeout: TEST_TIMEOUT_MS},
    async () => {
      const c = makeCases().find(
        c => c.workload === 'small-high-frequency' && c.mode === 'combined',
      );
      if (c === undefined || litestreamExecutable === undefined) {
        return;
      }

      const samples: {write: WriteResult; files: FileMetrics}[] = [];
      for (let rep = 0; rep < WARMUP_REPS + REPS; rep++) {
        const dbFile = new DbFile('sqlite-change-log-litestream-bench');
        const backupDir = mkdtempSync(
          join(tmpdir(), 'sqlite-change-log-litestream-'),
        );
        const db = setupDB(dbFile);
        const proc = spawn(
          litestreamExecutable,
          ['replicate', dbFile.path, `file://${join(backupDir, 'replica')}`],
          {stdio: ['ignore', 'pipe', 'pipe']},
        );
        try {
          await waitForLitestream(proc);
          const write = runCase(db, c);
          verifyCase(db, c, write);
          if (rep >= WARMUP_REPS) {
            samples.push({write, files: fileMetrics(db, dbFile.path)});
          }
        } finally {
          await stopLitestream(proc);
          db.close();
          dbFile.delete();
          rmSync(backupDir, {recursive: true, force: true});
        }
      }
      recordWriteSamples(
        'replicator/sqlite change-log litestream-v5 checkpoint pressure',
        samples,
      );
    },
  );
});
