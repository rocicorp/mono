/* oxlint-disable no-console */

// Benchmarks the SQLite write ceiling for the replication-manager shape:
// applying rows to the backup replica and appending the raw change stream to a
// SQLite-local change log.
//
// Two layouts are measured:
//
//   'combined'       one file, one transaction: the change log lives inside the
//                    replica, so every logical upstream transaction is exactly
//                    one SQLite transaction.
//   'separate-files' two files, two ordered transactions: the change log lives
//                    in `${replica}-change-log` and commits *before* the replica
//                    (design 4.4). This is the layout plan slice 7D implements.
//
// Deterministic correctness run:
//   SQLITE_CHANGE_LOG_CORRECTNESS=1 pnpm --filter zero-cache run bench sqlite-change-log-ceiling
//
// Go/no-go runs additionally set SQLITE_CHANGE_LOG_GO_NO_GO=1 plus
// SQLITE_CHANGE_LOG_MIN_CHANGES_PER_SECOND,
// SQLITE_CHANGE_LOG_MAX_COMMIT_P95_MS, and
// SQLITE_CHANGE_LOG_MAX_STALL_REGRESSION_PERCENT.
//
// Slice 7A knobs:
//   SQLITE_CHANGE_LOG_MODES                 apply,log,combined,separate-files
//   SQLITE_CHANGE_LOG_LOG_JOURNAL_MODE      wal,wal2   (change-log DB only)
//   SQLITE_CHANGE_LOG_LOG_SYNCHRONOUS       NORMAL,FULL (change-log DB only)
//   SQLITE_CHANGE_LOG_RETENTION_MINUTES     simulated retention window
//   SQLITE_CHANGE_LOG_RETENTION_WINDOWS     retention windows per bench run

import {spawn, spawnSync, type ChildProcess} from 'node:child_process';
import {existsSync, mkdtempSync, rmSync, statSync} from 'node:fs';
import {arch, cpus, platform, release, tmpdir, totalmem} from 'node:os';
import {delimiter, join} from 'node:path';
import {afterAll, afterEach, describe, expect, test} from 'vitest';
import {createManualBenchmarkRecorder} from '../../../../shared/src/bench.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Statement} from '../../../../zqlite/src/db.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {deleteLiteDB} from '../../db/delete-lite-db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {DbFile} from '../../test/lite.ts';
import {versionToLexi} from '../../types/lexi-version.ts';
import {getPragmaConfig} from '../../workers/replicator.ts';
import {ZERO_VERSION_COLUMN_NAME} from './schema/constants.ts';
import {applyPragmas} from './write-worker-client.ts';

type WriteMode = 'apply' | 'log' | 'combined' | 'separate-files';
type Workload = 'small-high-frequency' | 'mixed-row-schema' | 'oversized';
type JournalMode = 'wal' | 'wal2';
type Synchronous = 'NORMAL' | 'FULL';

/** Durability settings for the change-log DB only (design 4.6). */
type LogDurability = {
  readonly journalMode: JournalMode;
  readonly synchronous: Synchronous;
};

type MessageSize = {
  readonly bytes: number;
  readonly weight: number;
};

type BenchCase = {
  readonly workload: Workload;
  readonly mode: WriteMode;
  /** Defined only for 'separate-files'; the coupled layout has no second file. */
  readonly logDurability: LogDurability | undefined;
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

type CaseFileMetrics = {
  readonly replica: FileMetrics;
  /** Undefined in the coupled layout, where there is no second file. */
  readonly changeLog: FileMetrics | undefined;
};

/**
 * How the change log is trimmed while the workload runs.
 *
 * 'drain' purges everything below the latest durable transaction, which is what
 * the original bench measured. 'retention' applies the policy that actually
 * governs a catchup log (design 3.2): purge only what has aged out of the
 * retention window, which is gated on the confirmed backup watermark and runs
 * tens of minutes, not the 60s `sqliteChangeLogRetentionMs` floor.
 */
type PurgePolicy =
  | {readonly kind: 'drain'; readonly maxRows: number}
  | {
      readonly kind: 'retention';
      readonly maxRows: number;
      readonly retentionMs: number;
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
  /** Time spent in COMMIT; both commits in the separated layout. */
  readonly commitLatencyMs: readonly number[];
  /** Empty unless separated. The gap to the replica commit is the phantom window. */
  readonly logCommitLatencyMs: readonly number[];
  readonly replicaCommitLatencyMs: readonly number[];
  readonly transactionLatencyMs: readonly number[];
  readonly upstreamLoopStallMs: readonly number[];
  readonly purgeResults: readonly PurgeResult[];
  readonly purgedRows: number;
};

type RetainedLog = {
  readonly rows: number;
  readonly windowMs: number;
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

// The replica's settings are the production 'backup' ones and are held fixed so
// that separated runs stay comparable to previously published combined runs.
const REPLICA_JOURNAL_MODE: JournalMode = 'wal';
const REPLICA_SYNCHRONOUS: Synchronous = 'NORMAL';

// Design 4.6 proposes these for the change-log DB. The sweep below measures the
// alternatives rather than asserting the choice.
const DEFAULT_LOG_JOURNAL_MODES: readonly JournalMode[] = ['wal2'];
const DEFAULT_LOG_SYNCHRONOUS: readonly Synchronous[] = ['NORMAL'];
const SWEEP_LOG_JOURNAL_MODES: readonly JournalMode[] = ['wal', 'wal2'];
const SWEEP_LOG_SYNCHRONOUS: readonly Synchronous[] = ['NORMAL', 'FULL'];

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

// Design 3.2: realistic steady-state retention is ~15-30 minutes of change
// stream, gated on the confirmed S3 backup watermark. 20 is the midpoint.
const RETENTION_MINUTES = integerFromEnv(
  'SQLITE_CHANGE_LOG_RETENTION_MINUTES',
  20,
);
// How many retention windows one bench run stands for. Anything above 1 reaches
// steady state: the log ends holding one window of live rows, and the file
// carries the insert-and-purge churn of the windows before it.
const RETENTION_WINDOWS = integerFromEnv(
  'SQLITE_CHANGE_LOG_RETENTION_WINDOWS',
  3,
);
const RETENTION_MS = RETENTION_MINUTES * 60_000;
const SIMULATED_RUN_MS = RETENTION_MS * RETENTION_WINDOWS;
const SIMULATED_EPOCH_MS = 1_700_000_000_000;

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
          retention: {
            minutes: RETENTION_MINUTES,
            windows: RETENTION_WINDOWS,
            simulatedRunMs: SIMULATED_RUN_MS,
          },
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

function enumListFromEnv<T extends string>(
  name: string,
  allowed: readonly T[],
  fallback: readonly T[],
): T[] {
  const raw = process.env[name];
  if (!raw) {
    return [...fallback];
  }
  const values = raw
    .split(',')
    .map(part => part.trim())
    .filter(part => part.length > 0)
    .map(part => {
      const value = allowed.find(candidate => candidate === part);
      if (value === undefined) {
        throw new Error(
          `${name} must contain only ${allowed.join(', ')}; got ${raw}`,
        );
      }
      return value;
    });
  if (values.length === 0) {
    throw new Error(`${name} must contain at least one value`);
  }
  return [...new Set(values)];
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
  const modes = enumListFromEnv(
    'SQLITE_CHANGE_LOG_MODES',
    ['apply', 'log', 'combined', 'separate-files'] as const,
    GO_NO_GO
      ? ['apply', 'combined', 'separate-files']
      : ['combined', 'separate-files'],
  );
  if (GO_NO_GO && (!modes.includes('apply') || !modes.includes('combined'))) {
    throw new Error(
      'Go/no-go runs require both apply and combined write modes to measure regression',
    );
  }
  return modes;
}

function logDurabilitiesFromEnv(
  journalModeFallback: readonly JournalMode[],
  synchronousFallback: readonly Synchronous[],
): LogDurability[] {
  const journalModes = enumListFromEnv(
    'SQLITE_CHANGE_LOG_LOG_JOURNAL_MODE',
    ['wal', 'wal2'] as const,
    journalModeFallback,
  );
  const synchronousModes = enumListFromEnv(
    'SQLITE_CHANGE_LOG_LOG_SYNCHRONOUS',
    ['NORMAL', 'FULL'] as const,
    synchronousFallback,
  );
  return journalModes.flatMap(journalMode =>
    synchronousModes.map(synchronous => ({journalMode, synchronous})),
  );
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

function makeCases(
  logDurabilities: readonly LogDurability[] = logDurabilitiesFromEnv(
    DEFAULT_LOG_JOURNAL_MODES,
    DEFAULT_LOG_SYNCHRONOUS,
  ),
): BenchCase[] {
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
    // Only the separated layout has a change-log file whose durability can be
    // configured; the coupled layout inherits the replica's.
    (mode === 'separate-files' ? logDurabilities : [undefined]).flatMap(
      logDurability =>
        workloads.map(
          ({
            workload,
            logicalTxRows,
            messageSizes,
            schemaEveryTransactions,
          }): BenchCase => ({
            workload,
            mode,
            logDurability,
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

/**
 * The bench writes a whole run in seconds; production spreads the same stream
 * over the tens of minutes described in design 3.2. Mapping the transaction
 * index onto a simulated clock makes a real retention window a real fraction of
 * the stream, so a retention-gated purge leaves exactly one window behind
 * instead of draining the log to a 60s floor.
 */
function simulatedWriteTimeMs(txIndex: number, totalTransactions: number) {
  return (
    SIMULATED_EPOCH_MS +
    Math.round((SIMULATED_RUN_MS * (txIndex + 1)) / totalTransactions)
  );
}

/** Mirrors design 4.1. Slice 7B moves this derivation into production code. */
function changeLogFileName(replicaFile: string) {
  return `${replicaFile}-change-log`;
}

const REPLICA_SCHEMA_SQL = /*sql*/ `
  CREATE TABLE "bench_rows" (
    "id" INTEGER PRIMARY KEY,
    "indexed" INTEGER NOT NULL,
    "payload" TEXT NOT NULL,
    "${ZERO_VERSION_COLUMN_NAME}" TEXT NOT NULL
  );
  CREATE INDEX "bench_rows_indexed_idx" ON "bench_rows"("indexed");

  CREATE TABLE "_zero.replicationState" (
    "stateVersion" TEXT NOT NULL,
    "writeTimeMs" INTEGER,
    "lock" INTEGER PRIMARY KEY DEFAULT 1 CHECK ("lock" = 1)
  );

  INSERT INTO "_zero.replicationState" ("stateVersion", "writeTimeMs")
    VALUES ('00', ${SIMULATED_EPOCH_MS});
`;

const CHANGE_LOG_SCHEMA_SQL = /*sql*/ `
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

  INSERT INTO "_zero.changeLogStream"
    ("watermark", "pos", "change", "precommit", "writeTimeMs")
    VALUES
      ('00', 0, '{"tag":"begin"}', NULL, NULL),
      ('00', 1, '{"tag":"commit"}', '00', ${SIMULATED_EPOCH_MS});
`;

// Design 4.2. Present only in the separated layout, where the log has to
// describe which replica it belongs to.
const CHANGE_LOG_META_SQL = /*sql*/ `
  CREATE TABLE "_zero.changeLogMeta" (
    "replicaVersion" TEXT NOT NULL,
    "schemaVersion"  INTEGER NOT NULL,
    "lock"           INTEGER PRIMARY KEY DEFAULT 1 CHECK ("lock" = 1)
  );

  INSERT INTO "_zero.changeLogMeta" ("replicaVersion", "schemaVersion")
    VALUES ('00', 1);
`;

type BenchDBs = {
  readonly replica: Database;
  readonly replicaPath: string;
  /** The change-log handle. Identical to `replica` unless `separated`. */
  readonly log: Database;
  readonly logPath: string;
  readonly separated: boolean;
  readonly close: () => void;
};

function setupDBs(c: BenchCase, testName: string): BenchDBs {
  const dbFile = new DbFile(testName);
  cleanup.push(() => dbFile.delete());

  const replica = new Database(lc, dbFile.path);
  replica.pragma(`journal_mode = ${REPLICA_JOURNAL_MODE}`);
  replica.pragma(`synchronous = ${REPLICA_SYNCHRONOUS}`);
  applyPragmas(replica, getPragmaConfig('backup'));
  replica.exec(REPLICA_SCHEMA_SQL);

  if (c.mode !== 'separate-files') {
    replica.exec(CHANGE_LOG_SCHEMA_SQL);
    return {
      replica,
      replicaPath: dbFile.path,
      log: replica,
      logPath: dbFile.path,
      separated: false,
      close: () => replica.close(),
    };
  }

  const durability = c.logDurability;
  if (durability === undefined) {
    throw new Error('separate-files cases require a logDurability');
  }
  const logPath = changeLogFileName(dbFile.path);
  cleanup.push(() => deleteLiteDB(logPath));

  const log = new Database(lc, logPath);
  log.pragma(`journal_mode = ${durability.journalMode}`);
  log.pragma(`synchronous = ${durability.synchronous}`);
  // Design 4.6: nothing else owns this file, so wal_autocheckpoint keeps its
  // default rather than the replica's litestream-owned 0.
  applyPragmas(log, {busyTimeout: 30_000, analysisLimit: 1000});
  log.exec(CHANGE_LOG_SCHEMA_SQL);
  log.exec(CHANGE_LOG_META_SQL);

  return {
    replica,
    replicaPath: dbFile.path,
    log,
    logPath,
    separated: true,
    close: () => {
      log.close();
      replica.close();
    },
  };
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
    // wal2 mode alternates between two WAL files; both are on-disk cost.
    walBytes: fileSize(`${path}-wal`) + fileSize(`${path}-wal2`),
    freelistBytes: pageSize * freelistPages,
  };
}

function caseFileMetrics(dbs: BenchDBs): CaseFileMetrics {
  return {
    replica: fileMetrics(dbs.replica, dbs.replicaPath),
    changeLog: dbs.separated ? fileMetrics(dbs.log, dbs.logPath) : undefined,
  };
}

function prepareStatements(dbs: BenchDBs) {
  return {
    replicaRunner: new StatementRunner(dbs.replica),
    logRunner: new StatementRunner(dbs.log),
    upsertRow: dbs.replica.prepare(/*sql*/ `
      INSERT OR REPLACE INTO "bench_rows"
        ("id", "indexed", "payload", "${ZERO_VERSION_COLUMN_NAME}")
        VALUES (?, ?, ?, ?)
    `),
    insertLog: dbs.log.prepare(/*sql*/ `
      INSERT INTO "_zero.changeLogStream"
        ("watermark", "pos", "change", "precommit", "writeTimeMs")
        VALUES (?, ?, ?, ?, ?)
    `),
    updateWatermark: dbs.replica.prepare(/*sql*/ `
      UPDATE "_zero.replicationState"
        SET "stateVersion" = ?, "writeTimeMs" = ?
    `),
  };
}

function runCase(
  dbs: BenchDBs,
  c: BenchCase,
  purge?: PurgePolicy,
): WriteResult {
  if (c.sqliteTxRows !== c.logicalTxRows) {
    throw new Error(
      `sqliteTxRows (${c.sqliteTxRows}) must equal logicalTxRows (${c.logicalTxRows})`,
    );
  }

  const beginJSON = '{"tag":"begin"}';
  const commitJSON = '{"tag":"commit"}';
  const {replicaRunner, logRunner, upsertRow, insertLog, updateWatermark} =
    prepareStatements(dbs);
  const {separated} = dbs;
  const writesRows = c.mode !== 'log';
  const writesLog = c.mode !== 'apply';
  const commitLatencyMs: number[] = [];
  const logCommitLatencyMs: number[] = [];
  const replicaCommitLatencyMs: number[] = [];
  const transactionLatencyMs: number[] = [];
  const upstreamLoopStallMs: number[] = [];
  const purgeResults: PurgeResult[] = [];
  let payloadBytes = 0;
  let changeID = 1;

  const start = performance.now();
  for (let txIndex = 0; txIndex < c.totalTransactions; txIndex++) {
    const loopStart = performance.now();
    const watermark = versionToLexi(txIndex + 1);
    const writeTimeMs = simulatedWriteTimeMs(txIndex, c.totalTransactions);
    const isSchemaTransaction =
      c.schemaEveryTransactions !== undefined &&
      txIndex % c.schemaEveryTransactions === 0;
    const transactionStart = performance.now();
    try {
      // Matches ChangeProcessor's 'begin' handling: the replica transaction
      // opens first because it carries the indefinite SQLITE_BUSY retry for
      // litestream checkpoints. Only the commit order is load-bearing.
      replicaRunner.beginImmediate();
      if (separated) {
        logRunner.beginImmediate();
      }
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
            dbs.replica.exec(/*sql*/ `
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
          writeTimeMs,
        );
      }

      if (separated) {
        // Design 4.4 / plan slice 7D: the log commits first, then the replica's
        // state version, then the replica. A crash in between leaves a phantom,
        // never a hole.
        const logCommitStart = performance.now();
        logRunner.commit();
        const logCommitMs = performance.now() - logCommitStart;
        updateWatermark.run(watermark, writeTimeMs);
        const replicaCommitStart = performance.now();
        replicaRunner.commit();
        const replicaCommitMs = performance.now() - replicaCommitStart;
        logCommitLatencyMs.push(logCommitMs);
        replicaCommitLatencyMs.push(replicaCommitMs);
        // Time in COMMIT only, so this stays comparable with the single-commit
        // layouts; the interleaved state-version UPDATE is in the transaction
        // latency below.
        commitLatencyMs.push(logCommitMs + replicaCommitMs);
      } else {
        updateWatermark.run(watermark, writeTimeMs);
        const commitStart = performance.now();
        replicaRunner.commit();
        commitLatencyMs.push(performance.now() - commitStart);
      }
    } catch (e) {
      if (separated && dbs.log.inTransaction) {
        logRunner.rollback();
      }
      if (dbs.replica.inTransaction) {
        replicaRunner.rollback();
      }
      throw e;
    }
    transactionLatencyMs.push(performance.now() - transactionStart);

    if (purge !== undefined && writesLog) {
      purgeResults.push(
        purgeBatch(dbs.log, {
          externalFloor: watermark,
          stateVersionFloor: watermark,
          retentionCutoffMs:
            purge.kind === 'retention'
              ? writeTimeMs - purge.retentionMs
              : Number.MAX_SAFE_INTEGER,
          maxRows: purge.maxRows,
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
    logCommitLatencyMs,
    replicaCommitLatencyMs,
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

function verifyCase(dbs: BenchDBs, c: BenchCase, result: WriteResult) {
  const writesRows = c.mode !== 'log';
  const writesLog = c.mode !== 'apply';
  const schemaTransactions =
    c.schemaEveryTransactions === undefined
      ? 0
      : Math.ceil(c.totalTransactions / c.schemaEveryTransactions);
  const expectedRows = writesRows ? result.changes - schemaTransactions : 0;
  const expectedLogRows = writesLog
    ? 2 + result.changes + c.totalTransactions * 2 - result.purgedRows
    : 2;

  expect(
    dbs.replica
      .prepare(`SELECT count(*) AS n FROM "bench_rows"`)
      .get<{n: number}>().n,
  ).toBe(expectedRows);
  expect(countLogRows(dbs.log)).toBe(expectedLogRows);
  expect(
    dbs.replica
      .prepare(`SELECT "stateVersion" AS version FROM "_zero.replicationState"`)
      .get<{version: string}>().version,
  ).toBe(versionToLexi(c.totalTransactions));
  expect(
    dbs.replica
      .prepare(/*sql*/ `
        SELECT count(*) AS n
        FROM sqlite_schema
        WHERE type = 'table' AND name LIKE 'bench_schema_event_%'
      `)
      .get<{n: number}>().n,
  ).toBe(writesRows ? schemaTransactions : 0);
  expect(
    dbs.log
      .prepare(`SELECT max("watermark") AS head FROM "_zero.changeLogStream"`)
      .get<{head: string}>().head,
  ).toBe(writesLog ? versionToLexi(c.totalTransactions) : '00');
  assertCompleteTransactions(dbs.log);
}

function countLogRows(db: Database) {
  return db
    .prepare(`SELECT count(*) AS n FROM "_zero.changeLogStream"`)
    .get<{n: number}>().n;
}

/** The simulated-time span still held by the log, in ms. */
function retainedWindowMs(db: Database) {
  const {oldest, newest} = db
    .prepare(/*sql*/ `
      SELECT min("writeTimeMs") AS "oldest", max("writeTimeMs") AS "newest"
      FROM "_zero.changeLogStream"
      WHERE "writeTimeMs" IS NOT NULL
    `)
    .get<{oldest: number | null; newest: number | null}>();
  return oldest === null || newest === null ? 0 : newest - oldest;
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
    /** The confirmed backup watermark (design 3.2). */
    externalFloor: string;
    /**
     * The latest durable transaction, always preserved as a catchup boundary
     * (storer.ts:286-300). Passed in rather than read from
     * `_zero.replicationState`, which lives in the other file once separated.
     */
    stateVersionFloor: string;
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
      AND "watermark" < ?
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
      opts.stateVersionFloor,
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
  const durability =
    c.logDurability === undefined
      ? ''
      : ` logJournalMode=${c.logDurability.journalMode}` +
        ` logSynchronous=${c.logDurability.synchronous}`;
  return (
    `workload=${c.workload} mode=${c.mode} ` +
    `logicalTxRows=${c.logicalTxRows} sqliteTxRows=${c.sqliteTxRows}` +
    durability
  );
}

function fileMeasurements(
  samples: readonly {files: CaseFileMetrics}[],
  which: 'replica' | 'changeLog',
) {
  const metrics = samples.map(({files}) => files[which]);
  if (metrics.some(m => m === undefined)) {
    return undefined;
  }
  const present = metrics as readonly FileMetrics[];
  return {
    dbBytes: present.map(m => m.dbBytes),
    walBytes: present.map(m => m.walBytes),
    freelistBytes: present.map(m => m.freelistBytes),
  };
}

function recordWriteSamples(
  name: string,
  samples: readonly {write: WriteResult; files: CaseFileMetrics}[],
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
  const allLogCommits = samples.flatMap(({write}) => write.logCommitLatencyMs);
  const allReplicaCommits = samples.flatMap(
    ({write}) => write.replicaCommitLatencyMs,
  );
  const allTransactions = samples.flatMap(
    ({write}) => write.transactionLatencyMs,
  );
  const allUpstreamStalls = samples.flatMap(
    ({write}) => write.upstreamLoopStallMs,
  );
  recordLatency(`${name} commit latency`, allCommits);
  recordLatency(`${name} SQLite transaction latency`, allTransactions);
  recordLatency(`${name} upstream-loop SQLite stall`, allUpstreamStalls);
  if (allLogCommits.length > 0) {
    recordLatency(`${name} log commit latency`, allLogCommits);
    recordLatency(`${name} replica commit latency`, allReplicaCommits);
  }

  const replicaFiles = fileMeasurements(samples, 'replica');
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
      logCommitLatencyMs:
        allLogCommits.length > 0
          ? latencyPercentiles(allLogCommits)
          : undefined,
      replicaCommitLatencyMs:
        allReplicaCommits.length > 0
          ? latencyPercentiles(allReplicaCommits)
          : undefined,
      sqliteTransactionLatencyMs: latencyPercentiles(allTransactions),
      upstreamLoopSQLiteStallMs: latencyPercentiles(allUpstreamStalls),
      replicaFileBytes: replicaFiles,
      changeLogFileBytes: fileMeasurements(samples, 'changeLog'),
      // The headline win: what litestream ships and a restoring view-syncer
      // downloads is the replica file plus its WAL, and nothing else.
      litestreamBytes: samples.map(
        ({files}) => files.replica.dbBytes + files.replica.walBytes,
      ),
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
  purge?: PurgePolicy,
  testName = 'sqlite-change-log-ceiling-bench',
): {write: WriteResult; files: CaseFileMetrics} {
  const dbs = setupDBs(c, testName);
  try {
    const write = runCase(dbs, c, purge);
    verifyCase(dbs, c, write);
    return {write, files: caseFileMetrics(dbs)};
  } finally {
    dbs.close();
    runCleanup();
  }
}

function retentionSample(c: BenchCase): {
  write: WriteResult;
  files: CaseFileMetrics;
  retained: RetainedLog;
} {
  const dbs = setupDBs(c, 'sqlite-change-log-retention-bench');
  try {
    const write = runCase(dbs, c, {
      kind: 'retention',
      maxRows: PURGE_BATCH_ROWS,
      retentionMs: RETENTION_MS,
    });
    verifyCase(dbs, c, write);
    const retained = {
      rows: countLogRows(dbs.log),
      windowMs: retainedWindowMs(dbs.log),
    };
    // The run spans several windows, so aged-out stream must have been purged.
    expect(write.purgedRows).toBeGreaterThan(0);
    // And the purge must never have reached into the retention window: every
    // transaction at or after the final cutoff survives. The oldest survivor
    // lands within one simulated inter-transaction step of that cutoff, so the
    // retained span is (RETENTION_MS - step, RETENTION_MS] when the purge keeps
    // up, and larger when it falls behind. Only the lower bound is a guarantee;
    // the published `retainedWindowMs` shows whether it kept up.
    const stepMs = Math.ceil(SIMULATED_RUN_MS / c.totalTransactions);
    expect(retained.windowMs).toBeGreaterThan(RETENTION_MS - stepMs);
    return {write, files: caseFileMetrics(dbs), retained};
  } finally {
    dbs.close();
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

function goNoGoKey(c: BenchCase) {
  return c.logDurability === undefined
    ? `${c.workload}:${c.mode}`
    : `${c.workload}:${c.mode}:${c.logDurability.journalMode}:` +
        c.logDurability.synchronous;
}

function recordGoNoGoResult(c: BenchCase, samples: readonly WriteResult[]) {
  goNoGoResults.set(goNoGoKey(c), [...samples]);
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
    replica: {
      journalMode: REPLICA_JOURNAL_MODE,
      synchronous: REPLICA_SYNCHRONOUS,
      walAutocheckpoint: 0,
    },
    // Applies to the main sweep. The durability test overrides these, and every
    // result name carries the settings its case actually ran with.
    changeLog: {
      defaultJournalModes: enumListFromEnv(
        'SQLITE_CHANGE_LOG_LOG_JOURNAL_MODE',
        ['wal', 'wal2'] as const,
        DEFAULT_LOG_JOURNAL_MODES,
      ),
      defaultSynchronous: enumListFromEnv(
        'SQLITE_CHANGE_LOG_LOG_SYNCHRONOUS',
        ['NORMAL', 'FULL'] as const,
        DEFAULT_LOG_SYNCHRONOUS,
      ),
      walAutocheckpoint: 'default',
    },
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

/** The two layouts the separate-file design compares. */
function layoutCases(workload: Workload, cases = makeCases()) {
  return cases.filter(
    c =>
      c.workload === workload &&
      (c.mode === 'combined' || c.mode === 'separate-files'),
  );
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

  // Design 4.6 asserts wal2 + synchronous=NORMAL for the change-log DB. This
  // measures the alternatives so the default is a recorded decision with a
  // measured cost, per the slice 7A exit criteria.
  test('log durability trade', {timeout: TEST_TIMEOUT_MS}, () => {
    const cases = makeCases(
      logDurabilitiesFromEnv(SWEEP_LOG_JOURNAL_MODES, SWEEP_LOG_SYNCHRONOUS),
    ).filter(
      c => c.mode === 'separate-files' && c.workload === 'small-high-frequency',
    );
    for (const c of cases) {
      const samples = measuredSamples(() => writeSample(c));
      recordWriteSamples(
        `replicator/sqlite change-log durability ${caseName(c)}`,
        samples,
      );
    }
  });

  // The win: replica bytes under each layout once the log holds a full
  // retention window (design 3.2/3.3), which is what litestream ships and a
  // restoring view-syncer downloads.
  test('retention-window steady state', {timeout: TEST_TIMEOUT_MS}, () => {
    const cases = makeCases().filter(
      c =>
        // 'oversized' exists to exercise the batch-limit edge case, which the
        // idle-purge test covers directly.
        c.workload !== 'oversized' &&
        (c.mode === 'combined' || c.mode === 'separate-files'),
    );
    for (const c of cases) {
      const samples = measuredSamples(() => retentionSample(c));
      const name =
        'replicator/sqlite change-log retention window ' +
        `retentionMinutes=${RETENTION_MINUTES} ` +
        `windows=${RETENTION_WINDOWS} ${caseName(c)}`;
      recordWriteSamples(name, samples);

      const productivePurges = samples.flatMap(({write}) =>
        write.purgeResults.filter(({deletedRows}) => deletedRows > 0),
      );
      recordLatency(
        `${name} purge transaction latency`,
        productivePurges.map(({elapsedMs}) => elapsedMs),
      );
      publishedResults.push({
        name: `${name} retention`,
        measurements: {
          retainedRows: samples.map(({retained}) => retained.rows),
          retainedWindowMs: samples.map(({retained}) => retained.windowMs),
          purgedRows: samples.map(({write}) => write.purgedRows),
          purgeTransactions: productivePurges.length,
          purgeTransactionLatencyMs: latencyPercentiles(
            productivePurges.map(({elapsedMs}) => elapsedMs),
          ),
        },
      });
    }
  });

  test(
    'large catchup scan on a second connection',
    {timeout: TEST_TIMEOUT_MS},
    () => {
      for (const c of layoutCases('mixed-row-schema')) {
        const samples = measuredSamples(() => {
          const dbs = setupDBs(c, 'sqlite-change-log-catchup-bench');
          try {
            const write = runCase(dbs, c);
            verifyCase(dbs, c, write);
            const catchup = runCatchup(
              dbs.logPath,
              '00',
              versionToLexi(c.totalTransactions),
              READ_BATCH_ROWS,
            );
            expect(catchup.rows).toBe(write.changes + write.transactions * 2);
            return {catchup, files: caseFileMetrics(dbs)};
          } finally {
            dbs.close();
            runCleanup();
          }
        });

        const name =
          'replicator/sqlite change-log large catchup ' +
          `batchRows=${READ_BATCH_ROWS} ${caseName(c)}`;
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
            replicaFileBytes: fileMeasurements(samples, 'replica'),
            changeLogFileBytes: fileMeasurements(samples, 'changeLog'),
          },
        });
      }
    },
  );

  test(
    'small purge batches between commits',
    {timeout: TEST_TIMEOUT_MS},
    () => {
      for (const c of layoutCases('small-high-frequency')) {
        const samples = measuredSamples(() =>
          writeSample(c, {kind: 'drain', maxRows: BETWEEN_COMMIT_PURGE_ROWS}),
        );
        const name =
          'replicator/sqlite change-log between-commit purge ' +
          `batchRows=${BETWEEN_COMMIT_PURGE_ROWS} ${caseName(c)}`;
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
      }
    },
  );

  test(
    'idle purge drains oversized transactions',
    {timeout: TEST_TIMEOUT_MS},
    () => {
      for (const c of layoutCases('oversized')) {
        const samples = measuredSamples(() => {
          const dbs = setupDBs(c, 'sqlite-change-log-purge-bench');
          try {
            const write = runCase(dbs, c);
            verifyCase(dbs, c, write);
            const purges: PurgeResult[] = [];
            for (;;) {
              const result = purgeBatch(dbs.log, {
                externalFloor: versionToLexi(c.totalTransactions),
                stateVersionFloor: versionToLexi(c.totalTransactions),
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
            expect(countLogRows(dbs.log)).toBe(c.logicalTxRows + 2);
            assertCompleteTransactions(dbs.log);
            return {purges, files: caseFileMetrics(dbs)};
          } finally {
            dbs.close();
            runCleanup();
          }
        });

        const name =
          'replicator/sqlite change-log idle purge ' +
          `batchRows=${PURGE_BATCH_ROWS} ${caseName(c)}`;
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
            replicaFileBytes: fileMeasurements(samples, 'replica'),
            changeLogFileBytes: fileMeasurements(samples, 'changeLog'),
          },
        });
      }
    },
  );

  test.skipIf(litestreamExecutable === undefined)(
    'litestream v5/checkpoint pressure',
    {timeout: TEST_TIMEOUT_MS},
    async () => {
      if (litestreamExecutable === undefined) {
        return;
      }
      for (const c of layoutCases('small-high-frequency')) {
        const samples: {write: WriteResult; files: CaseFileMetrics}[] = [];
        for (let rep = 0; rep < WARMUP_REPS + REPS; rep++) {
          const dbs = setupDBs(c, 'sqlite-change-log-litestream-bench');
          const backupDir = mkdtempSync(
            join(tmpdir(), 'sqlite-change-log-litestream-'),
          );
          // Litestream replicates the replica only; the change-log file is
          // local by construction (design 4.1).
          const proc = spawn(
            litestreamExecutable,
            [
              'replicate',
              dbs.replicaPath,
              `file://${join(backupDir, 'replica')}`,
            ],
            {stdio: ['ignore', 'pipe', 'pipe']},
          );
          try {
            await waitForLitestream(proc);
            const write = runCase(dbs, c);
            verifyCase(dbs, c, write);
            if (rep >= WARMUP_REPS) {
              samples.push({write, files: caseFileMetrics(dbs)});
            }
          } finally {
            await stopLitestream(proc);
            dbs.close();
            runCleanup();
            rmSync(backupDir, {recursive: true, force: true});
          }
        }
        recordWriteSamples(
          'replicator/sqlite change-log litestream-v5 checkpoint pressure ' +
            caseName(c),
          samples,
        );
      }
    },
  );
});
