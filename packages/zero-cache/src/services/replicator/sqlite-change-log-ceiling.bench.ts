// Benchmarks the SQLite write ceiling for the replication-manager shape:
// applying rows to the backup replica and appending the raw change stream to a
// SQLite-local change log.

import {existsSync, statSync} from 'node:fs';
import {afterEach, describe, expect, test} from 'vitest';
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

type BenchCase = {
  readonly mode: WriteMode;
  readonly payloadBytes: number;
  readonly logicalTxRows: number;
  readonly sqliteTxRows: number;
  readonly totalChanges: number;
};

type Sample = {
  readonly elapsedMs: number;
  readonly changes: number;
  readonly payloadMB: number;
  readonly sqliteMB: number;
};

const DEFAULT_PAYLOAD_BYTES = [128, 1024, 4096, 16_384];
const DEFAULT_LOGICAL_TX_ROWS = [1, 100];
const DEFAULT_SQLITE_TX_ROWS = [1, 1000, 10_000];
const DEFAULT_MODES: WriteMode[] = ['combined'];

const BYTES_PER_MB = 1_000_000;
const TEST_TIMEOUT_MS = 3_600_000;

const WARMUP_REPS = nonNegativeIntegerFromEnv(
  'SQLITE_CHANGE_LOG_WARMUP_REPS',
  1,
);
const REPS = integerFromEnv('SQLITE_CHANGE_LOG_REPS', 5);
const TARGET_PAYLOAD_MB = integerFromEnv(
  'SQLITE_CHANGE_LOG_TARGET_PAYLOAD_MB',
  64,
);
const MIN_CHANGES = integerFromEnv('SQLITE_CHANGE_LOG_MIN_CHANGES', 1000);
const MAX_CHANGES = integerFromEnv('SQLITE_CHANGE_LOG_MAX_CHANGES', 100_000);

const lc = createSilentLogContext();
const benchmarkRecorder = createManualBenchmarkRecorder();

let cleanup: (() => void)[] = [];

afterEach(() => {
  runCleanup();
});

function runCleanup() {
  for (const fn of cleanup.reverse()) {
    fn();
  }
  cleanup = [];
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

function integerListFromEnv(name: string, fallback: readonly number[]) {
  const raw = process.env[name];
  if (!raw) {
    return [...fallback];
  }

  const values = raw
    .split(',')
    .map(part => part.trim())
    .filter(part => part.length > 0)
    .map(part => {
      const value = Number(part);
      if (!Number.isSafeInteger(value) || value <= 0) {
        throw new Error(`${name} must contain positive safe integers: ${raw}`);
      }
      return value;
    });

  if (values.length === 0) {
    throw new Error(`${name} must contain at least one value`);
  }
  const uniqueValues = [...new Set(values)];
  return uniqueValues.toSorted((a, b) => a - b);
}

function writeModesFromEnv() {
  const raw = process.env.SQLITE_CHANGE_LOG_MODES;
  if (!raw) {
    return DEFAULT_MODES;
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
  return [...new Set(modes)];
}

function roundUpToMultiple(value: number, multiple: number) {
  return Math.ceil(value / multiple) * multiple;
}

function normalizedSqliteTxRows(sqliteTxRows: number, logicalTxRows: number) {
  return roundUpToMultiple(
    Math.max(sqliteTxRows, logicalTxRows),
    logicalTxRows,
  );
}

function totalChanges(payloadBytes: number, logicalTxRows: number) {
  const targetChanges = Math.floor(
    (TARGET_PAYLOAD_MB * BYTES_PER_MB) / payloadBytes,
  );
  const bounded = Math.min(Math.max(targetChanges, MIN_CHANGES), MAX_CHANGES);
  return roundUpToMultiple(bounded, logicalTxRows);
}

function makeCases(): BenchCase[] {
  const payloadBytes = integerListFromEnv(
    'SQLITE_CHANGE_LOG_PAYLOAD_BYTES',
    DEFAULT_PAYLOAD_BYTES,
  );
  const logicalTxRows = integerListFromEnv(
    'SQLITE_CHANGE_LOG_LOGICAL_TX_ROWS',
    DEFAULT_LOGICAL_TX_ROWS,
  );
  const sqliteTxRows = integerListFromEnv(
    'SQLITE_CHANGE_LOG_SQLITE_TX_ROWS',
    DEFAULT_SQLITE_TX_ROWS,
  );
  const modes = writeModesFromEnv();

  const cases: BenchCase[] = [];
  for (const mode of modes) {
    for (const payloadByteCount of payloadBytes) {
      for (const logicalRows of logicalTxRows) {
        for (const sqliteRows of sqliteTxRows) {
          cases.push({
            mode,
            payloadBytes: payloadByteCount,
            logicalTxRows: logicalRows,
            sqliteTxRows: normalizedSqliteTxRows(sqliteRows, logicalRows),
            totalChanges: totalChanges(payloadByteCount, logicalRows),
          });
        }
      }
    }
  }

  return uniqueCases(cases);
}

function uniqueCases(cases: readonly BenchCase[]) {
  const unique = new Map<string, BenchCase>();
  for (const c of cases) {
    unique.set(caseName(c), c);
  }
  return [...unique.values()];
}

function makePayload(bytes: number) {
  const chunk = 'abcdefghijklmnopqrstuvwxyz0123456789';
  return chunk.repeat(Math.ceil(bytes / chunk.length)).slice(0, bytes);
}

function changeJSON(payload: string) {
  return JSON.stringify({
    tag: 'insert',
    relation: {
      schema: 'public',
      name: 'bench_rows',
      rowKey: {columns: ['id'], type: 'default'},
    },
    new: {
      id: 0,
      indexed: 0,
      payload,
    },
  });
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

    CREATE TABLE "_zero.changeLog" (
      "watermark" TEXT NOT NULL,
      "pos" INTEGER NOT NULL,
      "change" TEXT NOT NULL,
      "precommit" TEXT,
      PRIMARY KEY ("watermark", "pos")
    );

    CREATE TABLE "_zero.replicationState" (
      "stateVersion" TEXT NOT NULL,
      "writeTimeMs" INTEGER,
      "lock" INTEGER PRIMARY KEY DEFAULT 1 CHECK ("lock" = 1)
    );

    INSERT INTO "_zero.replicationState" ("stateVersion", "writeTimeMs")
      VALUES ('00', unixepoch('subsec') * 1000);
  `);

  return db;
}

function fileSize(path: string) {
  return existsSync(path) ? statSync(path).size : 0;
}

function dbAndWalBytes(path: string) {
  return {
    dbBytes: fileSize(path),
    walBytes: fileSize(`${path}-wal`),
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
      INSERT INTO "_zero.changeLog"
        ("watermark", "pos", "change", "precommit")
        VALUES (?, ?, ?, ?)
    `),
    updateWatermark: db.prepare(/*sql*/ `
      UPDATE "_zero.replicationState"
        SET "stateVersion" = ?, "writeTimeMs" = unixepoch('subsec') * 1000
    `),
  };
}

function runCase(db: Database, c: BenchCase) {
  const payload = makePayload(c.payloadBytes);
  const dataChangeJSON = changeJSON(payload);
  const beginJSON = '{"tag":"begin"}';
  const commitJSON = '{"tag":"commit"}';
  const logicalTxCount = c.totalChanges / c.logicalTxRows;
  const watermarks = Array.from({length: logicalTxCount}, (_, i) =>
    versionToLexi(i + 1),
  );
  const {runner, upsertRow, insertLog, updateWatermark} = prepareStatements(db);
  const writesRows = c.mode === 'apply' || c.mode === 'combined';
  const writesLog = c.mode === 'log' || c.mode === 'combined';

  const start = performance.now();
  let nextChangeID = 1;
  while (nextChangeID <= c.totalChanges) {
    const txEnd = Math.min(c.totalChanges, nextChangeID + c.sqliteTxRows - 1);
    const lastLogicalTxIndex = Math.floor((txEnd - 1) / c.logicalTxRows);
    try {
      runner.beginImmediate();
      for (let changeID = nextChangeID; changeID <= txEnd; changeID++) {
        const logicalTxIndex = Math.floor((changeID - 1) / c.logicalTxRows);
        const posInLogicalTx = (changeID - 1) % c.logicalTxRows;
        const watermark = watermarks[logicalTxIndex];

        if (writesLog && posInLogicalTx === 0) {
          insertLogRow(insertLog, watermark, 0, beginJSON, null);
        }

        if (writesRows) {
          upsertRow.run(changeID, changeID & 1023, payload, watermark);
        }
        if (writesLog) {
          insertLogRow(
            insertLog,
            watermark,
            posInLogicalTx + 1,
            dataChangeJSON,
            null,
          );
        }

        if (writesLog && posInLogicalTx === c.logicalTxRows - 1) {
          insertLogRow(
            insertLog,
            watermark,
            c.logicalTxRows + 1,
            commitJSON,
            watermark,
          );
        }
      }

      updateWatermark.run(watermarks[lastLogicalTxIndex]);
      runner.commit();
    } catch (e) {
      if (db.inTransaction) {
        runner.rollback();
      }
      throw e;
    }
    nextChangeID = txEnd + 1;
  }

  return performance.now() - start;
}

function insertLogRow(
  stmt: Statement,
  watermark: string,
  pos: number,
  change: string,
  precommit: string | null,
) {
  stmt.run(watermark, pos, change, precommit);
}

function verifyCase(db: Database, c: BenchCase) {
  const writesRows = c.mode === 'apply' || c.mode === 'combined';
  const writesLog = c.mode === 'log' || c.mode === 'combined';
  const logicalTxCount = c.totalChanges / c.logicalTxRows;
  const expectedLogRows = writesLog ? c.totalChanges + logicalTxCount * 2 : 0;
  const expectedRows = writesRows ? c.totalChanges : 0;

  expect(
    db.prepare(`SELECT count(*) AS n FROM "bench_rows"`).get<{n: number}>().n,
  ).toBe(expectedRows);
  expect(
    db.prepare(`SELECT count(*) AS n FROM "_zero.changeLog"`).get<{n: number}>()
      .n,
  ).toBe(expectedLogRows);
  expect(
    db
      .prepare(`SELECT "stateVersion" AS version FROM "_zero.replicationState"`)
      .get<{version: string}>().version,
  ).toBe(versionToLexi(logicalTxCount));
}

function runSample(c: BenchCase): Sample {
  const dbFile = new DbFile('sqlite-change-log-ceiling-bench');
  cleanup.push(() => dbFile.delete());
  const db = setupDB(dbFile);
  try {
    const before = dbAndWalBytes(dbFile.path);
    const elapsedMs = runCase(db, c);
    verifyCase(db, c);
    const after = dbAndWalBytes(dbFile.path);
    return {
      elapsedMs,
      changes: c.totalChanges,
      payloadMB: (c.totalChanges * c.payloadBytes) / BYTES_PER_MB,
      sqliteMB:
        (after.dbBytes - before.dbBytes + after.walBytes - before.walBytes) /
        BYTES_PER_MB,
    };
  } finally {
    db.close();
    runCleanup();
  }
}

function caseName(c: BenchCase) {
  return (
    `mode=${c.mode} payloadBytes=${c.payloadBytes} ` +
    `logicalTxRows=${c.logicalTxRows} sqliteTxRows=${c.sqliteTxRows}`
  );
}

function recordCase(c: BenchCase, samples: readonly Sample[]) {
  const name = `replicator/sqlite change-log ceiling ${caseName(c)}`;
  benchmarkRecorder.recordThroughputSamples(
    `${name} changes`,
    samples.map(({elapsedMs, changes}) => ({
      elapsedMs,
      operations: changes,
    })),
  );
  benchmarkRecorder.recordThroughputSamples(
    `${name} payload MB`,
    samples.map(({elapsedMs, payloadMB}) => ({
      elapsedMs,
      operations: payloadMB,
    })),
  );
  benchmarkRecorder.recordThroughputSamples(
    `${name} sqlite file MB`,
    samples.map(({elapsedMs, sqliteMB}) => ({
      elapsedMs,
      operations: sqliteMB,
    })),
  );
}

describe('replicator/sqlite change-log ceiling', () => {
  test('parameter sweep', {timeout: TEST_TIMEOUT_MS}, () => {
    for (const c of makeCases()) {
      const samples: Sample[] = [];
      for (let rep = 0; rep < WARMUP_REPS + REPS; rep++) {
        const sample = runSample(c);
        if (rep >= WARMUP_REPS) {
          samples.push(sample);
        }
      }
      recordCase(c, samples);
    }
  });
});
