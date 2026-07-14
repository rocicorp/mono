import {mkdtempSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {LogContext} from '@rocicorp/logger';
import {describe, expect, test} from 'vitest';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {initReplicationState} from '../services/replicator/schema/replication-state.ts';
import {
  isSQLiteCorruption,
  logSQLiteCorruptionDiagnostics,
} from './sqlite-corruption.ts';

describe('sqlite-corruption', () => {
  test('detects sqlite corruption errors through causes', () => {
    const sqliteError = Object.assign(
      new Error('database disk image is malformed'),
      {
        code: 'SQLITE_CORRUPT',
      },
    );
    const wrapped = new Error('wrapped', {cause: sqliteError});

    expect(isSQLiteCorruption(wrapped)).toBe(true);
    expect(
      isSQLiteCorruption(
        Object.assign(new Error('file is not a database'), {
          code: 'SQLITE_NOTADB',
        }),
      ),
    ).toBe(true);
    expect(isSQLiteCorruption(new Error('SQLITE_BUSY'))).toBe(false);
  });

  test('logs in-place diagnostics without row data', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlite-corruption-test-'));
    const dbPath = join(dir, 'replica.db');
    const db = new Database(createSilentLogContext(), dbPath);
    try {
      db.exec(`
        CREATE TABLE "_zero.versionHistory" (
          dataVersion INTEGER NOT NULL,
          schemaVersion INTEGER NOT NULL,
          minSafeVersion INTEGER NOT NULL,
          lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
        );
      `);
      db.prepare(
        `INSERT INTO "_zero.versionHistory" (dataVersion, schemaVersion, minSafeVersion)
         VALUES (1, 1, 1)`,
      ).run();
      initReplicationState(db, ['zero_pub'], '01');
      db.exec(`CREATE TABLE customer_data (secret TEXT)`);
      db.prepare(`INSERT INTO customer_data (secret) VALUES (?)`).run(
        'do-not-log',
      );
    } finally {
      db.close();
    }

    const sink = new TestLogSink();
    const lc = new LogContext('debug', undefined, sink);
    logSQLiteCorruptionDiagnostics(
      lc,
      'test-replica',
      dbPath,
      Object.assign(new Error('database disk image is malformed'), {
        code: 'SQLITE_CORRUPT',
      }),
    );

    const serializedLogs = JSON.stringify(sink.messages);
    expect(serializedLogs).toContain('SQLite corruption diagnostic');
    expect(serializedLogs).toContain('quick-check');
    expect(serializedLogs).toContain('integrity-check');
    expect(serializedLogs).toContain('"status":"ok"');
    expect(serializedLogs).toContain('"watermark":"01"');
    expect(serializedLogs).not.toContain('do-not-log');
  });

  test('logs file stats when readonly open fails', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sqlite-corruption-test-'));
    const dbPath = join(dir, 'replica.db');
    writeFileSync(dbPath, 'not sqlite');

    const sink = new TestLogSink();
    const lc = new LogContext('debug', undefined, sink);

    expect(() =>
      logSQLiteCorruptionDiagnostics(
        lc,
        'bad-replica',
        dbPath,
        new Error('file is not a database'),
      ),
    ).not.toThrow();

    const serializedLogs = JSON.stringify(sink.messages);
    expect(serializedLogs).toContain('file-stats');
    expect(serializedLogs).toContain('file is not a database');
  });
});
