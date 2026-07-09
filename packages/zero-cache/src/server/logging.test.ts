import {mkdtempSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {LogContext} from '@rocicorp/logger';
import {describe, expect, test} from 'vitest';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {registerSQLiteCorruptionDiagnosticTarget} from '../db/sqlite-corruption.ts';
import {logUncaughtException} from './logging.ts';

describe('server/logging', () => {
  test('logs last-chance SQLite corruption diagnostics before flushing', async () => {
    const dbPath = createSQLiteDB();
    const sink = new TestLogSink();
    const lc = new LogContext('debug', undefined, sink);
    const unregister = registerSQLiteCorruptionDiagnosticTarget({
      debugName: 'test-replica',
      dbPath,
    });

    try {
      await logUncaughtException(
        lc,
        sink,
        Object.assign(new Error('database disk image is malformed'), {
          code: 'SQLITE_CORRUPT',
        }),
        'uncaughtException',
      );
    } finally {
      unregister();
    }

    const serializedLogs = JSON.stringify(sink.messages);
    expect(serializedLogs).toContain('uncaughtException');
    expect(serializedLogs).toContain('SQLite replica corruption detected');
    expect(serializedLogs).toContain('file-stats');
    expect(serializedLogs).toContain('quick-check');
    expect(serializedLogs).toContain('integrity-check');
    expect(sink.flushCallCount).toBe(1);
  });

  test('skips last-chance diagnostics for non-corruption errors', async () => {
    const dbPath = createSQLiteDB();
    const sink = new TestLogSink();
    const lc = new LogContext('debug', undefined, sink);
    const unregister = registerSQLiteCorruptionDiagnosticTarget({
      debugName: 'test-replica',
      dbPath,
    });

    try {
      await logUncaughtException(
        lc,
        sink,
        new Error('boom'),
        'uncaughtException',
      );
    } finally {
      unregister();
    }

    const serializedLogs = JSON.stringify(sink.messages);
    expect(serializedLogs).toContain('uncaughtException');
    expect(serializedLogs).not.toContain('SQLite replica corruption detected');
    expect(serializedLogs).not.toContain('file-stats');
    expect(sink.flushCallCount).toBe(1);
  });
});

function createSQLiteDB() {
  const dir = mkdtempSync(join(tmpdir(), 'zero-cache-logging-test-'));
  const dbPath = join(dir, 'replica.db');
  const db = new Database(createSilentLogContext(), dbPath);
  try {
    db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)');
  } finally {
    db.close();
  }
  return dbPath;
}
