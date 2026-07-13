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
import {
  CompositeLogSink,
  createLogContext,
  logUncaughtException,
} from './logging.ts';

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

  test('flushes every sink in a composite', async () => {
    const first = new TestLogSink();
    const second = new TestLogSink();

    await new CompositeLogSink([first, second]).flush();

    expect(first.flushCallCount).toBe(1);
    expect(second.flushCallCount).toBe(1);
  });

  test('replaces the bootstrap uncaught-exception handler', () => {
    const existingHandlers = new Set(process.listeners('uncaughtException'));

    try {
      createLogContext(
        {log: {level: 'error', format: 'json'}},
        'test-worker',
        0,
        false,
      );
      const bootstrapHandler = process
        .listeners('uncaughtException')
        .find(handler => !existingHandlers.has(handler));
      expect(bootstrapHandler).toBeDefined();

      createLogContext({log: {level: 'error', format: 'json'}}, 'test-worker');

      expect(process.listeners('uncaughtException')).not.toContain(
        bootstrapHandler,
      );
    } finally {
      for (const handler of process.listeners('uncaughtException')) {
        if (!existingHandlers.has(handler)) {
          process.off('uncaughtException', handler);
        }
      }
    }
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
