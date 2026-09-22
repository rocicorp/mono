import {LogContext} from '@rocicorp/logger';
import {afterEach, describe, expect, test, vi} from 'vitest';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../../../shared/src/logging-test-utils.ts';
import type {LitestreamConfig} from '../../../config/normalize.ts';
import * as LitestreamCommands from '../../litestream/commands.ts';
import {restoreReplica} from './replica-restore.ts';

vi.mock('../../litestream/commands.ts', async importOriginal => {
  const actual = await importOriginal<typeof LitestreamCommands>();
  return {...actual, tryRestore: vi.fn()};
});

const config = {
  backupURL: 's3://backup-bucket/replica',
  executable: 'litestream',
  multipartConcurrency: 48,
  multipartSize: 16 * 1024 * 1024,
} as LitestreamConfig;

const lc = createSilentLogContext();

afterEach(() => {
  vi.clearAllMocks();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe('restoreReplica', () => {
  test('retries a failed restore before using a successful restore', async () => {
    vi.useFakeTimers();
    vi.mocked(LitestreamCommands.tryRestore)
      .mockRejectedValueOnce(
        new Error('NoSuchKey: the specified key does not exist'),
      )
      .mockResolvedValueOnce({
        restored: true,
        backupURL: config.backupURL,
        result: 'success',
      });

    const restored = restoreReplica(lc, config, '/tmp/replica.db', undefined);
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(5_000);
    await restored;

    expect(LitestreamCommands.tryRestore).toHaveBeenCalledTimes(2);
  });

  test('logs captured subprocess diagnostics while retrying', async () => {
    vi.useFakeTimers();
    const sink = new TestLogSink();
    const retryLog = new LogContext('debug', undefined, sink);
    const diagnostic =
      'litestream exited with code 1\nrequest failed: 503 ServiceUnavailable';
    const diagnosticError = new Error(diagnostic);
    vi.mocked(LitestreamCommands.tryRestore)
      .mockRejectedValueOnce(diagnosticError)
      .mockResolvedValueOnce({
        restored: true,
        backupURL: config.backupURL,
        result: 'success',
      });

    const restored = restoreReplica(
      retryLog,
      config,
      '/tmp/replica.db',
      undefined,
    );
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(5_000);
    await restored;

    const retry = sink.messages.find(
      ([level, , args]) =>
        level === 'warn' && String(args[0]).includes('restore attempt 1'),
    );
    expect(retry?.[2][1]).toBe(diagnosticError);
  });

  test('makes at most three total attempts for any restore failure', async () => {
    vi.useFakeTimers();
    vi.mocked(LitestreamCommands.tryRestore).mockRejectedValue(
      new Error('litestream exited with code 1'),
    );

    const restored = restoreReplica(lc, config, '/tmp/replica.db', undefined);
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(10_000);
    await restored;

    expect(LitestreamCommands.tryRestore).toHaveBeenCalledTimes(3);
  });

  test('does not retry the ordinary first-run no-backup result', async () => {
    vi.mocked(LitestreamCommands.tryRestore).mockResolvedValue({
      restored: false,
      backupURL: config.backupURL,
      result: 'no_backup',
    });

    await restoreReplica(lc, config, '/tmp/replica.db', undefined);

    expect(LitestreamCommands.tryRestore).toHaveBeenCalledTimes(1);
  });

  test('immediately falls back for invalid replica data', async () => {
    vi.mocked(LitestreamCommands.tryRestore).mockResolvedValue({
      restored: false,
      backupURL: config.backupURL,
      result: 'invalid_replica',
    });

    await restoreReplica(lc, config, '/tmp/replica.db', undefined);

    expect(LitestreamCommands.tryRestore).toHaveBeenCalledTimes(1);
  });
});
