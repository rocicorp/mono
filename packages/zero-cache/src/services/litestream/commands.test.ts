import * as childProcess from 'node:child_process';
import {existsSync, mkdtempSync, statSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {LogContext} from '@rocicorp/logger';
import {afterEach, describe, expect, test, vi} from 'vitest';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import type {ZeroConfig} from '../../config/zero-config.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  BackupNotFoundException,
  getLastBackupTime,
  parseBackupCreatedTimes,
  restoreReplica,
} from './commands.ts';
import * as litestreamMetrics from './metrics.ts';

// Wrap (don't replace) node:child_process.spawn so tests can assert the args it
// was called with while it still spawns the fake litestream executable. Node's
// built-in module exports are non-configurable, so vi.spyOn cannot be used here.
vi.mock('node:child_process', async importOriginal => {
  const actual = await importOriginal<typeof childProcess>();
  return {...actual, spawn: vi.fn(actual.spawn)};
});

// Writes a fake `litestream` executable that emits `sh` (a POSIX shell
// snippet that can branch on `$1`, the litestream subcommand) and returns the
// config pointing at it.
function configWithFakeLitestream(
  sh: string,
  replicaFile?: string,
): ZeroConfig {
  const dir = mkdtempSync(join(tmpdir(), 'litestream-test-'));
  const executable = join(dir, 'fake-litestream');
  writeFileSync(executable, `#!/bin/sh\n${sh}\n`, {mode: 0o755});
  return {
    port: 4848,
    log: {format: 'text'},
    replica: {file: replicaFile ?? join(dir, 'replica.db')},
    litestream: {
      executable,
      backupURL: 's3://fake-bucket/backup',
      configPath: './src/services/litestream/config.yml',
      logLevel: 'warn',
      restoreUsingV5: false,
      checkpointThresholdMB: 40,
      incrementalBackupIntervalMinutes: 15,
      snapshotBackupIntervalHours: 12,
      multipartConcurrency: 48,
      multipartSize: 16 * 1024 * 1024,
      restoreParallelism: 48,
    },
  } as unknown as ZeroConfig;
}

function createRestorableReplica(file: string, watermark: string) {
  const db = new Database(createSilentLogContext(), file);
  try {
    initReplicationState(db, ['zero_pub'], watermark);
  } finally {
    db.close();
  }
}

describe('litestream/commands parseBackupCreatedTimes', () => {
  const lc = createSilentLogContext();

  test('parses the created column from snapshot output', () => {
    const output =
      `replica  generation        index  size     created\n` +
      `s3       1862f44967b3863f  0      4546445  2026-06-10T01:11:32Z\n`;
    expect(parseBackupCreatedTimes(lc, 'snapshots', output)).toEqual([
      new Date('2026-06-10T01:11:32Z'),
    ]);
  });

  test('parses the created column (last) from wal output with extra columns', () => {
    const output =
      `replica  generation        index  offset  size  created\n` +
      `s3       1862f44967b3863f  0      0       100   2026-06-10T01:11:32Z\n` +
      `s3       1862f44967b3863f  1      4096    200   2026-06-10T01:12:00Z\n`;
    expect(parseBackupCreatedTimes(lc, 'wal', output)).toEqual([
      new Date('2026-06-10T01:11:32Z'),
      new Date('2026-06-10T01:12:00Z'),
    ]);
  });

  test('skips the header, blank lines, and short lines', () => {
    const output =
      `\n` +
      `replica  generation        index  size     created\n` +
      `\n` +
      `   \n` +
      `s3       1862f44967b3863f  0      4546445  2026-06-10T01:11:32Z\n`;
    expect(parseBackupCreatedTimes(lc, 'snapshots', output)).toEqual([
      new Date('2026-06-10T01:11:32Z'),
    ]);
  });

  test('returns empty for empty or header-only output', () => {
    expect(parseBackupCreatedTimes(lc, 'snapshots', '')).toEqual([]);
    expect(
      parseBackupCreatedTimes(
        lc,
        'snapshots',
        `replica  generation  index  size  created\n`,
      ),
    ).toEqual([]);
  });

  test('warns and skips lines with an unparseable created time', () => {
    const sink = new TestLogSink();
    const lc = new LogContext('debug', undefined, sink);
    const output =
      `replica  generation        index  size     created\n` +
      `s3       1862f44967b3863f  0      4546445  not-a-date\n` +
      `s3       1862f44967b3863f  1      4546445  2026-06-10T01:12:00Z\n`;
    expect(parseBackupCreatedTimes(lc, 'snapshots', output)).toEqual([
      new Date('2026-06-10T01:12:00Z'),
    ]);
    expect(
      sink.messages.some(
        ([level, , args]) =>
          level === 'warn' &&
          String(args[0]).includes('unexpected line in litestream snapshots'),
      ),
    ).toBe(true);
  });
});

describe('litestream/commands getLastBackupTime', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  const lc = createSilentLogContext();

  test('returns the most recent created time across snapshots and wal', async () => {
    const config = configWithFakeLitestream(
      `if [ "$1" = "snapshots" ]; then\n` +
        `  echo "replica generation index size created"\n` +
        `  echo "s3 gen 0 100 2025-04-24T00:00:00Z"\n` +
        `else\n` +
        `  echo "replica generation index offset size created"\n` +
        `  echo "s3 gen 1 0 100 2025-04-24T00:05:00Z"\n` +
        `  echo "s3 gen 2 0 100 2025-04-24T00:10:00Z"\n` +
        `fi`,
    );
    expect(await getLastBackupTime(lc, config)).toEqual(
      new Date('2025-04-24T00:10:00Z'),
    );
  });

  test('rejects when nothing is listed at the destination', async () => {
    const config = configWithFakeLitestream(`exit 0`);
    await expect(getLastBackupTime(lc, config)).rejects.toThrow(
      /no snapshots or WAL segments listed/,
    );
  });

  test('rejects when the litestream process exits non-zero', async () => {
    const config = configWithFakeLitestream(`exit 1`);
    await expect(getLastBackupTime(lc, config)).rejects.toThrow(
      /litestream (snapshots|wal) exited with code 1/,
    );
  });

  test('rejects (and kills the process) when listing times out', async () => {
    vi.useFakeTimers();
    const config = configWithFakeLitestream(`sleep 30`);
    const result = getLastBackupTime(lc, config);
    // Surface the rejection synchronously so the unhandled-rejection guard
    // does not fire before we await it below.
    const settled = result.then(
      v => ({ok: true as const, v}),
      e => ({ok: false as const, e}),
    );
    await vi.advanceTimersByTimeAsync(30_000);
    const outcome = await settled;
    expect(outcome.ok).toBe(false);
    expect(String(outcome.ok === false && outcome.e)).toMatch(
      /timed out listing backup state/,
    );
  });
});

describe('litestream/commands restoreReplica', () => {
  const lc = createSilentLogContext();

  afterEach(() => {
    vi.restoreAllMocks();
  });

  test('restores and validates a compatible replica', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'litestream-restore-test-'));
    const source = join(dir, 'source.db');
    const replica = join(dir, 'replica.db');
    createRestorableReplica(source, '01');
    const restoredDbBytesAdd = vi.fn();
    vi.spyOn(litestreamMetrics, 'litestreamRestoredDbBytes').mockReturnValue({
      add: restoredDbBytesAdd,
    } as unknown as ReturnType<
      typeof litestreamMetrics.litestreamRestoredDbBytes
    >);
    const config = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` +
        `  cp "${source}" "$6"\n` +
        `  exit 0\n` +
        `fi\n` +
        `exit 1`,
      replica,
    );

    await restoreReplica(lc, config, {
      replicaVersion: '01',
      minWatermark: '01',
    });

    expect(existsSync(replica)).toBe(true);
    expect(restoredDbBytesAdd).toHaveBeenCalledWith(
      statSync(replica).size,
      expect.objectContaining({result: 'success'}),
    );
  });

  test('does not record restored bytes when reusing an existing replica', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'litestream-restore-test-'));
    const replica = join(dir, 'replica.db');
    createRestorableReplica(replica, '01');
    const restoredDbBytesAdd = vi.fn();
    vi.spyOn(litestreamMetrics, 'litestreamRestoredDbBytes').mockReturnValue({
      add: restoredDbBytesAdd,
    } as unknown as ReturnType<
      typeof litestreamMetrics.litestreamRestoredDbBytes
    >);
    const config = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` + `  exit 0\n` + `fi\n` + `exit 1`,
      replica,
    );

    await restoreReplica(lc, config, {
      replicaVersion: '01',
      minWatermark: '01',
    });

    expect(restoredDbBytesAdd).not.toHaveBeenCalled();
  });

  test('reports a missing backup when restore exits without a replica', async () => {
    const restoreAttemptsAdd = vi.fn();
    vi.spyOn(litestreamMetrics, 'litestreamRestoreAttempts').mockReturnValue({
      add: restoreAttemptsAdd,
    } as unknown as ReturnType<
      typeof litestreamMetrics.litestreamRestoreAttempts
    >);
    const config = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` + `  exit 0\n` + `fi\n` + `exit 1`,
    );

    await expect(
      restoreReplica(lc, config, {
        replicaVersion: '01',
        minWatermark: '01',
      }),
    ).rejects.toThrow(BackupNotFoundException);
    expect(restoreAttemptsAdd).toHaveBeenCalledWith(
      1,
      expect.objectContaining({result: 'no_backup'}),
    );
  });

  test('deletes an incompatible restored replica', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'litestream-restore-test-'));
    const source = join(dir, 'source.db');
    const replica = join(dir, 'replica.db');
    createRestorableReplica(source, '01');
    const restoreRunsAdd = vi.fn();
    const restoreValidationRecordMs = vi.fn();
    vi.spyOn(litestreamMetrics, 'litestreamRestoreRuns').mockReturnValue({
      add: restoreRunsAdd,
    } as unknown as ReturnType<typeof litestreamMetrics.litestreamRestoreRuns>);
    vi.spyOn(
      litestreamMetrics,
      'litestreamRestoreValidationDuration',
    ).mockReturnValue({
      recordMs: restoreValidationRecordMs,
    } as unknown as ReturnType<
      typeof litestreamMetrics.litestreamRestoreValidationDuration
    >);
    const config = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` +
        `  cp "${source}" "$6"\n` +
        `  exit 0\n` +
        `fi\n` +
        `exit 1`,
      replica,
    );

    await expect(
      restoreReplica(lc, config, {
        replicaVersion: '02',
        minWatermark: '02',
      }),
    ).rejects.toThrow(BackupNotFoundException);

    expect(existsSync(replica)).toBe(false);
    expect(restoreValidationRecordMs).toHaveBeenCalledWith(
      expect.any(Number),
      expect.objectContaining({result: 'invalid_replica'}),
    );
    expect(restoreRunsAdd).toHaveBeenCalledWith(
      1,
      expect.objectContaining({result: 'invalid_replica'}),
    );
  });

  // INC-961: the mechanism that actually prevents the paging is spawning
  // `litestream restore` with piped (captured) stdio rather than inheriting the
  // pod's streams — with `stdio: 'inherit'` litestream's own ERROR would reach
  // the pod's stdout directly, before our retry logic could downgrade it.
  // Assert the spawn options so a regression back to inheriting is caught.
  test('spawns `litestream restore` with piped stdio (not inherited)', async () => {
    const spawnMock = vi.mocked(childProcess.spawn);
    spawnMock.mockClear();
    const dir = mkdtempSync(join(tmpdir(), 'litestream-restore-test-'));
    const source = join(dir, 'source.db');
    const replica = join(dir, 'replica.db');
    createRestorableReplica(source, '01');
    const config = configWithFakeLitestream(
      `if [ "$1" = "restore" ]; then\n` +
        `  cp "${source}" "$6"\n` +
        `  exit 0\n` +
        `fi\n` +
        `exit 1`,
      replica,
    );

    await restoreReplica(lc, config, {
      replicaVersion: '01',
      minWatermark: '01',
    });

    const restoreCall = spawnMock.mock.calls.find(
      call => Array.isArray(call[1]) && call[1][0] === 'restore',
    );
    expect(restoreCall).toBeDefined();
    // stdio is [stdin, stdout, stderr]; stdout/stderr must be piped so
    // litestream's output is captured rather than sent to the pod's stdout.
    expect(restoreCall?.[2]?.stdio).toEqual(['ignore', 'pipe', 'pipe']);
  });
});
