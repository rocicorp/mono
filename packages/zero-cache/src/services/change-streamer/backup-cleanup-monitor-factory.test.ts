import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import {Subscription} from '../../types/subscription.ts';
import {createBackupCleanupMonitor} from './backup-cleanup-monitor-factory.ts';
import type {ChangeStreamerService} from './change-streamer.ts';
import {Litestream3BackupMonitor} from './litestream3-backup-monitor.ts';
import {VfsBackupMonitor} from './vfs-backup-monitor.ts';

const changeStreamer = {
  id: 'change-streamer',
  run: () => Promise.resolve(),
  stop: () => Promise.resolve(),
  subscribe: () => Promise.resolve(Subscription.create<string>()),
  scheduleCleanup: vi.fn(),
  getChangeLogState: () =>
    Promise.resolve({
      replicaVersion: 'replica-version',
      minWatermark: 'min-watermark',
    }),
  getLastConsumedWatermark: () => null,
  onBackupWatermark: vi.fn(),
} satisfies ChangeStreamerService;

function configWithLitestream(
  litestream: Partial<NormalizedZeroConfig['litestream']>,
): NormalizedZeroConfig {
  return {
    litestream: {
      backupURL: undefined,
      port: 9090,
      backupUsingV5: false,
      restoreUsingV5: false,
      vfsProbeIntervalMs: 30_000,
      vfsProbeTimeoutMs: 30_000,
      ...litestream,
    },
  } as unknown as NormalizedZeroConfig;
}

describe('createBackupCleanupMonitor', () => {
  test('returns null when backup is not configured', () => {
    const monitor = createBackupCleanupMonitor({
      lc: createSilentLogContext(),
      config: configWithLitestream({backupURL: undefined}),
      replicaFile: '/tmp/replica.db',
      changeStreamer,
      initialCleanupDelayMs: 0,
    });

    expect(monitor).toBeNull();
  });

  test('creates the v3 backup monitor when backup is configured', async () => {
    const monitor = createBackupCleanupMonitor({
      lc: createSilentLogContext(),
      config: configWithLitestream({backupURL: 's3://bucket/prefix'}),
      replicaFile: '/tmp/replica.db',
      changeStreamer,
      initialCleanupDelayMs: 0,
      verifyBackupState: vi.fn().mockResolvedValue(new Date()),
    });

    expect(monitor).toBeInstanceOf(Litestream3BackupMonitor);
    expect(monitor?.id).toBe('backup-monitor');
    await monitor?.stop();
  });

  test('creates the v5 backup monitor when v5 backup is enabled', async () => {
    const monitor = createBackupCleanupMonitor({
      lc: createSilentLogContext(),
      config: configWithLitestream({
        backupURL: 's3://bucket/prefix',
        backupUsingV5: true,
        restoreUsingV5: true,
      }),
      replicaFile: '/tmp/replica.db',
      changeStreamer,
      initialCleanupDelayMs: 0,
      vfsBackupWatermarkSource: {
        readWatermark: vi.fn(),
      },
    });

    expect(monitor).toBeInstanceOf(VfsBackupMonitor);
    expect(monitor?.id).toBe('vfs-backup-monitor');
    await monitor?.stop();
  });
});
