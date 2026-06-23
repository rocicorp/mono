import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../../shared/src/logging-test-utils.ts';
import type {Subscription} from '../../types/subscription.ts';
import type {VfsBackupWatermark} from '../litestream/vfs-watermark-reader.ts';
import type {ChangeStreamerService} from './change-streamer.ts';
import type {SnapshotMessage} from './snapshot.ts';
import {
  VfsBackupMonitor,
  type VfsBackupWatermarkSource,
} from './vfs-backup-monitor.ts';

describe('change-streamer/vfs-backup-monitor', () => {
  const scheduled: string[] = [];
  let consumedWatermark: string | null = null;
  const changeStreamer = {
    scheduleCleanup: (watermark: string) => scheduled.push(watermark),
    getChangeLogState: () =>
      Promise.resolve({
        replicaVersion: '123',
        minWatermark: '1ab',
      }),
    getLastConsumedWatermark: () => consumedWatermark,
  } as unknown as ChangeStreamerService;

  let readWatermark: ReturnType<
    typeof vi.fn<() => Promise<VfsBackupWatermark>>
  >;
  let closeSource: ReturnType<typeof vi.fn<() => void>>;
  let source: VfsBackupWatermarkSource;
  let monitor: VfsBackupMonitor;

  beforeEach(() => {
    vi.useFakeTimers();
    scheduled.splice(0);
    consumedWatermark = null;
    readWatermark = vi.fn<() => Promise<VfsBackupWatermark>>();
    closeSource = vi.fn<() => void>();
    source = {
      readWatermark,
      close: closeSource,
    };
    monitor = new VfsBackupMonitor(
      createSilentLogContext(),
      's3://foo/bar',
      changeStreamer,
      100_000,
      30_000,
      source,
    );

    return () => {
      void monitor.stop();
      vi.useRealTimers();
    };
  });

  function getFirstMessage(
    sub: Subscription<SnapshotMessage>,
  ): Promise<SnapshotMessage> {
    const {promise, resolve} = resolver<SnapshotMessage>();
    void (async function () {
      for await (const msg of sub) {
        resolve(msg);
      }
    })();
    return promise;
  }

  function backupWatermark(
    watermark: string,
    observedAtMs: number,
  ): VfsBackupWatermark {
    return {
      watermark,
      writeTimeMs: observedAtMs - 1000,
      txid: `000000000000000${watermark}`,
      lagSeconds: 1,
      observedAtMs,
    };
  }

  test('schedules cleanup from the VFS-visible watermark after the cleanup delay', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);
    readWatermark.mockResolvedValue(backupWatermark('04', time));

    await monitor.checkWatermarkAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    vi.setSystemTime(time + 99_999);
    await monitor.checkWatermarkAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    vi.setSystemTime(time + 100_000);
    await monitor.checkWatermarkAndScheduleCleanup();
    expect(scheduled).toEqual(['04']);

    await monitor.stop();
    expect(closeSource).toHaveBeenCalledTimes(1);
  });

  test('blocks cleanup when the VFS probe fails', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);
    readWatermark
      .mockResolvedValueOnce(backupWatermark('04', time))
      .mockRejectedValueOnce(new Error('boom'))
      .mockResolvedValueOnce(backupWatermark('04', time));

    await monitor.checkWatermarkAndScheduleCleanup();

    vi.setSystemTime(time + 100_000);
    await monitor.checkWatermarkAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    await monitor.checkWatermarkAndScheduleCleanup();
    expect(scheduled).toEqual(['04']);
  });

  test('does not schedule a cached watermark newer than the current VFS read', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);
    readWatermark
      .mockResolvedValueOnce(backupWatermark('05', time))
      .mockResolvedValueOnce(backupWatermark('04', time));

    await monitor.checkWatermarkAndScheduleCleanup();

    vi.setSystemTime(time + 100_000);
    await monitor.checkWatermarkAndScheduleCleanup();
    expect(scheduled).toEqual(['04']);
  });

  test('pauses cleanup during snapshot reservation', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);
    readWatermark.mockResolvedValue(backupWatermark('04', time));

    await monitor.checkWatermarkAndScheduleCleanup();

    const sub = monitor.startSnapshotReservation('foo-bar');
    expect(await getFirstMessage(sub)).toEqual([
      'status',
      {
        tag: 'status',
        backupURL: 's3://foo/bar',
        replicaVersion: '123',
        minWatermark: '1ab',
      },
    ]);

    vi.setSystemTime(time + 100_000);
    await monitor.checkWatermarkAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    monitor.endReservation('foo-bar');
    await monitor.checkWatermarkAndScheduleCleanup();
    expect(scheduled).toEqual(['04']);
  });

  test('reports shadow ack lag between consumed and backup watermarks', async () => {
    const sink = new TestLogSink();
    const localMonitor = new VfsBackupMonitor(
      new LogContext('debug', undefined, sink),
      's3://foo/bar',
      changeStreamer,
      100_000,
      30_000,
      source,
    );

    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);
    consumedWatermark = '08';
    readWatermark.mockResolvedValue(backupWatermark('04', time));

    await localMonitor.checkWatermarkAndScheduleCleanup();

    const observed = sink.messages.find(
      ([, , args]) =>
        typeof args[0] === 'string' &&
        args[0].includes('observed backup watermark'),
    );
    expect(observed).toBeDefined();
    const fields = observed![2][1] as {
      consumedWatermark: string | null;
      shadowAckLagBytes: number | undefined;
    };
    expect(fields.consumedWatermark).toBe('08');
    // major('08') - major('04') = 8 - 4 = 4 WAL bytes the slot would
    // additionally retain under backup-driven ACK.
    expect(fields.shadowAckLagBytes).toBe(4);

    await localMonitor.stop();
  });
});
