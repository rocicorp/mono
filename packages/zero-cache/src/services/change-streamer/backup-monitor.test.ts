import {resolver} from '@rocicorp/resolver';
import nock from 'nock';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Subscription} from '../../types/subscription.ts';
import {BackupMonitor} from './backup-monitor.ts';
import type {ChangeStreamerService} from './change-streamer.ts';
import type {SnapshotMessage} from './snapshot.ts';

describe('change-streamer/backup-monitor', () => {
  const scheduled: string[] = [];
  const changeStreamer = {
    scheduleCleanup: (watermark: string) => scheduled.push(watermark),
    getChangeLogState: () =>
      Promise.resolve({
        replicaVersion: '123',
        minWatermark: '1ab',
      }),
  };
  let metricsResponse = 'unconfigured';
  let monitor: BackupMonitor;

  function setMetricsResponse(watermark: string, timestamp: string) {
    // Sample response from prometheus metrics handler
    metricsResponse = `# HELP litestream_db_size The current size of the real DB
# TYPE litestream_db_size gauge
litestream_db_size{db="/tmp/zbugs-sync-replica.db"} 3.183935488e+09
# HELP litestream_replica_progress The last replicated watermark and time of replication
# TYPE litestream_replica_progress gauge
litestream_replica_progress{db="/tmp/zbugs-sync-replica.db",name="file",watermark="${watermark}"} ${timestamp}
# HELP litestream_replica_validation_total The number of validations performed
# TYPE litestream_replica_validation_total counter
litestream_replica_validation_total{db="/tmp/zbugs-sync-replica.db",name="file",status="error"} 0
litestream_replica_validation_total{db="/tmp/zbugs-sync-replica.db",name="file",status="ok"} 0`;
  }

  beforeEach(() => {
    const lc = createSilentLogContext();

    vi.useFakeTimers();
    scheduled.splice(0);

    monitor = new BackupMonitor(
      lc,
      's3://foo/bar',
      'http://localhost:4850/metrics',
      changeStreamer as unknown as ChangeStreamerService,
      100_000, // 100 seconds
    );

    nock('http://localhost:4850')
      .persist()
      .get('/metrics')
      .reply(200, () => metricsResponse);

    return () => vi.useRealTimers();
  });

  function getFirstMessage(
    sub: Subscription<SnapshotMessage>,
  ): Promise<SnapshotMessage> {
    const {promise, resolve} = resolver<SnapshotMessage>();
    void (async function () {
      for await (const msg of sub) {
        resolve(msg);
        // To simulate an open connection, do not exit the loop.
      }
    })();
    return promise;
  }

  test('schedules overdue cleanup', async () => {
    setMetricsResponse('618ocqq8', '1.74545644476593e+09');

    await monitor.checkWatermarksAndScheduleCleanup();

    expect(scheduled).toEqual(['618ocqq8']);
  });

  test('schedules new cleanup at the right time', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);
    const nowSeconds = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', nowSeconds);

    await monitor.checkWatermarksAndScheduleCleanup();

    vi.setSystemTime(time + 99_999);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    vi.setSystemTime(time + 100_000);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618p0bw8']);
  });

  test('drops obsolete watermarks', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);

    const t1 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618ocqq8', t1);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    vi.setSystemTime(time + 10_000);
    const t2 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', t2);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    vi.setSystemTime(time + 110_000);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618p0bw8']);
  });

  test('only keeps one reservation per id', async () => {
    const sub1 = monitor.startSnapshotReservation('foo-bar');
    expect(await getFirstMessage(sub1)).toEqual([
      'status',
      {
        tag: 'status',
        backupURL: 's3://foo/bar',
        replicaVersion: '123',
        minWatermark: '1ab',
      },
    ]);
    expect(sub1.active).toBe(true);

    const sub2 = monitor.startSnapshotReservation('bar-foo');
    expect(await getFirstMessage(sub2)).toEqual([
      'status',
      {
        tag: 'status',
        backupURL: 's3://foo/bar',
        replicaVersion: '123',
        minWatermark: '1ab',
      },
    ]);
    expect(sub1.active).toBe(true);
    expect(sub2.active).toBe(true);

    const sub3 = monitor.startSnapshotReservation('bar-foo');
    expect(await getFirstMessage(sub3)).toEqual([
      'status',
      {
        tag: 'status',
        backupURL: 's3://foo/bar',
        replicaVersion: '123',
        minWatermark: '1ab',
      },
    ]);
    expect(sub1.active).toBe(true);
    expect(sub2.active).toBe(false);
    expect(sub3.active).toBe(true);
  });

  test('pauses cleanup during reservation', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);
    const nowSeconds = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', nowSeconds);

    await monitor.checkWatermarksAndScheduleCleanup();

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
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    monitor.endReservation('foo-bar');
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618p0bw8']);
  });

  test('extends cleanup delay due to reservation', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);
    const sub = monitor.startSnapshotReservation('boo-far');
    expect(await getFirstMessage(sub)).toEqual([
      'status',
      {
        tag: 'status',
        backupURL: 's3://foo/bar',
        replicaVersion: '123',
        minWatermark: '1ab',
      },
    ]);

    vi.setSystemTime(time + 50_000);
    const nowSeconds = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', nowSeconds);

    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    vi.setSystemTime(time + 125_000); // Reservation was held of 125 secs.
    monitor.endReservation('boo-far');
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    // No cleanup should be scheduled, even though 100 seconds passed,
    // as the delay should have been increased to 125 seconds.
    vi.setSystemTime(time + 174_999);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    vi.setSystemTime(time + 175_000);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618p0bw8']);
  });

  test('does not extend cleanup delay on prematurely terminated reservation', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);
    const sub = monitor.startSnapshotReservation('boo-far');
    expect(await getFirstMessage(sub)).toEqual([
      'status',
      {
        tag: 'status',
        backupURL: 's3://foo/bar',
        replicaVersion: '123',
        minWatermark: '1ab',
      },
    ]);

    vi.setSystemTime(time + 50_000);
    const nowSeconds = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', nowSeconds);

    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    // Hold the reservation for 125 secs but terminate unexpectedly.
    // This should *not* result in increasing the cleanup delay.
    vi.setSystemTime(time + 125_000);
    sub.cancel();
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    vi.setSystemTime(time + 149_999);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    vi.setSystemTime(time + 150_000); // delay should still be 100 secs
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618p0bw8']);
  });

  test('aborts in-flight fetch on stop', async () => {
    nock.cleanAll();
    const {promise: requestReceived, resolve: signalRequestReceived} =
      resolver<void>();
    const {promise: allowResponse, resolve: letResponseThrough} =
      resolver<void>();

    setMetricsResponse('618ocqq8', '1.74545644476593e+09');

    nock('http://localhost:4850')
      .get('/metrics')
      .reply(200, async () => {
        signalRequestReceived();
        await allowResponse;
        return metricsResponse;
      });

    const checkPromise = monitor.checkWatermarksAndScheduleCleanup();

    // Wait until the fetch is in-flight before aborting.
    await requestReceived;

    // Aborting the signal by stopping the monitor should cause the
    // in-flight fetch to reject with an AbortError, which is handled
    // gracefully (no warning logged, no cleanup scheduled).
    const stopPromise = monitor.stop();

    // Unblock the nock response handler so it doesn't hang.
    letResponseThrough();

    await checkPromise;
    await stopPromise;

    // Since the fetch was aborted, no watermarks were processed.
    expect(scheduled).toEqual([]);
  });
});
