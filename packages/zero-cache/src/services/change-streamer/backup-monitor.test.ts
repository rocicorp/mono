import {resolver} from '@rocicorp/resolver';
import nock from 'nock';
import {beforeAll, beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {DbFile} from '../../test/lite.ts';
import type {Subscription} from '../../types/subscription.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {BackupMonitor, WEDGED_SHUTDOWN_GRACE_MS} from './backup-monitor.ts';
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
  let replica: DbFile;

  // Mocks the verification of the actual backup state (i.e. the
  // `getLastBackupTime()` litestream CLI invocation in production).
  let lastActualBackupTime: () => Promise<Date>;
  const verifyBackupState = vi.fn(() => lastActualBackupTime());

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

  beforeAll(() => {
    replica = new DbFile('backup_monitor_test');
    initReplicationState(
      replica.connect(createSilentLogContext()),
      ['zero_pub'],
      '123',
    );

    return () => replica.delete();
  });

  beforeEach(() => {
    const lc = createSilentLogContext();

    vi.useFakeTimers();
    scheduled.splice(0);

    // By default, verification confirms whatever litestream claims
    // (i.e. the last actual upload happened "now").
    verifyBackupState.mockClear();
    lastActualBackupTime = () => Promise.resolve(new Date());

    monitor = new BackupMonitor(
      lc,
      replica.path,
      's3://foo/bar',
      'http://localhost:4850/metrics',
      changeStreamer as unknown as ChangeStreamerService,
      100_000, // 100 seconds
      verifyBackupState,
    );

    nock('http://localhost:4850')
      .persist()
      .get('/metrics')
      .reply(200, () => metricsResponse);

    return () => {
      nock.abortPendingRequests();
      nock.cleanAll();
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

  test('blocks purge when actual backup is older than claimed', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);
    const nowSeconds = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', nowSeconds);

    // Litestream claims the watermark was backed up "now", but the last
    // object actually uploaded to the backup destination is 10 minutes old.
    lastActualBackupTime = () => Promise.resolve(new Date(time - 10 * 60_000));

    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);
    expect(verifyBackupState).toHaveBeenCalledTimes(0);

    // The cleanup delay has passed, but the purge must be blocked because
    // the claimed backup time is not corroborated by an actual upload.
    vi.setSystemTime(time + 100_000);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);
    expect(verifyBackupState).toHaveBeenCalledTimes(1);

    vi.setSystemTime(time + 160_000);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);
    expect(verifyBackupState).toHaveBeenCalledTimes(2);

    // Once the backup destination reflects an upload at (or after) the
    // claimed backup time, the purge proceeds.
    lastActualBackupTime = () => Promise.resolve(new Date(time));
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618p0bw8']);
  });

  test('purges only up to the verified backup state', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);

    // The last object actually uploaded to the backup destination was
    // at `time`, regardless of what litestream metrics claim.
    lastActualBackupTime = () => Promise.resolve(new Date(time));

    const t1 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618ocqq8', t1);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    // The first watermark (claimed at `time`) becomes eligible and is
    // confirmed by the actual backup state.
    vi.setSystemTime(time + 10 * 60_000);
    const t2 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', t2);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618ocqq8']);

    // Both watermarks have passed the cleanup delay, but the second one
    // (claimed at time + 10 min) is not corroborated by the actual backup
    // state (last actual upload at `time`), so it must not be purged.
    vi.setSystemTime(time + 20 * 60_000);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618ocqq8']);

    // Once the actual backup state catches up, the second one is purged.
    lastActualBackupTime = () => Promise.resolve(new Date(time + 10 * 60_000));
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618ocqq8', '618p0bw8']);
  });

  test('blocks purge when backup verification fails', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);
    const nowSeconds = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', nowSeconds);

    lastActualBackupTime = () =>
      Promise.reject(new Error('cannot list backup'));

    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);

    // Verification fails, so the purge is conservatively skipped.
    vi.setSystemTime(time + 100_000);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);
    expect(verifyBackupState).toHaveBeenCalledTimes(1);

    // When verification recovers (and confirms the claim), the purge
    // proceeds.
    lastActualBackupTime = () => Promise.resolve(new Date());
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618p0bw8']);
  });

  test('skips verification confirmed by cached backup state', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);

    const t1 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618ocqq8', t1);
    await monitor.checkWatermarksAndScheduleCleanup();

    vi.setSystemTime(time + 10_000);
    const t2 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', t2);
    await monitor.checkWatermarksAndScheduleCleanup();

    // The first purge verifies against the backup destination, which
    // reports a last actual upload time that also covers the second
    // watermark (within the clock-skew slack).
    lastActualBackupTime = () => Promise.resolve(new Date(time + 10_000));
    vi.setSystemTime(time + 100_000);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618ocqq8']);
    expect(verifyBackupState).toHaveBeenCalledTimes(1);

    // The second watermark's claimed backup time is already confirmed by
    // the cached verification result, so the backup destination is not
    // consulted again.
    vi.setSystemTime(time + 110_000);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618ocqq8', '618p0bw8']);
    expect(verifyBackupState).toHaveBeenCalledTimes(1);
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

  test('shuts down when the backup stays wedged past the grace period', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);

    // Litestream keeps claiming a fresh backup, but the last object actually
    // uploaded is frozen 10 minutes in the past: the backup is wedged.
    setMetricsResponse('618p0bw8', (time / 1000).toPrecision(9));
    lastActualBackupTime = () => Promise.resolve(new Date(time - 10 * 60_000));

    const runResult = monitor.run();
    let rejection: unknown;
    void runResult.catch(e => (rejection = e));

    // First eligible check: the staleness clock starts, no shutdown yet.
    vi.setSystemTime(time + 100_000);
    await monitor.checkWatermarksAndScheduleCleanup();
    await Promise.resolve();
    expect(scheduled).toEqual([]);
    expect(rejection).toBeUndefined();

    // Still stale, but just shy of the grace period: still no shutdown.
    vi.setSystemTime(time + 100_000 + WEDGED_SHUTDOWN_GRACE_MS - 1);
    await monitor.checkWatermarksAndScheduleCleanup();
    await Promise.resolve();
    expect(rejection).toBeUndefined();

    // The backup has now been continuously stale for the full grace period,
    // so the process shuts down by rejecting the `run()` promise.
    vi.setSystemTime(time + 100_000 + WEDGED_SHUTDOWN_GRACE_MS);
    await monitor.checkWatermarksAndScheduleCleanup();
    await expect(runResult).rejects.toThrow(/wedged/);
    expect(scheduled).toEqual([]);
  });

  test('resets the staleness clock when the backup recovers', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);

    setMetricsResponse('618p0bw8', (time / 1000).toPrecision(9));
    lastActualBackupTime = () => Promise.resolve(new Date(time - 10 * 60_000));

    const runResult = monitor.run();
    let rejection: unknown;
    void runResult.catch(e => (rejection = e));

    // Stale right up until the last moment before the grace period elapses.
    vi.setSystemTime(time + 100_000);
    await monitor.checkWatermarksAndScheduleCleanup();
    vi.setSystemTime(time + 100_000 + WEDGED_SHUTDOWN_GRACE_MS - 1);
    await monitor.checkWatermarksAndScheduleCleanup();
    await Promise.resolve();
    expect(rejection).toBeUndefined();
    expect(scheduled).toEqual([]);

    // The backup recovers before the grace period elapses: the purge proceeds
    // and the staleness clock is reset.
    lastActualBackupTime = () => Promise.resolve(new Date(time));
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618p0bw8']);

    // A subsequent wedge must endure a *fresh* full grace period. Even though
    // the total time spent stale now far exceeds the grace period, it was not
    // continuous, so the process keeps running.
    setMetricsResponse('618p0bw9', ((time + 100_000) / 1000).toPrecision(9));
    lastActualBackupTime = () => Promise.resolve(new Date(time));
    vi.setSystemTime(time + 100_000 + WEDGED_SHUTDOWN_GRACE_MS + 100_000);
    await monitor.checkWatermarksAndScheduleCleanup();
    await Promise.resolve();
    expect(rejection).toBeUndefined();
    expect(scheduled).toEqual(['618p0bw8']);

    await monitor.stop();
  });
});
