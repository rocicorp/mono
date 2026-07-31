import {resolver} from '@rocicorp/resolver';
import nock from 'nock';
import {beforeAll, beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {DbFile} from '../../test/lite.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import type {ChangeStreamerService} from './change-streamer.ts';
import {
  INITIAL_BACKUP_GRACE_MS_PER_UNIT,
  Litestream3BackupMonitor,
  WEDGED_SHUTDOWN_GRACE_MS,
} from './litestream3-backup-monitor.ts';

describe('change-streamer/litestream3-backup-monitor', () => {
  const scheduled: string[] = [];
  const changeStreamer = {
    trackBackupWatermark: (watermark: string) => scheduled.push(watermark),
  };
  let metricsResponse = 'unconfigured';
  let monitor: Litestream3BackupMonitor;
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

    monitor = new Litestream3BackupMonitor(
      lc,
      replica.path,
      's3://foo/bar',
      'http://localhost:4850/metrics',
      changeStreamer as unknown as ChangeStreamerService,
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

  test('schedules cleanup', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);

    const t1 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618ocqq8', t1);

    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618ocqq8']);

    vi.setSystemTime(time + 10_000);
    const t2 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', t2);

    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618ocqq8', '618p0bw8']);
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
    expect(scheduled).toEqual(['618ocqq8']);

    // The second watermark is after the confirmed lastActualBackupTime.
    vi.setSystemTime(time + 10 * 60_000);
    const t2 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', t2);
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
    expect(verifyBackupState).toHaveBeenCalledTimes(1);

    // Verification fails, so the purge is conservatively skipped.
    vi.setSystemTime(time + 100_000);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual([]);
    expect(verifyBackupState).toHaveBeenCalledTimes(2);

    // When verification recovers (and confirms the claim), the purge
    // proceeds.
    lastActualBackupTime = () => Promise.resolve(new Date());
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618p0bw8']);
  });

  test('skips verification confirmed by cached backup state', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);

    // The first purge verifies against the backup destination, which
    // reports a last actual upload time that also covers the second
    // watermark (within the clock-skew slack).
    lastActualBackupTime = () => Promise.resolve(new Date(time + 10_000));
    const t1 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618ocqq8', t1);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618ocqq8']);
    expect(verifyBackupState).toHaveBeenCalledTimes(1);

    // The second watermark's claimed backup time is already confirmed by
    // the cached verification result, so the backup destination is not
    // consulted again.
    vi.setSystemTime(time + 10_000);
    const t2 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', t2);
    await monitor.checkWatermarksAndScheduleCleanup();
    expect(scheduled).toEqual(['618ocqq8', '618p0bw8']);
    expect(verifyBackupState).toHaveBeenCalledTimes(1);
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

  // Metrics with no `litestream_replica_progress` gauge: nothing has been
  // backed up yet (a cold start whose initial snapshot is still uploading).
  const noBackupMetrics = `# HELP litestream_db_size The current size of the real DB
# TYPE litestream_db_size gauge
litestream_db_size{db="/tmp/zbugs-sync-replica.db"} 1e+06`;

  test('shuts down when no restorable backup appears within the grace period', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);

    metricsResponse = noBackupMetrics;
    // The backup never becomes restorable (the listing keeps failing/empty).
    lastActualBackupTime = () =>
      Promise.reject(new Error('no snapshots or WAL segments listed'));

    const runResult = monitor.run();
    let rejection: unknown;
    void runResult.catch(e => (rejection = e));

    // The test replica is small, so the grace is the one-unit (~1 hour)
    // minimum. Just before it elapses, the process keeps waiting.
    vi.setSystemTime(time + INITIAL_BACKUP_GRACE_MS_PER_UNIT - 1);
    await monitor.checkWatermarksAndScheduleCleanup();
    await Promise.resolve();
    expect(rejection).toBeUndefined();

    // Once the grace period elapses with still no restorable backup, the
    // producer shuts itself down by rejecting run() so that *it* is flagged as
    // the cause rather than the view-syncers that are waiting on it.
    vi.setSystemTime(time + INITIAL_BACKUP_GRACE_MS_PER_UNIT);
    await monitor.checkWatermarksAndScheduleCleanup();
    await expect(runResult).rejects.toThrow(/no restorable backup/);
  });

  test('does not shut down once a restorable backup is confirmed', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);

    metricsResponse = noBackupMetrics;
    lastActualBackupTime = () =>
      Promise.reject(new Error('no snapshots or WAL segments listed'));

    const runResult = monitor.run();
    let rejection: unknown;
    void runResult.catch(e => (rejection = e));

    // Still no backup partway through the wait: no shutdown yet.
    vi.setSystemTime(time + INITIAL_BACKUP_GRACE_MS_PER_UNIT - 60_000);
    await monitor.checkWatermarksAndScheduleCleanup();
    await Promise.resolve();
    expect(rejection).toBeUndefined();

    // The first backup lands.
    lastActualBackupTime = () => Promise.resolve(new Date());

    // Past the deadline, but since a restorable backup has been confirmed the
    // process keeps running.
    vi.setSystemTime(time + INITIAL_BACKUP_GRACE_MS_PER_UNIT + 60_000);
    await monitor.checkWatermarksAndScheduleCleanup();
    await Promise.resolve();
    expect(rejection).toBeUndefined();

    await monitor.stop();
  });
});
