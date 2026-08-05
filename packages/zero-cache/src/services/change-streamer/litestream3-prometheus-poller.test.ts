import {resolver} from '@rocicorp/resolver';
import nock from 'nock';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Source} from '../../types/streams.ts';
import type {BackedUpWatermark} from './backup-monitor.ts';
import {
  INITIAL_BACKUP_GRACE_MS_PER_UNIT,
  Litestream3PrometheusPoller,
  WEDGED_SHUTDOWN_GRACE_MS,
} from './litestream3-prometheus-poller.ts';

describe('change-streamer/litestream3-prometheus-poller', () => {
  let scheduled: string[];
  // Set if the consumer loop's `for await` throws, i.e. the poller called
  // `fail()` on the stream (a wedged or missing-backup shutdown).
  let consumeError: unknown;

  let metricsResponse = 'unconfigured';
  let poller: Litestream3PrometheusPoller;
  let source: Source<BackedUpWatermark>;

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

  beforeEach(() => {
    const lc = createSilentLogContext();

    vi.useFakeTimers();
    // Fixed baseline so that `poller.start()` below (which captures
    // Date.now() as the initial-backup-deadline start time) is deterministic
    // and consistent with the `Date.UTC(2025, 3, 24)` each test sets.
    vi.setSystemTime(Date.UTC(2025, 3, 24));
    scheduled = [];
    consumeError = undefined;

    // By default, verification confirms whatever litestream claims
    // (i.e. the last actual upload happened "now").
    verifyBackupState.mockClear();
    lastActualBackupTime = () => Promise.resolve(new Date());

    poller = new Litestream3PrometheusPoller(
      lc,
      // Nonexistent replica file: #initialBackupGraceMs() falls back to the
      // 1-unit (~1hr) minimum grace, which is all these tests need.
      '/tmp/litestream3-prometheus-poller-test-replica-does-not-exist.db',
      's3://foo/bar',
      'http://localhost:4850/metrics',
      verifyBackupState,
    );
    source = poller.start();
    void (async () => {
      try {
        for await (const backedUp of source) {
          scheduled.push(backedUp.watermark);
        }
      } catch (e) {
        consumeError = e;
      }
    })();

    nock('http://localhost:4850')
      .persist()
      .get('/metrics')
      .reply(200, () => metricsResponse);

    return () => {
      source.cancel();
      nock.abortPendingRequests();
      nock.cleanAll();
      vi.useRealTimers();
    };
  });

  test('publishes watermarks', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);

    const t1 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618ocqq8', t1);

    await poller.checkVerifyAndPublishWatermarks();
    await vi.waitFor(() => expect(scheduled).toEqual(['618ocqq8']));

    vi.setSystemTime(time + 10_000);
    const t2 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', t2);

    await poller.checkVerifyAndPublishWatermarks();
    await vi.waitFor(() => expect(scheduled).toEqual(['618ocqq8', '618p0bw8']));
  });

  test('blocks publish when actual backup is older than claimed', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);
    const nowSeconds = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', nowSeconds);

    // Litestream claims the watermark was backed up "now", but the last
    // object actually uploaded to the backup destination is 10 minutes old.
    lastActualBackupTime = () => Promise.resolve(new Date(time - 10 * 60_000));

    await poller.checkVerifyAndPublishWatermarks();
    expect(verifyBackupState).toHaveBeenCalledTimes(1);
    expect(scheduled).toEqual([]);

    vi.setSystemTime(time + 160_000);
    await poller.checkVerifyAndPublishWatermarks();
    expect(verifyBackupState).toHaveBeenCalledTimes(2);
    expect(scheduled).toEqual([]);

    // Once the backup destination reflects an upload at (or after) the
    // claimed backup time, the purge proceeds.
    lastActualBackupTime = () => Promise.resolve(new Date(time));
    await poller.checkVerifyAndPublishWatermarks();
    await vi.waitFor(() => expect(scheduled).toEqual(['618p0bw8']));
  });

  test('publishes only up to the verified backup state', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);

    // The last object actually uploaded to the backup destination was
    // at `time`, regardless of what litestream metrics claim.
    lastActualBackupTime = () => Promise.resolve(new Date(time));

    const t1 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618ocqq8', t1);
    await poller.checkVerifyAndPublishWatermarks();
    await vi.waitFor(() => expect(scheduled).toEqual(['618ocqq8']));

    // The second watermark is after the confirmed lastActualBackupTime.
    vi.setSystemTime(time + 10 * 60_000);
    const t2 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', t2);
    await poller.checkVerifyAndPublishWatermarks();
    expect(scheduled).toEqual(['618ocqq8']);

    // Once the actual backup state catches up, the second one is purged.
    lastActualBackupTime = () => Promise.resolve(new Date(time + 10 * 60_000));
    await poller.checkVerifyAndPublishWatermarks();
    await vi.waitFor(() => expect(scheduled).toEqual(['618ocqq8', '618p0bw8']));
  });

  test('blocks publish when backup verification fails', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);
    const nowSeconds = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', nowSeconds);

    lastActualBackupTime = () =>
      Promise.reject(new Error('cannot list backup'));

    await poller.checkVerifyAndPublishWatermarks();
    expect(verifyBackupState).toHaveBeenCalledTimes(1);
    expect(scheduled).toEqual([]);

    // Verification fails, so the purge is conservatively skipped.
    vi.setSystemTime(time + 100_000);
    await poller.checkVerifyAndPublishWatermarks();
    expect(verifyBackupState).toHaveBeenCalledTimes(2);
    expect(scheduled).toEqual([]);

    // When verification recovers (and confirms the claim), the purge
    // proceeds.
    lastActualBackupTime = () => Promise.resolve(new Date());
    await poller.checkVerifyAndPublishWatermarks();
    await vi.waitFor(() => expect(scheduled).toEqual(['618p0bw8']));
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
    await poller.checkVerifyAndPublishWatermarks();
    await vi.waitFor(() => expect(scheduled).toEqual(['618ocqq8']));
    expect(verifyBackupState).toHaveBeenCalledTimes(1);

    // The second watermark's claimed backup time is already confirmed by
    // the cached verification result, so the backup destination is not
    // consulted again.
    vi.setSystemTime(time + 10_000);
    const t2 = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', t2);
    await poller.checkVerifyAndPublishWatermarks();
    await vi.waitFor(() => expect(scheduled).toEqual(['618ocqq8', '618p0bw8']));
    expect(verifyBackupState).toHaveBeenCalledTimes(1);
  });

  test('aborts in-flight fetch on cancel', async () => {
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

    const checkPromise = poller.checkVerifyAndPublishWatermarks();

    // Wait until the fetch is in-flight before canceling.
    await requestReceived;

    // Canceling the returned stream should abort the in-flight fetch,
    // which is handled gracefully (no watermark is scheduled).
    source.cancel();

    letResponseThrough();
    await checkPromise;

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

    // First eligible check: the staleness clock starts, no shutdown yet.
    vi.setSystemTime(time + 100_000);
    await poller.checkVerifyAndPublishWatermarks();
    await Promise.resolve();
    expect(scheduled).toEqual([]);
    expect(consumeError).toBeUndefined();

    // Still stale, but just shy of the grace period: still no shutdown.
    vi.setSystemTime(time + 100_000 + WEDGED_SHUTDOWN_GRACE_MS - 1);
    await poller.checkVerifyAndPublishWatermarks();
    await Promise.resolve();
    expect(consumeError).toBeUndefined();

    // The backup has now been continuously stale for the full grace period,
    // so the poller shuts down by failing the stream.
    vi.setSystemTime(time + 100_000 + WEDGED_SHUTDOWN_GRACE_MS);
    await poller.checkVerifyAndPublishWatermarks();
    await vi.waitFor(() => expect(consumeError).toBeInstanceOf(Error));
    expect((consumeError as Error).message).toMatch(/wedged/);
    expect(scheduled).toEqual([]);
  });

  test('resets the staleness clock when the backup recovers', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);

    setMetricsResponse('618p0bw8', (time / 1000).toPrecision(9));
    lastActualBackupTime = () => Promise.resolve(new Date(time - 10 * 60_000));

    // Stale right up until the last moment before the grace period elapses.
    vi.setSystemTime(time + 100_000);
    await poller.checkVerifyAndPublishWatermarks();
    vi.setSystemTime(time + 100_000 + WEDGED_SHUTDOWN_GRACE_MS - 1);
    await poller.checkVerifyAndPublishWatermarks();
    await Promise.resolve();
    expect(consumeError).toBeUndefined();
    expect(scheduled).toEqual([]);

    // The backup recovers before the grace period elapses: the purge proceeds
    // and the staleness clock is reset.
    lastActualBackupTime = () => Promise.resolve(new Date(time));
    await poller.checkVerifyAndPublishWatermarks();
    await vi.waitFor(() => expect(scheduled).toEqual(['618p0bw8']));

    // A subsequent wedge must endure a *fresh* full grace period. Even though
    // the total time spent stale now far exceeds the grace period, it was not
    // continuous, so the poller keeps running.
    setMetricsResponse('618p0bw9', ((time + 100_000) / 1000).toPrecision(9));
    lastActualBackupTime = () => Promise.resolve(new Date(time));
    vi.setSystemTime(time + 100_000 + WEDGED_SHUTDOWN_GRACE_MS + 100_000);
    await poller.checkVerifyAndPublishWatermarks();
    await Promise.resolve();
    expect(consumeError).toBeUndefined();
    expect(scheduled).toEqual(['618p0bw8']);
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

    // The test replica file doesn't exist, so the grace is the one-unit
    // (~1 hour) minimum. Just before it elapses, the poller keeps waiting.
    vi.setSystemTime(time + INITIAL_BACKUP_GRACE_MS_PER_UNIT - 1);
    await poller.checkVerifyAndPublishWatermarks();
    await Promise.resolve();
    expect(consumeError).toBeUndefined();

    // Once the grace period elapses with still no restorable backup, the
    // poller shuts itself down by failing the stream, so that *it* is
    // flagged as the cause rather than the view-syncers that are waiting
    // on it.
    vi.setSystemTime(time + INITIAL_BACKUP_GRACE_MS_PER_UNIT);
    await poller.checkVerifyAndPublishWatermarks();
    await vi.waitFor(() => expect(consumeError).toBeInstanceOf(Error));
    expect((consumeError as Error).message).toMatch(/no restorable backup/);
  });

  test('does not shut down once a restorable backup is confirmed', async () => {
    const time = Date.UTC(2025, 3, 24);
    vi.setSystemTime(time);

    metricsResponse = noBackupMetrics;
    lastActualBackupTime = () =>
      Promise.reject(new Error('no snapshots or WAL segments listed'));

    // Still no backup partway through the wait: no shutdown yet.
    vi.setSystemTime(time + INITIAL_BACKUP_GRACE_MS_PER_UNIT - 60_000);
    await poller.checkVerifyAndPublishWatermarks();
    await Promise.resolve();
    expect(consumeError).toBeUndefined();

    // The first backup lands.
    lastActualBackupTime = () => Promise.resolve(new Date());

    // Past the deadline, but since a restorable backup has been confirmed the
    // poller keeps running.
    vi.setSystemTime(time + INITIAL_BACKUP_GRACE_MS_PER_UNIT + 60_000);
    await poller.checkVerifyAndPublishWatermarks();
    await Promise.resolve();
    expect(consumeError).toBeUndefined();
  });
});
