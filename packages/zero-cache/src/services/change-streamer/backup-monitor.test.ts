import nock from 'nock';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {BackupMonitor} from './backup-monitor.ts';
import type {ChangeStreamerService} from './change-streamer.ts';

describe('change-streamer/backup-monitor', () => {
  const scheduled: string[] = [];
  const changeStreamer = {
    scheduleCleanup: (watermark: string) => scheduled.push(watermark),
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

  test('schedules overdue cleanup', async () => {
    setMetricsResponse('618ocqq8', '1.74545644476593e+09');

    await monitor.checkMetrics();

    vi.advanceTimersByTime(0);
    expect(scheduled).toEqual(['618ocqq8']);
  });

  test('schedules new cleanup', async () => {
    vi.setSystemTime(Date.UTC(2025, 3, 24));
    const nowSeconds = (Date.now() / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', nowSeconds);

    await monitor.checkMetrics();

    vi.advanceTimersByTime(99_999);
    expect(scheduled).toEqual([]);

    vi.advanceTimersByTime(1);
    expect(scheduled).toEqual(['618p0bw8']);
  });

  test('skips redundant watermarks cleanup', async () => {
    vi.setSystemTime(Date.UTC(2025, 3, 24));
    const firstWatermarkTime = (Date.UTC(2025, 3, 23) / 1000).toPrecision(9);
    setMetricsResponse('618ofzts', firstWatermarkTime);

    await monitor.checkMetrics();
    vi.advanceTimersByTime(0);
    expect(scheduled.pop()).toEqual('618ofzts');

    for (let i = 0; i < 5; i++) {
      await monitor.checkMetrics();
      vi.advanceTimersByTime(100);
      expect(scheduled).toEqual([]);
    }

    const secondWatermarkTime = ((Date.now() - 100_000) / 1000).toPrecision(9);
    setMetricsResponse('618oko80', secondWatermarkTime);

    await monitor.checkMetrics();
    vi.advanceTimersByTime(1);
    expect(scheduled.pop()).toEqual('618oko80');

    for (let i = 0; i < 5; i++) {
      await monitor.checkMetrics();
      vi.advanceTimersByTime(100);
      expect(scheduled).toEqual([]);
    }

    const thirdWatermarkTime = ((Date.now() - 100_000) / 1000).toPrecision(9);
    setMetricsResponse('618p0bw8', thirdWatermarkTime);

    await monitor.checkMetrics();
    vi.advanceTimersByTime(1);
    expect(scheduled.pop()).toEqual('618p0bw8');

    for (let i = 0; i < 5; i++) {
      await monitor.checkMetrics();
      vi.advanceTimersByTime(100);
      expect(scheduled).toEqual([]);
    }
  });
});
