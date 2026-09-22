import {describe, expect, test} from 'vitest';
import type {SyntheticClient} from './client.ts';
import type {BenchmarkConfig} from './config.ts';
import type {MetricSummary} from './metrics.ts';
import {buildResult, lagSlope, sanitizeConfig} from './results.ts';
import type {WriterStats} from './writer.ts';

describe('sanitizeConfig', () => {
  test('redacts cloudzero.apiKey, adminPassword, and pg.url credentials', () => {
    const config = {
      profile: 'feed-append',
      model: 'hot',
      adminPassword: 'super-secret-password',
      pg: {
        url: 'postgresql://postgres:secretpassword@db.example.com:5432/testdb',
        start: false,
        stopAfterRun: true,
        readyTimeoutMs: 5000,
      },
      cloudzero: {
        apiKey: 'bearer-token-12345',
        metricsUrl: 'http://example.com',
        stackId: 'my-stack',
      },
    } as unknown as BenchmarkConfig;

    const sanitized = sanitizeConfig(config);
    expect(sanitized.adminPassword).toBe('<REDACTED>');
    expect(sanitized.cloudzero?.apiKey).toBe('<REDACTED>');
    expect(sanitized.cloudzero?.stackId).toBe('my-stack');
    expect(sanitized.pg.url).toBe(
      'postgresql://postgres:<REDACTED>@db.example.com:5432/testdb',
    );
  });

  test('handles undefined secrets gracefully', () => {
    const config = {
      profile: 'feed-append',
      model: 'hot',
      pg: {
        url: 'postgresql://localhost:5432/testdb',
        start: false,
        stopAfterRun: true,
        readyTimeoutMs: 5000,
      },
    } as unknown as BenchmarkConfig;

    const sanitized = sanitizeConfig(config);
    expect(sanitized.adminPassword).toBeUndefined();
    expect(sanitized.cloudzero).toBeUndefined();
    expect(sanitized.pg.url).toBe('postgresql://localhost:5432/testdb');
  });
});

describe('lagSlope (OLS linear regression)', () => {
  test('returns 0 for fewer than 2 samples', () => {
    expect(lagSlope([])).toBe(0);
    expect(
      lagSlope([
        {
          elapsedMs: 0,
          committedSeq: 10,
          minObservedSeq: 10,
          seqLag: 0,
          connectedClients: 1,
        },
      ]),
    ).toBe(0);
  });

  test('computes exact slope for two samples', () => {
    expect(
      lagSlope([
        {
          elapsedMs: 0,
          committedSeq: 10,
          minObservedSeq: 10,
          seqLag: 0,
          connectedClients: 1,
        },
        {
          elapsedMs: 2000,
          committedSeq: 30,
          minObservedSeq: 10,
          seqLag: 20,
          connectedClients: 1,
        },
      ]),
    ).toBe(10); // 20 seq / 2 sec = 10 seq/s
  });

  test('computes zero slope for constant lag', () => {
    expect(
      lagSlope([
        {
          elapsedMs: 0,
          committedSeq: 10,
          minObservedSeq: 5,
          seqLag: 5,
          connectedClients: 1,
        },
        {
          elapsedMs: 2000,
          committedSeq: 25,
          minObservedSeq: 20,
          seqLag: 5,
          connectedClients: 1,
        },
        {
          elapsedMs: 4000,
          committedSeq: 45,
          minObservedSeq: 40,
          seqLag: 5,
          connectedClients: 1,
        },
      ]),
    ).toBe(0);
  });

  test('computes negative slope when lag is decreasing', () => {
    expect(
      lagSlope([
        {
          elapsedMs: 0,
          committedSeq: 30,
          minObservedSeq: 10,
          seqLag: 20,
          connectedClients: 1,
        },
        {
          elapsedMs: 2000,
          committedSeq: 40,
          minObservedSeq: 30,
          seqLag: 10,
          connectedClients: 1,
        },
        {
          elapsedMs: 4000,
          committedSeq: 50,
          minObservedSeq: 50,
          seqLag: 0,
          connectedClients: 1,
        },
      ]),
    ).toBe(-5); // -20 seq / 4 sec = -5 seq/s
  });
});

describe('buildResult with sloMetric', () => {
  const dummyConfig = {
    profile: 'forum',
    model: 'hot',
    writeRate: 3600,
    sloP99LagMs: 2000,
    sloMetric: 'e2e-serving',
    pg: {url: 'postgresql://localhost:5432'},
    outputPath: 'results/test.json',
  } as unknown as BenchmarkConfig;

  const dummyWriterStats = {
    startedAtMs: 0,
    finishedAtMs: 10000,
    committedRows: 36000,
    committedTransactions: 720,
    highestCommittedSeq: 36000,
    transactionLatencyMs: [1000],
    writeImpact: {
      clientGroupRowsImpacted: 0,
      clientGroupRowsVisible: 0,
      totalLogicalWrites: 36000,
      affectedActiveClientGroupWrites: 0,
    },
  } as unknown as WriterStats;

  test('passes when e2eServingLag p99 <= sloP99LagMs even if client-visible lag > slo', () => {
    const res = buildResult({
      config: dummyConfig,
      processes: [],
      writerStats: dummyWriterStats,
      samples: [
        {
          elapsedMs: 0,
          committedSeq: 0,
          minObservedSeq: 0,
          seqLag: 0,
          connectedClients: 1,
        },
      ],
      clients: [
        {
          stats: () => ({
            userID: 'u1',
            connected: true,
            connectionState: 'connected' as const,
            queries: [
              {
                clientID: 'c1',
                queryIndex: 0,
                queryName: 'q',
                initialSyncMs: 100,
                updates: 1,
                observedSeq: 36000,
                observedRows: 100,
                observeTotalMs: 1,
                observeMaxMs: 1,
                collectSignalsTotalMs: 1,
                collectSignalsMaxMs: 1,
                latencySamplesMs: [2500], // client-visible lag exceeds 2000ms
                lastResultType: 'complete',
              },
            ],
          }),
          latencySamplesMs: () => [2500],
          minObservedSeq: () => 36000,
        } as unknown as SyntheticClient,
      ],
      metricsSummary: {
        e2eServingLagMs: {
          count: 10,
          sum: 5000,
          avg: 500,
          min: 100,
          p50: 400,
          p75: 450,
          p90: 480,
          p95: 490,
          p99: 500, // e2e serving lag is well within 2000ms
          max: 550,
        },
      } as unknown as MetricSummary,
    });

    expect(res.summary.pass).toBe(true);
    expect(res.summary.failureReasons).toEqual([]);
  });

  test('fails when e2eServingLag p99 > sloP99LagMs', () => {
    const res = buildResult({
      config: dummyConfig,
      processes: [],
      writerStats: dummyWriterStats,
      samples: [
        {
          elapsedMs: 0,
          committedSeq: 0,
          minObservedSeq: 0,
          seqLag: 0,
          connectedClients: 1,
        },
      ],
      clients: [
        {
          stats: () => ({
            userID: 'u1',
            connected: true,
            connectionState: 'connected' as const,
            queries: [
              {
                clientID: 'c1',
                queryIndex: 0,
                queryName: 'q',
                initialSyncMs: 100,
                updates: 1,
                observedSeq: 36000,
                observedRows: 100,
                observeTotalMs: 1,
                observeMaxMs: 1,
                collectSignalsTotalMs: 1,
                collectSignalsMaxMs: 1,
                latencySamplesMs: [500],
                lastResultType: 'complete',
              },
            ],
          }),
          latencySamplesMs: () => [500],
          minObservedSeq: () => 36000,
        } as unknown as SyntheticClient,
      ],
      metricsSummary: {
        e2eServingLagMs: {
          count: 10,
          sum: 25000,
          avg: 2500,
          min: 1000,
          p50: 2200,
          p75: 2300,
          p90: 2400,
          p95: 2450,
          p99: 2500, // exceeds 2000ms
          max: 2600,
        },
      } as unknown as MetricSummary,
    });

    expect(res.summary.pass).toBe(false);
    expect(res.summary.failureReasons).toEqual([
      'p99 e2e serving lag 2500.0ms exceeded SLO 2000ms',
    ]);
  });

  test('fails when changes fail to replicate to clients by end of wait period', () => {
    const res = buildResult({
      config: dummyConfig,
      processes: [],
      writerStats: dummyWriterStats,
      samples: [
        {
          elapsedMs: 0,
          committedSeq: 36000,
          minObservedSeq: 30000,
          seqLag: 6000,
          connectedClients: 1,
        },
      ],
      clients: [
        {
          stats: () => ({
            userID: 'u1',
            connected: true,
            connectionState: 'connected' as const,
            queries: [
              {
                clientID: 'c1',
                queryIndex: 0,
                queryName: 'q',
                initialSyncMs: 100,
                updates: 1,
                observedSeq: 30000,
                observedRows: 100,
                observeTotalMs: 1,
                observeMaxMs: 1,
                collectSignalsTotalMs: 1,
                collectSignalsMaxMs: 1,
                latencySamplesMs: [500],
                lastResultType: 'complete',
              },
            ],
          }),
          latencySamplesMs: () => [500],
          minObservedSeq: () => 30000,
        } as unknown as SyntheticClient,
      ],
      metricsSummary: {
        e2eServingLagMs: {
          count: 10,
          sum: 5000,
          avg: 500,
          min: 100,
          p50: 400,
          p75: 450,
          p90: 480,
          p95: 490,
          p99: 500,
          max: 550,
        },
      } as unknown as MetricSummary,
    });

    expect(res.summary.pass).toBe(false);
    expect(res.summary.failureReasons).toContain(
      '6000 committed change(s) failed to replicate to all clients by the end of the wait period (observed up to seq 30000, expected seq 36000)',
    );
  });
});
