import {describe, expect, test} from 'vitest';
import {
  getDefaultZbugsQueries,
  getZbugsClientSchema,
} from '../../bench/pipeline/default-queries.ts';
import {OTelMetricsCollector} from '../../bench/pipeline/metrics-collector.ts';
import {
  formatBenchmarkReport,
  type BenchmarkResult,
} from '../../bench/pipeline/results.ts';

describe('bench pipeline components', () => {
  test('default queries and client schema generation', () => {
    const queries = getDefaultZbugsQueries();
    expect(queries).toHaveLength(5);
    expect(queries[0].table).toBe('issue');
    expect(queries[1].table).toBe('issue');
    expect(queries[2].table).toBe('comment');
    expect(queries[3].table).toBe('project');
    expect(queries[4].table).toBe('user');

    const {clientSchema, hash} = getZbugsClientSchema();
    expect(hash).toBeTruthy();
    expect(clientSchema.tables).toHaveProperty('issue');
    expect(clientSchema.tables).toHaveProperty('user');
    expect(clientSchema.tables).toHaveProperty('project');
  });

  test('metrics collector ingests and computes percentiles', async () => {
    const collector = new OTelMetricsCollector();
    const port = await collector.start();
    expect(port).toBeGreaterThan(0);

    // Simulate sending OTLP metrics payload
    const otlpPayload = {
      resourceMetrics: [
        {
          resource: {
            attributes: [
              {key: 'process.worker', value: {stringValue: 'syncer'}},
              {key: 'process.worker_index', value: {intValue: 0}},
            ],
          },
          scopeMetrics: [
            {
              metrics: [
                {
                  name: 'zero.sync.e2e_serving_lag',
                  histogram: {
                    dataPoints: [
                      {
                        count: 4,
                        sum: 0.4,
                        min: 0.05,
                        max: 0.2,
                        explicitBounds: [0.05, 0.1, 0.2],
                        bucketCounts: [1, 2, 1],
                      },
                    ],
                  },
                },
                {
                  name: 'zero.sync.pipeline-resets',
                  sum: {
                    dataPoints: [{asInt: 0}],
                  },
                },
              ],
            },
          ],
        },
      ],
    };

    const res = await fetch(collector.endpoint, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(otlpPayload),
    });
    expect(res.status).toBe(200);

    const summary = collector.getSummary();
    expect(summary.pipelineResets).toBe(0);
    expect(summary.e2eServingLagMs).not.toBeNull();
    expect(summary.e2eServingLagMs?.count).toBe(4);

    await collector.stop();
  });

  test('formatBenchmarkReport renders report with all key sections', () => {
    const mockResult: BenchmarkResult = {
      timestamp: '2026-08-14T20:00:00.000Z',
      topology: {
        numReplicationManagers: 1,
        numViewSyncers: 2,
        totalClients: 10,
        clientsPerViewSyncer: 5,
      },
      config: {
        writeRatePerSecond: 100,
        loadDurationSeconds: 30,
        sqliteChangeLogMode: 'serve',
        dbMode: 'docker',
      },
      loadStats: {
        writesAttempted: 3000,
        writesSucceeded: 3000,
        writesFailed: 0,
        durationMs: 30000,
        actualRate: 100,
      },
      clientStats: [
        {
          clientID: 'client_0',
          clientGroupID: 'group_0',
          viewSyncerIndex: 0,
          connected: true,
          initialHydrationDurationMs: 120,
          pokesReceived: 50,
          lastPokeTimestamp: 30000,
          lastPokeCookie: '01:00',
          lastPokeWatermark: '01',
          errors: [],
        },
      ],
      metrics: {
        replicationLagMs: {
          count: 100,
          sum: 5000,
          avg: 50,
          min: 10,
          p50: 45,
          p90: 80,
          p95: 90,
          p99: 120,
          max: 150,
        },
        e2eServingLagMs: {
          count: 50,
          sum: 4000,
          avg: 80,
          min: 20,
          p50: 75,
          p90: 110,
          p95: 130,
          p99: 160,
          max: 180,
        },
        advancementLatencyMs: {
          count: 50,
          sum: 1000,
          avg: 20,
          min: 5,
          p50: 18,
          p90: 30,
          p95: 35,
          p99: 40,
          max: 45,
        },
        viewSyncerLagMs: null,
        hydrationTimeMs: {
          count: 10,
          sum: 1200,
          avg: 120,
          min: 80,
          p50: 115,
          p90: 150,
          p95: 160,
          p99: 170,
          max: 180,
        },
        pipelineResets: 0,
        transactionsReplicated: 500,
        changesReplicated: 3000,
        flowControlWaits: 500,
        flowControlWaitDurationMs: null,
      },
    };

    const report = formatBenchmarkReport(mockResult);
    expect(report).toContain('ZERO-CACHE PIPELINE BENCHMARK REPORT');
    expect(report).toContain(
      '1 RM(s) | 2 View-Syncer(s) | 10 Connected Client(s)',
    );
    expect(report).toContain('Replication Lag (PG -> RM):');
    expect(report).toContain('Advancement Latency (IVM):');
    expect(report).toContain('E2E Serving Lag (PG -> Poke):');
    expect(report).toContain('Client Initial Hydration:');
  });
});
