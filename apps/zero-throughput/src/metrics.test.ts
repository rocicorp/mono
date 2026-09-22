import {describe, expect, test} from 'vitest';
import {OTelMetricsCollector} from './metrics.ts';

// Helper to build a minimal OTLP JSON body that the collector can ingest.
function otlpBody(
  worker: string,
  workerIndex: number,
  metrics: {
    name: string;
    type: 'gauge' | 'sum' | 'histogram';
    value?: number | undefined;
    count?: number | undefined;
    sum?: number | undefined;
    min?: number | undefined;
    max?: number | undefined;
    bounds?: number[] | undefined;
    bucketCounts?: number[] | undefined;
  }[],
): Record<string, unknown> {
  const metricsList = metrics.map(m => {
    const base = {name: m.name};
    if (m.type === 'gauge') {
      return {...base, gauge: {dataPoints: [{asDouble: m.value}]}};
    }
    if (m.type === 'sum') {
      return {...base, sum: {dataPoints: [{asDouble: m.value}]}};
    }
    // histogram
    return {
      ...base,
      histogram: {
        dataPoints: [
          {
            count: m.count,
            sum: m.sum,
            min: m.min,
            max: m.max,
            explicitBounds: m.bounds,
            bucketCounts: m.bucketCounts,
          },
        ],
      },
    };
  });

  return {
    resourceMetrics: [
      {
        resource: {
          attributes: [
            {key: 'worker', value: {stringValue: worker}},
            {key: 'workerIndex', value: {intValue: workerIndex}},
          ],
        },
        scopeMetrics: [{metrics: metricsList}],
      },
    ],
  };
}

describe('OTelMetricsCollector', () => {
  test('reset() scopes gauge readings to the measurement window', async () => {
    const collector = new OTelMetricsCollector();
    const port = await collector.start();

    // Push a pre-benchmark gauge reading
    await pushOTLP(
      port,
      otlpBody('vs', 0, [
        {name: 'zero.replication.total_lag', type: 'gauge', value: 999},
      ]),
    );

    // Reset — this should discard the pre-benchmark reading
    collector.reset();

    // Push benchmark-window gauge readings
    await pushOTLP(
      port,
      otlpBody('vs', 0, [
        {name: 'zero.replication.total_lag', type: 'gauge', value: 10},
      ]),
    );
    await pushOTLP(
      port,
      otlpBody('vs', 0, [
        {name: 'zero.replication.total_lag', type: 'gauge', value: 20},
      ]),
    );

    const summary = collector.getSummary();
    await collector.stop();

    // Should only see benchmark-window values, not the 999
    expect(summary.replicationLagMs).not.toBeNull();
    expect(summary.replicationLagMs?.min).toBe(10);
    expect(summary.replicationLagMs?.max).toBe(20);
    expect(summary.replicationLagMs?.count).toBe(2);
  });

  test('deduplicates cumulative histograms per worker to prevent over-counting', async () => {
    const collector = new OTelMetricsCollector();
    const port = await collector.start();
    collector.reset();

    // Simulate 3 cumulative OTLP pushes from the same worker.
    // Each push contains ALL events since process start (cumulative semantics).
    // Push 1: 10 events
    await pushOTLP(
      port,
      otlpBody('vs', 0, [
        {
          name: 'zero.sync.e2e_serving_lag',
          type: 'histogram',
          count: 10,
          sum: 1.0,
          min: 0.05,
          max: 0.2,
          bounds: [0.05, 0.1, 0.2, 0.5],
          bucketCounts: [2, 4, 3, 1, 0],
        },
      ]),
    );
    // Push 2: 20 events (cumulative, includes the first 10)
    await pushOTLP(
      port,
      otlpBody('vs', 0, [
        {
          name: 'zero.sync.e2e_serving_lag',
          type: 'histogram',
          count: 20,
          sum: 2.0,
          min: 0.03,
          max: 0.3,
          bounds: [0.05, 0.1, 0.2, 0.5],
          bucketCounts: [4, 8, 6, 2, 0],
        },
      ]),
    );
    // Push 3: 30 events (cumulative, includes the first 20)
    await pushOTLP(
      port,
      otlpBody('vs', 0, [
        {
          name: 'zero.sync.e2e_serving_lag',
          type: 'histogram',
          count: 30,
          sum: 3.0,
          min: 0.02,
          max: 0.4,
          bounds: [0.05, 0.1, 0.2, 0.5],
          bucketCounts: [6, 12, 9, 3, 0],
        },
      ]),
    );

    const summary = collector.getSummary();
    await collector.stop();

    // Should use only the latest push (30 events), NOT 10+20+30=60.
    expect(summary.e2eServingLagMs).not.toBeNull();
    expect(summary.e2eServingLagMs?.count).toBe(30);
    // Sum should be 3.0 * 1000 (multiplier) = 3000, not 6000
    expect(summary.e2eServingLagMs?.sum).toBe(3000);
  });

  test('combines latest histograms across different workers correctly', async () => {
    const collector = new OTelMetricsCollector();
    const port = await collector.start();
    collector.reset();

    // Worker 0: 2 cumulative pushes
    await pushOTLP(
      port,
      otlpBody('vs', 0, [
        {
          name: 'zero.sync.e2e_serving_lag',
          type: 'histogram',
          count: 10,
          sum: 1.0,
          bounds: [0.1, 0.5],
          bucketCounts: [5, 4, 1],
        },
      ]),
    );
    await pushOTLP(
      port,
      otlpBody('vs', 0, [
        {
          name: 'zero.sync.e2e_serving_lag',
          type: 'histogram',
          count: 20,
          sum: 2.0,
          bounds: [0.1, 0.5],
          bucketCounts: [10, 8, 2],
        },
      ]),
    );

    // Worker 1: 1 push
    await pushOTLP(
      port,
      otlpBody('vs', 1, [
        {
          name: 'zero.sync.e2e_serving_lag',
          type: 'histogram',
          count: 15,
          sum: 1.5,
          bounds: [0.1, 0.5],
          bucketCounts: [8, 5, 2],
        },
      ]),
    );

    const summary = collector.getSummary();
    await collector.stop();

    // Should combine latest from worker 0 (20 events) + worker 1 (15 events) = 35
    expect(summary.e2eServingLagMs?.count).toBe(35);
    // Sum: 2.0 * 1000 + 1.5 * 1000 = 3500
    expect(summary.e2eServingLagMs?.sum).toBe(3500);
  });

  test('counter baselines scope counters to measurement window', async () => {
    const collector = new OTelMetricsCollector();
    const port = await collector.start();

    // Push pre-benchmark counter values
    await pushOTLP(
      port,
      otlpBody('vs', 0, [
        {name: 'zero.sync.pipeline_resets', type: 'sum', value: 5},
      ]),
    );

    // Reset captures baseline of 5
    collector.reset();

    // Push post-benchmark counter value (cumulative 8 = baseline 5 + 3 new)
    await pushOTLP(
      port,
      otlpBody('vs', 0, [
        {name: 'zero.sync.pipeline_resets', type: 'sum', value: 8},
      ]),
    );

    const summary = collector.getSummary();
    await collector.stop();

    // Should report 3 (delta), not 8 (cumulative)
    expect(summary.pipelineResets).toBe(3);
  });

  test('counter baselines handle multiple workers independently', async () => {
    const collector = new OTelMetricsCollector();
    const port = await collector.start();

    // Pre-benchmark: worker 0 has 10, worker 1 has 20
    await pushOTLP(
      port,
      otlpBody('vs', 0, [
        {name: 'zero.sync.pipeline_resets', type: 'sum', value: 10},
      ]),
    );
    await pushOTLP(
      port,
      otlpBody('vs', 1, [
        {name: 'zero.sync.pipeline_resets', type: 'sum', value: 20},
      ]),
    );

    collector.reset();

    // Post-benchmark: worker 0 grew to 12 (+2), worker 1 grew to 25 (+5)
    await pushOTLP(
      port,
      otlpBody('vs', 0, [
        {name: 'zero.sync.pipeline_resets', type: 'sum', value: 12},
      ]),
    );
    await pushOTLP(
      port,
      otlpBody('vs', 1, [
        {name: 'zero.sync.pipeline_resets', type: 'sum', value: 25},
      ]),
    );

    const summary = collector.getSummary();
    await collector.stop();

    // Should be (12-10) + (25-20) = 7
    expect(summary.pipelineResets).toBe(7);
    expect(summary.workerRestarts).toBe(0);
  });

  test('detects worker restarts when counter drops below baseline', async () => {
    const collector = new OTelMetricsCollector();
    const port = await collector.start();

    // Pre-benchmark: worker has pipeline_resets = 10
    await pushOTLP(
      port,
      otlpBody('vs', 0, [
        {name: 'zero.sync.pipeline_resets', type: 'sum', value: 10},
      ]),
    );

    collector.reset();

    // Worker process restarts mid-benchmark — counter resets to 2
    // (2 is less than baseline 10, indicating a restart)
    await pushOTLP(
      port,
      otlpBody('vs', 0, [
        {name: 'zero.sync.pipeline_resets', type: 'sum', value: 2},
      ]),
    );

    const summary = collector.getSummary();
    await collector.stop();

    expect(summary.workerRestarts).toBe(1);
    // Post-restart counter value is reported as-is (not negative)
    expect(summary.pipelineResets).toBe(2);
  });
});

async function pushOTLP(
  port: number,
  body: Record<string, unknown>,
): Promise<void> {
  const res = await fetch(`http://127.0.0.1:${port}/v1/metrics`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body),
  });
  expect(res.status).toBe(200);
}
