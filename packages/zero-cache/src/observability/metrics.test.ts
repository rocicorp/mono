import {metrics} from '@opentelemetry/api';
import {
  DataPointType,
  MeterProvider,
  MetricReader,
  type CollectionResult,
} from '@opentelemetry/sdk-metrics';
import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import {getOrCreateLatencyHistogram} from './metrics.ts';

/**
 * A minimal synchronous MetricReader that lets us collect metrics on demand.
 */
class TestMetricReader extends MetricReader {
  // oxlint-disable-next-line require-await
  protected async onForceFlush(): Promise<void> {
    /* noop */
  }

  // oxlint-disable-next-line require-await
  protected async onShutdown(): Promise<void> {
    /* noop */
  }

  async collectMetrics(): Promise<CollectionResult> {
    return this.collect();
  }
}

describe('getOrCreateLatencyHistogram', () => {
  let meterProvider: MeterProvider;
  let reader: TestMetricReader;

  // Use a single meter provider for all tests since the internal histogram
  // cache in metrics.ts is module-scoped and only creates each histogram once.
  beforeAll(() => {
    reader = new TestMetricReader();
    meterProvider = new MeterProvider({
      readers: [reader],
    });
    metrics.setGlobalMeterProvider(meterProvider);
  });

  afterAll(async () => {
    await meterProvider?.shutdown();
    metrics.disable();
  });

  test('records in seconds with correct explicit bucket boundaries', async () => {
    const h = getOrCreateLatencyHistogram(
      'sync',
      'test-latency',
      'Test latency histogram',
    );

    // Record 3ms and 150ms (in milliseconds, as callers would)
    h.recordMs(3);
    h.recordMs(150);

    // Force a collection
    const {resourceMetrics} = await reader.collectMetrics();
    const metric = resourceMetrics.scopeMetrics
      .flatMap(sm => sm.metrics)
      .find(m => m.descriptor.name === 'zero.sync.test-latency');
    expect(metric).toBeDefined();

    // Verify unit is seconds
    expect(metric!.descriptor.unit).toBe('s');

    // Verify it's an explicit histogram (not exponential)
    expect(metric!.dataPointType).toBe(DataPointType.HISTOGRAM);
    const dp = metric!.dataPoints[0];
    expect(dp).toBeDefined();

    const value = dp.value as {
      buckets: {boundaries: number[]; counts: number[]};
      count: number;
      sum: number;
    };

    // Verify the bucket boundaries match our configured set
    expect(value.buckets.boundaries).toEqual([
      0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30,
    ]);

    // Verify count and sum
    expect(value.count).toBe(2);
    // 3ms + 150ms = 153ms = 0.153s
    expect(value.sum).toBeCloseTo(0.153, 6);

    // Verify the 3ms observation (0.003s) is in the right bucket:
    // boundaries: [0.001, 0.002, 0.005, ...]
    // 0.003 falls in (0.002, 0.005] which is bucket index 2
    expect(value.buckets.counts[2]).toBe(1); // 3ms in (2ms, 5ms]

    // Verify the 150ms observation (0.15s) is in the right bucket:
    // 0.15 falls in (0.1, 0.2] which is bucket index 7
    expect(value.buckets.counts[7]).toBe(1); // 150ms in (100ms, 200ms]
  });

  test('recordMs converts milliseconds to seconds', async () => {
    const h = getOrCreateLatencyHistogram(
      'sync',
      'test-conversion',
      'Test ms to s conversion',
    );

    h.recordMs(1000); // 1 second

    const {resourceMetrics} = await reader.collectMetrics();
    const metric = resourceMetrics.scopeMetrics
      .flatMap(sm => sm.metrics)
      .find(m => m.descriptor.name === 'zero.sync.test-conversion');
    expect(metric).toBeDefined();

    const dp = metric!.dataPoints[0];
    const value = dp.value as {sum: number; count: number};
    expect(value.sum).toBeCloseTo(1.0, 6); // 1000ms = 1.0s
    expect(value.count).toBe(1);
  });

  test('passes attributes through to the underlying histogram', async () => {
    const h = getOrCreateLatencyHistogram(
      'sync',
      'test-attrs',
      'Test attributes passthrough',
    );

    h.recordMs(5, {table: 'users', type: 'add'});

    const {resourceMetrics} = await reader.collectMetrics();
    const metric = resourceMetrics.scopeMetrics
      .flatMap(sm => sm.metrics)
      .find(m => m.descriptor.name === 'zero.sync.test-attrs');
    expect(metric).toBeDefined();

    const dp = metric!.dataPoints[0];
    expect(dp.attributes).toEqual({table: 'users', type: 'add'});
  });
});
