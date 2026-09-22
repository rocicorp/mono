import {metrics} from '@opentelemetry/api';
import {
  AggregationTemporality,
  DataPointType,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import {afterEach, describe, expect, test, vi} from 'vitest';

afterEach(() => {
  metrics.disable();
  vi.resetModules();
});

describe('createCopyMetricBatcher', () => {
  test('flushes below, at, and above the byte threshold', async () => {
    const {COPY_METRIC_BATCH_BYTES, createCopyMetricBatcher} =
      await import('./initial-sync.ts');
    const record = vi.fn();
    const batcher = createCopyMetricBatcher(record);

    batcher.add(COPY_METRIC_BATCH_BYTES - 1);
    expect(record).not.toHaveBeenCalled();

    batcher.add(1);
    expect(record).toHaveBeenLastCalledWith(COPY_METRIC_BATCH_BYTES, 2);

    batcher.add(COPY_METRIC_BATCH_BYTES + 1);
    expect(record).toHaveBeenLastCalledWith(COPY_METRIC_BATCH_BYTES + 1, 1);
  });

  test('flushes residual totals only once', async () => {
    const {createCopyMetricBatcher} = await import('./initial-sync.ts');
    const record = vi.fn();
    const batcher = createCopyMetricBatcher(record);

    batcher.add(100);
    batcher.add(200);
    batcher.flush();
    batcher.flush();

    expect(record).toHaveBeenCalledOnce();
    expect(record).toHaveBeenCalledWith(300, 2);
  });
});

test('exports exact byte and chunk totals with active OTel', async () => {
  const {COPY_METRIC_BATCH_BYTES, initialSyncCopyMetrics} =
    await import('./initial-sync.ts');
  const exporter = new InMemoryMetricExporter(
    AggregationTemporality.CUMULATIVE,
  );
  const provider = new MeterProvider({
    readers: [
      new PeriodicExportingMetricReader({
        exporter,
        exportIntervalMillis: 60_000,
      }),
    ],
  });

  try {
    expect(metrics.setGlobalMeterProvider(provider)).toBe(true);
    const copyMetrics = initialSyncCopyMetrics({
      syncMode: 'initial',
      copyFormat: 'binary',
    });

    copyMetrics.add(COPY_METRIC_BATCH_BYTES - 1);
    copyMetrics.add(2);
    copyMetrics.add(3);
    copyMetrics.flush();
    await provider.forceFlush();

    expect(
      metricValue(exporter, 'zero.replication.initial_sync_copy_stream'),
    ).toBe(COPY_METRIC_BATCH_BYTES + 4);
    expect(
      metricValue(exporter, 'zero.replication.initial_sync_copy_chunks'),
    ).toBe(3);
  } finally {
    await provider.shutdown();
  }
});

test('works with the no-op OTel provider', async () => {
  const {COPY_METRIC_BATCH_BYTES, initialSyncCopyMetrics} =
    await import('./initial-sync.ts');
  const copyMetrics = initialSyncCopyMetrics({
    syncMode: 'shadow',
    copyFormat: 'text',
  });

  expect(() => {
    copyMetrics.add(COPY_METRIC_BATCH_BYTES);
    copyMetrics.add(1);
    copyMetrics.flush();
    copyMetrics.flush();
  }).not.toThrow();
});

function metricValue(exporter: InMemoryMetricExporter, name: string) {
  const metric = exporter
    .getMetrics()
    .flatMap(resource => resource.scopeMetrics)
    .flatMap(scope => scope.metrics)
    .find(metric => metric.descriptor.name === name);

  expect(metric).toBeDefined();
  if (metric === undefined) {
    throw new Error(`${name} was not exported`);
  }
  expect(metric.dataPointType).toBe(DataPointType.SUM);
  if (metric.dataPointType !== DataPointType.SUM) {
    throw new Error(`${name} was not a sum`);
  }
  expect(metric.dataPoints).toHaveLength(1);
  expect(metric.dataPoints[0]?.attributes).toEqual({
    sync_mode: 'initial',
    copy_format: 'binary',
  });
  return metric.dataPoints[0]?.value;
}
