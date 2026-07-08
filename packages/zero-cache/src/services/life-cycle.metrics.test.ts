import {metrics} from '@opentelemetry/api';
import {
  AggregationTemporality,
  DataPointType,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import {expect, test, vi} from 'vitest';

test('records startup_duration if OTel starts after module import', async () => {
  metrics.disable();
  vi.resetModules();

  const {recordStartupDurationMs} = await import('./life-cycle.ts');
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

    recordStartupDurationMs(123);
    await provider.forceFlush();

    const startupDuration = exporter
      .getMetrics()
      .flatMap(resource => resource.scopeMetrics)
      .flatMap(scope => scope.metrics)
      .find(
        metric => metric.descriptor.name === 'zero.server.startup_duration',
      );

    expect(startupDuration).toBeDefined();
    if (startupDuration === undefined) {
      throw new Error('zero.server.startup_duration was not exported');
    }

    expect(startupDuration.dataPointType).toBe(DataPointType.HISTOGRAM);
    if (startupDuration.dataPointType !== DataPointType.HISTOGRAM) {
      throw new Error('zero.server.startup_duration was not a histogram');
    }

    expect(startupDuration.dataPoints).toHaveLength(1);
    expect(startupDuration.dataPoints[0]).toEqual(
      expect.objectContaining({
        attributes: {component: 'dispatcher'},
        value: expect.objectContaining({count: 1}),
      }),
    );
  } finally {
    await provider.shutdown();
    metrics.disable();
  }
});
