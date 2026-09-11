import {metrics} from '@opentelemetry/api';
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import {afterEach, expect, test, vi} from 'vitest';

afterEach(() => {
  metrics.disable();
  vi.resetModules();
});

function withProvider() {
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
  expect(metrics.setGlobalMeterProvider(provider)).toBe(true);
  return {
    exporter,
    provider,
    [Symbol.asyncDispose]: () => provider.shutdown(),
  };
}

function points(exporter: InMemoryMetricExporter, name: string) {
  return (
    exporter
      .getMetrics()
      .flatMap(resource => resource.scopeMetrics)
      .flatMap(scope => scope.metrics)
      .find(metric => metric.descriptor.name === name)?.dataPoints ?? []
  );
}

/** Blocks the loop across several ticks, the way a busy sync worker does. */
async function saturateFor(ms: number) {
  const until = performance.now() + ms;
  while (performance.now() < until) {
    const tick = performance.now() + 50;
    while (performance.now() < tick) {
      // Deliberately blocking.
    }
    await new Promise(resolve => setTimeout(resolve, 1));
  }
}

test('a saturated loop shows up as delay and utilization', async () => {
  await using otel = withProvider();
  const {startEventLoopMonitor} = await import('./event-loop.ts');
  const stop = startEventLoopMonitor();

  try {
    await saturateFor(200);
    await otel.provider.forceFlush();
  } finally {
    stop();
  }

  const delay = points(otel.exporter, 'zero.server.event_loop_delay');
  expect(delay.map(point => point.attributes.stat)).toEqual([
    'mean',
    'p50',
    'p99',
    'max',
  ]);
  // Well above the 10ms sampling floor and below the 50ms blocks, so this
  // fails if the monitor reports its own resolution instead of the loop.
  expect(
    delay.find(point => point.attributes.stat === 'max')?.value ?? 0,
  ).toBeGreaterThan(25);

  const utilization = points(
    otel.exporter,
    'zero.server.event_loop_utilization',
  );
  expect(utilization).toHaveLength(1);
  expect(utilization[0].attributes).toEqual({});
  expect(utilization[0].value).toBeGreaterThan(0.5);
  expect(utilization[0].value).toBeLessThanOrEqual(1);
});

test('stops observing once stopped', async () => {
  await using otel = withProvider();
  const {startEventLoopMonitor} = await import('./event-loop.ts');

  startEventLoopMonitor()();
  await otel.provider.forceFlush();

  expect(points(otel.exporter, 'zero.server.event_loop_delay')).toEqual([]);
  expect(points(otel.exporter, 'zero.server.event_loop_utilization')).toEqual(
    [],
  );
});
