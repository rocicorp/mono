/**
 * Asserts the exported values of the worker-wide IVM time-slice queue metrics.
 * `observability/metrics.ts` caches every instrument in module scope, so this
 * file resets modules and imports `view-syncer.ts` through a fresh provider,
 * matching `replicator/sqlite-change-log-metrics.test.ts`. The reset also
 * clears the module-scoped queue-depth counter between tests.
 */

import {metrics} from '@opentelemetry/api';
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import {afterEach, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';

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

function histogram(exporter: InMemoryMetricExporter, name: string) {
  const points =
    exporter
      .getMetrics()
      .flatMap(resource => resource.scopeMetrics)
      .flatMap(scope => scope.metrics)
      .find(metric => metric.descriptor.name === name)?.dataPoints ?? [];
  expect(points).toHaveLength(1);
  const {count, sum, min, max} = points[0].value as {
    count: number;
    sum: number | undefined;
    min: number | undefined;
    max: number | undefined;
  };
  return {count, sum, min, max};
}

test('an uncontended slice waits behind nobody', async () => {
  await using otel = withProvider();
  const {TimeSliceTimer} = await import('./view-syncer.ts');
  const timer = new TimeSliceTimer(createSilentLogContext());

  await timer.start();
  await timer.yieldProcess();
  timer.stop();
  await otel.provider.forceFlush();

  expect(histogram(otel.exporter, 'zero.sync.ivm.slice-queue-depth')).toEqual({
    count: 2,
    sum: 0,
    min: 0,
    max: 0,
  });
  // The wait is real even with an empty queue: a slice always gives up at
  // least one event-loop iteration.
  expect(histogram(otel.exporter, 'zero.sync.ivm.slice-wait-time').count).toBe(
    2,
  );
});

test('a slice queued behind a peer is counted as queue depth', async () => {
  await using otel = withProvider();
  const {TimeSliceTimer} = await import('./view-syncer.ts');
  const lc = createSilentLogContext();

  // Two client groups asking for a slice in the same tick: the second one
  // joins the queue while the first still holds it.
  const first = new TimeSliceTimer(lc).start();
  const second = new TimeSliceTimer(lc).start();
  (await first).stop();
  (await second).stop();
  await otel.provider.forceFlush();

  expect(histogram(otel.exporter, 'zero.sync.ivm.slice-queue-depth')).toEqual({
    count: 2,
    sum: 1,
    min: 0,
    max: 1,
  });
});
