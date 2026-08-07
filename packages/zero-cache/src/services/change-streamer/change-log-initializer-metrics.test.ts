/**
 * Asserts the exported value of the initialization comparison's classification
 * counter, which
 * `change-log-initializer.test.ts` cannot: `observability/metrics.ts` caches
 * every instrument in module scope, so an instrument created against a previous
 * test's meter provider would be handed back to the next one. Each test here
 * therefore resets modules and imports through a fresh provider, matching
 * `replicator/sqlite-change-log-metrics.test.ts`.
 *
 * `divergent` is the alerting series (`P§:10`) and `inconclusive` is the one
 * that is charted rather than alerted, so the label has to be right on both.
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
import {EMPTY_COOKIE_SET} from '../replicator/change-log-cookies.ts';
import type {InitializationParameters} from './change-log-initializer.ts';

afterEach(() => {
  metrics.disable();
  vi.resetModules();
});

const METRIC = 'zero.replica.sqlite_change_log.init_compare_result';

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

/** The counter's value per `result` label. */
function byResult(exporter: InMemoryMetricExporter) {
  const points =
    exporter
      .getMetrics()
      .flatMap(resource => resource.scopeMetrics)
      .flatMap(scope => scope.metrics)
      .find(metric => metric.descriptor.name === METRIC)?.dataPoints ?? [];
  return Object.fromEntries(
    points.map(point => [point.attributes.result, point.value as number]),
  );
}

const AT = (lastWatermark: string): InitializationParameters => ({
  lastWatermark,
  backfillRequests: [],
  cookies: EMPTY_COOKIE_SET,
});

async function compare(replica: () => InitializationParameters) {
  const {ChangeLogInitializer} = await import('./change-log-initializer.ts');
  const init = new ChangeLogInitializer(
    createSilentLogContext(),
    {initFromPgChangeLog: true, initFromReplica: true},
    {
      pgChangeLog: () => Promise.resolve(AT('02')),
      replica,
      // The fold is never reached: the two cases below are decided at the
      // watermark, and the third never gets as far as a comparison.
      changeLog: () => undefined,
    },
  );
  await init.initialize();
  return init.compare();
}

test('an agreement is counted as equal', async () => {
  await using otel = withProvider();
  expect(await compare(() => AT('02'))).toBe('equal');
  await otel.provider.forceFlush();

  expect(byResult(otel.exporter)).toEqual({equal: 1});
});

test('a log that cannot span the interval is counted as inconclusive', async () => {
  await using otel = withProvider();
  expect(await compare(() => AT('01'))).toBe('inconclusive');
  await otel.provider.forceFlush();

  expect(byResult(otel.exporter)).toEqual({inconclusive: 1});
});

test('a replica that cannot be read is counted as an error', async () => {
  await using otel = withProvider();
  expect(
    await compare(() => {
      throw new Error('replica is gone');
    }),
  ).toBeUndefined();
  await otel.provider.forceFlush();

  // Counted at the read rather than at the comparison, which never runs.
  expect(byResult(otel.exporter)).toEqual({error: 1});
});
