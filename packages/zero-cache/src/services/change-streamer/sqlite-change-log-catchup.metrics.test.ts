/**
 * `observability/metrics.ts` caches every instrument in module scope, so an
 * instrument created against a previous test's meter provider would be handed
 * back to the next one. Each test therefore resets modules and imports through
 * a fresh provider, matching `snapshot-reservations.metrics.test.ts`.
 */

import {metrics, type Attributes} from '@opentelemetry/api';
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import {LogContext} from '@rocicorp/logger';
import {afterEach, expect, test, vi} from 'vitest';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {Subscription} from '../../types/subscription.ts';
import type {WatermarkedChange} from './change-streamer.ts';
import {Forwarder} from './forwarder.ts';
import type {SQLiteChangeLogCatchupReader} from './sqlite-change-log-catchup.ts';
import type {CatchupPlan} from './sqlite-change-log-reader.ts';
import {Subscriber} from './subscriber.ts';

afterEach(() => {
  metrics.disable();
  vi.resetModules();
});

const CATCHUP_RESULTS = 'zero.replication.sqlite_change_log.catchup_results';

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
  return {exporter, provider};
}

function pointsFor(
  exporter: InMemoryMetricExporter,
  name: string,
): {attributes: Attributes; value: number}[] {
  return (
    exporter
      .getMetrics()
      .at(-1)
      ?.scopeMetrics.flatMap(scope => scope.metrics)
      .find(metric => metric.descriptor.name === name)
      ?.dataPoints.map(point => ({
        attributes: point.attributes,
        value: point.value as number,
      })) ?? []
  );
}

/** A log that is always ready and never has anything to hand back. */
class EmptyReader implements SQLiteChangeLogCatchupReader {
  plan(): CatchupPlan {
    return {kind: 'range', minWatermark: '01', headWatermark: '06'};
  }

  read(): AsyncIterable<readonly WatermarkedChange[]> {
    return (async function* () {})();
  }

  close(): void {}
}

test('log_warm is recorded per catchup, not per coordinator', async () => {
  const {exporter, provider} = withProvider();
  try {
    const {SQLiteChangeLogCatchup} =
      await import('./sqlite-change-log-catchup.ts');
    const lc = new LogContext('error', undefined, new TestLogSink());
    using coordinator = new SQLiteChangeLogCatchup(
      lc,
      new Forwarder(lc),
      new EmptyReader(),
      {batchSize: 2, barrierTimeoutMs: 1_000},
    );

    // One coordinator serves both: `coldReadPercent` routes some subscribers
    // to a log that is still inside its warm-up window, so the classification
    // cannot be a property of the coordinator they share.
    for (const [id, logWarm] of [
      ['warm-subscriber', true],
      ['cold-subscriber', false],
    ] as const) {
      const downstream = Subscription.create<string>();
      const subscriber = new Subscriber(5, id, '01', downstream, () => ({
        tag: 'status',
      }));
      expect(
        await coordinator.catchup(subscriber, () => '06', {logWarm}),
      ).toEqual({kind: 'registered'});
      await subscriber.setCaughtUp();
    }

    await provider.forceFlush();
    const classifications = pointsFor(exporter, CATCHUP_RESULTS).map(
      ({attributes}) => [attributes.classification, attributes.log_warm],
    );
    expect(classifications).toHaveLength(2);
    expect(classifications).toEqual(
      expect.arrayContaining([
        ['range', true],
        ['range', false],
      ]),
    );
  } finally {
    await provider.shutdown();
  }
});
