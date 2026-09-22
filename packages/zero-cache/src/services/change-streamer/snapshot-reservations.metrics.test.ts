/**
 * `observability/metrics.ts` caches every instrument in module scope, so an
 * instrument created against a previous test's meter provider would be
 * handed back to the next one. Each test therefore resets modules and
 * imports through a fresh provider, matching `backup-monitor.metrics.test.ts`.
 */

import {metrics, type Attributes} from '@opentelemetry/api';
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import {afterEach, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {BackupConfig} from './change-streamer-service.ts';

afterEach(() => {
  metrics.disable();
  vi.resetModules();
});

const CONFIRM_DURATION =
  'zero.replica.litestream.snapshot.reservation_confirm_duration';
const RESERVATION_DURATION =
  'zero.replica.litestream.snapshot.reservation_duration';

const BACKUP_CONFIG: BackupConfig = {
  backupURL: 's3://foo/bar',
  litestreamVersion: 'v5',
};

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

/** The most recent export's points for one instrument, with their attributes. */
function pointsFor(
  exporter: InMemoryMetricExporter,
  name: string,
): {attributes: Attributes; count: number}[] {
  return (
    exporter
      .getMetrics()
      .at(-1)
      ?.scopeMetrics.flatMap(scope => scope.metrics)
      .find(metric => metric.descriptor.name === name)
      ?.dataPoints.map(point => ({
        attributes: point.attributes,
        count: (point.value as {count: number}).count,
      })) ?? []
  );
}

test('confirmFor() records the wait by the source the bounds came from', async () => {
  const {exporter, provider} = withProvider();
  try {
    const {SnapshotReservations} = await import('./snapshot-reservations.ts');
    const reservations = new SnapshotReservations(
      createSilentLogContext(),
      BACKUP_CONFIG,
    );
    reservations.open('task-1');
    reservations.open('task-2');
    reservations.confirmFor('task-1', 'replica-v1', 'sqlite-min', 'sqlite');
    reservations.confirmFor('task-2', 'replica-v1', 'pg-min', 'pg');
    // Already confirmed: not a second wait.
    reservations.confirmFor('task-1', 'replica-v1', 'sqlite-min', 'sqlite');

    await provider.forceFlush();
    const points = pointsFor(exporter, CONFIRM_DURATION);
    expect(
      points.map(({attributes, count}) => [attributes.source, count]).sort(),
    ).toEqual([
      ['pg', 1],
      ['sqlite', 1],
    ]);
    expect(points[0].attributes).toMatchObject({
      role: 'view_syncer',
      backup_scheme: 's3',
      litestream: 'v5',
    });
  } finally {
    await provider.shutdown();
  }
});

test('a reservation that never confirmed is distinguishable at close', async () => {
  const {exporter, provider} = withProvider();
  try {
    const {SnapshotReservations} = await import('./snapshot-reservations.ts');
    const reservations = new SnapshotReservations(
      createSilentLogContext(),
      BACKUP_CONFIG,
    );

    // Confirmed, then closed by the service once the follower subscribed.
    reservations.open('confirmed-task');
    reservations.confirmFor(
      'confirmed-task',
      'replica-v1',
      'sqlite-min',
      'sqlite',
    );
    reservations.close('confirmed-task');

    // Gave up while still waiting for its bounds.
    const pending = reservations.open('waiting-task');
    pending.cancel();

    await provider.forceFlush();
    expect(
      pointsFor(exporter, RESERVATION_DURATION).map(({attributes}) => [
        attributes.result,
        attributes.confirmed,
      ]),
    ).toEqual([
      ['closed', true],
      ['cancelled', false],
    ]);
    // No confirmation was recorded for the reservation that never got one.
    expect(pointsFor(exporter, CONFIRM_DURATION)).toHaveLength(1);
  } finally {
    await provider.shutdown();
  }
});
