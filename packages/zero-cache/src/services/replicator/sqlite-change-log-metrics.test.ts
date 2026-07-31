/**
 * Asserts the exported values of the SQLite change log's metrics, which the
 * observability tests beside this file cannot: `observability/metrics.ts`
 * caches every instrument in module scope, so an instrument created against a
 * previous test's meter provider would be handed back to the next one. Each
 * test here therefore resets modules and imports through a fresh provider,
 * matching `change-source/pg/initial-sync-metrics.test.ts`.
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
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {ReseedReason} from './change-log-db.ts';
import type {
  ChangeLogCommit,
  SQLiteChangeLogInfo,
} from './sqlite-change-log-observability.ts';

afterEach(() => {
  metrics.disable();
  vi.resetModules();
});

const METRIC = 'zero.replica.sqlite_change_log';

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

function dataPoints(exporter: InMemoryMetricExporter, name: string) {
  const metric = exporter
    .getMetrics()
    .flatMap(resource => resource.scopeMetrics)
    .flatMap(scope => scope.metrics)
    .find(metric => metric.descriptor.name === name);
  return metric?.dataPoints;
}

function counts(exporter: InMemoryMetricExporter, name: string) {
  return Object.fromEntries(
    (dataPoints(exporter, name) ?? []).map(point => [
      // Every counter here is either unlabeled or labeled by `reason`.
      point.attributes.reason ?? '',
      point.value,
    ]),
  );
}

function histogram(
  exporter: InMemoryMetricExporter,
  name: string,
  attributes: Record<string, string> = {},
) {
  const points = (dataPoints(exporter, name) ?? []).filter(point =>
    Object.entries(attributes).every(
      ([key, value]) => point.attributes[key] === value,
    ),
  );
  expect(points).toHaveLength(1);
  const {value} = points[0];
  expect(typeof value).toBe('object');
  return value as {count: number; sum: number | undefined};
}

test('each reseed reason increments reconcile_wipes with its own label', async () => {
  await using otel = withProvider();
  const {recordSQLiteChangeLogReconcile} =
    await import('./sqlite-change-log-observability.ts');
  const lc = createSilentLogContext();
  const reasons: ReseedReason[] = [
    'created',
    'schema-mismatch',
    'identity-mismatch',
    'gap',
  ];

  for (const reason of reasons) {
    recordSQLiteChangeLogReconcile(lc, {
      action: 'reseeded',
      head: '05',
      reason,
    });
  }
  // Twice, to show the `gap` series counts rather than latches. A nonzero rate
  // in steady state is the alert that the log is not leading the resume
  // watermark.
  recordSQLiteChangeLogReconcile(lc, {
    action: 'reseeded',
    head: '05',
    reason: 'gap',
  });
  recordSQLiteChangeLogReconcile(lc, {
    action: 'truncated',
    head: '05',
    rows: 27,
  });
  recordSQLiteChangeLogReconcile(lc, {action: 'none', head: '05'});
  await otel.provider.forceFlush();

  expect(counts(otel.exporter, `${METRIC}.reconcile_wipes`)).toEqual({
    'created': 1,
    'schema-mismatch': 1,
    'identity-mismatch': 1,
    'gap': 2,
  });
  expect(counts(otel.exporter, `${METRIC}.reconcile_truncated_rows`)).toEqual({
    '': 27,
  });
});

test('a consistent log emits no wipe or truncation series at all', async () => {
  await using otel = withProvider();
  const {recordSQLiteChangeLogReconcile} =
    await import('./sqlite-change-log-observability.ts');

  recordSQLiteChangeLogReconcile(createSilentLogContext(), {
    action: 'none',
    head: '05',
  });
  await otel.provider.forceFlush();

  expect(
    dataPoints(otel.exporter, `${METRIC}.reconcile_wipes`),
  ).toBeUndefined();
  expect(
    dataPoints(otel.exporter, `${METRIC}.reconcile_truncated_rows`),
  ).toBeUndefined();
});

const BEGIN: ChangeStreamData = [
  'begin',
  {tag: 'begin'},
  {commitWatermark: '06'},
];
const COMMIT: ChangeStreamData = ['commit', {tag: 'commit'}, {watermark: '06'}];

const COMMIT_RESULT: ChangeLogCommit = {
  watermark: '06',
  stats: {rows: 2, estimatedBytes: 100, hash: '0'.repeat(64)},
  logCommitMs: 4,
};

const INFO: SQLiteChangeLogInfo = {
  epoch: null,
  generation: '01',
  replicaID: null,
  schemaVersion: 2,
  seededAtMs: 1_700_000_000_000,
  seedWatermark: '02',
  headWatermark: '02',
  rows: 2,
  estimatedBytes: 100,
};

test('records the commit that sits on the forward path', async () => {
  await using otel = withProvider();
  const {SQLiteChangeLogObserver} =
    await import('./sqlite-change-log-observability.ts');
  const observer = new SQLiteChangeLogObserver(createSilentLogContext(), INFO);

  observer.messageProcessed(BEGIN, null, 1);
  // The 20 ms is the whole write of the commit message; the commit inside it is
  // the part that precedes the forward.
  observer.messageProcessed(COMMIT, COMMIT_RESULT, 20);
  await otel.provider.forceFlush();

  // Seconds, per the latency-histogram convention.
  expect(
    histogram(otel.exporter, `${METRIC}.log_commit_duration`),
  ).toMatchObject({count: 1, sum: 0.004});
  // There is no second commit to order against any more: the replica's belongs
  // to a subscriber downstream of this loop.
  expect(
    dataPoints(otel.exporter, `${METRIC}.replica_commit_duration`),
  ).toBeUndefined();
  expect(
    dataPoints(otel.exporter, `${METRIC}.commit_duration`),
  ).toBeUndefined();
});

test('a failed commit times the attempt but not the commit', async () => {
  await using otel = withProvider();
  const {SQLiteChangeLogObserver} =
    await import('./sqlite-change-log-observability.ts');
  const observer = new SQLiteChangeLogObserver(createSilentLogContext(), INFO);

  observer.messageProcessed(BEGIN, null, 1);
  observer.messageFailed(COMMIT, new Error('commit failed'), 20);
  await otel.provider.forceFlush();

  // A failed commit produces no statistics, so there is nothing to attribute a
  // duration to: the attempt is timed only by the message-processing histogram.
  expect(
    dataPoints(otel.exporter, `${METRIC}.log_commit_duration`),
  ).toBeUndefined();
  expect(
    histogram(otel.exporter, `${METRIC}.message_processing_duration`, {
      tag: 'commit',
      outcome: 'error',
    }),
  ).toMatchObject({count: 1, sum: 0.02});
});
