/**
 * Asserts the exported value of the v17 migration's unresolvable-row counter,
 * which `backfilling.test.ts` cannot: `observability/metrics.ts` caches every
 * instrument in module scope, so an instrument created against a previous
 * test's meter provider would be handed back to the next one. Each test here
 * therefore resets modules and imports through a fresh provider, matching
 * `replicator/sqlite-change-log-metrics.test.ts`.
 */

import {metrics} from '@opentelemetry/api';
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import {afterEach, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {CREATE_COLUMN_METADATA_TABLE} from './column-metadata.ts';
import {CREATE_TABLE_METADATA_TABLE} from './table-metadata.ts';

afterEach(() => {
  metrics.disable();
  vi.resetModules();
});

const METRIC = 'zero.replica.backfilling_unresolvable_rows';

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

function total(exporter: InMemoryMetricExporter, name: string) {
  const points = exporter
    .getMetrics()
    .flatMap(resource => resource.scopeMetrics)
    .flatMap(scope => scope.metrics)
    .find(metric => metric.descriptor.name === name)?.dataPoints;
  return points?.reduce((sum, point) => sum + (point.value as number), 0);
}

async function migrate(columnMetadata: [string, string, string | null][]) {
  const {CREATE_BACKFILLING_TABLE, populateBackfillingFromColumnMetadata} =
    await import('./backfilling.ts');
  const lc = createSilentLogContext();
  const db = new Database(lc, ':memory:');
  db.exec(
    CREATE_BACKFILLING_TABLE +
      CREATE_TABLE_METADATA_TABLE +
      CREATE_COLUMN_METADATA_TABLE,
  );
  const insert = db.prepare(/*sql*/ `
    INSERT INTO "_zero.column_metadata"
      (table_name, column_name, upstream_type, is_not_null, is_enum, is_array,
       backfill)
      VALUES (?, ?, 'text', 0, 0, 0, ?)
  `);
  columnMetadata.forEach(row => insert.run(...row));
  populateBackfillingFromColumnMetadata(lc, db);
  db.close();
}

test('the counter reports zero when every identity is recoverable', async () => {
  await using otel = withProvider();
  await migrate([
    // No dot in the lite name: `liteTableName()` only omits the schema for
    // `public`, so this is exact.
    ['foo', 'a', '{"fooID":1}'],
    // Not backfilling, so not copied at all.
    ['my.bar', 'b', null],
  ]);
  await otel.provider.forceFlush();

  // Published rather than absent, so that the alert has a series to watch.
  expect(total(otel.exporter, METRIC)).toBe(0);
});

test('the counter reports a dotted table with no metadata row', async () => {
  await using otel = withProvider();
  await migrate([
    ['my.foo', 'a', '{"fooID":1}'],
    ['your.bar', 'b', '{"fooID":2}'],
    ['baz', 'c', '{"fooID":3}'],
  ]);
  await otel.provider.forceFlush();

  expect(total(otel.exporter, METRIC)).toBe(2);
});
