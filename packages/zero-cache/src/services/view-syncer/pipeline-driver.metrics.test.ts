/**
 * Asserts the attributes that `zero.sync.ivm.advance-time` exports, which the
 * behavioral tests beside this file cannot: `observability/metrics.ts` caches
 * every instrument in module scope, so an instrument created against a previous
 * test's meter provider would be handed back to the next one. This file
 * therefore resets modules and imports the driver through a fresh provider,
 * matching `replicator/sqlite-change-log-metrics.test.ts`.
 */

import {metrics} from '@opentelemetry/api';
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import {LogContext} from '@rocicorp/logger';
import {afterEach, expect, test, vi} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import type {Timer} from './pipeline-driver.ts';

afterEach(() => {
  metrics.disable();
  vi.resetModules();
});

const METRIC = 'zero.sync.ivm.advance-time';

const SHARD_ID: ShardID = {appID: 'zeroz', shardNum: 1};
const MUTATIONS_TABLE = `${upstreamSchema(SHARD_ID)}.mutations`;

const ISSUES_AND_COMMENTS: AST = {
  table: 'issues',
  orderBy: [['id', 'desc']],
  related: [
    {
      system: 'client',
      correlation: {parentField: ['id'], childField: ['issueID']},
      subquery: {
        table: 'comments',
        alias: 'comments',
        orderBy: [['id', 'desc']],
      },
    },
  ],
};

const NO_TIME_ADVANCEMENT_TIMER: Timer = {
  elapsedLap: () => 0,
  totalElapsed: () => 0,
};

const clientSchema = createSchema({
  tables: [
    table('issues').columns({id: string(), closed: boolean()}).primaryKey('id'),
    table('comments')
      .columns({id: string(), issueID: string(), upvotes: number()})
      .primaryKey('id'),
  ],
});

/**
 * Builds a replica with one `issues`/`comments` pipeline, through modules
 * imported after the meter provider is installed so the driver's instruments
 * are created against `exporter`.
 */
async function setup() {
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

  const [
    {PipelineDriver},
    {Snapshotter},
    {CREATE_STORAGE_TABLE, DatabaseStorage},
    {Database},
    {InspectorDelegate},
    {DbFile},
    {initReplicationState},
    {populateFromExistingTables},
    {listTables},
    {ReplicationMessages, fakeReplicator},
  ] = await Promise.all([
    import('./pipeline-driver.ts'),
    import('./snapshotter.ts'),
    import('../../../../zqlite/src/database-storage.ts'),
    import('../../../../zqlite/src/db.ts'),
    import('../../server/inspector-delegate.ts'),
    import('../../test/lite.ts'),
    import('../replicator/schema/replication-state.ts'),
    import('../replicator/schema/column-metadata.ts'),
    import('../../db/lite-tables.ts'),
    import('../replicator/test-utils.ts'),
  ]);

  const lc = new LogContext('error', undefined, new TestLogSink());
  const dbFile = new DbFile('pipeline_driver_metrics_test');
  dbFile.connect(lc).pragma('journal_mode = wal2');

  const storage = new Database(lc, ':memory:');
  storage.prepare(CREATE_STORAGE_TABLE).run();

  const db = dbFile.connect(lc);
  initReplicationState(db, ['zero_data'], '123');
  db.exec(/*sql*/ `
    CREATE TABLE "${MUTATIONS_TABLE}" (
      "clientGroupID"  TEXT,
      "clientID"       TEXT,
      "mutationID"     INTEGER,
      "result"         TEXT,
      _0_version       TEXT NOT NULL,
      PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
    );
    CREATE TABLE issues (
      id TEXT PRIMARY KEY,
      closed BOOL,
      _0_version TEXT NOT NULL
    );
    CREATE TABLE comments (
      id TEXT PRIMARY KEY,
      issueID TEXT,
      upvotes INTEGER,
      _0_version TEXT NOT NULL
    );

    INSERT INTO issues (id, closed, _0_version) VALUES ('1', 0, '123');
    INSERT INTO comments (id, issueID, upvotes, _0_version) VALUES ('10', '1', 0, '123');
    `);
  populateFromExistingTables(db, listTables(db, false));

  const pipelines = new PipelineDriver(
    lc,
    testLogConfig,
    new Snapshotter(lc, dbFile.path, {appID: SHARD_ID.appID}),
    SHARD_ID,
    new DatabaseStorage(storage).createClientGroupStorage('foo-client-group'),
    'pipeline-driver.metrics.test.ts',
    new InspectorDelegate(undefined),
    () => 200,
  );
  pipelines.init(clientSchema);
  [
    ...pipelines.addQuery(
      'hash1',
      'queryID1',
      ISSUES_AND_COMMENTS,
      NO_TIME_ADVANCEMENT_TIMER,
    ),
  ];

  return {
    exporter,
    provider,
    replicator: fakeReplicator(lc, db),
    messages: new ReplicationMessages({
      issues: 'id',
      comments: 'id',
      [MUTATIONS_TABLE]: ['clientGroupID', 'clientID', 'mutationID'],
    }),
    advance: () => [...pipelines.advance(NO_TIME_ADVANCEMENT_TIMER).changes],
    [Symbol.asyncDispose]: async () => {
      pipelines.destroy();
      dbFile.delete();
      await provider.shutdown();
    },
  };
}

/** `table`/`type` label pairs of every recorded observation, with counts. */
function advanceCounts(exporter: InMemoryMetricExporter) {
  const points =
    exporter
      .getMetrics()
      .flatMap(resource => resource.scopeMetrics)
      .flatMap(scope => scope.metrics)
      .find(metric => metric.descriptor.name === METRIC)?.dataPoints ?? [];
  return Object.fromEntries(
    points.map(point => [
      `${point.attributes.table}/${point.attributes.type}`,
      (point.value as {count: number}).count,
    ]),
  );
}

test('advance-time attributes each change to the table and change type', async () => {
  await using ctx = await setup();

  ctx.replicator.processTransaction(
    '134',
    ctx.messages.insert('issues', {id: '2', closed: 0}),
  );
  ctx.advance();

  ctx.replicator.processTransaction(
    '135',
    ctx.messages.update('comments', {id: '10', issueID: '1', upvotes: 5}),
  );
  ctx.advance();

  ctx.replicator.processTransaction(
    '136',
    ctx.messages.delete('comments', {id: '10'}),
  );
  ctx.advance();

  await ctx.provider.forceFlush();

  expect(advanceCounts(ctx.exporter)).toEqual({
    'issues/add': 1,
    'comments/edit': 1,
    'comments/remove': 1,
  });
});

test('a primary key change is attributed as a remove and an add', async () => {
  await using ctx = await setup();

  ctx.replicator.processTransaction(
    '134',
    ctx.messages.update(
      'comments',
      {id: '11', issueID: '1', upvotes: 0},
      {id: '10'},
    ),
  );
  ctx.advance();

  await ctx.provider.forceFlush();

  expect(advanceCounts(ctx.exporter)).toEqual({
    'comments/remove': 1,
    'comments/add': 1,
  });
});
