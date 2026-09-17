import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {listTables} from '../../db/lite-tables.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import {populateFromExistingTables} from '../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../replicator/test-utils.ts';
import {PipelineDriver, type RowChange, type Timer} from './pipeline-driver.ts';
import {Snapshotter} from './snapshotter.ts';
import {TimeSliceTimer} from './view-syncer.ts';

/**
 * A `{scalar: true}` gate over a *compound* correlation used to be honored
 * whenever the subquery was provably single-row, and the rewrite then compared
 * only the first correlation pair.
 *
 * With `(parent.a, parent.b) = (child.x, child.y)` and the pinned child row
 * `{x: 'x1', y: 'y1'}`, the gate means `a = 'x1' AND b = 'y1'`, but the
 * rewrite emitted `a = 'x1'` alone. On this fixture that admitted `p2`
 * (`a='x1', b='y2'`), which the EXISTS excludes — verified against `main` at
 * 16019afa4, where hydration returns `['p1', 'p2']` instead of `['p1']`.
 *
 * Worse, the companion watched only `childField[0]`, so editing the child's
 * `y` left the resolved value unchanged, raised no `ResetPipelinesSignal`, and
 * left the wrong parent set in place indefinitely. That is what the last test
 * here covers.
 */
const NO_TIME_ADVANCEMENT_TIMER: Timer = {
  elapsedLap: () => 0,
  totalElapsed: () => 0,
};

describe('view-syncer/pipeline-driver compound scalar correlation', () => {
  const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
  const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
  let dbFile: DbFile;
  let lc: LogContext;
  let logSink: TestLogSink;
  let pipelines: PipelineDriver;
  let replicator: FakeReplicator;

  beforeEach(() => {
    logSink = new TestLogSink();
    lc = new LogContext('warn', undefined, logSink);
    dbFile = new DbFile('compound_scalar_test');
    dbFile.connect(lc).pragma('journal_mode = wal2');

    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    pipelines = new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
      shardID,
      new DatabaseStorage(storage).createClientGroupStorage('foo-client-group'),
      'pipeline-driver.compound-scalar.test.ts',
      new InspectorDelegate(undefined),
      () => 200,
    );

    const db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], '123');
    db.exec(/*sql*/ `
      CREATE TABLE "${mutationsTableName}" (
        "clientGroupID"  TEXT,
        "clientID"       TEXT,
        "mutationID"     INTEGER,
        "result"         TEXT,
        _0_version       TEXT NOT NULL,
        PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
      );
      CREATE TABLE parent (
        id TEXT PRIMARY KEY,
        a TEXT,
        b TEXT,
        _0_version TEXT NOT NULL
      );
      CREATE TABLE child (
        id TEXT PRIMARY KEY,
        x TEXT,
        y TEXT,
        _0_version TEXT NOT NULL
      );

      INSERT INTO child  (id, x, y, _0_version) VALUES ('c1', 'x1', 'y1', '123');
      -- p1 matches both pairs; p2 matches only the first; p3 only the second.
      INSERT INTO parent (id, a, b, _0_version) VALUES ('p1', 'x1', 'y1', '123');
      INSERT INTO parent (id, a, b, _0_version) VALUES ('p2', 'x1', 'y2', '123');
      INSERT INTO parent (id, a, b, _0_version) VALUES ('p3', 'x9', 'y1', '123');
      `);
    populateFromExistingTables(db, listTables(db, false));
    replicator = fakeReplicator(lc, db);
    pipelines.init(clientSchema);
  });

  afterEach(() => {
    dbFile.delete();
  });

  const clientSchema = createSchema({
    tables: [
      table('parent')
        .columns({id: string(), a: string(), b: string()})
        .primaryKey('id'),
      table('child')
        .columns({id: string(), x: string(), y: string()})
        .primaryKey('id'),
    ],
  });

  const messages = new ReplicationMessages({
    parent: 'id',
    child: 'id',
    [mutationsTableName]: ['clientGroupID', 'clientID', 'mutationID'],
  });

  /** The repro from the reachability trace, verbatim in AST form. */
  const COMPOUND_SCALAR: AST = {
    table: 'parent',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      scalar: true,
      related: {
        system: 'client',
        correlation: {parentField: ['a', 'b'], childField: ['x', 'y']},
        subquery: {
          table: 'child',
          alias: 'zsubq_child',
          orderBy: [['id', 'asc']],
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'id'},
            right: {type: 'literal', value: 'c1'},
          },
        },
      },
    },
  };

  function hydrate(ast: AST): RowChange[] {
    return [
      ...pipelines.addQuery(
        'hash',
        'q',
        ast,
        new TimeSliceTimer(lc).startWithoutYielding(),
      ),
    ].filter(change => change !== 'yield');
  }

  function parentIDs(changes: readonly RowChange[]): string[] {
    return changes
      .filter(c => c.table === 'parent')
      .map(c => c.rowKey.id as string)
      .sort();
  }

  test('only the parent matching every correlation pair is admitted', () => {
    // Against main at 16019afa4 this returns ['p1', 'p2']: the rewrite kept
    // `a = 'x1'` and dropped `b = 'y1'`.
    expect(parentIDs(hydrate(COMPOUND_SCALAR))).toEqual(['p1']);
  });

  test('the gate survives as a real EXISTS and the hint is reported', () => {
    hydrate(COMPOUND_SCALAR);

    expect(pipelines.queries().get('q')?.transformedAst.where?.type).toBe(
      'correlatedSubquery',
    );
    expect(
      logSink.messages
        .filter(([level]) => level === 'warn')
        .map(([, , args]) => String(args[0])),
    ).toMatchInlineSnapshot(`
      [
        "Ignoring {scalar: true} on the "child" subquery of query q: its relationship correlates more than one column, and a scalar rewrite can only compare one. The gate runs as a plain EXISTS.",
      ]
    `);
  });

  test('changing the second correlated column moves the parent set', () => {
    // The case the old companion could not see: `x` never changes, so the
    // resolved value stayed 'x1', no reset fired, and the stale parent set
    // survived. As a plain EXISTS the change streams incrementally instead.
    expect(parentIDs(hydrate(COMPOUND_SCALAR))).toEqual(['p1']);

    replicator.processTransaction(
      '134',
      messages.update('child', {id: 'c1', x: 'x1', y: 'y2'}),
    );

    const changes = [
      ...pipelines.advance(NO_TIME_ADVANCEMENT_TIMER).changes,
    ].filter(change => change !== 'yield');

    // p1 no longer matches `b = y`, and p2 now does.
    expect(
      changes
        .filter(c => c.table === 'parent')
        .map(c => `${c.type === 1 ? 'remove' : 'add'} ${c.rowKey.id as string}`)
        .sort(),
    ).toEqual(['add p2', 'remove p1']);
  });

  test('the compound gate agrees with the same query written without the hint', () => {
    const gate = COMPOUND_SCALAR.where as {scalar?: boolean};
    const withoutHint: AST = {
      ...COMPOUND_SCALAR,
      where: {...gate, scalar: undefined} as AST['where'],
    };

    const scalar = parentIDs(hydrate(COMPOUND_SCALAR));
    pipelines.removeQuery('q');
    const plain = parentIDs(hydrate(withoutHint));

    expect(scalar).toEqual(plain);
  });
});
