import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {AST, Condition} from '../../../../zero-protocol/src/ast.ts';
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
import {ResetPipelinesSignal, Snapshotter} from './snapshotter.ts';
import {TimeSliceTimer} from './view-syncer.ts';

/**
 * The production shape that motivated parent-literal propagation: a root
 * pinned to one assignment by a literal, gated by `whereExists('assignments',
 * …, {scalar: true})` whose subquery correlates `assignments.id` to that same
 * column but carries no literal of its own.
 *
 * Every mutation here runs against the identical query twice — once with the
 * `{scalar: true}` hint (now honored through the correlation) and once as the
 * plain EXISTS the hint used to degrade to — and the two must agree about
 * which trackers the client is entitled to see.
 */
const NO_TIME_ADVANCEMENT_TIMER: Timer = {
  elapsedLap: () => 0,
  totalElapsed: () => 0,
};

const REMOVE = 1;

describe('view-syncer/pipeline-driver scalar gate propagation', () => {
  const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
  const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
  let dbFile: DbFile;
  let lc: LogContext;
  let logSink: TestLogSink;
  let pipelines: PipelineDriver;
  let replicator: FakeReplicator;

  /**
   * Builds a fresh replica and driver. Called again between the two halves of
   * {@link bothPaths}, which needs each path to see the same starting state.
   */
  function seed(granted = true) {
    dbFile = new DbFile('scalar_propagation_test');
    dbFile.connect(lc).pragma('journal_mode = wal2');

    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    pipelines = new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
      shardID,
      new DatabaseStorage(storage).createClientGroupStorage('foo-client-group'),
      'pipeline-driver.scalar-propagation.test.ts',
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
      CREATE TABLE trackers (
        id TEXT PRIMARY KEY,
        assignmentID TEXT,
        _0_version TEXT NOT NULL
      );
      CREATE TABLE assignments (
        id TEXT PRIMARY KEY,
        ownerID TEXT,
        _0_version TEXT NOT NULL
      );
      CREATE TABLE grants (
        id TEXT PRIMARY KEY,
        assignmentID TEXT,
        teacherID TEXT,
        _0_version TEXT NOT NULL
      );

      INSERT INTO assignments (id, ownerID, _0_version) VALUES ('a1', 'other', '123');
      INSERT INTO assignments (id, ownerID, _0_version) VALUES ('a2', 'other', '123');
      INSERT INTO trackers (id, assignmentID, _0_version) VALUES ('t1', 'a1', '123');
      INSERT INTO trackers (id, assignmentID, _0_version) VALUES ('t2', 'a1', '123');
      INSERT INTO trackers (id, assignmentID, _0_version) VALUES ('t3', 'a2', '123');
      `);
    if (granted) {
      db.exec(/*sql*/ `
        INSERT INTO grants (id, assignmentID, teacherID, _0_version)
          VALUES ('g1', 'a1', 'me', '123');
        `);
    }
    populateFromExistingTables(db, listTables(db, false));
    replicator = fakeReplicator(lc, db);
    pipelines.init(clientSchema);
  }

  beforeEach(() => {
    logSink = new TestLogSink();
    lc = new LogContext('warn', undefined, logSink);
    seed();
  });

  afterEach(() => {
    dbFile.delete();
  });

  const clientSchema = createSchema({
    tables: [
      table('trackers')
        .columns({id: string(), assignmentID: string()})
        .primaryKey('id'),
      table('assignments')
        .columns({id: string(), ownerID: string()})
        .primaryKey('id'),
      table('grants')
        .columns({id: string(), assignmentID: string(), teacherID: string()})
        .primaryKey('id'),
    ],
  });

  const messages = new ReplicationMessages({
    trackers: 'id',
    assignments: 'id',
    grants: 'id',
    [mutationsTableName]: ['clientGroupID', 'clientID', 'mutationID'],
  });

  /** `someone granted me access OR I own it` — no literal on `assignments.id`. */
  const ACCESS: Condition = {
    type: 'or',
    conditions: [
      {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          system: 'client',
          correlation: {parentField: ['id'], childField: ['assignmentID']},
          subquery: {
            table: 'grants',
            alias: 'zsubq_grants',
            orderBy: [['id', 'asc']],
            where: {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'teacherID'},
              right: {type: 'literal', value: 'me'},
            },
          },
        },
      },
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'ownerID'},
        right: {type: 'literal', value: 'me'},
      },
    ],
  };

  function trackersForAssignment(scalar: boolean): AST {
    return {
      table: 'trackers',
      orderBy: [['id', 'asc']],
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'assignmentID'},
            right: {type: 'literal', value: 'a1'},
          },
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            scalar: scalar || undefined,
            related: {
              system: 'client',
              correlation: {parentField: ['assignmentID'], childField: ['id']},
              subquery: {
                table: 'assignments',
                alias: 'zsubq_assignments',
                orderBy: [['id', 'asc']],
                where: ACCESS,
              },
            },
          },
        ],
      },
    };
  }

  const SCALAR = trackersForAssignment(true);
  const EXISTS = trackersForAssignment(false);

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

  /** The rows the query was actually asked for, ignoring gate support rows. */
  function trackerIDs(changes: readonly RowChange[]): string[] {
    return changes
      .filter(c => c.table === 'trackers')
      .map(c => c.rowKey.id as string)
      .sort();
  }

  function warnings(): string[] {
    return logSink.messages
      .filter(([level]) => level === 'warn')
      .map(([, , args]) => String(args[0]));
  }

  /**
   * Hydrates, applies a mutation, and reports what the client ends up
   * entitled to see plus whether the pipeline had to be reset to get there.
   * A reset is how an honored scalar gate invalidates: the view-syncer
   * rehydrates, so both paths are correct as long as the row sets match.
   */
  function afterMutation(ast: AST, mutate: () => void) {
    const before = trackerIDs(hydrate(ast));
    let visible = before;
    mutate();

    try {
      for (const change of pipelines.advance(NO_TIME_ADVANCEMENT_TIMER)
        .changes) {
        if (change === 'yield' || change.table !== 'trackers') {
          continue;
        }
        const id = change.rowKey.id as string;
        visible =
          change.type === REMOVE
            ? visible.filter(t => t !== id)
            : [...new Set([...visible, id])];
      }
    } catch (e) {
      if (!(e instanceof ResetPipelinesSignal)) {
        throw e;
      }
      // What the view-syncer does with the signal: tear the pipelines down
      // and rebuild them on the snapshot the mutation produced.
      pipelines.reset(clientSchema);
      return {before, trackers: trackerIDs(hydrate(ast)), reset: true};
    }
    return {before, trackers: visible.sort(), reset: false};
  }

  /** Runs `mutate` against both gate shapes from the same starting replica. */
  function bothPaths(mutate: () => void, granted = true) {
    dbFile.delete();
    seed(granted);
    const scalar = afterMutation(SCALAR, mutate);
    dbFile.delete();
    seed(granted);
    const exists = afterMutation(EXISTS, mutate);
    return {scalar, exists};
  }

  test('the hint is honored and the gate collapses to the parent literal', () => {
    const changes = hydrate(SCALAR);

    expect(warnings()).toEqual([]);
    expect(pipelines.queries().get('q')?.transformedAst.where).toEqual({
      type: 'and',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'assignmentID'},
          right: {type: 'literal', value: 'a1'},
        },
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'assignmentID'},
          right: {type: 'literal', value: 'a1'},
        },
      ],
    });

    // The trackers the client asked for plus exactly one companion row: the
    // gating assignment.
    expect(changes.map(c => `${c.table}:${c.rowKey.id as string}`)).toEqual([
      'trackers:t1',
      'trackers:t2',
      'assignments:a1',
    ]);
  });

  test('hydration entitles the client to the same trackers as the EXISTS path', () => {
    const scalar = trackerIDs(hydrate(SCALAR));
    pipelines.removeQuery('q');
    const exists = trackerIDs(hydrate(EXISTS));

    expect(scalar).toEqual(['t1', 't2']);
    expect(scalar).toEqual(exists);
  });

  test('the EXISTS path streams the access graph the scalar path does not', () => {
    const tables = hydrate(EXISTS).map(c => c.table);

    expect(tables.filter(t => t === 'grants').length).toBeGreaterThan(0);
    expect(hydrate(SCALAR).map(c => c.table)).not.toContain('grants');
  });

  test('revoking access hides the trackers on both paths', () => {
    const {scalar, exists} = bothPaths(() =>
      replicator.processTransaction(
        '134',
        messages.delete('grants', {id: 'g1'}),
      ),
    );

    expect(scalar.before).toEqual(['t1', 't2']);
    expect(scalar.trackers).toEqual([]);
    expect(scalar.trackers).toEqual(exists.trackers);
    expect(scalar.reset).toBe(true);
  });

  test('granting access reveals the trackers on both paths', () => {
    const {scalar, exists} = bothPaths(
      () =>
        replicator.processTransaction(
          '134',
          messages.insert('grants', {
            id: 'g2',
            assignmentID: 'a1',
            teacherID: 'me',
          }),
        ),
      false,
    );

    expect(scalar.before).toEqual([]);
    expect(scalar.trackers).toEqual(['t1', 't2']);
    expect(scalar.trackers).toEqual(exists.trackers);
    expect(scalar.reset).toBe(true);
  });

  test('deleting the gating assignment hides the trackers on both paths', () => {
    const {scalar, exists} = bothPaths(() =>
      replicator.processTransaction(
        '134',
        messages.delete('assignments', {id: 'a1'}),
      ),
    );

    expect(scalar.before).toEqual(['t1', 't2']);
    expect(scalar.trackers).toEqual([]);
    expect(scalar.trackers).toEqual(exists.trackers);
    expect(scalar.reset).toBe(true);
  });

  test('an ownership change that grants access agrees on both paths', () => {
    const {scalar, exists} = bothPaths(
      () =>
        replicator.processTransaction(
          '134',
          messages.update('assignments', {id: 'a1', ownerID: 'me'}),
        ),
      false,
    );

    expect(scalar.before).toEqual([]);
    expect(scalar.trackers).toEqual(['t1', 't2']);
    expect(scalar.trackers).toEqual(exists.trackers);
    expect(scalar.reset).toBe(true);
  });

  test('an ownership change that leaves the gate open does not reset', () => {
    // The grant still applies, so access never flips and the companion row
    // change streams like any other row the client already holds.
    const {scalar, exists} = bothPaths(() =>
      replicator.processTransaction(
        '134',
        messages.update('assignments', {id: 'a1', ownerID: 'someone-new'}),
      ),
    );

    expect(scalar.trackers).toEqual(['t1', 't2']);
    expect(scalar.trackers).toEqual(exists.trackers);
    expect(scalar.reset).toBe(false);
  });

  test('a tracker inserted for the gated assignment appears on both paths', () => {
    const {scalar, exists} = bothPaths(() =>
      replicator.processTransaction(
        '134',
        messages.insert('trackers', {id: 't4', assignmentID: 'a1'}),
      ),
    );

    expect(scalar.trackers).toEqual(['t1', 't2', 't4']);
    expect(scalar.trackers).toEqual(exists.trackers);
    expect(scalar.reset).toBe(false);
  });

  test('a gate the parent cannot pin still warns and runs as EXISTS', () => {
    const conditions = (SCALAR.where as {conditions: readonly Condition[]})
      .conditions;
    hydrate({
      ...SCALAR,
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            op: '=',
            // A literal on a column the correlation does not use.
            left: {type: 'column', name: 'id'},
            right: {type: 'literal', value: 't1'},
          },
          conditions[1],
        ],
      },
    });

    expect(warnings()).toMatchInlineSnapshot(`
      [
        "Ignoring {scalar: true} on the "assignments" subquery of query q: it does not constrain every column of any unique key [(id)] to a literal with "=", so it is not provably limited to one row. The gate runs as a plain EXISTS.",
      ]
    `);
  });
});
