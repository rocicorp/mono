// A flipped EXISTS branch under an OR tags each row with the child rows that
// made it pass (its witnesses), and the Streamer syncs those witnesses to the
// client by refcounting every row in an add/remove subtree. These tests drive
// the real PipelineDriver over a SQLite replica with
//   issue WHERE assignee IS NULL OR EXISTS(project WHERE active, flip)
// and check that the incremental RowChange stream leaves a client holding the
// same rows a fresh hydration would.
import type {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import type {Database as DB} from '../../../../zqlite/src/db.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../replicator/test-utils.ts';
import {PipelineDriver, type RowChange} from './pipeline-driver.ts';
import {Snapshotter} from './snapshotter.ts';

describe('view-syncer/pipeline-driver union witnesses', () => {
  let dbFile: DbFile;
  let db: DB;
  let lc: LogContext;
  let pipelines: PipelineDriver;
  let replicator: FakeReplicator;

  beforeEach(() => {
    lc = createSilentLogContext();
    dbFile = new DbFile('pipelines_union_witness_test');
    dbFile.connect(lc).pragma('journal_mode = wal2');
    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    pipelines = new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: 'zeroz'}),
      {appID: 'zeroz', shardNum: 1},
      new DatabaseStorage(storage).createClientGroupStorage('cg'),
      'pipeline-driver.union-witness.test.ts',
      new InspectorDelegate(undefined),
      () => 200,
    );
    db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], '123');
    db.exec(/*sql*/ `
      CREATE TABLE "zeroz.mutations" (
        "clientGroupID"  TEXT,
        "clientID"       TEXT,
        "mutationID"     INTEGER,
        "result"         TEXT,
        _0_version       TEXT NOT NULL,
        PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
      );
      CREATE TABLE project (
        id TEXT PRIMARY KEY,
        active BOOL,
        _0_version TEXT NOT NULL
      );
      CREATE TABLE issue (
        id TEXT PRIMARY KEY,
        assignee TEXT,
        projectID TEXT,
        _0_version TEXT NOT NULL
      );
    `);
    replicator = fakeReplicator(lc, db);
  });

  afterEach(() => {
    dbFile.delete();
  });

  const project = table('project')
    .columns({id: string(), active: boolean()})
    .primaryKey('id');
  const issue = table('issue')
    .columns({
      id: string(),
      assignee: string().optional(),
      projectID: string(),
    })
    .primaryKey('id');
  const clientSchema = createSchema({tables: [project, issue]});
  const messages = new ReplicationMessages({project: 'id', issue: 'id'});
  const timer = {elapsedLap: () => 0, totalElapsed: () => 0};

  // Unassigned issues, plus every issue in an active project.
  const UNASSIGNED_OR_IN_ACTIVE_PROJECT: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    where: {
      type: 'or',
      conditions: [
        {
          type: 'simple',
          op: 'IS',
          left: {type: 'column', name: 'assignee'},
          right: {type: 'literal', value: null},
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          flip: true,
          related: {
            system: 'client',
            correlation: {parentField: ['projectID'], childField: ['id']},
            subquery: {
              table: 'project',
              alias: 'project',
              orderBy: [['id', 'asc']],
              where: {
                type: 'simple',
                op: '=',
                left: {type: 'column', name: 'active'},
                right: {type: 'literal', value: true},
              },
            },
          },
        },
      ],
    },
  };

  /** view-syncer's per-(row, query) refCounts plus the last row contents. */
  type Held = Map<string, {count: number; row: unknown}>;

  function fold(held: Held, changes: Iterable<RowChange | 'yield'>): void {
    for (const change of changes) {
      if (change === 'yield') {
        continue;
      }
      const key = `${change.table}:${(change.rowKey as {id: string}).id}`;
      const entry = held.get(key) ?? {count: 0, row: undefined};
      switch (change.type) {
        case ChangeType.ADD:
          entry.count++;
          entry.row = change.row;
          break;
        case ChangeType.REMOVE:
          entry.count--;
          break;
        case ChangeType.EDIT:
          entry.row = change.row;
          break;
      }
      held.set(key, entry);
    }
  }

  /** What the client ends up holding: rows with a positive count. */
  function clientRows(held: Held): string[] {
    return [...held]
      .filter(([, e]) => e.count > 0)
      .map(([k, e]) => `${k} ${JSON.stringify(e.row)}`)
      .sort();
  }

  function expectIncrementalMatchesFresh(
    setup: string,
    txns: Parameters<FakeReplicator['processTransaction']>[],
  ): string[] {
    db.exec(setup);
    pipelines.init(clientSchema);
    const incremental: Held = new Map();
    fold(
      incremental,
      pipelines.addQuery('h', 'q', UNASSIGNED_OR_IN_ACTIVE_PROJECT, timer),
    );
    for (const txn of txns) {
      replicator.processTransaction(...txn);
      fold(incremental, pipelines.advance(timer).changes);
    }
    // Re-adding the same queryID tears the pipeline down and re-hydrates
    // from the post-advance snapshot: what a fresh client would receive.
    const fresh: Held = new Map();
    fold(
      fresh,
      pipelines.addQuery('h', 'q', UNASSIGNED_OR_IN_ACTIVE_PROJECT, timer),
    );
    const rows = clientRows(incremental);
    expect(rows).toEqual(clientRows(fresh));
    return rows;
  }

  test('deleting an unassigned issue keeps the project another issue still needs', () => {
    const rows = expectIncrementalMatchesFresh(
      /*sql*/ `
      INSERT INTO project VALUES ('p1', 1, '123');
      INSERT INTO issue VALUES ('i1', NULL, 'p1', '123');
      INSERT INTO issue VALUES ('i2', 'ann', 'p1', '123');`,
      [['124', messages.delete('issue', {id: 'i1'})]],
    );
    expect(rows).toEqual([
      'issue:i2 {"id":"i2","assignee":"ann","projectID":"p1","_0_version":"123"}',
      'project:p1 {"id":"p1","active":true,"_0_version":"123"}',
    ]);
  });

  test('archiving a project after an unassigned issue arrived removes the project', () => {
    const rows = expectIncrementalMatchesFresh(
      /*sql*/ `INSERT INTO project VALUES ('p1', 1, '123');`,
      [
        [
          '124',
          messages.insert('issue', {id: 'i1', assignee: null, projectID: 'p1'}),
        ],
        ['125', messages.update('project', {id: 'p1', active: false})],
      ],
    );
    expect(rows).toEqual([
      'issue:i1 {"id":"i1","assignee":null,"projectID":"p1","_0_version":"124"}',
    ]);
  });

  test('assigning an issue sends the project that now witnesses it', () => {
    const rows = expectIncrementalMatchesFresh(
      /*sql*/ `
      INSERT INTO project VALUES ('p1', 1, '123');
      INSERT INTO issue VALUES ('i1', NULL, 'p1', '123');`,
      [
        [
          '124',
          messages.update('issue', {
            id: 'i1',
            assignee: 'ann',
            projectID: 'p1',
          }),
        ],
      ],
    );
    expect(rows).toEqual([
      'issue:i1 {"id":"i1","assignee":"ann","projectID":"p1","_0_version":"124"}',
      'project:p1 {"id":"p1","active":true,"_0_version":"123"}',
    ]);
  });
});
