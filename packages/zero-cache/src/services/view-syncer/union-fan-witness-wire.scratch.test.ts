// Scratch: does the union-fan witness asymmetry reach the wire? Drives the real
// PipelineDriver + Streamer over a SQLite replica with
//   issue WHERE assignee IS NULL OR EXISTS(project WHERE active, flip)
// and folds the emitted RowChange stream the way view-syncer's refCounts do.
// Not for commit.
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

describe('union-fan witness over the wire', () => {
  let dbFile: DbFile;
  let db: DB;
  let lc: LogContext;
  let pipelines: PipelineDriver;
  let replicator: FakeReplicator;

  beforeEach(() => {
    lc = createSilentLogContext();
    dbFile = new DbFile('ufi_witness_wire');
    dbFile.connect(lc).pragma('journal_mode = wal2');
    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    pipelines = new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: 'zeroz'}),
      {appID: 'zeroz', shardNum: 1},
      new DatabaseStorage(storage).createClientGroupStorage('cg'),
      'ufi-witness-wire',
      new InspectorDelegate(undefined),
      () => 200,
    );
    db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], '123');
    db.exec(`
      CREATE TABLE "zeroz.mutations" (
        "clientGroupID"  TEXT,
        "clientID"       TEXT,
        "mutationID"     INTEGER,
        "result"         TEXT,
        _0_version       TEXT NOT NULL,
        PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
      );
      CREATE TABLE project (id TEXT PRIMARY KEY, active BOOL, _0_version TEXT NOT NULL);
      CREATE TABLE issue (id TEXT PRIMARY KEY, assignee TEXT, projectID TEXT, _0_version TEXT NOT NULL);
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
    .columns({id: string(), assignee: string().optional(), projectID: string()})
    .primaryKey('id');
  const clientSchema = createSchema({tables: [project, issue]});
  const messages = new ReplicationMessages({project: 'id', issue: 'id'});
  const timer = {elapsedLap: () => 0, totalElapsed: () => 0};

  // "Unassigned issues, plus every issue in an active project."
  const QUERY: AST = {
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

  /** view-syncer's per-(row, query) refCounts + last row contents. */
  type Fp = Map<string, {rc: number; row: unknown}>;
  function fold(fp: Fp, changes: Iterable<RowChange | 'yield'>, label: string) {
    const list = [...changes].filter(c => c !== 'yield') as RowChange[];
    console.log(
      label,
      JSON.stringify(
        list.map(c => ({type: c.type, table: c.table, key: c.rowKey, row: c.row})),
      ),
    );
    for (const c of list) {
      const key = `${c.table}:${(c.rowKey as {id: string}).id}`;
      const e = fp.get(key) ?? {rc: 0, row: undefined};
      if (c.type === ChangeType.ADD) {
        e.rc++;
        e.row = c.row;
      } else if (c.type === ChangeType.REMOVE) {
        e.rc--;
      } else {
        e.row = c.row;
      }
      fp.set(key, e);
    }
  }
  /** What the client ends up holding: rows with a positive count. */
  const held = (fp: Fp) =>
    [...fp]
      .filter(([, e]) => e.rc > 0)
      .map(([k, e]) => `${k} ${JSON.stringify(e.row)}`)
      .sort();

  function scenario(
    setup: string,
    txns: Parameters<FakeReplicator['processTransaction']>[],
  ) {
    db.exec(setup);
    pipelines.init(clientSchema);
    const inc: Fp = new Map();
    fold(inc, pipelines.addQuery('h', 'q', QUERY, timer), '  HYDRATE');
    for (const t of txns) {
      replicator.processTransaction(...t);
      fold(inc, pipelines.advance(timer).changes, '  ADVANCE');
    }
    // Re-adding the same queryID tears down and re-hydrates from the
    // post-advance snapshot: what a fresh client would receive.
    const fresh: Fp = new Map();
    fold(fresh, pipelines.addQuery('h', 'q', QUERY, timer), '  FRESH  ');
    console.log('  incremental client holds', held(inc));
    console.log('  fresh client holds      ', held(fresh));
    expect(held(inc)).toEqual(held(fresh));
  }

  test('A. delete unassigned i1: p1 must stay, i2 still needs it', () =>
    scenario(
      `INSERT INTO project VALUES ('p1', 1, '123');
       INSERT INTO issue VALUES ('i1', NULL, 'p1', '123');
       INSERT INTO issue VALUES ('i2', 'ann', 'p1', '123');`,
      [['124', messages.delete('issue', {id: 'i1'})]],
    ));

  test('B. archive p1 after unassigned i1 arrives: p1 must leave', () =>
    scenario(
      `INSERT INTO project VALUES ('p1', 1, '123');`,
      [
        ['124', messages.insert('issue', {id: 'i1', assignee: null, projectID: 'p1'})],
        ['125', messages.update('project', {id: 'p1', active: false})],
      ],
    ));

  test('C. assign i1: p1 must arrive as its witness', () =>
    scenario(
      `INSERT INTO project VALUES ('p1', 1, '123');
       INSERT INTO issue VALUES ('i1', NULL, 'p1', '123');`,
      [['124', messages.update('issue', {id: 'i1', assignee: 'ann', projectID: 'p1'})]],
    ));
});
