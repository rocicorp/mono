/* eslint-disable @typescript-eslint/naming-convention */
import {afterEach, beforeEach, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.js';
import {DbFile} from '../../test/lite.js';
import {Database} from '../../../../zqlite/src/db.js';
import {PipelineDriver} from './pipeline-driver.js';
import {CREATE_STORAGE_TABLE, DatabaseStorage} from './database-storage.js';
import {Snapshotter} from './snapshotter.js';
import {initReplicationState} from '../replicator/schema/replication-state.js';
import {initChangeLog} from '../replicator/schema/change-log.js';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../replicator/test-utils.js';
import type {LogContext} from '@rocicorp/logger';
import type {AST} from '../../../../zero-protocol/src/ast.js';

let dbFile: DbFile;
let db: Database;
let lc: LogContext;
let pipelines: PipelineDriver;
let replicator: FakeReplicator;

const TRACKABLE_AST: AST = {
  table: 'TYL_trackable',
  where: {
    type: 'correlatedSubquery',
    related: {
      system: 'client',
      correlation: {parentField: ['id'], childField: ['trackableId']},
      subquery: {
        table: 'TYL_trackableGroup',
        alias: 'zsubq_trackableGroup',
        where: {
          type: 'simple',
          left: {type: 'column', name: 'group'},
          right: {type: 'literal', value: 'archived'},
          op: '=',
        },
        orderBy: [
          ['trackableId', 'asc'],
          ['group', 'asc'],
        ],
      },
    },
    op: 'NOT EXISTS',
  },
  related: [
    {
      system: 'client',
      correlation: {parentField: ['id'], childField: ['trackableId']},
      subquery: {
        table: 'TYL_trackableGroup',
        alias: 'trackableGroup',
        orderBy: [
          ['trackableId', 'asc'],
          ['group', 'asc'],
        ],
      },
    },
  ],
  orderBy: [['id', 'asc']],
};

beforeEach(() => {
  lc = createSilentLogContext();
  dbFile = new DbFile('pipelines_test');
  dbFile.connect(lc).pragma('journal_mode = wal2');

  const storage = new Database(lc, ':memory:');
  storage.prepare(CREATE_STORAGE_TABLE).run();

  pipelines = new PipelineDriver(
    lc,
    new Snapshotter(lc, dbFile.path),
    new DatabaseStorage(storage).createClientGroupStorage('foo-client-group'),
    'pipeline-driver.test.ts',
  );

  db = dbFile.connect(lc);
  initReplicationState(db, ['zero_data'], '123');
  initChangeLog(db);
  db.exec(/* sql */ `
      CREATE TABLE "zero.schemaVersions" (
        "lock"                INTEGER PRIMARY KEY,
        "minSupportedVersion" INTEGER,
        "maxSupportedVersion" INTEGER,
        _0_version            TEXT NOT NULL
      );
      INSERT INTO "zero.schemaVersions" ("lock", "minSupportedVersion", "maxSupportedVersion", _0_version)    
        VALUES (1, 1, 1, '123');  
      CREATE TABLE TYL_trackableGroup (
        "trackableId" TEXT NOT NULL,
        "group" TEXT NOT NULL,
        "user_id" TEXT NOT NULL,
        _0_version TEXT NOT NULL,
        PRIMARY KEY ("trackableId", "group")
      );
      CREATE TABLE TYL_trackable (
        "id" TEXT PRIMARY KEY NOT NULL,
        "name" TEXT NOT NULL,
        _0_version TEXT NOT NULL
      );

      INSERT INTO TYL_trackable VALUES ('001', 'trackable 1', '123');
  `);
  replicator = fakeReplicator(lc, db);
});

afterEach(() => {
  dbFile.delete();
});

const messages = new ReplicationMessages({
  TYL_trackable: 'id',
  TYL_trackableGroup: ['trackableId', 'group'],
});
// const zeroMessages = new ReplicationMessages({schemaVersions: 'lock'}, 'zero');

/**
 * This reproduces a user report with queries that use `NOT EXIST`.
 *
 * Thread: https://discord.com/channels/830183651022471199/1326515508534579240/1326515834763612241
 *
 * Summary of what they saw:
 * They have the following query:
 *
 * ```ts
 * const query = newQuery(queryDelegate, schema.tables.TYL_trackable)
 *    .where(({not, exists}) =>
 *      not(exists('trackableGroup', q => q.where('group', '=', 'archived'))),
 *    )
 *    .related('trackableGroup');
 * ```
 *
 * And a single `trackableSource` row. 0 trackableGroup rows.
 *
 * After adding a `trackableGroup` row with `group` set to `archived`, the `trackable` row returned
 * by the query is removed. As expected.
 *
 * When deleting the `trackableGroup` row, the `trackable` row is not returned by the query.
 *
 * This repro is surfacing that the `pipeline-driver` is sending the wrong number of `add` and `remove` changes
 * for `trackableGroup`. After the `trackableGroup` row is deleted, the client is still left with 1 `trackableGroup`
 * row on their device.
 */
test('repro', () => {
  pipelines.init();
  expect([...pipelines.addQuery('hash1', TRACKABLE_AST)]).toMatchInlineSnapshot(
    `
    [
      {
        "queryHash": "hash1",
        "row": {
          "_0_version": "123",
          "id": "001",
          "name": "trackable 1",
        },
        "rowKey": {
          "id": "001",
        },
        "table": "TYL_trackable",
        "type": "add",
      },
    ]
  `,
  );

  replicator.processTransaction(
    '134',
    messages.insert('TYL_trackableGroup', {
      trackableId: '001',
      group: 'archived',
      user_id: '001',
    }),
  );

  // This should add a row for `trackableGroup` and remove a row for `trackable`.
  // A bug here is that `trackableGroup` is both added and removed (no-op)! This is a problem as it prevents the
  // UI from correctly running the `NOT EXISTS` query if any other query is holding onto `trackable`.
  expect([...pipelines.advance().changes]).toMatchInlineSnapshot(`
    [
      {
        "queryHash": "hash1",
        "row": {
          "_0_version": "134",
          "group": "archived",
          "trackableId": "001",
          "user_id": "001",
        },
        "rowKey": {
          "group": "archived",
          "trackableId": "001",
        },
        "table": "TYL_trackableGroup",
        "type": "add",
      },
      {
        "queryHash": "hash1",
        "row": undefined,
        "rowKey": {
          "id": "001",
        },
        "table": "TYL_trackable",
        "type": "remove",
      },
      {
        "queryHash": "hash1",
        "row": undefined,
        "rowKey": {
          "group": "archived",
          "trackableId": "001",
        },
        "table": "TYL_trackableGroup",
        "type": "remove",
      },
    ]
  `);

  replicator.processTransaction(
    '135',
    messages.delete('TYL_trackableGroup', {
      trackableId: '001',
      group: 'archived',
    }),
  );

  // the archived group was removed. The trackable should be returned
  // and trackableGroup be removed.
  expect([...pipelines.advance().changes]).toMatchInlineSnapshot(`
    [
      {
        "queryHash": "hash1",
        "row": {
          "_0_version": "123",
          "id": "001",
          "name": "trackable 1",
        },
        "rowKey": {
          "id": "001",
        },
        "table": "TYL_trackable",
        "type": "add",
      },
      {
        "queryHash": "hash1",
        "row": {
          "_0_version": "134",
          "group": "archived",
          "trackableId": "001",
          "user_id": "001",
        },
        "rowKey": {
          "group": "archived",
          "trackableId": "001",
        },
        "table": "TYL_trackableGroup",
        "type": "add",
      },
      {
        "queryHash": "hash1",
        "row": undefined,
        "rowKey": {
          "group": "archived",
          "trackableId": "001",
        },
        "table": "TYL_trackableGroup",
        "type": "remove",
      },
    ]
  `);
});
