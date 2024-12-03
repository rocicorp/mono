import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.js';
import type {AST} from '../../../../zero-protocol/src/ast.js';
import type {Database as DB} from '../../../../zqlite/src/db.js';
import {Database} from '../../../../zqlite/src/db.js';
import {DbFile} from '../../test/lite.js';
import {initChangeLog} from '../replicator/schema/change-log.js';
import {initReplicationState} from '../replicator/schema/replication-state.js';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../replicator/test-utils.js';
import {CREATE_STORAGE_TABLE, DatabaseStorage} from './database-storage.js';
import {PipelineDriver} from './pipeline-driver.js';
import {Snapshotter} from './snapshotter.js';

describe('view-syncer/pipeline-driver', () => {
  let dbFile: DbFile;
  let db: DB;
  let lc: LogContext;
  let pipelines: PipelineDriver;
  let replicator: FakeReplicator;

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
    );

    db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], '123');
    initChangeLog(db);
    db.exec(`
      CREATE TABLE "zero.schemaVersions" (
        "lock"                INTEGER PRIMARY KEY,
        "minSupportedVersion" INTEGER,
        "maxSupportedVersion" INTEGER,
        _0_version            TEXT NOT NULL
      );
      INSERT INTO "zero.schemaVersions" ("lock", "minSupportedVersion", "maxSupportedVersion", _0_version)    
        VALUES (1, 1, 1, '00');  
      CREATE TABLE issues (
        id TEXT PRIMARY KEY,
        closed BOOL,
        ignored TIMESTAMPTZ,
        _0_version TEXT NOT NULL
      );
      CREATE TABLE comments (
        id TEXT PRIMARY KEY, 
        issueID TEXT,
        upvotes INTEGER,
        ignored BYTEA,
         _0_version TEXT NOT NULL);
      CREATE TABLE "issueLabels" (
        issueID TEXT,
        labelID TEXT,
        _0_version TEXT NOT NULL,
        PRIMARY KEY (issueID, labelID)
      );
      CREATE TABLE "labels" (
        id TEXT PRIMARY KEY,
        name TEXT,
        _0_version TEXT NOT NULL
      );

      INSERT INTO ISSUES (id, closed, ignored, _0_version) VALUES ('1', 0, 1728345600000, '00');
      INSERT INTO ISSUES (id, closed, ignored, _0_version) VALUES ('2', 1, 1722902400000, '00');
      INSERT INTO ISSUES (id, closed, ignored, _0_version) VALUES ('3', 0, null, '00');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('10', '1', 0, '00');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('20', '2', 1, '00');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('21', '2', 10000, '00');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('22', '2', 20000, '00');

      INSERT INTO "issueLabels" (issueID, labelID, _0_version) VALUES ('1', '1', '00');
      INSERT INTO "labels" (id, name, _0_version) VALUES ('1', 'bug', '00');
      `);
    replicator = fakeReplicator(lc, db);
  });

  const ISSUES_AND_COMMENTS: AST = {
    table: 'issues',
    orderBy: [['id', 'desc']],
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['issueID'],
        },
        subquery: {
          table: 'comments',
          alias: 'comments',
          orderBy: [['id', 'desc']],
        },
      },
    ],
  };

  const ISSUES_QUERY_WITH_EXISTS: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        correlation: {
          parentField: ['id'],
          childField: ['issueID'],
        },
        subquery: {
          table: 'issueLabels',
          alias: 'labels',
          orderBy: [
            ['issueID', 'asc'],
            ['labelID', 'asc'],
          ],
          where: {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            related: {
              correlation: {
                parentField: ['labelID'],
                childField: ['id'],
              },
              subquery: {
                table: 'labels',
                alias: 'labels',
                orderBy: [['id', 'asc']],
                where: {
                  type: 'simple',
                  left: {
                    type: 'column',
                    name: 'name',
                  },
                  op: '=',
                  right: {
                    type: 'literal',
                    value: 'bug',
                  },
                },
              },
            },
          },
        },
      },
    },
  };

  const messages = new ReplicationMessages({
    issues: 'id',
    comments: 'id',
    issueLabels: ['issueID', 'labelID'],
  });
  const zeroMessages = new ReplicationMessages(
    {schemaVersions: 'lock'},
    'zero',
  );

  test('replica version', () => {
    pipelines.init();
    expect(pipelines.replicaVersion).toBe('123');
  });

  test('add query', () => {
    pipelines.init();

    expect([...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)])
      .toMatchInlineSnapshot(`
        [
          {
            "queryHash": "hash1",
            "row": {
              "_0_version": "00",
              "closed": false,
              "id": "3",
            },
            "rowKey": {
              "id": "3",
            },
            "table": "issues",
            "type": "add",
          },
          {
            "queryHash": "hash1",
            "row": {
              "_0_version": "00",
              "closed": true,
              "id": "2",
            },
            "rowKey": {
              "id": "2",
            },
            "table": "issues",
            "type": "add",
          },
          {
            "queryHash": "hash1",
            "row": {
              "_0_version": "00",
              "id": "22",
              "issueID": "2",
              "upvotes": 20000,
            },
            "rowKey": {
              "id": "22",
            },
            "table": "comments",
            "type": "add",
          },
          {
            "queryHash": "hash1",
            "row": {
              "_0_version": "00",
              "id": "21",
              "issueID": "2",
              "upvotes": 10000,
            },
            "rowKey": {
              "id": "21",
            },
            "table": "comments",
            "type": "add",
          },
          {
            "queryHash": "hash1",
            "row": {
              "_0_version": "00",
              "id": "20",
              "issueID": "2",
              "upvotes": 1,
            },
            "rowKey": {
              "id": "20",
            },
            "table": "comments",
            "type": "add",
          },
          {
            "queryHash": "hash1",
            "row": {
              "_0_version": "00",
              "closed": false,
              "id": "1",
            },
            "rowKey": {
              "id": "1",
            },
            "table": "issues",
            "type": "add",
          },
          {
            "queryHash": "hash1",
            "row": {
              "_0_version": "00",
              "id": "10",
              "issueID": "1",
              "upvotes": 0,
            },
            "rowKey": {
              "id": "10",
            },
            "table": "comments",
            "type": "add",
          },
        ]
      `);
  });

  test('insert', () => {
    pipelines.init();
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)];

    replicator.processTransaction(
      '134',
      messages.insert('comments', {id: '31', issueID: '3', upvotes: BigInt(0)}),
      messages.insert('comments', {
        id: '41',
        issueID: '4',
        upvotes: BigInt(Number.MAX_SAFE_INTEGER),
      }),
      messages.insert('issues', {id: '4', closed: 0}),
    );

    expect([...pipelines.advance().changes]).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "123",
            "id": "31",
            "issueID": "3",
            "upvotes": 0,
          },
          "rowKey": {
            "id": "31",
          },
          "table": "comments",
          "type": "add",
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "4",
          },
          "rowKey": {
            "id": "4",
          },
          "table": "issues",
          "type": "add",
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "123",
            "id": "41",
            "issueID": "4",
            "upvotes": 9007199254740991,
          },
          "rowKey": {
            "id": "41",
          },
          "table": "comments",
          "type": "add",
        },
      ]
    `);
  });

  test('delete', () => {
    pipelines.init();
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)];

    replicator.processTransaction(
      '134',
      messages.delete('issues', {id: '1'}),
      messages.delete('comments', {id: '21'}),
    );

    expect([...pipelines.advance().changes]).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "id": "21",
          },
          "table": "comments",
          "type": "remove",
        },
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": "remove",
        },
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
          "type": "remove",
        },
      ]
    `);
  });

  test('update', () => {
    pipelines.init();
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)];

    replicator.processTransaction(
      '134',
      messages.update('comments', {id: '22', issueID: '3'}),
    );

    expect([...pipelines.advance().changes]).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "id": "22",
          },
          "table": "comments",
          "type": "remove",
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "123",
            "id": "22",
            "issueID": "3",
            "upvotes": 20000,
          },
          "rowKey": {
            "id": "22",
          },
          "table": "comments",
          "type": "add",
        },
      ]
    `);

    replicator.processTransaction(
      '135',
      messages.update('comments', {id: '22', upvotes: 10}),
    );

    expect([...pipelines.advance().changes]).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "134",
            "id": "22",
            "issueID": "3",
            "upvotes": 10,
          },
          "rowKey": {
            "id": "22",
          },
          "table": "comments",
          "type": "edit",
        },
      ]
    `);
  });

  test('reset', () => {
    pipelines.init();
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)];
    expect(pipelines.addedQueries()).toEqual(new Set(['hash1']));

    replicator.processTransaction(
      '134',
      messages.addColumn('issues', 'newColumn', {dataType: 'TEXT', pos: 0}),
    );

    pipelines.advanceWithoutDiff();
    pipelines.reset();

    expect(pipelines.addedQueries()).toEqual(new Set());

    // The newColumn should be reflected after a reset.
    expect([...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)])
      .toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "3",
            "newColumn": null,
          },
          "rowKey": {
            "id": "3",
          },
          "table": "issues",
          "type": "add",
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "123",
            "closed": true,
            "id": "2",
            "newColumn": null,
          },
          "rowKey": {
            "id": "2",
          },
          "table": "issues",
          "type": "add",
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "00",
            "id": "22",
            "issueID": "2",
            "upvotes": 20000,
          },
          "rowKey": {
            "id": "22",
          },
          "table": "comments",
          "type": "add",
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "00",
            "id": "21",
            "issueID": "2",
            "upvotes": 10000,
          },
          "rowKey": {
            "id": "21",
          },
          "table": "comments",
          "type": "add",
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "00",
            "id": "20",
            "issueID": "2",
            "upvotes": 1,
          },
          "rowKey": {
            "id": "20",
          },
          "table": "comments",
          "type": "add",
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "1",
            "newColumn": null,
          },
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": "add",
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "00",
            "id": "10",
            "issueID": "1",
            "upvotes": 0,
          },
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
          "type": "add",
        },
      ]
    `);
  });

  test('whereExists query', () => {
    pipelines.init();
    [...pipelines.addQuery('hash1', ISSUES_QUERY_WITH_EXISTS)];

    replicator.processTransaction(
      '134',
      messages.delete('issueLabels', {issueID: '1', labelID: '1'}),
    );

    expect([...pipelines.advance().changes]).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "issueID": "1",
            "labelID": "1",
          },
          "table": "issueLabels",
          "type": "remove",
        },
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "id": "1",
          },
          "table": "labels",
          "type": "remove",
        },
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": "remove",
        },
      ]
    `);
  });

  test('getRow', () => {
    pipelines.init();

    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)];

    // Post-hydration
    expect(pipelines.getRow('issues', {id: '1'})).toEqual({
      id: '1',
      closed: false,
      ['_0_version']: '00',
    });

    expect(pipelines.getRow('comments', {id: '22'})).toEqual({
      id: '22',
      issueID: '2',
      upvotes: 20000,
      ['_0_version']: '00',
    });

    replicator.processTransaction(
      '134',
      messages.update('comments', {id: '22', issueID: '3', upvotes: 20000}),
    );
    [...pipelines.advance().changes];

    // Post-advancement
    expect(pipelines.getRow('comments', {id: '22'})).toEqual({
      id: '22',
      issueID: '3',
      upvotes: 20000,
      ['_0_version']: '123',
    });
  });

  test('schemaVersions change and insert', () => {
    pipelines.init();
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)];

    replicator.processTransaction(
      '134',
      messages.insert('issues', {id: '4', closed: 0}),
      zeroMessages.insert('schemaVersions', {
        lock: true,
        minSupportedVersion: 1,
        maxSupportedVersion: 2,
      }),
    );

    expect(pipelines.currentSchemaVersions()).toEqual({
      minSupportedVersion: 1,
      maxSupportedVersion: 1,
    });

    expect([...pipelines.advance().changes]).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "4",
          },
          "rowKey": {
            "id": "4",
          },
          "table": "issues",
          "type": "add",
        },
      ]
    `);

    expect(pipelines.currentSchemaVersions()).toEqual({
      minSupportedVersion: 1,
      maxSupportedVersion: 2,
    });
  });

  test('multiple advancements', () => {
    pipelines.init();
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)];

    replicator.processTransaction(
      '134',
      messages.insert('issues', {id: '4', closed: 0}),
    );

    expect([...pipelines.advance().changes]).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "4",
          },
          "rowKey": {
            "id": "4",
          },
          "table": "issues",
          "type": "add",
        },
      ]
    `);

    replicator.processTransaction(
      '156',
      messages.insert('comments', {id: '41', issueID: '4', upvotes: 10}),
    );

    expect([...pipelines.advance().changes]).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "134",
            "id": "41",
            "issueID": "4",
            "upvotes": 10,
          },
          "rowKey": {
            "id": "41",
          },
          "table": "comments",
          "type": "add",
        },
      ]
    `);

    replicator.processTransaction('189', messages.delete('issues', {id: '4'}));

    expect([...pipelines.advance().changes]).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "id": "4",
          },
          "table": "issues",
          "type": "remove",
        },
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "id": "41",
          },
          "table": "comments",
          "type": "remove",
        },
      ]
    `);
  });

  test('remove query', () => {
    pipelines.init();
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)];

    expect([...pipelines.addedQueries()]).toEqual(['hash1']);
    pipelines.removeQuery('hash1');
    expect([...pipelines.addedQueries()]).toEqual([]);

    replicator.processTransaction(
      '134',
      messages.insert('comments', {id: '31', issueID: '3', upvotes: 0}),
      messages.insert('comments', {id: '41', issueID: '4', upvotes: 0}),
      messages.insert('issues', {id: '4', closed: 1}),
    );

    expect(pipelines.currentVersion()).toBe('00');
    expect([...pipelines.advance().changes]).toHaveLength(0);
    expect(pipelines.currentVersion()).toBe('123');
  });

  test('push fails on out of bounds numbers', () => {
    pipelines.init();
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)];

    replicator.processTransaction(
      '134',
      messages.insert('comments', {
        id: '31',
        issueID: '3',
        upvotes: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
      }),
    );

    expect(() => [...pipelines.advance().changes]).toThrowError();
  });
});
