import {LogContext} from '@rocicorp/logger';
import Database, {Database as DB} from 'better-sqlite3';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {beforeEach, describe, expect, test} from 'vitest';
import {DbFile} from 'zero-cache/src/test/lite.js';
import {AST} from 'zql/src/zql/ast/ast.js';
import {initChangeLog} from '../replicator/schema/change-log.js';
import {initReplicationState} from '../replicator/schema/replication-state.js';
import {fakeReplicator, ReplicationMessages} from '../replicator/test-utils.js';
import {CREATE_STORAGE_TABLE, DatabaseStorage} from './database-storage.js';
import {PipelineDriver} from './pipeline-driver.js';
import {Snapshotter} from './snapshotter.js';

describe('view-syncer/pipeline-driver', () => {
  let dbFile: DbFile;
  let db: DB;
  let lc: LogContext;
  let pipelines: PipelineDriver;

  beforeEach(() => {
    dbFile = new DbFile('pipelines_test');
    lc = createSilentLogContext();
    const storage = new Database(':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();

    pipelines = new PipelineDriver(
      lc,
      new Snapshotter(lc, dbFile.path),
      new DatabaseStorage(storage).createClientGroupStorage('foo-client-group'),
    );

    db = dbFile.connect();
    initReplicationState(db, ['zero_data'], '0/123');
    initChangeLog(db);
    db.exec(`
      CREATE TABLE issues (
        id TEXT PRIMARY KEY,
        closed BOOL,
        _0_version TEXT NOT NULL
      );
      CREATE TABLE comments (
        id TEXT PRIMARY KEY, 
        issueID TEXT,
        upvotes INTEGER,
         _0_version TEXT NOT NULL);

      INSERT INTO ISSUES (id, closed, _0_version) VALUES ('1', 0, '00');
      INSERT INTO ISSUES (id, closed, _0_version) VALUES ('2', 1, '00');
      INSERT INTO ISSUES (id, closed, _0_version) VALUES ('3', 0, '00');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('10', '1', 0, '00');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('20', '2', 1, '00');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('21', '2', 10000, '00');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('22', '2', 20000, '00');
      `);
  });

  const ISSUES_AND_COMMENTS: AST = {
    table: 'issues',
    orderBy: [['id', 'desc']],
    related: [
      {
        correlation: {parentField: 'id', childField: 'issueID', op: '='},
        subquery: {
          table: 'comments',
          alias: 'comments',
          orderBy: [['id', 'desc']],
        },
      },
    ],
  };

  const messages = new ReplicationMessages({issues: 'id', comments: 'id'});

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
        },
      ]
    `);
  });

  test('insert', () => {
    pipelines.init();
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)];

    const replicator = fakeReplicator(lc, db);
    replicator.processTransaction(
      '0/234',
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
            "_0_version": "183",
            "id": "31",
            "issueID": "3",
            "upvotes": 0,
          },
          "rowKey": {
            "id": "31",
          },
          "table": "comments",
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "183",
            "closed": false,
            "id": "4",
          },
          "rowKey": {
            "id": "4",
          },
          "table": "issues",
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "183",
            "id": "41",
            "issueID": "4",
            "upvotes": 9007199254740991,
          },
          "rowKey": {
            "id": "41",
          },
          "table": "comments",
        },
      ]
    `);
  });

  test('delete', () => {
    pipelines.init();
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)];

    const replicator = fakeReplicator(lc, db);
    replicator.processTransaction(
      '0/234',
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
        },
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
        },
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
        },
      ]
    `);
  });

  test('update', () => {
    pipelines.init();
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)];

    const replicator = fakeReplicator(lc, db);
    replicator.processTransaction(
      '0/234',
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
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "183",
            "id": "22",
            "issueID": "3",
            "upvotes": 20000,
          },
          "rowKey": {
            "id": "22",
          },
          "table": "comments",
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

    const replicator = fakeReplicator(lc, db);
    replicator.processTransaction(
      '0/234',
      messages.update('comments', {id: '22', issueID: '3', upvotes: 20000}),
    );
    [...pipelines.advance().changes];

    // Post-advancement
    expect(pipelines.getRow('comments', {id: '22'})).toEqual({
      id: '22',
      issueID: '3',
      upvotes: 20000,
      ['_0_version']: '183',
    });
  });

  test('multiple advancements', () => {
    pipelines.init();
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)];

    const replicator = fakeReplicator(lc, db);
    replicator.processTransaction(
      '0/234',
      messages.insert('issues', {id: '4', closed: 0}),
    );

    expect([...pipelines.advance().changes]).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "183",
            "closed": false,
            "id": "4",
          },
          "rowKey": {
            "id": "4",
          },
          "table": "issues",
        },
      ]
    `);

    replicator.processTransaction(
      '0/456',
      messages.insert('comments', {id: '41', issueID: '4', upvotes: 10}),
    );

    expect([...pipelines.advance().changes]).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "1fo",
            "id": "41",
            "issueID": "4",
            "upvotes": 10,
          },
          "rowKey": {
            "id": "41",
          },
          "table": "comments",
        },
      ]
    `);

    replicator.processTransaction(
      '0/789',
      messages.delete('issues', {id: '4'}),
    );

    expect([...pipelines.advance().changes]).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "id": "4",
          },
          "table": "issues",
        },
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "id": "41",
          },
          "table": "comments",
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

    const replicator = fakeReplicator(lc, db);
    replicator.processTransaction(
      '0/234',
      messages.insert('comments', {id: '31', issueID: '3', upvotes: 0}),
      messages.insert('comments', {id: '41', issueID: '4', upvotes: 0}),
      messages.insert('issues', {id: '4', closed: 1}),
    );

    expect(pipelines.currentVersion()).toBe('00');
    expect([...pipelines.advance().changes]).toHaveLength(0);
    expect(pipelines.currentVersion()).toBe('183');
  });

  test('push fails on out of bounds numbers', () => {
    pipelines.init();
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS)];

    const replicator = fakeReplicator(lc, db);
    replicator.processTransaction(
      '0/234',
      messages.insert('comments', {
        id: '31',
        issueID: '3',
        upvotes: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
      }),
    );

    expect(() => [...pipelines.advance().changes]).toThrowError();
  });
});
