import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {Database as DB} from '../../../../zqlite/src/db.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {DbFile} from '../../test/lite.ts';
import {initChangeLog} from '../replicator/schema/change-log.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../replicator/test-utils.ts';
import {CREATE_STORAGE_TABLE, DatabaseStorage} from './database-storage.ts';
import {PipelineDriver} from './pipeline-driver.ts';
import {ResetPipelinesSignal, Snapshotter} from './snapshotter.ts';
import {Timer} from './view-syncer.ts';
import {getMutationResultsQuery} from './cvr.ts';

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
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: 'zeroz'}),
      {appID: 'zeroz', shardNum: 1},
      new DatabaseStorage(storage).createClientGroupStorage('foo-client-group'),
      'pipeline-driver.test.ts',
    );

    db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], '123');
    initChangeLog(db);
    db.exec(`
      CREATE TABLE "zeroz.schemaVersions" (
        -- Note: Using "INT" to avoid the special semantics of "INTEGER PRIMARY KEY" in SQLite.
        "lock"                INT PRIMARY KEY,
        "minSupportedVersion" INT,
        "maxSupportedVersion" INT,
        _0_version            TEXT NOT NULL
      );
      INSERT INTO "zeroz.schemaVersions" ("lock", "minSupportedVersion", "maxSupportedVersion", _0_version)    
        VALUES (1, 1, 1, '123');
      CREATE TABLE "zeroz.mutations" (
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
        ignored INET,
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
        legacyID "TEXT|NOT_NULL",
        _0_version TEXT NOT NULL,
        PRIMARY KEY (issueID, labelID)
      );
      CREATE UNIQUE INDEX issues_a ON issueLabels (legacyID);  -- Test that this doesn't trip up IVM.
      CREATE TABLE "labels" (
        id TEXT PRIMARY KEY,
        name TEXT,
        _0_version TEXT NOT NULL
      );

      INSERT INTO ISSUES (id, closed, ignored, _0_version) VALUES ('1', 0, 1728345600000, '123');
      INSERT INTO ISSUES (id, closed, ignored, _0_version) VALUES ('2', 1, 1722902400000, '123');
      INSERT INTO ISSUES (id, closed, ignored, _0_version) VALUES ('3', 0, null, '123');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('10', '1', 0, '123');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('20', '2', 1, '123');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('21', '2', 10000, '123');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('22', '2', 20000, '123');

      INSERT INTO "issueLabels" (issueID, labelID, legacyID, _0_version) VALUES ('1', '1', '1-1', '123');
      INSERT INTO "labels" (id, name, _0_version) VALUES ('1', 'bug', '123');

      CREATE TABLE uniques (
        id "TEXT|NOT_NULL",
        name "TEXT|NOT_NULL",
        _0_version TEXT NOT NULL
      );
      CREATE UNIQUE INDEX uniques_id ON uniques (id);
      CREATE UNIQUE INDEX uniques_name ON uniques (name);

      INSERT INTO "uniques" (id, name, _0_version) VALUES ('foo', 'bar', '123');
      INSERT INTO "uniques" (id, name, _0_version) VALUES ('boo', 'dar', '123');
      `);
    replicator = fakeReplicator(lc, db);
  });

  afterEach(() => {
    dbFile.delete();
  });

  const ISSUES_AND_COMMENTS: AST = {
    table: 'issues',
    orderBy: [['id', 'desc']],
    related: [
      {
        system: 'client',
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
        system: 'client',
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
              system: 'client',
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

  const ISSUES_QUERY_WITH_EXISTS_FROM_PERMISSIONS: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        system: 'permissions',
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
              system: 'permissions',
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

  const ISSUES_QUERY_WITH_EXISTS_FROM_PERMISSIONS2: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        system: 'client',
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
              system: 'permissions',
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

  const UNIQUES_QUERY: AST = {
    table: 'uniques',
    orderBy: [['id', 'desc']],
  };

  const messages = new ReplicationMessages({
    'issues': 'id',
    'comments': 'id',
    'issueLabels': ['issueID', 'labelID'],
    'uniques': 'id',
    'zeroz.mutations': ['clientGroupID', 'clientID', 'mutationID'],
  });
  const zeroMessages = new ReplicationMessages(
    {schemaVersions: 'lock'},
    'zeroz',
  );

  function startTimer() {
    return new Timer().start();
  }

  function changes() {
    return [...pipelines.advance(startTimer()).changes];
  }

  test('replica version', () => {
    pipelines.init(null);
    expect(pipelines.replicaVersion).toBe('123');
  });

  test('add query', () => {
    pipelines.init(null);

    expect([...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS, startTimer())])
      .toMatchInlineSnapshot(`
        [
          {
            "queryHash": "hash1",
            "row": {
              "_0_version": "123",
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
              "_0_version": "123",
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
              "_0_version": "123",
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
              "_0_version": "123",
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
              "_0_version": "123",
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
              "_0_version": "123",
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

    // Adding a query with the same hash should be a noop.
    expect([
      ...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS, startTimer()),
    ]).toMatchInlineSnapshot(`[]`);
  });

  test('insert', () => {
    pipelines.init(null);
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS, startTimer())];

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

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "134",
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
            "_0_version": "134",
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
            "_0_version": "134",
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
    pipelines.init(null);
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS, startTimer())];

    replicator.processTransaction(
      '134',
      messages.delete('issues', {id: '1'}),
      messages.delete('comments', {id: '21'}),
    );

    expect(changes()).toMatchInlineSnapshot(`
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

  test('truncate', () => {
    pipelines.init(null);
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS, startTimer())];

    replicator.processTransaction('134', messages.truncate('comments'));

    expect(() => changes()).toThrowError(ResetPipelinesSignal);
  });

  test('update', () => {
    pipelines.init(null);
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS, startTimer())];

    replicator.processTransaction(
      '134',
      messages.update('comments', {id: '22', issueID: '3', upvotes: 20000}),
    );

    expect(changes()).toMatchInlineSnapshot(`
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
            "_0_version": "134",
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
      messages.update('comments', {id: '22', issueID: '3', upvotes: 10}),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "135",
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

  test('timeout on slow advancement', () => {
    pipelines.init(null);
    [
      ...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS, {
        totalElapsed: () => 10,
      }),
    ];

    replicator.processTransaction(
      '134',
      // Timeout only kicks in at 20 changes.
      ...Array.from({length: 20}, (_, i) =>
        messages.insert('issues', {id: String(30 + i)}),
      ),
    );

    // 6ms is larger than half of the hydration time.
    expect(() => [
      ...pipelines.advance({totalElapsed: () => 6}).changes,
    ]).toThrow(ResetPipelinesSignal);
  });

  test('reset', () => {
    pipelines.init(null);
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS, startTimer())];
    expect(pipelines.addedQueries()).toEqual(new Set(['hash1']));

    replicator.processTransaction(
      '134',
      messages.addColumn('issues', 'newColumn', {dataType: 'TEXT', pos: 0}),
    );

    pipelines.advanceWithoutDiff();
    pipelines.reset(null);

    expect(pipelines.addedQueries()).toEqual(new Set());

    // The newColumn should be reflected after a reset.
    expect([...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS, startTimer())])
      .toMatchInlineSnapshot(`
        [
          {
            "queryHash": "hash1",
            "row": {
              "_0_version": "134",
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
              "_0_version": "134",
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
              "_0_version": "123",
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
              "_0_version": "123",
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
              "_0_version": "123",
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
              "_0_version": "134",
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
              "_0_version": "123",
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

  test('update unique non-primary key', () => {
    pipelines.init(null);
    expect([...pipelines.addQuery('hash1', UNIQUES_QUERY, startTimer())])
      .toMatchInlineSnapshot(`
        [
          {
            "queryHash": "hash1",
            "row": {
              "_0_version": "123",
              "id": "foo",
              "name": "bar",
            },
            "rowKey": {
              "id": "foo",
              "name": "bar",
            },
            "table": "uniques",
            "type": "add",
          },
          {
            "queryHash": "hash1",
            "row": {
              "_0_version": "123",
              "id": "boo",
              "name": "dar",
            },
            "rowKey": {
              "id": "boo",
              "name": "dar",
            },
            "table": "uniques",
            "type": "add",
          },
        ]
      `);

    replicator.processTransaction(
      '134',
      messages.update('uniques', {id: 'boo', name: 'far'}),
    );

    // Although this can be considered an edit of a row keyed by {id: 'boo'},
    // rows are ultimately referred to by their union key ['id', 'name'],
    // in which case this update must be represented as:
    // - `remove{id: 'boo', name: 'dar'}`
    // - `add{id: 'boo', name: 'far'}`
    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "id": "boo",
            "name": "dar",
          },
          "table": "uniques",
          "type": "remove",
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "134",
            "id": "boo",
            "name": "far",
          },
          "rowKey": {
            "id": "boo",
            "name": "far",
          },
          "table": "uniques",
          "type": "add",
        },
      ]
    `);
  });

  test('whereExists query', () => {
    pipelines.init(null);
    [...pipelines.addQuery('hash1', ISSUES_QUERY_WITH_EXISTS, startTimer())];

    replicator.processTransaction(
      '134',
      messages.delete('issueLabels', {
        issueID: '1',
        labelID: '1',
        legacyID: '1-1',
      }),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
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
            "issueID": "1",
            "labelID": "1",
            "legacyID": "1-1",
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
      ]
    `);
  });

  test('whereExists added by permissions return no rows', () => {
    pipelines.init(null);
    expect([
      ...pipelines.addQuery(
        'hash1',
        ISSUES_QUERY_WITH_EXISTS_FROM_PERMISSIONS,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "1",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": "add",
        },
      ]
    `);

    expect([
      ...pipelines.addQuery(
        'hash2',
        ISSUES_QUERY_WITH_EXISTS_FROM_PERMISSIONS2,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash2",
          "row": {
            "_0_version": "123",
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
          "queryHash": "hash2",
          "row": {
            "_0_version": "123",
            "issueID": "1",
            "labelID": "1",
            "legacyID": "1-1",
          },
          "rowKey": {
            "issueID": "1",
            "labelID": "1",
            "legacyID": "1-1",
          },
          "table": "issueLabels",
          "type": "add",
        },
      ]
    `);
  });

  test('whereExists generates the correct number of add and remove changes', () => {
    const query: AST = {
      table: 'issues',
      where: {
        type: 'and',
        conditions: [
          {
            op: '=',
            left: {
              name: 'closed',
              type: 'column',
            },
            type: 'simple',
            right: {
              type: 'literal',
              value: true,
            },
          },
          {
            op: 'EXISTS',
            type: 'correlatedSubquery',
            related: {
              subquery: {
                alias: 'zsubq_labels',
                table: 'issueLabels',
                where: {
                  op: 'EXISTS',
                  type: 'correlatedSubquery',
                  related: {
                    subquery: {
                      alias: 'zsubq_labels',
                      table: 'labels',
                      where: {
                        op: '=',
                        left: {
                          name: 'name',
                          type: 'column',
                        },
                        type: 'simple',
                        right: {
                          type: 'literal',
                          value: 'bug',
                        },
                      },
                      orderBy: [['id', 'asc']],
                    },
                    system: 'client',
                    correlation: {
                      childField: ['id'],
                      parentField: ['labelID'],
                    },
                  },
                },
                orderBy: [
                  ['issueID', 'asc'],
                  ['labelID', 'asc'],
                ],
              },
              system: 'client',
              correlation: {
                childField: ['issueID'],
                parentField: ['id'],
              },
            },
          },
        ],
      },
      orderBy: [['id', 'desc']],
      related: [
        {
          subquery: {
            alias: 'issueLabels',
            table: 'issueLabels',
            orderBy: [
              ['issueID', 'asc'],
              ['labelID', 'asc'],
            ],
            related: [
              {
                hidden: true,
                subquery: {
                  alias: 'labels',
                  table: 'labels',
                  orderBy: [['id', 'asc']],
                },
                system: 'client',
                correlation: {
                  childField: ['id'],
                  parentField: ['labelID'],
                },
              },
            ],
          },
          system: 'client',
          correlation: {
            childField: ['issueID'],
            parentField: ['id'],
          },
        },
      ],
    };

    pipelines.init(null);
    [...pipelines.addQuery('hash1', query, startTimer())];

    replicator.processTransaction(
      '134',
      messages.insert('issueLabels', {
        issueID: '2',
        labelID: '1',
        legacyID: '2-1',
      }),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "123",
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
            "_0_version": "134",
            "issueID": "2",
            "labelID": "1",
            "legacyID": "2-1",
          },
          "rowKey": {
            "issueID": "2",
            "labelID": "1",
            "legacyID": "2-1",
          },
          "table": "issueLabels",
          "type": "add",
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "123",
            "id": "1",
            "name": "bug",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "labels",
          "type": "add",
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "134",
            "issueID": "2",
            "labelID": "1",
            "legacyID": "2-1",
          },
          "rowKey": {
            "issueID": "2",
            "labelID": "1",
            "legacyID": "2-1",
          },
          "table": "issueLabels",
          "type": "add",
        },
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "123",
            "id": "1",
            "name": "bug",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "labels",
          "type": "add",
        },
      ]
    `);

    replicator.processTransaction(
      '135',
      messages.delete('issueLabels', {
        issueID: '2',
        labelID: '1',
        legacyID: '2-1',
      }),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "id": "2",
          },
          "table": "issues",
          "type": "remove",
        },
        {
          "queryHash": "hash1",
          "row": undefined,
          "rowKey": {
            "issueID": "2",
            "labelID": "1",
            "legacyID": "2-1",
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
            "issueID": "2",
            "labelID": "1",
            "legacyID": "2-1",
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
      ]
    `);
  });

  test('getRow', () => {
    pipelines.init(null);

    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS, startTimer())];

    // Post-hydration
    expect(pipelines.getRow('issues', {id: '1'})).toEqual({
      id: '1',
      closed: false,
      ['_0_version']: '123',
    });

    expect(pipelines.getRow('comments', {id: '22'})).toEqual({
      id: '22',
      issueID: '2',
      upvotes: 20000,
      ['_0_version']: '123',
    });

    replicator.processTransaction(
      '134',
      messages.update('comments', {id: '22', issueID: '3', upvotes: 20000}),
    );
    changes();

    // Post-advancement
    expect(pipelines.getRow('comments', {id: '22'})).toEqual({
      id: '22',
      issueID: '3',
      upvotes: 20000,
      ['_0_version']: '134',
    });

    [...pipelines.addQuery('hash2', ISSUES_QUERY_WITH_EXISTS, startTimer())];

    // getRow should work with any row key
    expect(
      pipelines.getRow('issueLabels', {issueID: '1', labelID: '1'}),
    ).toEqual({
      issueID: '1',
      labelID: '1',
      legacyID: '1-1',
      ['_0_version']: '123',
    });

    expect(pipelines.getRow('issueLabels', {legacyID: '1-1'})).toEqual({
      issueID: '1',
      labelID: '1',
      legacyID: '1-1',
      ['_0_version']: '123',
    });
  });

  test('get mutation results', () => {
    pipelines.init(null);
    const mutationResultsQuery = getMutationResultsQuery('zeroz', 'cg1');

    replicator.processTransaction(
      '134',
      messages.insert('zeroz.mutations', {
        clientGroupID: 'cg1',
        clientID: 'c1',
        mutationID: 1,
        result: {},
      }),
    );

    [
      ...pipelines.addQuery(
        mutationResultsQuery.id,
        mutationResultsQuery.ast,
        startTimer(),
      ),
    ];

    expect(
      pipelines.getRow('zeroz.mutations', {
        clientGroupID: 'cg1',
        clientID: 'c1',
        mutationID: 1,
      }),
    ).toMatchInlineSnapshot(`undefined`);
  });

  test('schemaVersions change and insert', () => {
    pipelines.init(null);
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS, startTimer())];

    replicator.processTransaction(
      '134',
      messages.insert('issues', {id: '4', closed: 0}),
      zeroMessages.update('schemaVersions', {
        lock: true,
        minSupportedVersion: 1,
        maxSupportedVersion: 2,
      }),
    );

    expect(pipelines.currentSchemaVersions()).toEqual({
      minSupportedVersion: 1,
      maxSupportedVersion: 1,
    });

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "134",
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
    pipelines.init(null);
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS, startTimer())];

    replicator.processTransaction(
      '134',
      messages.insert('issues', {id: '4', closed: 0}),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "134",
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

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryHash": "hash1",
          "row": {
            "_0_version": "156",
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

    expect(changes()).toMatchInlineSnapshot(`
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
    pipelines.init(null);
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS, startTimer())];

    expect([...pipelines.addedQueries()]).toEqual(['hash1']);
    pipelines.removeQuery('hash1');
    expect([...pipelines.addedQueries()]).toEqual([]);

    replicator.processTransaction(
      '134',
      messages.insert('comments', {id: '31', issueID: '3', upvotes: 0}),
      messages.insert('comments', {id: '41', issueID: '4', upvotes: 0}),
      messages.insert('issues', {id: '4', closed: 1}),
    );

    expect(pipelines.currentVersion()).toBe('123');
    expect(changes()).toHaveLength(0);
    expect(pipelines.currentVersion()).toBe('134');
  });

  test('push fails on out of bounds numbers', () => {
    pipelines.init(null);
    [...pipelines.addQuery('hash1', ISSUES_AND_COMMENTS, startTimer())];

    replicator.processTransaction(
      '134',
      messages.insert('comments', {
        id: '31',
        issueID: '3',
        upvotes: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
      }),
    );

    expect(() => changes()).toThrowError();
  });
});
