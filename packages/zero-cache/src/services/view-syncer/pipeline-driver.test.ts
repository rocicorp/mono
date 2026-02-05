import type {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import type {Database as DB} from '../../../../zqlite/src/db.ts';
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
import {getMutationResultsQuery} from './cvr.ts';
import {PipelineDriver, type Timer} from './pipeline-driver.ts';
import {ResetPipelinesSignal, Snapshotter} from './snapshotter.ts';
import {TimeSliceTimer} from './view-syncer.ts';

const NO_TIME_ADVANCEMENT_TIMER: Timer = {
  elapsedLap: () => 0,
  totalElapsed: () => 0,
};

describe('view-syncer/pipeline-driver', () => {
  const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
  const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
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
      new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
      shardID,
      new DatabaseStorage(storage).createClientGroupStorage('foo-client-group'),
      'pipeline-driver.test.ts',
      new InspectorDelegate(undefined),
      () => 200 /** yield threshold */,
    );

    db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], '123');
    db.exec(`
      CREATE TABLE "${mutationsTableName}" (
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
        stillBeingBackfilled TEXT,
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

    // Initialize ColumnMetadata and mark a column as being backfilled,
    // to verify that it does not appear in the pipeline results.
    populateFromExistingTables(db, listTables(db, false));
    db.prepare(
      /*sql*/ `
      UPDATE "_zero.column_metadata" 
        SET backfill = '{"upstreamID":123}'
        WHERE table_name = 'comments' 
         AND column_name = 'stillBeingBackfilled'
      `,
    ).run();
    replicator = fakeReplicator(lc, db);
  });

  afterEach(() => {
    dbFile.delete();
  });

  const issues = table('issues')
    .columns({
      id: string(),
      closed: boolean(),
    })
    .primaryKey('id');
  const comments = table('comments')
    .columns({
      id: string(),
      issueID: string(),
      upvotes: number(),
    })
    .primaryKey('id');
  const issueLabels = table('issueLabels')
    .columns({
      issueID: string(),
      labelID: string(),
      legacyID: string(),
    })
    .primaryKey('issueID', 'labelID');
  const labels = table('labels')
    .columns({
      id: string(),
      name: string(),
    })
    .primaryKey('id');
  const uniques = table('uniques')
    .columns({
      id: string(),
      name: string(),
    })
    .primaryKey('id');

  const clientSchema = createSchema({
    tables: [issues, comments, issueLabels, labels, uniques],
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

  const ISSUES_WITH_SCALAR_SUBQUERY: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
    where: {
      type: 'scalarSubquery',
      op: '=',
      parentField: 'id',
      childField: 'issueID',
      subquery: {
        table: 'comments',
        orderBy: [['id', 'asc']],
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: '10'},
        },
      },
    },
  };

  const ISSUES_WITH_NONEXISTENT_SCALAR_SUBQUERY: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
    where: {
      type: 'scalarSubquery',
      op: '=',
      parentField: 'id',
      childField: 'issueID',
      subquery: {
        table: 'comments',
        orderBy: [['id', 'asc']],
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: 'nonexistent'},
        },
      },
    },
  };

  const messages = new ReplicationMessages({
    issues: 'id',
    comments: 'id',
    issueLabels: ['issueID', 'labelID'],
    uniques: 'id',
    [mutationsTableName]: ['clientGroupID', 'clientID', 'mutationID'],
  });

  function startTimer() {
    return new TimeSliceTimer(lc).startWithoutYielding();
  }

  function changes(timer: Timer = NO_TIME_ADVANCEMENT_TIMER) {
    return [...pipelines.advance(timer).changes];
  }

  test('replica version', () => {
    pipelines.init(clientSchema);
    expect(pipelines.replicaVersion).toBe('123');
  });

  test('add query', () => {
    pipelines.init(clientSchema);

    expect([
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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

  test('insert', () => {
    pipelines.init(clientSchema);
    [
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
    pipelines.init(clientSchema);
    [
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    replicator.processTransaction(
      '134',
      messages.delete('issues', {id: '1'}),
      messages.delete('comments', {id: '21'}),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": "remove",
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
          "type": "remove",
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "21",
          },
          "table": "comments",
          "type": "remove",
        },
      ]
    `);
  });

  test('truncate', () => {
    pipelines.init(clientSchema);
    [
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    replicator.processTransaction('134', messages.truncate('comments'));

    expect(() => changes()).toThrowError(ResetPipelinesSignal);
  });

  test('update', () => {
    pipelines.init(clientSchema);
    [
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    replicator.processTransaction(
      '134',
      messages.update('comments', {id: '22', issueID: '3', upvotes: 20000}),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "22",
          },
          "table": "comments",
          "type": "remove",
        },
        {
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
    pipelines.init(clientSchema);
    [
      ...pipelines.addQuery('hash1', 'queryID1', ISSUES_AND_COMMENTS, {
        // hydration time
        totalElapsed: () => 100,
        elapsedLap: () => 100,
      }),
    ];

    replicator.processTransaction('134', messages.insert('issues', {id: 'i1'}));

    // 60ms is larger than half of the hydration time.
    expect(() => [
      ...pipelines.advance({totalElapsed: () => 60, elapsedLap: () => 60})
        .changes,
    ]).toThrowErrorMatchingInlineSnapshot(
      `[ResetPipelinesSignal: Advancement exceeded timeout at 0 of 1 changes after 60 ms. Advancement time limited based on total hydration time of 100 ms.]`,
    );

    // Test that after reset hydration and advancement work.
    pipelines.reset(clientSchema);

    expect(pipelines.queries()).toEqual(new Map());

    [
      ...pipelines.addQuery('hash1', 'queryID1', ISSUES_AND_COMMENTS, {
        // hydration time
        totalElapsed: () => 100,
        elapsedLap: () => 100,
      }),
    ];

    replicator.processTransaction('140', messages.insert('issues', {id: 'i1'}));

    expect(() => [
      ...pipelines.advance({totalElapsed: () => 20, elapsedLap: () => 20})
        .changes,
    ]).not.toThrow();
  });

  test('advancement timeout has a minimum limit', () => {
    pipelines.init(clientSchema);
    [
      ...pipelines.addQuery('hash1', 'queryID1', ISSUES_AND_COMMENTS, {
        // very low hydration time
        totalElapsed: () => 25,
        elapsedLap: () => 25,
      }),
    ];

    replicator.processTransaction('134', messages.insert('issues', {id: 'i1'}));

    // 29 is larger than the hydration time but less than the minimum
    // advancement time limit
    expect(() => [
      ...pipelines.advance({totalElapsed: () => 29, elapsedLap: () => 29})
        .changes,
    ]).not.toThrow();
  });

  test('reset', () => {
    pipelines.init(clientSchema);
    [
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    expect(pipelines.queries().size).toEqual(1);
    expect(pipelines.queries().get('queryID1')?.transformationHash).toEqual(
      'hash1',
    );
    expect(pipelines.queries().get('queryID1')?.transformedAst).toEqual(
      ISSUES_AND_COMMENTS,
    );

    replicator.processTransaction(
      '134',
      messages.addColumn('issues', 'newColumn', {dataType: 'TEXT', pos: 0}),
    );

    pipelines.advanceWithoutDiff();
    pipelines.reset(clientSchema);

    expect(pipelines.queries()).toEqual(new Map());

    // The newColumn should be reflected after a reset.
    expect([
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
    pipelines.init(clientSchema);
    expect([
      ...pipelines.addQuery('hash1', 'queryID1', UNIQUES_QUERY, startTimer()),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "foo",
            "name": "bar",
          },
          "rowKey": {
            "id": "foo",
          },
          "table": "uniques",
          "type": "add",
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "boo",
            "name": "dar",
          },
          "rowKey": {
            "id": "boo",
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
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "id": "boo",
            "name": "far",
          },
          "rowKey": {
            "id": "boo",
          },
          "table": "uniques",
          "type": "edit",
        },
      ]
    `);
  });

  test('unique constraint conflict due to changelog compression', () => {
    pipelines.init(clientSchema);
    expect([
      ...pipelines.addQuery('hash1', 'queryID1', UNIQUES_QUERY, startTimer()),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "foo",
            "name": "bar",
          },
          "rowKey": {
            "id": "foo",
          },
          "table": "uniques",
          "type": "add",
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "boo",
            "name": "dar",
          },
          "rowKey": {
            "id": "boo",
          },
          "table": "uniques",
          "type": "add",
        },
      ]
    `);

    replicator.processTransaction(
      '134',
      messages.delete('uniques', {id: 'foo'}),
      messages.insert('uniques', {id: 'baz', name: 'bar'}),
      messages.insert('uniques', {id: 'foo', name: 'wuzzy'}),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "foo",
          },
          "table": "uniques",
          "type": "remove",
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "id": "baz",
            "name": "bar",
          },
          "rowKey": {
            "id": "baz",
          },
          "table": "uniques",
          "type": "add",
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "id": "foo",
            "name": "wuzzy",
          },
          "rowKey": {
            "id": "foo",
          },
          "table": "uniques",
          "type": "add",
        },
      ]
    `);
  });

  test('whereExists query', () => {
    pipelines.init(clientSchema);
    [
      ...pipelines.addQuery(
        'hash1',
        'queryID',
        ISSUES_QUERY_WITH_EXISTS,
        startTimer(),
      ),
    ];

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
          "queryID": "queryID",
          "row": undefined,
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": "remove",
        },
        {
          "queryID": "queryID",
          "row": undefined,
          "rowKey": {
            "issueID": "1",
            "labelID": "1",
          },
          "table": "issueLabels",
          "type": "remove",
        },
        {
          "queryID": "queryID",
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
    pipelines.init(clientSchema);
    expect([
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_QUERY_WITH_EXISTS_FROM_PERMISSIONS,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
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
        'queryID',
        ISSUES_QUERY_WITH_EXISTS_FROM_PERMISSIONS2,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID",
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
          "queryID": "queryID",
          "row": {
            "_0_version": "123",
            "issueID": "1",
            "labelID": "1",
            "legacyID": "1-1",
          },
          "rowKey": {
            "issueID": "1",
            "labelID": "1",
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

    pipelines.init(clientSchema);
    [...pipelines.addQuery('hash1', 'queryID1', query, startTimer())];

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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "issueID": "2",
            "labelID": "1",
            "legacyID": "2-1",
          },
          "rowKey": {
            "issueID": "2",
            "labelID": "1",
          },
          "table": "issueLabels",
          "type": "add",
        },
        {
          "queryID": "queryID1",
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
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "issueID": "2",
            "labelID": "1",
            "legacyID": "2-1",
          },
          "rowKey": {
            "issueID": "2",
            "labelID": "1",
          },
          "table": "issueLabels",
          "type": "add",
        },
        {
          "queryID": "queryID1",
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
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "2",
          },
          "table": "issues",
          "type": "remove",
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "issueID": "2",
            "labelID": "1",
          },
          "table": "issueLabels",
          "type": "remove",
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "1",
          },
          "table": "labels",
          "type": "remove",
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "issueID": "2",
            "labelID": "1",
          },
          "table": "issueLabels",
          "type": "remove",
        },
        {
          "queryID": "queryID1",
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
    pipelines.init(clientSchema);

    [
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

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

    [
      ...pipelines.addQuery(
        'hash2',
        'queryID2',
        ISSUES_QUERY_WITH_EXISTS,
        startTimer(),
      ),
    ];

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
    pipelines.init(clientSchema);
    const mutationResultsQuery = getMutationResultsQuery(
      upstreamSchema(shardID),
      'cg1',
    );

    replicator.processTransaction(
      '134',
      messages.insert(mutationsTableName, {
        clientGroupID: 'cg1',
        clientID: 'c1',
        mutationID: 1,
        result: {},
      }),
    );

    [
      ...pipelines.addQuery(
        mutationResultsQuery.id,
        'queryID1',
        mutationResultsQuery.ast,
        startTimer(),
      ),
    ];

    expect(
      pipelines.getRow(mutationsTableName, {
        clientGroupID: 'cg1',
        clientID: 'c1',
        mutationID: 1,
      }),
    ).toMatchInlineSnapshot(`undefined`);
  });

  test('multiple advancements', () => {
    pipelines.init(clientSchema);
    [
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    replicator.processTransaction(
      '134',
      messages.insert('issues', {id: '4', closed: 0}),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
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
          "queryID": "queryID1",
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
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "4",
          },
          "table": "issues",
          "type": "remove",
        },
        {
          "queryID": "queryID1",
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
    pipelines.init(clientSchema);
    [
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    expect(pipelines.queries().size).toEqual(1);
    expect(pipelines.queries().get('queryID1')?.transformationHash).toEqual(
      'hash1',
    );
    expect(pipelines.queries().get('queryID1')?.transformedAst).toEqual(
      ISSUES_AND_COMMENTS,
    );

    pipelines.removeQuery('queryID1');
    expect(pipelines.queries()).toEqual(new Map());

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
    pipelines.init(clientSchema);
    [
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

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

  test('scalar subquery resolves to literal', () => {
    pipelines.init(clientSchema);

    // Comment '10' has issueID='1', so the subquery resolves to id = '1'
    const results = [
      ...pipelines.addQuery(
        'hash-scalar',
        'queryScalar',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    expect(results).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryScalar",
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

    // The transformedAst should have the scalar subquery resolved to a simple condition
    expect(
      pipelines.queries().get('queryScalar')?.transformedAst.where,
    ).toEqual({
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'id'},
      right: {type: 'literal', value: '1'},
    });
  });

  test('scalar subquery with no matching rows', () => {
    pipelines.init(clientSchema);

    const results = [
      ...pipelines.addQuery(
        'hash-scalar-none',
        'queryScalarNone',
        ISSUES_WITH_NONEXISTENT_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    expect(results).toEqual([]);

    // The transformedAst should have ALWAYS_FALSE
    expect(
      pipelines.queries().get('queryScalarNone')?.transformedAst.where,
    ).toEqual({
      type: 'simple',
      op: '=',
      left: {type: 'literal', value: 1},
      right: {type: 'literal', value: 0},
    });
  });

  test('scalar subquery in AND with other conditions', () => {
    pipelines.init(clientSchema);

    const queryWithAnd: AST = {
      table: 'issues',
      orderBy: [['id', 'asc']],
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'closed'},
            right: {type: 'literal', value: false},
          },
          {
            type: 'scalarSubquery',
            op: '=',
            parentField: 'id',
            childField: 'issueID',
            subquery: {
              table: 'comments',
              orderBy: [['id', 'asc']],
              where: {
                type: 'simple',
                op: '=',
                left: {type: 'column', name: 'id'},
                right: {type: 'literal', value: '10'},
              },
            },
          },
        ],
      },
    };

    const results = [
      ...pipelines.addQuery(
        'hash-scalar-and',
        'queryScalarAnd',
        queryWithAnd,
        startTimer(),
      ),
    ];

    // Issue '1' is not closed and matches the subquery
    expect(results).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryScalarAnd",
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

    // The transformedAst should have the scalar subquery resolved within the AND
    expect(
      pipelines.queries().get('queryScalarAnd')?.transformedAst.where,
    ).toEqual({
      type: 'and',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'closed'},
          right: {type: 'literal', value: false},
        },
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: '1'},
        },
      ],
    });
  });

  test('advancement after scalar subquery resolution', () => {
    pipelines.init(clientSchema);

    // This resolves to `issues WHERE id = '1'`
    [
      ...pipelines.addQuery(
        'hash-scalar',
        'queryScalar',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    replicator.processTransaction(
      '134',
      messages.insert('issues', {id: '5', closed: 0}),
      messages.update('issues', {id: '1', closed: 1}),
    );

    // Only the edit to issue '1' should appear (it matches the resolved filter),
    // NOT the insert of issue '5' (which doesn't match id = '1').
    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryScalar",
          "row": {
            "_0_version": "134",
            "closed": true,
            "id": "1",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": "edit",
        },
      ]
    `);
  });
});
