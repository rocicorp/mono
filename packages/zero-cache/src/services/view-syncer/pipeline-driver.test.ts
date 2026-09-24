import {LogContext} from '@rocicorp/logger';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  type MockInstance,
  test,
  vi,
} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../shared/src/must.ts';
import type {
  AST,
  Condition,
  CorrelatedSubquery,
  CorrelatedSubqueryCondition,
} from '../../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
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
import {TableSource} from '../../../../zqlite/src/table-source.ts';
import type {ZeroConfig} from '../../config/zero-config.ts';
import {listTables} from '../../db/lite-tables.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import type {RowKey} from '../../types/row-key.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import {populateFromExistingTables} from '../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../replicator/test-utils.ts';
import {getMutationResultsQuery} from './cvr.ts';
import {DeferredWritesBudget} from './deferred-writes-budget.ts';
import {testDeferredWritesBudget} from './deferred-writes-test-util.ts';
import {PipelineDriver, type RowChange, type Timer} from './pipeline-driver.ts';
import {rowIDSignatureUnit} from './row-set-signature.ts';
import type {RowID} from './schema/types.ts';
import {SnapshotRowCache} from './snapshot-row-cache.ts';
import {ResetPipelinesSignal, Snapshotter} from './snapshotter.ts';
import {TimeSliceTimer} from './view-syncer.ts';

const NO_TIME_ADVANCEMENT_TIMER: Timer = {
  elapsedLap: () => 0,
  totalElapsed: () => 0,
};

// By default, as with `deferIvmWrites` on, view-syncer derivation is held in
// an in-memory batch overlay. `vitest.config.write-through-ivm.ts` and
// `vitest.config.mixed-ivm.ts` run this whole file again with it written to
// (and rolled back out of) the replica snapshot, for every advancement or for
// some, and some switch partway. Every result here must be identical either
// way.
const deferredWritesBudget = testDeferredWritesBudget;

describe('view-syncer/pipeline-driver', () => {
  const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
  const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
  let dbFile: DbFile;
  let db: DB;
  let lc: LogContext;
  let logSink: TestLogSink;
  let pipelines: PipelineDriver;
  let replicator: FakeReplicator;

  beforeEach(() => {
    logSink = new TestLogSink();
    lc = new LogContext('error', undefined, logSink);
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
      undefined,
      undefined,
      deferredWritesBudget(),
    );

    db = dbFile.connect(lc);
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
      CREATE TABLE issues (
        id TEXT PRIMARY KEY,
        closed BOOL,
        ignored BYTEA,
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

      CREATE TABLE backfilling (id TEXT PRIMARY KEY, _0_version TEXT NOT NULL);
      `);

    // Initialize ColumnMetadata and mark columns/tables as being backfilled,
    // to verify that it does not appear in the pipeline results.
    populateFromExistingTables(db, listTables(db, false));
    db.exec(/*sql*/ `
      UPDATE "_zero.column_metadata" 
        SET backfill = '{"upstreamID":123}'
        WHERE table_name = 'comments' 
         AND column_name = 'stillBeingBackfilled';
      UPDATE "_zero.column_metadata" 
        SET backfill = '{"upstreamID":456}'
        WHERE table_name = 'backfilling' ;
      `);
    replicator = fakeReplicator(lc, db);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    dbFile.delete();
  });

  /**
   * Spies on the `destroy` of every source connection made from now on, so a
   * test can check that a pipeline built for a query was torn down. The
   * `failFetchOf` connection (1-based) throws when fetched.
   */
  function trackSourceConnections(failFetchOf?: number): MockInstance[] {
    const destroys: MockInstance[] = [];
    const connect = TableSource.prototype.connect;
    vi.spyOn(TableSource.prototype, 'connect').mockImplementation(function (
      this: TableSource,
      ...args: Parameters<TableSource['connect']>
    ) {
      const input = connect.apply(this, args);
      destroys.push(vi.spyOn(input, 'destroy'));
      if (destroys.length === failFetchOf) {
        vi.spyOn(input, 'fetch').mockImplementation(() => {
          throw new Error('simulated fetch failure');
        });
      }
      return input;
    });
    return destroys;
  }

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

  const subsetClientSchema = createSchema({
    tables: [issues],
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
      type: 'correlatedSubquery',
      op: 'EXISTS',
      scalar: true,
      related: {
        correlation: {
          parentField: ['id'],
          childField: ['issueID'],
        },
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
    },
  };

  const ISSUES_WITH_NONEXISTENT_SCALAR_SUBQUERY: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      scalar: true,
      related: {
        correlation: {
          parentField: ['id'],
          childField: ['issueID'],
        },
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
    },
  };

  const messages = new ReplicationMessages({
    issues: 'id',
    comments: 'id',
    issueLabels: ['issueID', 'labelID'],
    uniques: 'id',
    backfilling: 'id',
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
        },
      ]
    `);
  });

  test('logs query identity when query hydration fails', () => {
    pipelines.init(clientSchema);

    expect(() => [
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        {table: 'doesNotExist'},
        startTimer(),
        'myQuery',
      ),
    ]).toThrowError(/doesNotExist/);

    const failureLog = logSink.messages.find(
      ([level, context, args]) =>
        level === 'error' &&
        context?.queryHash === 'queryID1' &&
        args[0] === 'query hydration failed',
    );
    expect(failureLog?.[1]).toMatchObject({
      queryHash: 'queryID1',
      queryName: 'myQuery',
      transformationHash: 'hash1',
    });
  });

  test('logs query pipeline lifecycle', () => {
    logSink.messages.length = 0;
    lc = new LogContext(
      'info',
      {
        taskID: 'task-a',
        worker: 'syncer',
        workerIndex: 2,
        component: 'view-syncer',
        clientGroupID: 'foo-client-group',
        instance: 'view-syncer-instance',
      },
      logSink,
    );

    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    pipelines = new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
      shardID,
      new DatabaseStorage(storage).createClientGroupStorage('foo-client-group'),
      'foo-client-group',
      new InspectorDelegate(undefined),
      () => 200 /** yield threshold */,
      undefined,
      undefined,
      deferredWritesBudget(),
    );
    pipelines.init(clientSchema);

    [
      ...pipelines.addQuery(
        'transformation-hash-1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        NO_TIME_ADVANCEMENT_TIMER,
      ),
    ];
    [
      ...pipelines.addQuery(
        'transformation-hash-1',
        'queryID2',
        ISSUES_AND_COMMENTS,
        NO_TIME_ADVANCEMENT_TIMER,
      ),
    ];
    pipelines.removeQuery('queryID1');

    const lifecycleContexts = logSink.messages
      .filter(
        ([level, context, args]) =>
          level === 'info' &&
          context?.zeroEvent !== undefined &&
          args[0] === 'query pipeline lifecycle',
      )
      .map(([, context]) => context);

    expect(lifecycleContexts.map(c => c?.zeroEvent)).toEqual([
      'query-pipeline-hydrate-start',
      'query-pipeline-hydrate-finish',
      'query-pipeline-hydrate-start',
      'query-pipeline-hydrate-finish',
      'query-pipeline-stop',
    ]);

    const query1Start = lifecycleContexts.find(
      c =>
        c?.zeroEvent === 'query-pipeline-hydrate-start' &&
        c.queryHash === 'queryID1',
    );
    const query1Finish = lifecycleContexts.find(
      c =>
        c?.zeroEvent === 'query-pipeline-hydrate-finish' &&
        c.queryHash === 'queryID1',
    );
    const query1Stop = lifecycleContexts.find(
      c => c?.zeroEvent === 'query-pipeline-stop' && c.queryHash === 'queryID1',
    );
    const query2Start = lifecycleContexts.find(
      c =>
        c?.zeroEvent === 'query-pipeline-hydrate-start' &&
        c.queryHash === 'queryID2',
    );
    const query2Finish = lifecycleContexts.find(
      c =>
        c?.zeroEvent === 'query-pipeline-hydrate-finish' &&
        c.queryHash === 'queryID2',
    );

    expect(query1Start).toMatchObject({
      taskID: 'task-a',
      worker: 'syncer',
      workerIndex: 2,
      component: 'view-syncer',
      clientGroupID: 'foo-client-group',
      instance: 'view-syncer-instance',
      queryHash: 'queryID1',
      transformationHash: 'transformation-hash-1',
      hydrationReason: 'query-set-sync',
    });
    expect(query1Finish).toMatchObject({
      queryHash: 'queryID1',
      pipelineRunID: query1Start?.pipelineRunID,
      hydrationRowCount: expect.any(Number),
      hydrationTimeMs: expect.any(Number),
    });
    expect(query2Start).toMatchObject({
      queryHash: 'queryID2',
      transformationHash: 'transformation-hash-1',
    });
    expect(query2Finish).toMatchObject({
      queryHash: 'queryID2',
      pipelineRunID: query2Start?.pipelineRunID,
    });
    expect(query1Stop).toMatchObject({
      zeroEvent: 'query-pipeline-stop',
      queryHash: 'queryID1',
      pipelineRunID: query1Start?.pipelineRunID,
      stopReason: 'remove-query',
      pipelineLifetimeMs: expect.any(Number),
    });
  });

  test('abandoned hydration tears down its pipeline', () => {
    pipelines.init(clientSchema);
    const hydration = pipelines
      .addQuery('hash1', 'queryID1', ISSUES_AND_COMMENTS, startTimer())
      [Symbol.iterator]();
    // Consume the first row, then abandon the hydration, as a consumer that
    // stops iterating early does.
    expect(hydration.next().done).toBe(false);
    hydration.return?.();

    expect(pipelines.queries().has('queryID1')).toBe(false);
    // The rows it yielded must not leave a partial signature behind.
    expect(pipelines.rowSetSignature('queryID1')).toBeUndefined();

    // The abandoned pipeline is disconnected from its sources, so a change to
    // a table it was reading no longer produces output for it.
    replicator.processTransaction(
      '134',
      messages.insert('issues', {id: '4', closed: 0}),
    );
    expect(changes()).toEqual([]);
  });

  // Clients can query comments with an empty issue-ID list. Opposite sort
  // orders make these distinct queries that can coexist in a client group.
  // Removing the first query must not remove index state needed to destroy
  // the second (nor when the first query was the last one with a nonempty
  // IN list on that column).
  test.each([
    {name: 'another empty IN query', firstIssueIDs: []},
    {name: 'the last nonempty IN query', firstIssueIDs: ['3']},
  ])('removes an empty IN query after $name', ({firstIssueIDs}) => {
    pipelines.init(clientSchema);
    const firstQuery: AST = {
      table: 'comments',
      orderBy: [['id', 'desc']],
      where: {
        type: 'simple',
        op: 'IN',
        left: {type: 'column', name: 'issueID'},
        right: {type: 'literal', value: firstIssueIDs},
      },
    };
    const emptyQuery: AST = {
      table: 'comments',
      orderBy: [['id', 'asc']],
      where: {
        type: 'simple',
        op: 'IN',
        left: {type: 'column', name: 'issueID'},
        right: {type: 'literal', value: []},
      },
    };
    expect([
      ...pipelines.addQuery('hash1', 'queryID1', firstQuery, startTimer()),
    ]).toEqual([]);
    expect([
      ...pipelines.addQuery('hash2', 'queryID2', emptyQuery, startTimer()),
    ]).toEqual([]);
    expect(pipelines.queries().size).toBe(2);
    pipelines.removeQuery('queryID1');
    expect([...pipelines.queries().keys()]).toEqual(['queryID2']);
    pipelines.removeQuery('queryID2');
    expect(pipelines.queries().size).toBe(0);
  });

  test('failed scalar subquery resolution tears down earlier companions', () => {
    pipelines.init(clientSchema);
    const scalar =
      ISSUES_WITH_SCALAR_SUBQUERY.where as CorrelatedSubqueryCondition;
    // Two scalar subqueries: the first resolves and connects a companion
    // pipeline to `comments`; executing the second fails at fetch time.
    const ast: AST = {
      ...ISSUES_WITH_SCALAR_SUBQUERY,
      where: {type: 'and', conditions: [scalar, scalar]},
    };
    const destroys = trackSourceConnections(2);
    expect(() => [
      ...pipelines.addQuery('hash1', 'queryID1', ast, startTimer()),
    ]).toThrow('simulated fetch failure');
    expect(pipelines.queries().has('queryID1')).toBe(false);

    // Both companions connected before the second one failed, and both
    // connections were torn down.
    expect(destroys).toHaveLength(2);
    for (const destroy of destroys) {
      expect(destroy).toHaveBeenCalledTimes(1);
    }
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
      messages.insert('backfilling', {id: 123}), // should be ignored
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "21",
          },
          "table": "comments",
          "type": 1,
        },
      ]
    `);
  });

  test('delete with limit refills deficit', () => {
    pipelines.init(clientSchema);
    const ISSUES_LIMIT_2: AST = {
      table: 'issues',
      orderBy: [['id', 'asc']],
      limit: 2,
    };

    const initial = [
      ...pipelines.addQuery(
        'hash-limit',
        'queryLimit',
        ISSUES_LIMIT_2,
        startTimer(),
      ),
    ] as RowChange[];
    expect(initial.map(r => (r.row as {id: string}).id)).toEqual(['1', '2']);

    replicator.processTransaction('134', messages.delete('issues', {id: '1'}));

    const advChanges = changes();
    expect(advChanges).toEqual([
      {
        queryID: 'queryLimit',
        table: 'issues',
        type: 1, // REMOVE
        row: undefined,
        rowKey: {id: '1'},
      },
      {
        queryID: 'queryLimit',
        table: 'issues',
        type: 0, // ADD
        row: {
          _0_version: '123',
          closed: false,
          id: '3',
        },
        rowKey: {id: '3'},
      },
    ]);
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
          "type": 1,
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
          "type": 0,
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
          "type": 2,
        },
      ]
    `);
  });

  test('rowSetSignature reflects hydrate + advance deltas', () => {
    const toID = (c: RowChange): RowID => ({
      schema: '',
      table: c.table,
      rowKey: c.rowKey as RowKey,
    });
    const sigFromChanges = (changes: readonly RowChange[]) => {
      let sig = 0n;
      for (const c of changes) {
        if (c.type === ChangeType.EDIT) continue;
        sig ^= rowIDSignatureUnit(toID(c));
      }
      return sig;
    };
    const onlyRowChanges = (
      xs: Iterable<RowChange | 'yield'>,
    ): readonly RowChange[] =>
      [...xs].filter((c): c is RowChange => c !== 'yield');

    pipelines.init(clientSchema);
    const hydrated = onlyRowChanges(
      pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    );
    expect(pipelines.rowSetSignature('queryID1')).toEqual(
      sigFromChanges(hydrated),
    );

    // Delete issues/1 (cascades to comments/10) and insert a fresh issues/4.
    replicator.processTransaction(
      '134',
      messages.delete('issues', {id: '1'}),
      messages.insert('issues', {id: '4', closed: 0}),
    );
    const advanced = onlyRowChanges(changes());
    expect(pipelines.rowSetSignature('queryID1')).toEqual(
      sigFromChanges([...hydrated, ...advanced]),
    );

    // An update that doesn't touch relationship keys yields EDITs only;
    // the signature must stay the same.
    const sigBeforeEdit = pipelines.rowSetSignature('queryID1');
    replicator.processTransaction(
      '135',
      messages.update('comments', {id: '22', issueID: '2', upvotes: 99}),
    );
    const afterEdit = onlyRowChanges(changes());
    expect(afterEdit.length).toBeGreaterThan(0);
    expect(afterEdit.every(c => c.type === ChangeType.EDIT)).toBe(true);
    expect(pipelines.rowSetSignature('queryID1')).toEqual(sigBeforeEdit);

    // removeQuery clears the entry.
    pipelines.removeQuery('queryID1');
    expect(pipelines.rowSetSignature('queryID1')).toBeUndefined();
  });

  test('rowSetSignature resets on re-execution (addQuery with same queryID)', () => {
    const toID = (c: RowChange): RowID => ({
      schema: '',
      table: c.table,
      rowKey: c.rowKey as RowKey,
    });
    const sigFromChanges = (changes: readonly RowChange[]) => {
      let sig = 0n;
      for (const c of changes) {
        if (c.type === ChangeType.EDIT) continue;
        sig ^= rowIDSignatureUnit(toID(c));
      }
      return sig;
    };
    const onlyRowChanges = (
      xs: Iterable<RowChange | 'yield'>,
    ): readonly RowChange[] =>
      [...xs].filter((c): c is RowChange => c !== 'yield');

    pipelines.init(clientSchema);

    const firstChanges = onlyRowChanges(
      pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    );
    const firstSig = pipelines.rowSetSignature('queryID1');
    expect(firstSig).toEqual(sigFromChanges(firstChanges));
    expect(firstSig).not.toEqual(0n);

    // Re-execute with a new transformation hash. addQuery internally calls
    // removeQuery, which must reset the signature before hydration accumulates
    // from 0. If it didn't, the second hydration's XORs would cancel the
    // first's (same AST, same rows) and land at 0n.
    const secondChanges = onlyRowChanges(
      pipelines.addQuery(
        'hash2',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    );
    expect(pipelines.rowSetSignature('queryID1')).toEqual(
      sigFromChanges(secondChanges),
    );
    expect(pipelines.rowSetSignature('queryID1')).toEqual(firstSig);
  });

  test('rowSetSignature is maintained independently per query', () => {
    const toID = (c: RowChange): RowID => ({
      schema: '',
      table: c.table,
      rowKey: c.rowKey as RowKey,
    });
    const sigFromChanges = (changes: readonly RowChange[]) => {
      let sig = 0n;
      for (const c of changes) {
        if (c.type === ChangeType.EDIT) continue;
        sig ^= rowIDSignatureUnit(toID(c));
      }
      return sig;
    };
    const onlyRowChanges = (
      xs: Iterable<RowChange | 'yield'>,
    ): readonly RowChange[] =>
      [...xs].filter((c): c is RowChange => c !== 'yield');

    const ISSUES_ONLY: AST = {table: 'issues', orderBy: [['id', 'desc']]};

    pipelines.init(clientSchema);

    const commentedHydrated = onlyRowChanges(
      pipelines.addQuery(
        'hash-issues-comments',
        'qIssuesComments',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    );
    const issuesHydrated = onlyRowChanges(
      pipelines.addQuery(
        'hash-issues',
        'qIssuesOnly',
        ISSUES_ONLY,
        startTimer(),
      ),
    );

    expect(pipelines.rowSetSignature('qIssuesComments')).toEqual(
      sigFromChanges(commentedHydrated),
    );
    expect(pipelines.rowSetSignature('qIssuesOnly')).toEqual(
      sigFromChanges(issuesHydrated),
    );

    const issuesOnlySigBefore = pipelines.rowSetSignature('qIssuesOnly');

    // Delete a comment: only qIssuesComments reads from the comments table,
    // so only its signature should change. qIssuesOnly's pipeline never sees
    // the event, so its signature is untouched.
    replicator.processTransaction(
      '134',
      messages.delete('comments', {id: '22'}),
    );
    const advanced = onlyRowChanges(changes());
    expect(advanced.length).toBeGreaterThan(0);
    expect(advanced.every(c => c.queryID === 'qIssuesComments')).toBe(true);

    expect(pipelines.rowSetSignature('qIssuesComments')).toEqual(
      sigFromChanges([...commentedHydrated, ...advanced]),
    );
    expect(pipelines.rowSetSignature('qIssuesOnly')).toEqual(
      issuesOnlySigBefore,
    );
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

  function warnLoggingDrivers(
    clientGroupIDs: string[],
    logConfig: Partial<typeof testLogConfig>,
  ): PipelineDriver[] {
    const warnLC = new LogContext('warn', undefined, logSink);
    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    const databaseStorage = new DatabaseStorage(storage);
    return clientGroupIDs.map(clientGroupID => {
      const driver = new PipelineDriver(
        warnLC,
        {...testLogConfig, ...logConfig},
        new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
        shardID,
        databaseStorage.createClientGroupStorage(clientGroupID),
        clientGroupID,
        new InspectorDelegate(undefined),
        () => 200 /** yield threshold */,
      );
      driver.init(clientSchema);
      return driver;
    });
  }

  function warnings(zeroEvent: string) {
    return logSink.messages.filter(
      ([level, , args]) =>
        level === 'warn' &&
        (args[1] as {zeroEvent?: string} | undefined)?.zeroEvent === zeroEvent,
    );
  }

  test('logs slow query advancements once per query shape across client groups', () => {
    const drivers = warnLoggingDrivers(['cg1', 'cg2'], {
      slowAdvanceThreshold: 0,
    });
    for (const driver of drivers) {
      [
        ...driver.addQuery(
          'hash1',
          'queryID1',
          ISSUES_AND_COMMENTS,
          startTimer(),
        ),
      ];
    }

    replicator.processTransaction(
      '134',
      messages.insert('comments', {id: '41', issueID: '1', upvotes: 10}),
      messages.insert('comments', {id: '42', issueID: '2', upvotes: 20}),
    );
    for (const driver of drivers) {
      [...driver.advance(startTimer()).changes];
    }

    // Only the first client group logs the shape.
    expect(warnings('query-slow-advance')).toEqual([
      [
        'warn',
        {clientGroupID: 'cg1'},
        [
          expect.stringMatching(
            /^Slow query advancement: \d+ ms for 2 of 2 changes$/,
          ),
          {
            zeroEvent: 'query-slow-advance',
            queryHash: 'queryID1',
            transformationHash: 'hash1',
            queryShape: expect.any(String),
            advanceTimeMs: expect.any(Number),
            changes: 2,
            advancementChanges: 2,
            timeMsByTable: {comments: expect.any(Number)},
            zql:
              "issues.related('comments', q => q.orderBy('id', 'desc'))" +
              ".orderBy('id', 'desc')",
          },
        ],
      ],
    ]);
  });

  test('does not log advancements within the slow advance threshold', () => {
    const [driver] = warnLoggingDrivers(['cg1'], {
      slowAdvanceThreshold: 60_000,
    });
    [
      ...driver.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];
    replicator.processTransaction(
      '134',
      messages.insert('comments', {id: '41', issueID: '1', upvotes: 10}),
    );
    [...driver.advance(startTimer()).changes];
    expect(warnings('query-slow-advance')).toEqual([]);
  });

  test('logs the slowest queries of an advancement that times out', () => {
    const [driver] = warnLoggingDrivers(['cg1'], {
      slowAdvanceThreshold: 60_000,
    });
    const hydrationTimer = {totalElapsed: () => 50, elapsedLap: () => 50};
    [
      ...driver.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        hydrationTimer,
      ),
    ];
    [
      ...driver.addQuery(
        'hash2',
        'queryID2',
        ISSUES_QUERY_WITH_EXISTS,
        hydrationTimer,
      ),
    ];

    replicator.processTransaction(
      '134',
      messages.insert('comments', {id: '41', issueID: '1', upvotes: 10}),
    );

    // The advancement is on time until the comment is being pushed: the
    // first two reads of the timer are before the push starts. Then 60ms is
    // larger than half of the total hydration time of 100ms.
    let timerReads = 0;
    const advanceTimer = {
      totalElapsed: () => (++timerReads <= 2 ? 0 : 60),
      elapsedLap: () => 0,
    };
    expect(() => [...driver.advance(advanceTimer).changes]).toThrow(
      ResetPipelinesSignal,
    );

    // The comment is pushed only to the query that reads comments. The
    // timeout is logged regardless of the slow advance threshold.
    expect(warnings('query-advance-timeout')).toEqual([
      [
        'warn',
        {clientGroupID: 'cg1'},
        [
          expect.stringMatching(
            /^Advancement of 1 changes timed out\. The queries that took the most time: queryID1 \(\d+ ms\)$/,
          ),
          {
            zeroEvent: 'query-advance-timeout',
            reason: expect.stringContaining('Advancement exceeded timeout'),
            advancementChanges: 1,
            queries: [
              {
                queryHash: 'queryID1',
                transformationHash: 'hash1',
                queryShape: expect.any(String),
                advanceTimeMs: expect.any(Number),
                changes: 1,
                zql: expect.stringContaining("related('comments'"),
              },
            ],
          },
        ],
      ],
    ]);
  });

  test('advanceWithoutDiff picks up a schema change before hydration', () => {
    // The client schema only covers `issues`, so dropping a `comments`
    // column is not a client-visible schema error, but it does invalidate
    // the table specs computed at init().
    pipelines.init(subsetClientSchema);

    // A schema change lands after init() but before the first hydration
    // (e.g. while the view-syncer waits for the replica to catch up to
    // the CVR). advanceWithoutDiff() does not iterate the change log diff,
    // so it must check for the RESET op explicitly.
    replicator.processTransaction(
      '134',
      messages.dropColumn('comments', 'upvotes'),
    );

    expect(() =>
      pipelines.advanceWithoutDiff(),
    ).toThrowErrorMatchingInlineSnapshot(
      `[ResetPipelinesSignal: schema changed between 123 and 134]`,
    );

    // After a reset at the new head, hydration uses the current specs.
    pipelines.reset(subsetClientSchema);

    expect(
      [
        ...pipelines.addQuery(
          'hash1',
          'queryID1',
          ISSUES_AND_COMMENTS,
          startTimer(),
        ),
      ]
        .filter(
          (change): change is RowChange =>
            change !== 'yield' && change.table === 'comments',
        )
        .map(change => change.row),
    ).toEqual([
      // Rows of a reset table are reported at the bumped minRowVersion.
      {_0_version: '134', id: '22', issueID: '2'},
      {_0_version: '134', id: '21', issueID: '2'},
      {_0_version: '134', id: '20', issueID: '2'},
      {_0_version: '134', id: '10', issueID: '1'},
    ]);
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

    // Update one of the rows after the schema change.
    replicator.processTransaction('135', messages.update('issues', {id: '2'}));

    expect(() => pipelines.advanceWithoutDiff()).toThrow(ResetPipelinesSignal);
    pipelines.reset(clientSchema);

    expect(pipelines.queries()).toEqual(new Map());

    // Under the hood, the row versions are the same but the minRowVersion is
    // bumped in the tableMetadata.
    expect(
      db.prepare(`SELECT id, _0_version FROM issues ORDER BY id`).all(),
    ).toMatchObject([
      {id: '1', _0_version: '123'},
      {id: '2', _0_version: '135'},
      {id: '3', _0_version: '123'},
    ]);

    expect(
      db.prepare(`SELECT minRowVersion FROM "_zero.tableMetadata"`).get(),
    ).toMatchObject({minRowVersion: '134'});

    // The newColumn should be reflected after a reset, with the bumped
    // minRowVersion for older rows.
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
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "135",
            "closed": true,
            "id": "2",
            "newColumn": null,
          },
          "rowKey": {
            "id": "2",
          },
          "table": "issues",
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 2,
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
          "type": 0,
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
          "type": 0,
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
          "type": 1,
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
          "type": 0,
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
          "type": 0,
        },
      ]
    `);
  });

  test('client groups sharing a row cache advance past a skipped unique-key edit', () => {
    // Two client groups on one worker share a SnapshotRowCache. Group `a`
    // cannot observe `foo`, so it skips `foo`'s edit, while group `b` applies
    // it. The unique-key conflict probe for `baz` must still give each group
    // the answer for its own `prev` snapshot.
    const rowCache = new SnapshotRowCache(100);
    const budget = deferredWritesBudget();
    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    const databaseStorage = new DatabaseStorage(storage);
    const makeDriver = (clientGroupID: string) =>
      new PipelineDriver(
        lc,
        testLogConfig,
        new Snapshotter(
          lc,
          dbFile.path,
          {appID: shardID.appID},
          undefined,
          rowCache,
        ),
        shardID,
        databaseStorage.createClientGroupStorage(clientGroupID),
        'pipeline-driver.test.ts',
        new InspectorDelegate(undefined),
        () => 200 /** yield threshold */,
        undefined,
        undefined,
        budget,
      );
    const a = makeDriver('a');
    const b = makeDriver('b');
    a.init(clientSchema);
    b.init(clientSchema);
    [
      ...a.addQuery(
        'hash1',
        'queryID1',
        {
          ...UNIQUES_QUERY,
          where: {
            type: 'simple',
            left: {type: 'column', name: 'id'},
            op: '=',
            right: {type: 'literal', value: 'boo'},
          },
        },
        startTimer(),
      ),
    ];
    [...b.addQuery('hash1', 'queryID1', UNIQUES_QUERY, startTimer())];

    replicator.processTransaction(
      '134',
      messages.update('uniques', {id: 'foo', name: 'wuzzy'}),
      messages.insert('uniques', {id: 'baz', name: 'bar'}),
    );

    expect([...a.advance(NO_TIME_ADVANCEMENT_TIMER).changes]).toEqual([]);
    expect(
      Array.from(b.advance(NO_TIME_ADVANCEMENT_TIMER).changes, c =>
        c === 'yield' ? c : `${c.type}:${String(c.rowKey.id)}`,
      ),
    ).toEqual([`${ChangeType.EDIT}:foo`, `${ChangeType.ADD}:baz`]);
  });

  describe('deferred writes budget', () => {
    function makeDriver(
      clientGroupID: string,
      budget: DeferredWritesBudget | undefined,
      {
        rowCache,
        query = UNIQUES_QUERY,
      }: {
        rowCache?: SnapshotRowCache | undefined;
        query?: AST | undefined;
      } = {},
    ) {
      const storage = new Database(lc, ':memory:');
      storage.prepare(CREATE_STORAGE_TABLE).run();
      const driver = new PipelineDriver(
        lc,
        testLogConfig,
        new Snapshotter(
          lc,
          dbFile.path,
          {appID: shardID.appID},
          undefined,
          rowCache,
        ),
        shardID,
        new DatabaseStorage(storage).createClientGroupStorage(clientGroupID),
        'pipeline-driver.test.ts',
        new InspectorDelegate(undefined),
        () => 200 /** yield threshold */,
        undefined,
        undefined,
        budget,
      );
      driver.init(clientSchema);
      [...driver.addQuery('hash1', 'queryID1', query, startTimer())];
      return driver;
    }

    const summarize = (changes: Iterable<RowChange | 'yield'>) =>
      Array.from(changes, c =>
        c === 'yield'
          ? c
          : `${c.type}:${String(c.rowKey.id)}:${String(c.row?.name)}`,
      );

    // Each transaction displaces a row through the unique `name` column. The
    // change log compresses them to 2, 3 and 1 entries.
    const transactions = [
      [
        messages.delete('uniques', {id: 'foo'}),
        messages.insert('uniques', {id: 'baz', name: 'bar'}),
        messages.insert('uniques', {id: 'foo', name: 'wuzzy'}),
      ],
      [
        messages.delete('uniques', {id: 'boo'}),
        messages.insert('uniques', {id: 'qux', name: 'dar'}),
        messages.update('uniques', {id: 'baz', name: 'zap'}),
      ],
      [messages.update('uniques', {id: 'foo', name: 'bar'})],
    ];

    test('write mode can change between advancements', () => {
      const writeThrough = makeDriver('write-through', undefined);
      const deferred = makeDriver(
        'deferred',
        new DeferredWritesBudget(Infinity, Infinity),
      );
      // Fits the 2 and 1 change advancements, but not the 3 change one.
      const mixedBudget = new DeferredWritesBudget(2, Infinity);
      const tryReserve = vi.spyOn(mixedBudget, 'tryReserve');
      const mixed = makeDriver('mixed', mixedBudget);

      transactions.forEach((txn, i) => {
        replicator.processTransaction(`${134 + i}`, ...txn);
        const expected = summarize(
          writeThrough.advance(NO_TIME_ADVANCEMENT_TIMER).changes,
        );
        expect(
          summarize(deferred.advance(NO_TIME_ADVANCEMENT_TIMER).changes),
        ).toEqual(expected);
        expect(
          summarize(mixed.advance(NO_TIME_ADVANCEMENT_TIMER).changes),
        ).toEqual(expected);
        expect(mixedBudget.reservedRows).toBe(0);
      });
      expect(tryReserve.mock.calls).toEqual([[2], [3], [1]]);
      expect(tryReserve.mock.results.map(r => r.value)).toEqual([
        true,
        false,
        true,
      ]);
    });

    test.each([
      {deferring: 'a', first: 'a'},
      {deferring: 'a', first: 'b'},
      {deferring: 'b', first: 'a'},
      {deferring: 'b', first: 'b'},
    ] as const)(
      'client groups in different modes share a row cache: $deferring defers, $first advances first',
      ({deferring, first}) => {
        // As in 'client groups sharing a row cache advance past a skipped
        // unique-key edit', but only one group holds its changes in memory
        // (and reads `prev` as it was). The other does not fit in its budget,
        // so it writes them through (and reads `prev` as it writes to it).
        // Whichever advances first fills the row cache.
        const rowCache = new SnapshotRowCache(100);
        const budgetFor = (id: string) =>
          new DeferredWritesBudget(id === deferring ? Infinity : 0, Infinity);
        const drivers = {
          // Cannot observe `foo`, so it skips `foo`'s edit.
          a: makeDriver('a', budgetFor('a'), {
            rowCache,
            query: {
              ...UNIQUES_QUERY,
              where: {
                type: 'simple',
                left: {type: 'column', name: 'id'},
                op: '=',
                right: {type: 'literal', value: 'boo'},
              },
            },
          }),
          b: makeDriver('b', budgetFor('b'), {rowCache}),
        };

        replicator.processTransaction(
          '134',
          messages.update('uniques', {id: 'foo', name: 'wuzzy'}),
          messages.insert('uniques', {id: 'baz', name: 'bar'}),
        );

        const order =
          first === 'a' ? (['a', 'b'] as const) : (['b', 'a'] as const);
        const results: Record<string, string[]> = {};
        for (const id of order) {
          results[id] = summarize(
            drivers[id].advance(NO_TIME_ADVANCEMENT_TIMER).changes,
          );
        }
        expect(results).toEqual({
          a: [],
          b: [`${ChangeType.EDIT}:foo:wuzzy`, `${ChangeType.ADD}:baz:bar`],
        });
      },
    );

    test('an advancement writes through while another holds the budget', () => {
      // Fits the 2 changes of one advancement of the first transaction at a
      // time, or the 3 of the second.
      const budget = new DeferredWritesBudget(3, Infinity);
      const tryReserve = vi.spyOn(budget, 'tryReserve');
      const writeThrough = makeDriver('write-through', undefined);
      const a = makeDriver('a', budget);
      const b = makeDriver('b', budget);

      replicator.processTransaction('134', ...transactions[0]);
      const expected = summarize(
        writeThrough.advance(NO_TIME_ADVANCEMENT_TIMER).changes,
      );

      // `a` is partway through its advancement, which holds its reservation.
      const aChanges = a
        .advance(NO_TIME_ADVANCEMENT_TIMER)
        .changes[Symbol.iterator]();
      const aFirst = aChanges.next();
      expect(aFirst.done).toBe(false);
      expect(budget.reservedRows).toBe(2);

      // So `b`'s changes do not fit, and it writes them through.
      expect(summarize(b.advance(NO_TIME_ADVANCEMENT_TIMER).changes)).toEqual(
        expected,
      );
      expect(budget.reservedRows).toBe(2);

      // `a` finishes, and releases its reservation.
      expect(
        summarize([
          aFirst.value as RowChange | 'yield',
          ...{[Symbol.iterator]: () => aChanges},
        ]),
      ).toEqual(expected);
      expect(budget.reservedRows).toBe(0);

      // Then `b`'s next advancement fits.
      replicator.processTransaction('135', ...transactions[1]);
      expect(summarize(b.advance(NO_TIME_ADVANCEMENT_TIMER).changes)).toEqual(
        summarize(writeThrough.advance(NO_TIME_ADVANCEMENT_TIMER).changes),
      );
      expect(tryReserve.mock.calls).toEqual([[2], [2], [3]]);
      expect(tryReserve.mock.results.map(r => r.value)).toEqual([
        true,
        false,
        true,
      ]);
    });

    test('pending rows never exceed the reserved changes', () => {
      const budget = new DeferredWritesBudget(Infinity, Infinity);
      const driver = makeDriver('bounded', budget);
      let maxPendingRows = 0;
      let reservedRows = 0;
      // The timer is also consulted after each change has been applied,
      // which is when the sources hold the most rows.
      const timer: Timer = {
        elapsedLap: () => 0,
        totalElapsed: () => {
          maxPendingRows = Math.max(maxPendingRows, driver.pendingRows);
          reservedRows = budget.reservedRows;
          return 0;
        },
      };
      transactions.forEach((txn, i) => {
        replicator.processTransaction(`${134 + i}`, ...txn);
        maxPendingRows = 0;
        const {numChanges, changes} = driver.advance(timer);
        [...changes];
        expect(reservedRows).toBe(numChanges);
        expect(maxPendingRows).toBeGreaterThan(0);
        expect(maxPendingRows).toBeLessThanOrEqual(numChanges);
        expect(driver.pendingRows).toBe(0);
        expect(budget.reservedRows).toBe(0);
      });
    });

    test('a deferred delete preserves a row moved away from the change-log key', () => {
      const byName = new ReplicationMessages({uniques: 'name'});
      const writeThrough = makeDriver('write-through', undefined);
      const deferred = makeDriver(
        'deferred',
        new DeferredWritesBudget(Infinity, Infinity),
      );

      // The log compresses this to SET(baz), DEL(bar). The delete's snapshot
      // probe still finds foo/bar, but the pending row is now foo/baz.
      replicator.processTransaction(
        '134',
        byName.update('uniques', {id: 'foo', name: 'baz'}, {name: 'bar'}),
        byName.insert('uniques', {id: 'temporary', name: 'bar'}),
        byName.delete('uniques', {name: 'bar'}),
      );

      const expected = summarize(
        writeThrough.advance(NO_TIME_ADVANCEMENT_TIMER).changes,
      );
      expect(expected).toEqual([`${ChangeType.EDIT}:foo:baz`]);
      const {numChanges, changes} = deferred.advance(NO_TIME_ADVANCEMENT_TIMER);
      expect(numChanges).toBe(2);
      expect(summarize(changes)).toEqual(expected);
    });

    test('an entry counts twice when the change log is keyed by another unique key', () => {
      // `uniques` has `id` as its primary key in the client schema, but is
      // keyed by its other unique key, `name`, upstream and in the change log.
      const byName = new ReplicationMessages({uniques: 'name'});
      const budget = new DeferredWritesBudget(Infinity, Infinity);
      const writeThrough = makeDriver('write-through', undefined);
      const driver = makeDriver('by-name', budget);
      let maxPendingRows = 0;
      let reservedRows = 0;
      const timer: Timer = {
        elapsedLap: () => 0,
        totalElapsed: () => {
          maxPendingRows = Math.max(maxPendingRows, driver.pendingRows);
          reservedRows = budget.reservedRows;
          return 0;
        },
      };
      // Changes the client primary key of the row named `bar`: one change log
      // entry, but a remove and an add, of two rows, to the source.
      replicator.processTransaction(
        '134',
        byName.update('uniques', {id: 'foo2', name: 'bar'}),
      );

      const expected = summarize(
        writeThrough.advance(NO_TIME_ADVANCEMENT_TIMER).changes,
      );
      const {numChanges, changes} = driver.advance(timer);
      expect(summarize(changes)).toEqual(expected);
      expect(numChanges).toBe(1);
      expect({maxPendingRows, reservedRows}).toEqual({
        maxPendingRows: 2,
        reservedRows: 2,
      });
    });

    test('an entry that removes a row does not count twice', () => {
      const byName = new ReplicationMessages({uniques: 'name'});
      const budget = new DeferredWritesBudget(Infinity, Infinity);
      const tryReserveMore = vi.spyOn(budget, 'tryReserveMore');
      const writeThrough = makeDriver('write-through', undefined);
      const driver = makeDriver('by-name', budget);
      replicator.processTransaction(
        '134',
        byName.delete('uniques', {name: 'bar'}),
      );

      const expected = summarize(
        writeThrough.advance(NO_TIME_ADVANCEMENT_TIMER).changes,
      );
      expect(
        summarize(driver.advance(NO_TIME_ADVANCEMENT_TIMER).changes),
      ).toEqual(expected);
      expect(expected).toEqual([`${ChangeType.REMOVE}:foo:undefined`]);
      expect(tryReserveMore).not.toHaveBeenCalled();
    });

    test('an entry whose second row does not fit writes through', () => {
      const byName = new ReplicationMessages({uniques: 'name'});
      // Room for the one entry, but not for the second row it changes.
      const budget = new DeferredWritesBudget(1, Infinity);
      const tryReserve = vi.spyOn(budget, 'tryReserve');
      const tryReserveMore = vi.spyOn(budget, 'tryReserveMore');
      const writeThrough = makeDriver('write-through', undefined);
      const driver = makeDriver('by-name', budget);
      replicator.processTransaction(
        '134',
        byName.update('uniques', {id: 'foo2', name: 'bar'}),
      );

      const expected = summarize(
        writeThrough.advance(NO_TIME_ADVANCEMENT_TIMER).changes,
      );
      expect(
        summarize(driver.advance(NO_TIME_ADVANCEMENT_TIMER).changes),
      ).toEqual(expected);
      expect(tryReserve.mock.calls).toEqual([[1]]);
      expect(tryReserve.mock.results.map(r => r.value)).toEqual([true]);
      expect(tryReserveMore.mock.calls).toEqual([[1]]);
      expect(tryReserveMore.mock.results.map(r => r.value)).toEqual([false]);
      expect(driver.pendingRows).toBe(0);
      expect(budget.rowOverruns).toBe(0);
      expect([budget.reservedRows, budget.heldBytes]).toEqual([0, 0]);
    });

    test('an advancement that holds more rows than it reserved writes through the rest', () => {
      // Undercounts the changes, which the reservation is based on.
      const advance = Snapshotter.prototype.advance;
      vi.spyOn(Snapshotter.prototype, 'advance').mockImplementation(function (
        this: Snapshotter,
        ...args
      ) {
        const diff = advance.apply(this, args);
        Object.defineProperty(diff, 'changes', {value: 0});
        return diff;
      });
      const budget = new DeferredWritesBudget(Infinity, Infinity);
      const holdBytes = vi.spyOn(budget, 'holdBytes');
      const writeThrough = makeDriver('write-through', undefined);
      const driver = makeDriver('undercounted', budget);

      transactions.forEach((txn, i) => {
        replicator.processTransaction(`${134 + i}`, ...txn);
        expect(
          summarize(driver.advance(NO_TIME_ADVANCEMENT_TIMER).changes),
        ).toEqual(
          summarize(writeThrough.advance(NO_TIME_ADVANCEMENT_TIMER).changes),
        );
      });
      // Once per advancement, after its first change.
      expect(budget.rowOverruns).toBe(transactions.length);
      expect(holdBytes).toHaveBeenCalledTimes(transactions.length);
      expect([budget.reservedRows, budget.heldBytes]).toEqual([0, 0]);
    });

    test.each(['return', 'throw'] as const)(
      'what an advancement holds is dropped and released when it is abandoned: $0',
      how => {
        const budget = new DeferredWritesBudget(Infinity, Infinity);
        const driver = makeDriver('abandoned', budget);
        replicator.processTransaction('134', ...transactions[0]);

        const {numChanges, changes} = driver.advance(NO_TIME_ADVANCEMENT_TIMER);
        // Nothing is reserved until the changes are iterated.
        expect(budget.reservedRows).toBe(0);
        const iter = changes[Symbol.iterator]();
        while (budget.heldBytes === 0) {
          expect(iter.next().done).toBe(false);
        }
        expect(budget.reservedRows).toBe(numChanges);
        expect(driver.pendingRows).toBeGreaterThan(0);

        if (how === 'return') {
          iter.return?.();
        } else {
          const err = new Error('abandoned');
          expect(() => iter.throw?.(err)).toThrow(err);
        }
        expect(driver.pendingRows).toBe(0);
        expect([budget.reservedRows, budget.heldBytes]).toEqual([0, 0]);
      },
    );

    // Counting only the tables the client group reads would take a scan of
    // the change log entries.
    test('reserves the changes to every table', () => {
      const budget = new DeferredWritesBudget(Infinity, Infinity);
      const tryReserve = vi.spyOn(budget, 'tryReserve');
      const driver = makeDriver('uniques-only', budget);
      replicator.processTransaction(
        '134',
        messages.insert('issues', {id: '4', closed: 0}),
        messages.insert('issues', {id: '5', closed: 1}),
        messages.update('uniques', {id: 'foo', name: 'wuzzy'}),
      );

      const {numChanges, changes} = driver.advance(NO_TIME_ADVANCEMENT_TIMER);
      [...changes];
      expect(numChanges).toBe(3);
      expect(tryReserve.mock.calls).toEqual([[3]]);
    });

    test.each(['switching', 'deferring'] as const)(
      'an advancement over the byte budget writes through the rest: $0 advances first',
      first => {
        // The group that switches shares a row cache with one that holds all
        // of its changes in memory, and so reads `prev` as it was. Whichever
        // advances first fills the row cache.
        const rowCache = new SnapshotRowCache(100);
        const writeThrough = makeDriver('write-through', undefined);
        // Holding any bytes takes it past the budget.
        const budget = new DeferredWritesBudget(Infinity, 1);
        const holdBytes = vi.spyOn(budget, 'holdBytes');
        const drivers = {
          switching: makeDriver('switching', budget, {rowCache}),
          deferring: makeDriver(
            'deferring',
            new DeferredWritesBudget(Infinity, Infinity),
            {rowCache},
          ),
        };
        const order =
          first === 'switching'
            ? (['switching', 'deferring'] as const)
            : (['deferring', 'switching'] as const);

        transactions.forEach((txn, i) => {
          replicator.processTransaction(`${134 + i}`, ...txn);
          const expected = summarize(
            writeThrough.advance(NO_TIME_ADVANCEMENT_TIMER).changes,
          );
          holdBytes.mockClear();
          for (const id of order) {
            expect(
              summarize(drivers[id].advance(NO_TIME_ADVANCEMENT_TIMER).changes),
            ).toEqual(expected);
          }
          // Only after the first change, after which it writes through.
          expect(holdBytes).toHaveBeenCalledTimes(1);
          expect(holdBytes.mock.results[0].value).toBe(false);
          expect([budget.reservedRows, budget.heldBytes]).toEqual([0, 0]);
        });
      },
    );

    test('the bytes held by client groups add up', () => {
      // A budget whose byte limit is set once the bytes a client group holds
      // have been measured, which is after the drivers are created.
      class Budget extends DeferredWritesBudget {
        limit = Infinity;
        override holdBytes(bytes: number): boolean {
          return super.holdBytes(bytes) && this.heldBytes <= this.limit;
        }
      }
      const alone = new DeferredWritesBudget(Infinity, Infinity);
      const budget = new Budget(Infinity, Infinity);
      const drivers = {
        writeThrough: makeDriver('write-through', undefined),
        alone: makeDriver('alone', alone),
        a: makeDriver('a', budget),
        b: makeDriver('b', budget),
      };
      replicator.processTransaction('134', ...transactions[0]);
      const expected = summarize(
        drivers.writeThrough.advance(NO_TIME_ADVANCEMENT_TIMER).changes,
      );

      // The most bytes one client group holds for the advancement. The timer
      // is also consulted after each change's bytes are added up.
      let mostBytes = 0;
      const timer: Timer = {
        elapsedLap: () => 0,
        totalElapsed: () => {
          mostBytes = Math.max(mostBytes, alone.heldBytes);
          return 0;
        },
      };
      expect(summarize(drivers.alone.advance(timer).changes)).toEqual(expected);
      expect(mostBytes).toBeGreaterThan(0);
      expect(alone.heldBytes).toBe(0);

      // Enough for either client group, but not both.
      budget.limit = mostBytes;

      // `a` is partway through its advancement, holding some bytes.
      const aChanges = drivers.a
        .advance(NO_TIME_ADVANCEMENT_TIMER)
        .changes[Symbol.iterator]();
      const aSoFar: (RowChange | 'yield')[] = [];
      while (budget.heldBytes === 0) {
        const next = aChanges.next();
        expect(next.done).toBe(false);
        aSoFar.push(next.value as RowChange | 'yield');
      }
      const aBytes = budget.heldBytes;

      // So `b` takes the total past the budget, and writes through the rest.
      expect(
        summarize(drivers.b.advance(NO_TIME_ADVANCEMENT_TIMER).changes),
      ).toEqual(expected);
      expect(budget.heldBytes).toBe(aBytes);
      expect(budget.reservedRows).toBe(2);

      // `a` finishes within the budget.
      expect(
        summarize([...aSoFar, ...{[Symbol.iterator]: () => aChanges}]),
      ).toEqual(expected);
      expect(budget.heldBytes).toBe(0);
      expect(budget.reservedRows).toBe(0);
    });
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
          "type": 1,
        },
        {
          "queryID": "queryID",
          "row": undefined,
          "rowKey": {
            "issueID": "1",
            "labelID": "1",
          },
          "table": "issueLabels",
          "type": 1,
        },
        {
          "queryID": "queryID",
          "row": undefined,
          "rowKey": {
            "id": "1",
          },
          "table": "labels",
          "type": 1,
        },
      ]
    `);
  });

  test('hydrationStats counts rows output and rows read', () => {
    pipelines.init(clientSchema);
    expect(pipelines.hydrationStats('queryID')).toBeUndefined();

    const rows = [
      ...pipelines.addQuery(
        'hash1',
        'queryID',
        ISSUES_QUERY_WITH_EXISTS,
        startTimer(),
      ),
    ].filter(change => change !== 'yield');

    const stats = must(pipelines.hydrationStats('queryID'));
    expect(stats.rowCount).toBe(rows.length);
    // Only issue 1 has a label, but every issue and its label edges are read
    // to find that out.
    expect(stats.rowsRead).toBeGreaterThan(stats.rowCount);

    // A second hydration only counts its own reads.
    [
      ...pipelines.addQuery(
        'hash2',
        'queryID2',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];
    const stats2 = must(pipelines.hydrationStats('queryID2'));
    // 3 issues and 4 comments, each read once.
    expect(stats2).toEqual({rowCount: 7, rowsRead: 7, planWarnings: []});
    expect(pipelines.hydrationStats('queryID')).toEqual(stats);

    pipelines.removeQuery('queryID');
    expect(pipelines.hydrationStats('queryID')).toBeUndefined();
  });

  test('logs plan warnings once per query shape across client groups', () => {
    // Plan warnings need table statistics.
    db.exec('ANALYZE');
    const warnLC = new LogContext('warn', undefined, logSink);
    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    const databaseStorage = new DatabaseStorage(storage);
    const [cg1, cg2] = ['cg1', 'cg2'].map(clientGroupID => {
      const driver = new PipelineDriver(
        warnLC,
        // SQLite sorts the tiny comments table (~1 row per issue) rather than
        // scan it by id, which a threshold of 2 rows leaves out.
        {...testLogConfig, planWarningRowThreshold: 2},
        new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
        shardID,
        databaseStorage.createClientGroupStorage(clientGroupID),
        clientGroupID,
        new InspectorDelegate(undefined),
        () => 200 /** yield threshold */,
        true /** enablePlanner */,
      );
      driver.init(clientSchema);
      return driver;
    });

    // The comments of each issue are looked up by issueID, which has no
    // index.
    const commentsWarning = {
      type: 'missing-index',
      table: 'comments',
      path: ['comments'],
      perRow: true,
      columns: ['issueID'],
      rows: 4,
      suggestedIndex: ['issueID', 'id'],
    };
    for (const driver of [cg1, cg2]) {
      [
        ...driver.addQuery(
          'hash1',
          'queryID',
          ISSUES_AND_COMMENTS,
          startTimer(),
        ),
      ];
      expect(driver.hydrationStats('queryID')?.planWarnings).toEqual([
        commentsWarning,
      ]);
    }

    const planWarningLogs = logSink.messages.filter(
      ([level, context]) =>
        level === 'warn' && context?.zeroEvent === undefined,
    );
    // Only the first client group logs the shape.
    expect(planWarningLogs).toEqual([
      [
        'warn',
        {clientGroupID: 'cg1'},
        [
          "Query plan warning: Each lookup of comments (related 'comments') " +
            'by issueID scans all ~4 rows because no index covers issueID, ' +
            'and it runs once per parent row. Consider adding an index on ' +
            'comments (issueID, id) upstream.',
          {
            zeroEvent: 'query-plan-warning',
            queryHash: 'queryID',
            transformationHash: 'hash1',
            queryShape: expect.any(String),
            warnings: [commentsWarning],
            zql:
              "issues.related('comments', q => q.orderBy('id', 'desc'))" +
              ".orderBy('id', 'desc')",
          },
        ],
      ],
    ]);
  });

  test('subset client schema can hydrate whereExists helper tables', () => {
    pipelines.init(subsetClientSchema);

    expect([
      ...pipelines.addQuery(
        'hash-subset-schema-exists',
        'querySubsetSchemaExists',
        ISSUES_QUERY_WITH_EXISTS,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "querySubsetSchemaExists",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "1",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "querySubsetSchemaExists",
          "row": {
            "_0_version": "123",
            "issueID": "1",
            "labelID": "1",
            "legacyID": "1-1",
          },
          "rowKey": {
            "legacyID": "1-1",
          },
          "table": "issueLabels",
          "type": 0,
        },
        {
          "queryID": "querySubsetSchemaExists",
          "row": {
            "_0_version": "123",
            "id": "1",
            "name": "bug",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "labels",
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 0,
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
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "issueID": "2",
            "labelID": "1",
          },
          "table": "issueLabels",
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "1",
          },
          "table": "labels",
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "issueID": "2",
            "labelID": "1",
          },
          "table": "issueLabels",
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "1",
          },
          "table": "labels",
          "type": 1,
        },
      ]
    `);
  });

  test('child change pushed to several parents streams each row once', () => {
    // Adding issueLabels 2-1 is pushed to comments 20, 21 and 22 in turn.
    // Issue 2 enters the query at comment 20, and its subtree must be read
    // then: read after the push, it would already hold comments 21 and 22,
    // which are then added again by their own pushes.
    const query: AST = {
      table: 'issues',
      orderBy: [['id', 'asc']],
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          system: 'client',
          correlation: {parentField: ['id'], childField: ['issueID']},
          subquery: {
            table: 'comments',
            alias: 'zsubq_comments',
            orderBy: [['id', 'asc']],
            where: {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              related: {
                system: 'client',
                correlation: {
                  parentField: ['issueID'],
                  childField: ['issueID'],
                },
                subquery: {
                  table: 'issueLabels',
                  alias: 'zsubq_issueLabels',
                  orderBy: [
                    ['issueID', 'asc'],
                    ['labelID', 'asc'],
                  ],
                },
              },
            },
          },
        },
      },
    };

    pipelines.init(clientSchema);
    [...pipelines.addQuery('hash1', 'queryID', query, startTimer())];

    replicator.processTransaction(
      '134',
      messages.insert('issueLabels', {
        issueID: '2',
        labelID: '1',
        legacyID: '2-1',
      }),
    );

    const counts = new Map<string, number>();
    for (const change of changes()) {
      if (change === 'yield') {
        continue;
      }
      const {table, rowKey} = change;
      const key = `${table} ${JSON.stringify(rowKey)}`;
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    expect(Object.fromEntries(counts)).toEqual({
      'issues {"id":"2"}': 1,
      'comments {"id":"20"}': 1,
      'comments {"id":"21"}': 1,
      'comments {"id":"22"}': 1,
      'issueLabels {"issueID":"2","labelID":"1"}': 3,
    });
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
          "type": 0,
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
          "type": 0,
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
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "41",
          },
          "table": "comments",
          "type": 1,
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

  test('prunes unused tables when queries are removed', () => {
    pipelines.init(clientSchema);

    // Query 1 uses issues and comments
    [
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    // Query 2 uses only issues
    const ONLY_ISSUES: AST = {
      table: 'issues',
      orderBy: [['id', 'desc']],
    };
    [...pipelines.addQuery('hash2', 'queryID2', ONLY_ISSUES, startTimer())];

    const advanceSpy = vi.spyOn(Snapshotter.prototype, 'advance');

    // Advance 1: Both queries active -> issues and comments are both observed
    changes();
    const observed1 = advanceSpy.mock.calls.at(-1)?.[2];
    expect(observed1?.has('issues')).toBe(true);
    expect(observed1?.has('comments')).toBe(true);
    expect(observed1?.has('labels')).toBe(false);

    // Remove query 1: comments has no more connections, so it should be pruned.
    // issues is still used by query 2.
    pipelines.removeQuery('queryID1');

    changes();
    const observed2 = advanceSpy.mock.calls.at(-1)?.[2];
    expect(observed2?.has('issues')).toBe(true);
    expect(observed2?.has('comments')).toBe(false);

    // Remove query 2: issues should now be pruned as well.
    pipelines.removeQuery('queryID2');

    changes();
    const observed3 = advanceSpy.mock.calls.at(-1)?.[2];
    expect(observed3?.has('issues')).toBe(false);
    expect(observed3?.has('comments')).toBe(false);

    // Re-adding a query on issues recreates the table source and works
    [...pipelines.addQuery('hash2', 'queryID2', ONLY_ISSUES, startTimer())];
    changes();
    const observed4 = advanceSpy.mock.calls.at(-1)?.[2];
    expect(observed4?.has('issues')).toBe(true);

    advanceSpy.mockRestore();
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
          "type": 0,
        },
        {
          "queryID": "queryScalar",
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
          "type": 0,
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

  test('subset client schema can hydrate scalar subquery companion tables', () => {
    pipelines.init(subsetClientSchema);

    expect([
      ...pipelines.addQuery(
        'hash-scalar-subset-schema',
        'queryScalarSubsetSchema',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryScalarSubsetSchema",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "1",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryScalarSubsetSchema",
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
          "type": 0,
        },
      ]
    `);

    expect(
      pipelines.queries().get('queryScalarSubsetSchema')?.transformedAst.where,
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

  test('scalar NOT EXISTS subquery with no matching rows', () => {
    pipelines.init(clientSchema);

    // No comment has id='nonexistent', so nothing can satisfy the correlated
    // EXISTS — its negation holds for every issue.
    const results = [
      ...pipelines.addQuery(
        'hash-scalar-not-none',
        'queryScalarNotNone',
        {
          ...ISSUES_WITH_NONEXISTENT_SCALAR_SUBQUERY,
          where: {
            ...(ISSUES_WITH_NONEXISTENT_SCALAR_SUBQUERY.where as CorrelatedSubqueryCondition),
            op: 'NOT EXISTS',
          },
        },
        startTimer(),
      ),
    ];

    expect(results.filter(r => r !== 'yield').map(r => r.rowKey)).toEqual([
      {id: '1'},
      {id: '2'},
      {id: '3'},
    ]);

    expect(
      pipelines.queries().get('queryScalarNotNone')?.transformedAst.where,
    ).toEqual({
      type: 'simple',
      op: '=',
      left: {type: 'literal', value: 1},
      right: {type: 'literal', value: 1},
    });
  });

  describe('unhonored scalar hints are reported', () => {
    // The driver is built at 'error' by default, which swallows warnings.
    function warningsFrom(query: AST): string[] {
      logSink.messages.length = 0;
      const warnLc = new LogContext('warn', undefined, logSink);
      const storage = new Database(warnLc, ':memory:');
      storage.prepare(CREATE_STORAGE_TABLE).run();
      pipelines = new PipelineDriver(
        warnLc,
        testLogConfig,
        new Snapshotter(warnLc, dbFile.path, {appID: shardID.appID}),
        shardID,
        new DatabaseStorage(storage).createClientGroupStorage(
          'foo-client-group',
        ),
        'pipeline-driver.test.ts',
        new InspectorDelegate(undefined),
        () => 200 /** yield threshold */,
      );
      pipelines.init(clientSchema);
      [...pipelines.addQuery('hash-warn', 'queryWarn', query, startTimer())];
      return logSink.messages
        .filter(([level]) => level === 'warn')
        .map(([, , args]) => String(args[0]));
    }

    test('an unpinned subquery warns, naming the table and its unique keys', () => {
      const warnings = warningsFrom({
        ...ISSUES_WITH_SCALAR_SUBQUERY,
        where: {
          ...(ISSUES_WITH_SCALAR_SUBQUERY.where as CorrelatedSubqueryCondition),
          related: {
            correlation: {parentField: ['id'], childField: ['issueID']},
            subquery: {
              table: 'comments',
              // The gate survives as a real EXISTS, which needs an alias —
              // the builder always emits one.
              alias: 'zsubq_comments',
              orderBy: [['id', 'asc']],
              // `upvotes` is not unique, so this pins nothing.
              where: {
                type: 'simple',
                op: '=',
                left: {type: 'column', name: 'upvotes'},
                right: {type: 'literal', value: 0},
              },
            },
          },
        },
      });

      expect(warnings).toHaveLength(1);
      expect(warnings[0]).toMatchInlineSnapshot(
        `"Ignoring {scalar: true} on the "comments" subquery of query queryWarn: it does not constrain every column of any unique key [(id)] to a literal with "=", so it is not provably limited to one row. The gate runs as a plain EXISTS."`,
      );
    });

    test('a subquery pinned on the primary key is silent', () => {
      expect(warningsFrom(ISSUES_WITH_SCALAR_SUBQUERY)).toEqual([]);
    });

    test('a subquery pinned on a non-primary unique index is silent', () => {
      // `uniques` has unique indexes on both `id` and `name`. Pinning `name`
      // is honored, which the client schema — primary keys only — could not
      // have known. This is why the check lives here and not in the builder.
      expect(
        warningsFrom({
          table: 'issues',
          orderBy: [['id', 'asc']],
          where: {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            scalar: true,
            related: {
              correlation: {parentField: ['id'], childField: ['id']},
              subquery: {
                table: 'uniques',
                orderBy: [['id', 'asc']],
                where: {
                  type: 'simple',
                  op: '=',
                  left: {type: 'column', name: 'name'},
                  right: {type: 'literal', value: 'bar'},
                },
              },
            },
          },
        }),
      ).toEqual([]);
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
            type: 'correlatedSubquery',
            op: 'EXISTS',
            scalar: true,
            related: {
              correlation: {
                parentField: ['id'],
                childField: ['issueID'],
              },
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
          "type": 0,
        },
        {
          "queryID": "queryScalarAnd",
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
          "type": 0,
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
          "type": 2,
        },
      ]
    `);
  });

  test('subset client schema advances scalar companion tables', () => {
    pipelines.init(subsetClientSchema);

    [
      ...pipelines.addQuery(
        'hash-scalar-subset-schema',
        'queryScalarSubsetSchema',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    replicator.processTransaction(
      '134',
      messages.update('comments', {id: '10', issueID: '1', upvotes: 5}),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryScalarSubsetSchema",
          "row": {
            "_0_version": "134",
            "id": "10",
            "issueID": "1",
            "upvotes": 5,
          },
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
          "type": 2,
        },
      ]
    `);
  });

  test('companion pipeline throws ResetPipelinesSignal when scalar value changes', () => {
    pipelines.init(clientSchema);

    // Resolves comment '10' (issueID='1'), so query becomes `issues WHERE id = '1'`
    [
      ...pipelines.addQuery(
        'hash-scalar',
        'queryScalar',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    // Change comment '10' issueID from '1' to '2' — the scalar value changes
    replicator.processTransaction(
      '134',
      messages.update('comments', {id: '10', issueID: '2', upvotes: 0}),
    );

    expect(() => changes()).toThrowError(ResetPipelinesSignal);
  });

  test('companion pipeline does not throw when scalar value stays same', () => {
    pipelines.init(clientSchema);

    // Resolves comment '10' (issueID='1'), so query becomes `issues WHERE id = '1'`
    [
      ...pipelines.addQuery(
        'hash-scalar',
        'queryScalar',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    // Change a different column (upvotes) on comment '10' — issueID stays '1'
    replicator.processTransaction(
      '134',
      messages.update('comments', {id: '10', issueID: '1', upvotes: 5}),
    );

    // No ResetPipelinesSignal, and the companion row change is synced
    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryScalar",
          "row": {
            "_0_version": "134",
            "id": "10",
            "issueID": "1",
            "upvotes": 5,
          },
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
          "type": 2,
        },
      ]
    `);
  });

  test('companion pipeline throws ResetPipelinesSignal when companion row deleted', () => {
    pipelines.init(clientSchema);

    // Resolves comment '10' (issueID='1'), so query becomes `issues WHERE id = '1'`
    [
      ...pipelines.addQuery(
        'hash-scalar',
        'queryScalar',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    // Delete comment '10' — the scalar value goes from '1' to undefined (no row)
    replicator.processTransaction(
      '134',
      messages.delete('comments', {id: '10'}),
    );

    expect(() => changes()).toThrowError(ResetPipelinesSignal);
  });

  test('companion pipeline throws ResetPipelinesSignal when companion row added', () => {
    pipelines.init(clientSchema);

    replicator.processTransaction(
      '134',
      messages.delete('comments', {id: '10'}),
    );

    changes();

    [
      ...pipelines.addQuery(
        'hash-scalar',
        'queryScalar',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    // Insert comment '10' — the scalar value goes from undefined to '1'
    replicator.processTransaction(
      '135',
      messages.insert('comments', {id: '10', issueID: '1', upvotes: 0}),
    );

    expect(() => changes()).toThrowError(ResetPipelinesSignal);
  });

  describe('correlated predicate pushdown', () => {
    type Connection = {
      readonly table: string;
      readonly filters: Condition | undefined;
    };

    /** Records the table and the filter of each source connection made from now on. */
    function recordConnections(): Connection[] {
      const connections: Connection[] = [];
      const connect = TableSource.prototype.connect;
      vi.spyOn(TableSource.prototype, 'connect').mockImplementation(function (
        this: TableSource,
        ...args: Parameters<TableSource['connect']>
      ) {
        connections.push({table: this.tableSchema.name, filters: args[1]});
        return connect.apply(this, args);
      });
      return connections;
    }

    function filtersOf(connections: Connection[], table: string) {
      return connections.filter(c => c.table === table).map(c => c.filters);
    }

    function eq(column: string, value: string): Condition {
      return {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: column},
        right: {type: 'literal', value},
      };
    }

    /** Replaces `pipelines` with a driver built with `config`. */
    function usePipelines(config: Partial<ZeroConfig> | undefined) {
      const storage = new Database(lc, ':memory:');
      storage.prepare(CREATE_STORAGE_TABLE).run();
      pipelines = new PipelineDriver(
        lc,
        testLogConfig,
        new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
        shardID,
        new DatabaseStorage(storage).createClientGroupStorage(
          'foo-client-group',
        ),
        'pipeline-driver.test.ts',
        new InspectorDelegate(undefined),
        () => 200 /** yield threshold */,
        false,
        config as ZeroConfig | undefined,
        deferredWritesBudget(),
      );
    }

    const CHANGE_TYPES = {
      [ChangeType.ADD]: 'ADD',
      [ChangeType.REMOVE]: 'REMOVE',
      [ChangeType.EDIT]: 'EDIT',
    };

    function summary(changes: Iterable<RowChange | 'yield'>): string[] {
      const out: string[] = [];
      for (const c of changes) {
        if (c !== 'yield') {
          const key = Object.values(c.rowKey).join(',');
          out.push(`${CHANGE_TYPES[c.type]} ${c.table} ${key}`);
        }
      }
      return out;
    }

    const COMMENTS: CorrelatedSubquery = {
      system: 'client',
      correlation: {parentField: ['id'], childField: ['issueID']},
      subquery: {
        table: 'comments',
        alias: 'comments',
        orderBy: [['id', 'asc']],
      },
    };

    const PUSHDOWN_CONFIGS = [
      ['default', undefined, true],
      ['on', {enableCorrelatedPredicatePushdown: true}, true],
      ['off', {enableCorrelatedPredicatePushdown: false}, false],
    ] as const;

    test.each(PUSHDOWN_CONFIGS)(
      'the flag reaches hydration and the scalar resolver (%s)',
      (_, config, pushed) => {
        usePipelines(config);
        const connections = recordConnections();
        pipelines.init(clientSchema);

        // The scalar gate is pinned on the issueLabels primary key and nests
        // an EXISTS on labels, so the pass pushes `id = '1'` into the labels
        // connection that the scalar resolver builds. The gate resolves to
        // `id = '1'` on issues, so the pass pushes `issueID = '1'` into the
        // related comments connection that hydration builds.
        const results = pipelines.addQuery(
          'hash-pushdown',
          'queryPushdown',
          {
            table: 'issues',
            orderBy: [['id', 'asc']],
            where: {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              scalar: true,
              related: {
                correlation: {parentField: ['id'], childField: ['issueID']},
                subquery: {
                  table: 'issueLabels',
                  orderBy: [
                    ['issueID', 'asc'],
                    ['labelID', 'asc'],
                  ],
                  where: {
                    type: 'and',
                    conditions: [
                      eq('issueID', '1'),
                      eq('labelID', '1'),
                      {
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
                          },
                        },
                      },
                    ],
                  },
                },
              },
            },
            related: [COMMENTS],
          },
          startTimer(),
        );

        expect(summary(results)).toEqual([
          'ADD issues 1',
          'ADD comments 10',
          'ADD issueLabels 1,1',
        ]);
        expect(filtersOf(connections, 'labels')).toEqual([
          pushed ? eq('id', '1') : undefined,
        ]);
        expect(filtersOf(connections, 'comments')).toEqual([
          pushed ? eq('issueID', '1') : undefined,
        ]);
      },
    );

    test.each(PUSHDOWN_CONFIGS)(
      'a literal pushed from a scalar gate follows the row behind it (%s)',
      (_, config, pushed) => {
        usePipelines(config);
        const connections = recordConnections();
        pipelines.init(clientSchema);

        // Comment '10' resolves the gate to `id = '1'`, and the pass copies
        // that literal into the related comments as `issueID = '1'`.
        const query: AST = {
          ...ISSUES_WITH_SCALAR_SUBQUERY,
          related: [COMMENTS],
        };
        const hydrate = () =>
          summary(
            pipelines.addQuery(
              'hash-scalar-related',
              'queryScalarRelated',
              query,
              startTimer(),
            ),
          );

        expect(hydrate()).toEqual([
          'ADD issues 1',
          'ADD comments 10',
          'ADD comments 10',
        ]);
        // The first comments connection is the scalar resolver's `id = '10'`.
        expect(filtersOf(connections, 'comments')).toEqual([
          eq('id', '10'),
          pushed ? eq('issueID', '1') : undefined,
        ]);

        replicator.processTransaction(
          '134',
          messages.insert('comments', {id: '11', issueID: '1', upvotes: 0}),
          messages.insert('comments', {id: '23', issueID: '3', upvotes: 0}),
          messages.update('comments', {id: '20', issueID: '2', upvotes: 2}),
        );
        expect(summary(changes())).toEqual(['ADD comments 11']);

        // Moving comment '10' to issue '2' changes the resolved literal. The
        // driver asks to be reset, and the view-syncer then hydrates again.
        replicator.processTransaction(
          '135',
          messages.update('comments', {id: '10', issueID: '2', upvotes: 0}),
        );
        expect(() => changes()).toThrowError(ResetPipelinesSignal);
        pipelines.reset(clientSchema);
        pipelines.advanceWithoutDiff();
        connections.length = 0;

        expect(hydrate()).toEqual([
          'ADD issues 2',
          'ADD comments 10',
          'ADD comments 20',
          'ADD comments 21',
          'ADD comments 22',
          'ADD comments 10',
        ]);
        expect(filtersOf(connections, 'comments')).toEqual([
          eq('id', '10'),
          pushed ? eq('issueID', '2') : undefined,
        ]);

        replicator.processTransaction(
          '136',
          messages.insert('comments', {id: '12', issueID: '1', upvotes: 0}),
          messages.insert('comments', {id: '24', issueID: '2', upvotes: 0}),
        );
        expect(summary(changes())).toEqual(['ADD comments 24']);
      },
    );
  });
});
