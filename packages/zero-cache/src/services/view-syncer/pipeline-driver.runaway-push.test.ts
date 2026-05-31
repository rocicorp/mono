import type {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
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
import {PipelineDriver} from './pipeline-driver.ts';
import {Snapshotter} from './snapshotter.ts';

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
      new InspectorDelegate(undefined),
      () => 200 /** yield threshold */,
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
      CREATE TABLE issue (
        id TEXT PRIMARY KEY,
        creatorID TEXT,
        _0_version TEXT NOT NULL
      );
      CREATE TABLE user (
        id TEXT PRIMARY KEY, 
        name TEXT,
         _0_version TEXT NOT NULL);
      CREATE TABLE comment (
        id TEXT PRIMARY KEY, 
        issueID TEXT,
         _0_version TEXT NOT NULL);

      INSERT INTO user (id, name, _0_version) VALUES ('u1', 'fuzzy', '123');
      INSERT INTO issue (id, creatorID, _0_version)
        WITH RECURSIVE cnt(n) AS (
            SELECT 1
            UNION ALL
            SELECT n + 1 FROM cnt WHERE n < 1000
        )
        SELECT
        'i' || n, -- Concatenates 'i' with the number (1-1000)
        'u1',        '123'
        FROM cnt;
      `);
    replicator = fakeReplicator(lc, db);
  });

  afterEach(() => {
    dbFile.delete();
  });

  const issue = table('issue')
    .columns({
      id: string(),
      creatorID: string(),
    })
    .primaryKey('id');
  const user = table('user')
    .columns({
      id: string(),
      name: string(),
    })
    .primaryKey('id');
  const comment = table('comment')
    .columns({
      id: string(),
      issueID: string(),
    })
    .primaryKey('id');

  const clientSchema = createSchema({
    tables: [issue, user, comment],
  });

  const ISSUES_WITH_CREATOR: AST = {
    table: 'issue',
    orderBy: [['id', 'desc']],
    related: [
      {
        system: 'client',
        correlation: {
          parentField: ['creatorID'],
          childField: ['id'],
        },
        subquery: {
          table: 'user',
          alias: 'creator',
          orderBy: [['id', 'desc']],
        },
      },
    ],
  };

  const ISSUES_WITH_CREATOR_EXISTS_COMMENT_AST: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {
          parentField: ['creatorID'],
          childField: ['id'],
        },
        subquery: {
          table: 'user',
          alias: 'creator',
          orderBy: [['id', 'desc']],
        },
      },
    ],
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
          table: 'comment',
          alias: 'comments',
          orderBy: [['id', 'asc']],
        },
      },
    },
  };

  const messages = new ReplicationMessages({
    issue: 'id',
    user: 'id',
  });

  test('timeout on single change that causes lot of push processing and push output', () => {
    pipelines.init(clientSchema);
    [
      ...pipelines.addQuery('hash1', 'queryID1', ISSUES_WITH_CREATOR, {
        totalElapsed: () => 1000,
        elapsedLap: () => 1000,
      }),
    ];

    // This change will cause a child change push for each of the 1000
    // issue related to user 'u1'.
    replicator.processTransaction(
      '134',
      messages.update('user', {id: 'u1', name: 'wuzzy'}),
    );

    let elapsed = 0;
    expect(() => [
      ...pipelines.advance({elapsedLap: () => 0, totalElapsed: () => elapsed++})
        .changes,
    ]).toThrowErrorMatchingInlineSnapshot(
      `[ResetPipelinesSignal: Advancement exceeded timeout at 0 of 1 changes after 501 ms. Advancement time limited based on total hydration time of 1000 ms.]`,
    );
  });

  test('timeout on single change that causes lot of push processing but no push output', () => {
    pipelines.init(clientSchema);
    [
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        ISSUES_WITH_CREATOR_EXISTS_COMMENT_AST,
        {
          totalElapsed: () => 1000,
          elapsedLap: () => 1000,
        },
      ),
    ];

    // This change will fetch each of the 1000 issue related to user 'u1', but
    // has no push output because none of them pass the exists comments filter.
    replicator.processTransaction(
      '134',
      messages.update('user', {id: 'u1', name: 'wuzzy'}),
    );

    let elapsed = 0;
    expect(() => [
      ...pipelines.advance({elapsedLap: () => 0, totalElapsed: () => elapsed++})
        .changes,
    ]).toThrowErrorMatchingInlineSnapshot(
      `[ResetPipelinesSignal: Advancement exceeded timeout at 0 of 1 changes after 501 ms. Advancement time limited based on total hydration time of 1000 ms.]`,
    );
  });

  test("timeout on many changes that don't fetch any rows", () => {
    pipelines.init(clientSchema);
    [
      ...pipelines.addQuery('hash1', 'queryID1', ISSUES_WITH_CREATOR, {
        totalElapsed: () => 1000,
        elapsedLap: () => 1000,
      }),
    ];

    // This change will cause a child change push for each of the 1000
    // issue related to user 'u1'.
    replicator.processTransaction(
      '134',
      ...[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map(n =>
        messages.insert('issue', {id: `i${1000 + n}`}),
      ),
    );

    let changeCount = 0;
    expect(() => {
      for (const _ of pipelines.advance({
        elapsedLap: () => 0,
        totalElapsed: () => (changeCount + 1) * 100,
      }).changes) {
        changeCount++;
      }
    }).toThrowErrorMatchingInlineSnapshot(
      `[ResetPipelinesSignal: Advancement exceeded timeout at 5 of 10 changes after 600 ms. Advancement time limited based on total hydration time of 1000 ms.]`,
    );
    expect(changeCount).toEqual(5);
  });
});
