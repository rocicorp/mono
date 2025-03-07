import {beforeAll, expect, test} from 'vitest';
import {
  createTableSQL,
  schema,
} from '../../../zql/src/query/test/test-schemas.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {getConnectionURI, testDBs} from '../../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../../zero-cache/src/types/pg.ts';
import type {Query} from '../../../zql/src/query/query.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {initialSync} from '../../../zero-cache/src/services/change-source/pg/initial-sync.ts';
import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {newQueryDelegate} from '../../../zqlite/src/test/source-factory.ts';
import {
  newQuery,
  type QueryDelegate,
} from '../../../zql/src/query/query-impl.ts';
import type {LogConfig} from '../../../otel/src/log-options.ts';

const lc = createSilentLogContext();
const logConfig: LogConfig = {
  format: 'text',
  level: 'debug',
  ivmSampling: 0,
  slowRowThreshold: 0,
};

let pg: PostgresDB;
let sqlite: Database;
type Schema = typeof schema;
let issueQuery: Query<Schema, 'issue'>;
let queryDelegate: QueryDelegate;

beforeAll(async () => {
  pg = await testDBs.create('discord-repro');
  await pg.unsafe(createTableSQL);
  sqlite = new Database(lc, ':memory:');

  await pg.unsafe(/*sql*/ `
    INSERT INTO "issue" ("id", "title", "description", "closed", "ownerId") VALUES (
      'issue1', 'Test Issue 1', 'Description for issue 1', false, 'user1'
    );

    INSERT INTO "user" ("id", "name") VALUES (
      'user1', 'User 1'
    );

    INSERT INTO "comment" ("id", "authorId", "issueId", text, "createdAt") VALUES (
      'comment1', 'user1', 'issue1', 'Comment 1', 0
    );
  `);

  await initialSync(
    new LogContext('debug', {}, consoleLogSink),
    {appID: 'discord_repro', shardNum: 0, publications: []},
    sqlite,
    getConnectionURI(pg),
    {tableCopyWorkers: 1, rowBatchSize: 10000},
  );

  queryDelegate = newQueryDelegate(lc, logConfig, sqlite, schema);
  issueQuery = newQuery(queryDelegate, schema, 'issue');
});

test.fails(
  'discord report https://discord.com/channels/830183651022471199/1347550174968287233/1347552521865920616',
  () => {
    /**
   The discord query:
   eb.or(
        eb.cmp('ownerId', '=', authData.sub!),
        eb.and(
            eb.cmp('shared', '=', true),
            eb.exists('states', (q) => q.where('userId', '=', authData.sub!))
        )
    )

    Below is the same form. Using `closed` to stand in for `shared` and `comments` to stand in for `states`.

    The bug report is seeing no rows returned whereas this repro retracts the wrong rows.

    The "no rows returned" error could be due to the combination of currently active queries in their app.
   */
    const q = issueQuery
      .where('id', 'issue1')
      .where(eb =>
        eb.or(
          eb.cmp('ownerId', '=', 'user1'),
          eb.and(
            eb.cmp('closed', '=', false),
            eb.exists('comments', q => q.where('authorId', '=', 'user1')),
          ),
        ),
      )
      .related('comments');

    const view = q.materialize();

    expect(view.data).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "comments": [
          {
            "authorId": "user1",
            "createdAt": 0,
            "id": "comment1",
            "issueId": "issue1",
            "text": "Comment 1",
          },
        ],
        "description": "Description for issue 1",
        "id": "issue1",
        "ownerId": "user1",
        "title": "Test Issue 1",
      },
    ]
  `);

    queryDelegate.getSource('issue')?.push({
      type: 'edit',
      oldRow: {
        id: 'issue1',
        title: 'Test Issue 1',
        description: 'Description for issue 1',
        closed: false,
        ownerId: 'user1',
      },
      row: {
        id: 'issue1',
        title: 'Test Issue 1',
        description: 'Description for issue 1',
        closed: true,
        ownerId: 'user1',
      },
    });

    // the data post-edit should be the same as the view when hydrated from scratch
    // but it is not! `view.data` is empty!
    expect(view.data).toEqual(q.materialize().data);
  },
);
