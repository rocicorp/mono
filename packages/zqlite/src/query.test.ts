import {beforeEach, expect, expectTypeOf, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {must} from '../../shared/src/must.ts';
import {newQuery, type QueryDelegate} from '../../zql/src/query/query-impl.ts';
import {schema} from '../../zql/src/query/test/test-schemas.ts';
import {Database} from './db.ts';
import type {LogConfig} from '../../otel/src/log-options.ts';
import {newQueryDelegate} from './test/source-factory.ts';

let queryDelegate: QueryDelegate;

const lc = createSilentLogContext();
const logConfig: LogConfig = {
  format: 'text',
  level: 'debug',
  ivmSampling: 0,
  slowRowThreshold: 0,
};

beforeEach(() => {
  const db = new Database(createSilentLogContext(), ':memory:');
  queryDelegate = newQueryDelegate(lc, logConfig, db, schema);

  const userSource = must(queryDelegate.getSource('user'));
  const issueSource = must(queryDelegate.getSource('issue'));
  const labelSource = must(queryDelegate.getSource('label'));

  userSource.push({
    type: 'add',
    row: {
      id: '0001',
      name: 'Alice',
      metadata: JSON.stringify({
        registrar: 'github',
        login: 'alicegh',
      }),
    },
  });
  userSource.push({
    type: 'add',
    row: {
      id: '0002',
      name: 'Bob',
      metadata: JSON.stringify({
        registar: 'google',
        login: 'bob@gmail.com',
        altContacts: ['bobwave', 'bobyt', 'bobplus'],
      }),
    },
  });
  issueSource.push({
    type: 'add',
    row: {
      id: '0001',
      title: 'issue 1',
      description: 'description 1',
      closed: false,
      ownerId: '0001',
    },
  });
  issueSource.push({
    type: 'add',
    row: {
      id: '0002',
      title: 'issue 2',
      description: 'description 2',
      closed: false,
      ownerId: '0002',
    },
  });
  issueSource.push({
    type: 'add',
    row: {
      id: '0003',
      title: 'issue 3',
      description: 'description 3',
      closed: false,
      ownerId: null,
    },
  });

  labelSource.push({
    type: 'add',
    row: {
      id: '0001',
      name: 'bug',
    },
  });
});

test('row type', () => {
  const query = newQuery(queryDelegate, schema, 'issue')
    .whereExists('labels', q => q.where('name', '=', 'bug'))
    .related('labels');
  type RT = ReturnType<typeof query.run>;
  expectTypeOf<RT>().toEqualTypeOf<
    Promise<
      {
        readonly id: string;
        readonly title: string;
        readonly description: string;
        readonly closed: boolean;
        readonly ownerId: string | null;
        readonly labels: readonly {
          readonly id: string;
          readonly name: string;
        }[];
      }[]
    >
  >();
});

test('basic query', async () => {
  const query = newQuery(queryDelegate, schema, 'issue');
  const data = await query.run();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "description": "description 1",
        "id": "0001",
        "ownerId": "0001",
        "title": "issue 1",
      },
      {
        "closed": false,
        "description": "description 2",
        "id": "0002",
        "ownerId": "0002",
        "title": "issue 2",
      },
      {
        "closed": false,
        "description": "description 3",
        "id": "0003",
        "ownerId": null,
        "title": "issue 3",
      },
    ]
  `);
});

test('null compare', async () => {
  let rows = await newQuery(queryDelegate, schema, 'issue')
    .where('ownerId', 'IS', null)
    .run();

  expect(rows).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "description": "description 3",
        "id": "0003",
        "ownerId": null,
        "title": "issue 3",
      },
    ]
  `);

  rows = await newQuery(queryDelegate, schema, 'issue')
    .where('ownerId', 'IS NOT', null)
    .run();

  expect(rows).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "description": "description 1",
        "id": "0001",
        "ownerId": "0001",
        "title": "issue 1",
      },
      {
        "closed": false,
        "description": "description 2",
        "id": "0002",
        "ownerId": "0002",
        "title": "issue 2",
      },
    ]
  `);
});

test('or', async () => {
  const query = newQuery(queryDelegate, schema, 'issue').where(({or, cmp}) =>
    or(cmp('ownerId', '=', '0001'), cmp('ownerId', '=', '0002')),
  );
  const data = await query.run();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "description": "description 1",
        "id": "0001",
        "ownerId": "0001",
        "title": "issue 1",
      },
      {
        "closed": false,
        "description": "description 2",
        "id": "0002",
        "ownerId": "0002",
        "title": "issue 2",
      },
    ]
  `);
});

test('where exists retracts when an edit causes a row to no longer match', () => {
  const query = newQuery(queryDelegate, schema, 'issue')
    .whereExists('labels', q => q.where('name', '=', 'bug'))
    .related('labels');

  const view = query.materialize();

  expect(view.data).toMatchInlineSnapshot(`[]`);

  const labelSource = must(queryDelegate.getSource('issueLabel'));
  labelSource.push({
    type: 'add',
    row: {
      issueId: '0001',
      labelId: '0001',
    },
  });

  expect(view.data).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "description": "description 1",
        "id": "0001",
        "labels": [
          {
            "id": "0001",
            "name": "bug",
          },
        ],
        "ownerId": "0001",
        "title": "issue 1",
      },
    ]
  `);

  labelSource.push({
    type: 'remove',
    row: {
      issueId: '0001',
      labelId: '0001',
    },
  });

  expect(view.data).toMatchInlineSnapshot(`[]`);
});

test('schema applied `one`', async () => {
  // test only one item is returned when `one` is applied to a relationship in the schema
  const commentSource = must(queryDelegate.getSource('comment'));
  const revisionSource = must(queryDelegate.getSource('revision'));
  commentSource.push({
    type: 'add',
    row: {
      id: '0001',
      authorId: '0001',
      issueId: '0001',
      text: 'comment 1',
      createdAt: 1,
    },
  });
  commentSource.push({
    type: 'add',
    row: {
      id: '0002',
      authorId: '0002',
      issueId: '0001',
      text: 'comment 2',
      createdAt: 2,
    },
  });
  revisionSource.push({
    type: 'add',
    row: {
      id: '0001',
      authorId: '0001',
      commentId: '0001',
      text: 'revision 1',
    },
  });
  const query = newQuery(queryDelegate, schema, 'issue')
    .related('owner')
    .related('comments', q => q.related('author').related('revisions'))
    .where('id', '=', '0001');
  const data = await query.run();
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "comments": [
          {
            "author": {
              "id": "0001",
              "metadata": "{"registrar":"github","login":"alicegh"}",
              "name": "Alice",
            },
            "authorId": "0001",
            "createdAt": 1,
            "id": "0001",
            "issueId": "0001",
            "revisions": [
              {
                "authorId": "0001",
                "commentId": "0001",
                "id": "0001",
                "text": "revision 1",
              },
            ],
            "text": "comment 1",
          },
          {
            "author": {
              "id": "0002",
              "metadata": "{"registar":"google","login":"bob@gmail.com","altContacts":["bobwave","bobyt","bobplus"]}",
              "name": "Bob",
            },
            "authorId": "0002",
            "createdAt": 2,
            "id": "0002",
            "issueId": "0001",
            "revisions": [],
            "text": "comment 2",
          },
        ],
        "description": "description 1",
        "id": "0001",
        "owner": {
          "id": "0001",
          "metadata": "{"registrar":"github","login":"alicegh"}",
          "name": "Alice",
        },
        "ownerId": "0001",
        "title": "issue 1",
      },
    ]
  `);
});
