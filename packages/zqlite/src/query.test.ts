import {beforeEach, expect, expectTypeOf, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {must} from '../../shared/src/must.ts';
import type {QueryDelegate} from '../../zql/src/query/query-delegate.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {schema} from '../../zql/src/query/test/test-schemas.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import {Database} from './db.ts';
import {
  mapResultToClientNames,
  newQueryDelegate,
} from './test/source-factory.ts';

let queryDelegate: QueryDelegate;

const lc = createSilentLogContext();

beforeEach(() => {
  const db = new Database(createSilentLogContext(), ':memory:');
  queryDelegate = newQueryDelegate(lc, testLogConfig, db, schema);

  const userSource = must(queryDelegate.getSource('users'));
  const issueSource = must(queryDelegate.getSource('issues'));
  const labelSource = must(queryDelegate.getSource('label'));

  consume(
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
    }),
  );
  consume(
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
    }),
  );
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '0001',
        title: 'issue 1',
        description: 'description 1',
        closed: false,
        owner_id: '0001',
      },
    }),
  );
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '0002',
        title: 'issue 2',
        description: 'description 2',
        closed: false,
        owner_id: '0002',
      },
    }),
  );
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '0003',
        title: 'issue 3',
        description: 'description 3',
        closed: false,
        owner_id: null,
      },
    }),
  );

  consume(
    labelSource.push({
      type: 'add',
      row: {
        id: '0001',
        name: 'bug',
      },
    }),
  );
});

test('row type', () => {
  const query = newQuery(schema, 'issue')
    .whereExists('labels', q => q.where('name', '=', 'bug'))
    .related('labels');

  const rows = queryDelegate.run(query);
  expectTypeOf(rows).toEqualTypeOf<
    Promise<
      {
        readonly id: string;
        readonly title: string;
        readonly description: string;
        readonly closed: boolean;
        readonly ownerId: string | null;
        readonly createdAt: number;
        readonly labels: readonly {
          readonly id: string;
          readonly name: string;
        }[];
      }[]
    >
  >();
});

test('basic query', async () => {
  const query = newQuery(schema, 'issue');
  const data = mapResultToClientNames(
    await queryDelegate.run(query),
    schema,
    'issue',
  );
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "createdAt": null,
        "description": "description 1",
        "id": "0001",
        "ownerId": "0001",
        "title": "issue 1",
      },
      {
        "closed": false,
        "createdAt": null,
        "description": "description 2",
        "id": "0002",
        "ownerId": "0002",
        "title": "issue 2",
      },
      {
        "closed": false,
        "createdAt": null,
        "description": "description 3",
        "id": "0003",
        "ownerId": null,
        "title": "issue 3",
      },
    ]
  `);
});

test('null compare', async () => {
  let query = newQuery(schema, 'issue').where('ownerId', 'IS', null);
  let rows = await queryDelegate.run(query);
  expect(mapResultToClientNames(rows, schema, 'issue')).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "createdAt": null,
        "description": "description 3",
        "id": "0003",
        "ownerId": null,
        "title": "issue 3",
      },
    ]
  `);

  query = newQuery(schema, 'issue').where('ownerId', 'IS NOT', null);
  rows = await queryDelegate.run(query);

  expect(rows).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "createdAt": null,
        "description": "description 1",
        "id": "0001",
        "owner_id": "0001",
        "title": "issue 1",
        Symbol(rc): 1,
      },
      {
        "closed": false,
        "createdAt": null,
        "description": "description 2",
        "id": "0002",
        "owner_id": "0002",
        "title": "issue 2",
        Symbol(rc): 1,
      },
    ]
  `);
});

test('or', async () => {
  const query = newQuery(schema, 'issue').where(({or, cmp}) =>
    or(cmp('ownerId', '=', '0001'), cmp('ownerId', '=', '0002')),
  );
  const data = mapResultToClientNames(
    await queryDelegate.run(query),
    schema,
    'issue',
  );
  expect(data).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "createdAt": null,
        "description": "description 1",
        "id": "0001",
        "ownerId": "0001",
        "title": "issue 1",
      },
      {
        "closed": false,
        "createdAt": null,
        "description": "description 2",
        "id": "0002",
        "ownerId": "0002",
        "title": "issue 2",
      },
    ]
  `);
});

test('where exists retracts when an edit causes a row to no longer match', () => {
  const query = newQuery(schema, 'issue')
    .whereExists('labels', q => q.where('name', '=', 'bug'))
    .related('labels');

  const view = queryDelegate.materialize(query);

  expect(view.data).toMatchInlineSnapshot(`[]`);

  const labelSource = must(queryDelegate.getSource('issueLabel'));
  consume(
    labelSource.push({
      type: 'add',
      row: {
        issueId: '0001',
        labelId: '0001',
      },
    }),
  );

  expect(mapResultToClientNames(view.data, schema, 'issue'))
    .toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "createdAt": null,
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

  consume(
    labelSource.push({
      type: 'remove',
      row: {
        issueId: '0001',
        labelId: '0001',
      },
    }),
  );

  expect(view.data).toMatchInlineSnapshot(`[]`);
});

test('schema applied `one`', async () => {
  // test only one item is returned when `one` is applied to a relationship in the schema
  const commentSource = must(queryDelegate.getSource('comments'));
  const revisionSource = must(queryDelegate.getSource('revision'));
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '0001',
        authorId: '0001',
        issue_id: '0001',
        text: 'comment 1',
        createdAt: 1,
      },
    }),
  );
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '0002',
        authorId: '0002',
        issue_id: '0001',
        text: 'comment 2',
        createdAt: 2,
      },
    }),
  );
  consume(
    revisionSource.push({
      type: 'add',
      row: {
        id: '0001',
        authorId: '0001',
        commentId: '0001',
        text: 'revision 1',
      },
    }),
  );
  const query = newQuery(schema, 'issue')
    .related('owner')
    .related('comments', q => q.related('author').related('revisions'))
    .where('id', '=', '0001');
  const data = mapResultToClientNames(
    await queryDelegate.run(query),
    schema,
    'issue',
  );
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
        "createdAt": null,
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
