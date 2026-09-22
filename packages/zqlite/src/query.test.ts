import {beforeEach, expect, expectTypeOf, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {must} from '../../shared/src/must.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeRemove,
} from '../../zql/src/ivm/source.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import type {QueryDelegate} from '../../zql/src/query/query-delegate.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {schema} from '../../zql/src/query/test/test-schemas.ts';
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
    userSource.push(
      makeSourceChangeAdd({
        id: '0001',
        name: 'Alice',
        metadata: JSON.stringify({
          registrar: 'github',
          login: 'alicegh',
        }),
      }),
    ),
  );
  consume(
    userSource.push(
      makeSourceChangeAdd({
        id: '0002',
        name: 'Bob',
        metadata: JSON.stringify({
          registar: 'google',
          login: 'bob@gmail.com',
          altContacts: ['bobwave', 'bobyt', 'bobplus'],
        }),
      }),
    ),
  );
  consume(
    issueSource.push(
      makeSourceChangeAdd({
        id: '0001',
        title: 'issue 1',
        description: 'description 1',
        closed: false,
        owner_id: '0001',
      }),
    ),
  );
  consume(
    issueSource.push(
      makeSourceChangeAdd({
        id: '0002',
        title: 'issue 2',
        description: 'description 2',
        closed: false,
        owner_id: '0002',
      }),
    ),
  );
  consume(
    issueSource.push(
      makeSourceChangeAdd({
        id: '0003',
        title: 'issue 3',
        description: 'description 3',
        closed: false,
        owner_id: null,
      }),
    ),
  );

  consume(
    labelSource.push(
      makeSourceChangeAdd({
        id: '0001',
        name: 'bug',
      }),
    ),
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

test('json path filter', async () => {
  // Self-contained delegate so `metadata` is stored as proper JSON (objects),
  // not the double-encoded strings the shared beforeEach seeds.
  const db = new Database(createSilentLogContext(), ':memory:');
  const qd = newQueryDelegate(lc, testLogConfig, db, schema);
  const users = must(qd.getSource('users'));
  consume(
    users.push(
      makeSourceChangeAdd({
        id: 'j1',
        name: 'Alice',
        metadata: {registrar: 'github', login: 'alicegh'},
      }),
    ),
  );
  consume(
    users.push(
      makeSourceChangeAdd({
        id: 'j2',
        name: 'Bob',
        // typo key 'registar' => no 'registrar'
        metadata: {
          registar: 'google',
          login: 'bob@gmail.com',
          altContacts: ['bobwave', 'bobyt'],
        },
      }),
    ),
  );

  const ids = async (
    // oxlint-disable-next-line no-explicit-any
    q: any,
  ): Promise<string[]> => {
    const rows = (await qd.run(q)) as ReadonlyArray<{id: string}>;
    return rows.map(r => r.id);
  };

  // Object-key path.
  expect(
    await ids(
      newQuery(schema, 'user').where(({cmp, json}) =>
        cmp(json('metadata', 'registrar'), '=', 'github'),
      ),
    ),
  ).toEqual(['j1']);

  // Array-index segment.
  expect(
    await ids(
      newQuery(schema, 'user').where(({cmp, json}) =>
        cmp(json('metadata', 'altContacts', 0), '=', 'bobwave'),
      ),
    ),
  ).toEqual(['j2']);

  // LIKE on a string leaf.
  expect(
    await ids(
      newQuery(schema, 'user').where(({cmp, json}) =>
        cmp(json('metadata', 'login'), 'LIKE', 'alice%'),
      ),
    ),
  ).toEqual(['j1']);

  // IS NULL matches a missing key (Bob has no 'registrar'), with the same
  // result on the SQL pushdown and the in-memory predicate.
  expect(
    await ids(
      newQuery(schema, 'user').where(({cmp, json}) =>
        cmp(json('metadata', 'registrar'), 'IS', null),
      ),
    ),
  ).toEqual(['j2']);
});

test('json path: negative or fractional array index throws at build time', () => {
  // The engines disagree on negative indices (Postgres `#>>` counts from the
  // end; JS/SQLite yield null), so the builder rejects them up front. `as
  // number` sidesteps the compile-time check for a literal to exercise the
  // runtime one.
  expect(() =>
    newQuery(schema, 'user').where(({cmp, json}) =>
      cmp(json('metadata', 'altContacts', -1 as number), '=', 'x'),
    ),
  ).toThrow(/non-negative integer/);
  expect(() =>
    newQuery(schema, 'user').where(({cmp, json}) =>
      cmp(json('metadata', 'altContacts', 1.5 as number), '=', 'x'),
    ),
  ).toThrow(/non-negative integer/);
  // Beyond the safe-integer range the index stringifies as `1e+21`, which
  // SQLite rejects as a bad JSON path — so it is rejected up front too.
  expect(() =>
    newQuery(schema, 'user').where(({cmp, json}) =>
      cmp(json('metadata', 'altContacts', 1e21 as number), '=', 'x'),
    ),
  ).toThrow(/non-negative integer/);
});

test('json path filter: type-strict pushdown and key escaping', async () => {
  // `metadata` is typed in the test schema, so use an untyped builder to reach
  // the runtime behaviour on data that does not conform to the declared shape.
  const db = new Database(createSilentLogContext(), ':memory:');
  const qd = newQueryDelegate(lc, testLogConfig, db, schema);
  const users = must(qd.getSource('users'));
  const seed = (id: string, metadata: Record<string, ReadonlyJSONValue>) =>
    consume(users.push(makeSourceChangeAdd({id, name: id, metadata})));
  seed('k1', {registrar: 'github', count: 3, flagged: true});
  // Wrong JSON type at every key.
  seed('k2', {registrar: 42, count: 'n/a', flagged: 1});
  seed('k3', {});
  seed('k4', {'a"b': 'quote', 'c\\d': 'backslash', 'tags': ['x']});

  const ids = async (
    // oxlint-disable-next-line no-explicit-any
    factory: (eb: any) => unknown,
  ): Promise<string[]> => {
    // oxlint-disable-next-line no-explicit-any
    const q = newQuery(schema, 'user').where(factory as any);
    const rows = (await qd.run(q)) as ReadonlyArray<{id: string}>;
    return rows.map(r => r.id).sort();
  };

  // A bare json_extract would say 'n/a' > 5 (TEXT sorts above numbers), 42
  // LIKE '4%', and true = 1; the json_type gate makes each a non-match.
  expect(await ids(eb => eb.cmp(eb.json('metadata', 'count'), '>', 0))).toEqual(
    ['k1'],
  );
  expect(
    await ids(eb => eb.cmp(eb.json('metadata', 'registrar'), 'LIKE', '4%')),
  ).toEqual([]);
  // `flagged = 1` matches only the row whose flagged is the *number* 1 (k2),
  // never the boolean true (k1); `= true` is the reverse.
  expect(
    await ids(eb => eb.cmp(eb.json('metadata', 'flagged'), '=', 1)),
  ).toEqual(['k2']);
  expect(
    await ids(eb => eb.cmp(eb.json('metadata', 'flagged'), '=', true)),
  ).toEqual(['k1']);
  expect(
    await ids(eb => eb.cmp(eb.json('metadata', 'count'), 'IN', [3, 10])),
  ).toEqual(['k1']);
  // A mismatched leaf matches a negated operator; a missing one never does.
  expect(
    await ids(eb => eb.cmp(eb.json('metadata', 'registrar'), '!=', 'github')),
  ).toEqual(['k2']);
  expect(
    await ids(eb => eb.cmp(eb.json('metadata', 'count'), 'NOT IN', [3])),
  ).toEqual(['k2']);
  // k1's count is the number 3: a mismatch for the string pattern, hence a
  // match for the negated operator, alongside k2's genuine non-match.
  expect(
    await ids(eb => eb.cmp(eb.json('metadata', 'count'), 'NOT LIKE', '3%')),
  ).toEqual(['k1', 'k2']);
  // An empty NOT IN matches every non-null leaf but not a missing one (bare SQL
  // `NULL NOT IN ()` would be TRUE).
  expect(
    await ids(eb => eb.cmp(eb.json('metadata', 'count'), 'NOT IN', [])),
  ).toEqual(['k1', 'k2']);

  // Keys containing `"` and `\` need JSON (backslash) escaping in the SQLite
  // path; SQL-style `""` doubling would yield NULL or a bad-path error.
  expect(
    await ids(eb => eb.cmp(eb.json('metadata', 'a"b'), '=', 'quote')),
  ).toEqual(['k4']);
  expect(
    await ids(eb => eb.cmp(eb.json('metadata', 'c\\d'), '=', 'backslash')),
  ).toEqual(['k4']);
  // A string segment never indexes an array (matches the in-memory reader).
  expect(
    await ids(eb => eb.cmp(eb.json('metadata', 'tags', '0'), '=', 'x')),
  ).toEqual([]);
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
    labelSource.push(
      makeSourceChangeAdd({
        issueId: '0001',
        labelId: '0001',
      }),
    ),
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
    labelSource.push(
      makeSourceChangeRemove({
        issueId: '0001',
        labelId: '0001',
      }),
    ),
  );

  expect(view.data).toMatchInlineSnapshot(`[]`);
});

test('schema applied `one`', async () => {
  // test only one item is returned when `one` is applied to a relationship in the schema
  const commentSource = must(queryDelegate.getSource('comments'));
  const revisionSource = must(queryDelegate.getSource('revision'));
  consume(
    commentSource.push(
      makeSourceChangeAdd({
        id: '0001',
        authorId: '0001',
        issue_id: '0001',
        text: 'comment 1',
        createdAt: 1,
      }),
    ),
  );
  consume(
    commentSource.push(
      makeSourceChangeAdd({
        id: '0002',
        authorId: '0002',
        issue_id: '0001',
        text: 'comment 2',
        createdAt: 2,
      }),
    ),
  );
  consume(
    revisionSource.push(
      makeSourceChangeAdd({
        id: '0001',
        authorId: '0001',
        commentId: '0001',
        text: 'revision 1',
      }),
    ),
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
