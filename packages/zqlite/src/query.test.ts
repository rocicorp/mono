import {beforeEach, expect, expectTypeOf, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.js';
import {must} from '../../shared/src/must.js';
import {MemoryStorage} from '../../zql/src/ivm/memory-storage.js';
import type {Source} from '../../zql/src/ivm/source.js';
import {newQuery, type QueryDelegate} from '../../zql/src/query/query-impl.js';
import {schema} from '../../zql/src/query/test/test-schemas.js';
import {Database} from './db.js';
import {TableSource, toSQLiteTypeName} from './table-source.js';

let queryDelegate: QueryDelegate;
beforeEach(() => {
  const db = new Database(createSilentLogContext(), ':memory:');
  const sources = new Map<string, Source>();
  queryDelegate = {
    getSource: (name: string) => {
      let source = sources.get(name);
      if (source) {
        return source;
      }

      const tableSchema = schema.tables[name as keyof typeof schema.tables];

      // create the SQLite table
      db.exec(`
      CREATE TABLE "${name}" (
        ${Object.entries(tableSchema.columns)
          .map(([name, c]) => `"${name}" ${toSQLiteTypeName(c.type)}`)
          .join(', ')},
        PRIMARY KEY (${tableSchema.primaryKey.map(k => `"${k}"`).join(', ')})
      )`);

      source = new TableSource(
        'query.test.ts',
        db,
        name,
        tableSchema.columns,
        tableSchema.primaryKey,
      );

      sources.set(name, source);
      return source;
    },

    createStorage() {
      return new MemoryStorage();
    },
    addServerQuery() {
      return () => {};
    },
    onTransactionCommit() {
      return () => {};
    },
    batchViewUpdates<T>(applyViewUpdates: () => T): T {
      return applyViewUpdates();
    },
  };

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
  >();
});

test('basic query', () => {
  const query = newQuery(queryDelegate, schema, 'issue');
  const data = query.run();
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

test('null compare', () => {
  let rows = newQuery(queryDelegate, schema, 'issue')
    .where('ownerId', '=', null)
    .run();

  expect(rows).toEqual([]);

  rows = newQuery(queryDelegate, schema, 'issue')
    .where('ownerId', '!=', null)
    .run();

  expect(rows).toEqual([]);

  rows = newQuery(queryDelegate, schema, 'issue')
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

  rows = newQuery(queryDelegate, schema, 'issue')
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

test('or', () => {
  const query = newQuery(queryDelegate, schema, 'issue').where(({or, cmp}) =>
    or(cmp('ownerId', '=', '0001'), cmp('ownerId', '=', '0002')),
  );
  const data = query.run();
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
