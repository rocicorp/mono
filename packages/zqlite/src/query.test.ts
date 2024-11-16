import {beforeEach, expect, test} from 'vitest';
import {Database} from './db.js';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.js';
import type {Source} from '../../zql/src/ivm/source.js';
import {TableSource, toSQLiteTypeName} from './table-source.js';
import {MemoryStorage} from '../../zql/src/ivm/memory-storage.js';
import {newQuery, type QueryDelegate} from '../../zql/src/query/query-impl.js';
import {schemas} from '../../zql/src/query/test/testSchemas.js';
import type {TableSchema} from '../../zero-schema/src/table-schema.js';
import {must} from '../../shared/src/must.js';

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
      const schema = (schemas as unknown as Record<string, TableSchema>)[name];

      // create the SQLite table
      db.exec(`
      CREATE TABLE "${name}" (
        ${Object.entries(schema.columns)
          .map(([name, c]) => `"${name}" ${toSQLiteTypeName(c.type)}`)
          .join(', ')},
        PRIMARY KEY (${schema.primaryKey.map(k => `"${k}"`).join(', ')})
      )`);

      source = new TableSource(db, name, schema.columns, schema.primaryKey);

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
});

test('basic query', () => {
  const query = newQuery(queryDelegate, schemas.issue);
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
  let rows = newQuery(queryDelegate, schemas.issue)
    .where('ownerId', '=', null)
    .run();

  expect(rows).toEqual([]);

  rows = newQuery(queryDelegate, schemas.issue)
    .where('ownerId', '!=', null)
    .run();

  expect(rows).toEqual([]);

  rows = newQuery(queryDelegate, schemas.issue)
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

  rows = newQuery(queryDelegate, schemas.issue)
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
