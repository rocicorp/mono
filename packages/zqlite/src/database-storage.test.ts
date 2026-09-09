import {afterEach} from 'node:test';
import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../zqlite/src/database-storage.ts';
import {Database} from '../../zqlite/src/db.ts';

describe('view-syncer/database-storage', () => {
  let db: Database;
  let storage: DatabaseStorage;

  beforeEach(() => {
    db = new Database(createSilentLogContext(), ':memory:');
    db.prepare(CREATE_STORAGE_TABLE).run();
    storage = new DatabaseStorage(db);
  });

  afterEach(() => {
    db.close();
  });

  function dumpDB() {
    return db.prepare('SELECT * FROM storage').all();
  }

  test('json values', () => {
    const store = storage.createClientGroupStorage('foo-bar').createStorage();
    store.set('int', 1);
    store.set('string', '2');
    store.set('bool', true);
    store.set('null', null);
    store.set('array', [1, 2, 3]);
    store.set('object', {foo: 'bar'});

    expect(store.get('int')).toBe(1);
    expect(store.get('string')).toBe('2');
    expect(store.get('bool')).toBe(true);
    expect(store.get('null')).toBe(null);
    expect(store.get('array')).toEqual([1, 2, 3]);
    expect(store.get('object')).toEqual({foo: 'bar'});

    expect(dumpDB()).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "foo-bar",
          "key": "int",
          "op": 1,
          "val": "1",
        },
        {
          "clientGroupID": "foo-bar",
          "key": "string",
          "op": 1,
          "val": ""2"",
        },
        {
          "clientGroupID": "foo-bar",
          "key": "bool",
          "op": 1,
          "val": "true",
        },
        {
          "clientGroupID": "foo-bar",
          "key": "null",
          "op": 1,
          "val": "null",
        },
        {
          "clientGroupID": "foo-bar",
          "key": "array",
          "op": 1,
          "val": "[1,2,3]",
        },
        {
          "clientGroupID": "foo-bar",
          "key": "object",
          "op": 1,
          "val": "{"foo":"bar"}",
        },
      ]
    `);
  });

  test('get non-existent', () => {
    const store = storage.createClientGroupStorage('foo-bar').createStorage();
    expect(store.get('foo')).toBeUndefined;
  });

  test('del', () => {
    const store = storage.createClientGroupStorage('foo-bar').createStorage();
    store.set('foo', 'bar');
    store.set('bar', 'baz');
    store.set('boo', 'doo');

    store.del('bar');
    store.del('bo'); // non-existent
    expect(store.get('bar')).toBeUndefined();
    expect(store.get('boo')).toBe('doo');
    expect(store.get('foo')).toBe('bar');
  });

  test('client group / operator isolation and destroy', () => {
    const cg1 = storage.createClientGroupStorage('foo-bar');
    const cg2 = storage.createClientGroupStorage('bar-foo');

    const stores = [
      cg1.createStorage(),
      cg1.createStorage(),
      cg2.createStorage(),
      cg2.createStorage(),
    ];

    stores.forEach((s, i) => {
      s.set('foo', i);
    });
    stores.forEach((s, i) => {
      expect(s.get('foo')).toBe(i);
    });

    expect(dumpDB()).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "foo-bar",
          "key": "foo",
          "op": 1,
          "val": "0",
        },
        {
          "clientGroupID": "foo-bar",
          "key": "foo",
          "op": 2,
          "val": "1",
        },
        {
          "clientGroupID": "bar-foo",
          "key": "foo",
          "op": 1,
          "val": "2",
        },
        {
          "clientGroupID": "bar-foo",
          "key": "foo",
          "op": 2,
          "val": "3",
        },
      ]
    `);

    cg2.destroy();

    expect(dumpDB()).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "foo-bar",
          "key": "foo",
          "op": 1,
          "val": "0",
        },
        {
          "clientGroupID": "foo-bar",
          "key": "foo",
          "op": 2,
          "val": "1",
        },
      ]
    `);
  });

  test('set duplicate key', () => {
    const store = storage.createClientGroupStorage('foo-bar').createStorage();
    store.set('foo', '2');
    expect(store.get('foo')).toBe('2');

    store.set('foo', '3');
    expect(store.get('foo')).toBe('3');

    expect(dumpDB()).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "foo-bar",
          "key": "foo",
          "op": 1,
          "val": ""3"",
        },
      ]
    `);
  });
});
