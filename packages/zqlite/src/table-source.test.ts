import type {PipelineEntity} from 'zql/src/zql/ivm/types.js';
import Database from 'better-sqlite3';
import {describe, expect, test} from 'vitest';
import {createContext} from './context.js';
import {ZQLite} from './zqlite.js';

test('add', () => {
  const db = new Database(':memory:');
  const context = createContext(new ZQLite(db), db);

  db.prepare('CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)').run();

  const source = context.getSource('foo');
  source.add({id: 1, name: 'one'});
  source.add({id: 2, name: 'two'});
  source.add({id: 3, name: 'three'});

  const stmt = db.prepare('SELECT * FROM foo');
  const rows = stmt.all();
  expect(rows).toEqual([
    {id: 1, name: 'one'},
    {id: 2, name: 'two'},
    {id: 3, name: 'three'},
  ]);
});

test('delete', () => {
  const db = new Database(':memory:');
  const context = createContext(new ZQLite(db), db);

  db.prepare('CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)').run();

  const source = context.getSource('foo');
  source.add({id: 1, name: 'one'});
  source.add({id: 2, name: 'two'});
  source.add({id: 3, name: 'three'});

  source.delete({id: 2, name: 'two'});

  const stmt = db.prepare('SELECT * FROM foo');
  const rows = stmt.all();
  expect(rows).toEqual([
    {id: 1, name: 'one'},
    {id: 3, name: 'three'},
  ]);
});

describe('message upstream', () => {
  const db = new Database(':memory:');
  const m = new ZQLite(db);
  const context = createContext(m, db);
  db.prepare('CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)').run();

  const source = context.getSource('foo');
  source.add({id: 1, name: 'one'});
  source.add({id: 2, name: 'two'});
  source.add({id: 3, name: 'three'});

  test.each([
    {
      name: 'bare selects',
      sql: 'SELECT * FROM foo',
      message: {type: 'pull', id: 1, hoistedConditions: []} as const,
    },
    {
      name: 'select with conditions',
      sql: 'SELECT * FROM foo WHERE id = 1',
      message: {
        type: 'pull',
        id: 1,
        hoistedConditions: [
          {
            selector: ['foo', 'id'],
            op: '=',
            value: 1,
          },
        ],
      } as const,
    },
    {
      name: 'select with ordering',
      sql: 'SELECT * FROM foo ORDER BY id DESC',
      message: {
        type: 'pull',
        id: 1,
        hoistedConditions: [],
        order: [[['foo', 'id'], 'desc']],
      } as const,
    },
  ])('$name', ({sql, message}) => {
    const stmt = db.prepare(sql);
    const rows = stmt.all();

    let items: PipelineEntity[] = [];

    m.tx(() =>
      source.stream.messageUpstream(message, {
        newDifference: (_version, data) => {
          items = [...data].map(d => d[0]);
        },
        commit: () => {},
      }),
    );

    expect(rows).toEqual(items);
  });
});
