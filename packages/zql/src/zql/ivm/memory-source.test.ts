import {expect, test} from 'vitest';
import {Ordering} from '../ast/ast.js';
import {Catch} from './catch.js';
import {compareRowsTest} from './data.test.js';
import {MemorySource} from './memory-source.js';
import type {PrimaryKeys, SchemaValue} from './schema.js';
import {runCases} from './test/source-cases.js';

runCases(
  (
    name: string,
    columns: Record<string, SchemaValue>,
    primaryKeys: PrimaryKeys,
  ) => new MemorySource(name, columns, primaryKeys),
);

test('schema', () => {
  compareRowsTest((order: Ordering) => {
    const ms = new MemorySource('table', {a: {type: 'string'}}, ['a']);
    return ms.connect(order).getSchema().compareRows;
  });
});

test('indexes get cleaned up when not needed', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'string'}, b: {type: 'string'}, c: {type: 'string'}},
    ['a'],
  );
  expect(ms.getIndexKeys()).toEqual([JSON.stringify([['a', 'asc']])]);

  const conn1 = ms.connect([['b', 'asc']]);
  const c1 = new Catch(conn1);
  c1.fetch();
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([['b', 'asc']]),
  ]);

  const conn2 = ms.connect([['b', 'asc']]);
  const c2 = new Catch(conn2);
  c2.fetch();
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([['b', 'asc']]),
  ]);

  const conn3 = ms.connect([['c', 'asc']]);
  const c3 = new Catch(conn3);
  c3.fetch();
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([['b', 'asc']]),
    JSON.stringify([['c', 'asc']]),
  ]);

  conn3.destroy();
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([['b', 'asc']]),
  ]);

  conn2.destroy();
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([['b', 'asc']]),
  ]);

  conn1.destroy();
  expect(ms.getIndexKeys()).toEqual([JSON.stringify([['a', 'asc']])]);
});
