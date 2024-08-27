import {expect, test} from 'vitest';
import {Ordering} from '../ast2/ast.js';
import {compareRowsTest} from './data.test.js';
import {MemorySource} from './memory-source.js';
import {runCases} from './test/source-cases.js';
import {ValueType} from './schema.js';
import {Catch} from './catch.js';

runCases(
  (
    name: string,
    columns: Record<string, ValueType>,
    primaryKeys: readonly string[],
  ) => new MemorySource(name, columns, primaryKeys),
);

test('schema', () => {
  compareRowsTest((order: Ordering) => {
    const ms = new MemorySource('table', {a: 'string'}, ['a']);
    return ms.connect(order).getSchema().compareRows;
  });
});

test('indexes get cleaned up when not needed', () => {
  const ms = new MemorySource(
    'table',
    {a: 'string', b: 'string', c: 'string'},
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

  ms.disconnect(conn3);
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([['b', 'asc']]),
  ]);

  ms.disconnect(conn2);
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([['b', 'asc']]),
  ]);

  ms.disconnect(conn1);
  expect(ms.getIndexKeys()).toEqual([JSON.stringify([['a', 'asc']])]);
});
