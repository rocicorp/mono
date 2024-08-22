import {expect, test} from 'vitest';
import {Ordering} from '../ast2/ast.js';
import {compareRowsTest} from './data.test.js';
import {MemorySource} from './memory-source.js';
import {runCases} from './test/source-cases.js';
import {ValueType} from './schema.js';
import {Catch} from './catch.js';

runCases(
  (
    _name: string,
    columns: Record<string, ValueType>,
    primaryKeys: readonly string[],
  ) => new MemorySource(columns, primaryKeys),
);

test('schema', () => {
  compareRowsTest((order: Ordering) => {
    const ms = new MemorySource({a: 'string'}, ['a']);
    const out = new Catch(ms);
    ms.addOutput(out, order);
    return ms.getSchema(out).compareRows;
  });
});

test('indexes get cleaned up when not needed', () => {
  const ms = new MemorySource({a: 'string', b: 'string', c: 'string'}, ['a']);
  expect(ms.getIndexKeys()).toEqual([JSON.stringify([['a', 'asc']])]);

  const c1 = new Catch(ms);
  ms.addOutput(c1, [['b', 'asc']]);
  c1.hydrate();
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([['b', 'asc']]),
  ]);

  const c2 = new Catch(ms);
  ms.addOutput(c2, [['b', 'asc']]);
  c2.hydrate();
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([['b', 'asc']]),
  ]);

  const c3 = new Catch(ms);
  ms.addOutput(c3, [['c', 'asc']]);
  c3.hydrate();
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([['b', 'asc']]),
    JSON.stringify([['c', 'asc']]),
  ]);

  ms.removeOutput(c3);
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([['b', 'asc']]),
  ]);

  ms.removeOutput(c2);
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([['b', 'asc']]),
  ]);

  ms.removeOutput(c1);
  expect(ms.getIndexKeys()).toEqual([JSON.stringify([['a', 'asc']])]);
});
