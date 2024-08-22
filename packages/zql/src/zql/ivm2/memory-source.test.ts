import {test} from 'vitest';
import {Ordering} from '../ast2/ast.js';
import {compareRowsTest} from './data.test.js';
import {MemorySource} from './memory-source.js';
import {runCases} from './test/source-cases.js';
import {ValueType} from './schema.js';

runCases(
  (
    _name: string,
    _columns: Record<string, ValueType>,
    order: Ordering,
    _primaryKeys: readonly string[],
  ) => new MemorySource(order),
);

test('schema', () => {
  compareRowsTest((order: Ordering) => {
    const ms = new MemorySource(order);
    return ms.schema.compareRows;
  });
});
