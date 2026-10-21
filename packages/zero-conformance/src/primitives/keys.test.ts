import {test} from 'vitest';
import {toPrimaryKeyString} from '../../../zero-client/src/client/keys.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {interestingStrings, Random} from '../random.ts';
import {emitVectors} from '../vectors.ts';

test('primary-key-string', () => {
  const r = new Random(0x6b6579);
  const cases: {table: string; primaryKey: CompoundKey; row: Row}[] = [];
  const scalars = [
    ...interestingStrings,
    0,
    -0,
    1,
    -1,
    1.5,
    1e21,
    1e-7,
    2 ** 53,
    123456789,
    true,
    false,
  ];
  for (const v of scalars) {
    cases.push({table: 'issue', primaryKey: ['id'], row: {id: v, other: 'x'}});
  }
  for (let i = 0; i < 100; i++) {
    cases.push({
      table: r.pick(['issueLabel', 'a/b', 'T']),
      primaryKey: ['a', 'b'],
      row: {a: r.pick(scalars), b: r.pick(scalars), c: 1},
    });
  }
  cases.push({
    table: 'three',
    primaryKey: ['z', 'a', 'm'],
    row: {a: 1, m: 'x', z: true},
  });
  emitVectors(
    'primary-key-string',
    'primary-key-string',
    'toPrimaryKeyString(table, primaryKey, row): the `e/` storage key of a row.',
    cases.map(c => ({
      ...c,
      key: toPrimaryKeyString(c.table, c.primaryKey, c.row),
    })),
  );
});
