import {expect, expectTypeOf, test} from 'vitest';
import {zeroForTest} from './test-utils.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';

import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {createBuilder} from '../../../zql/src/query/named.ts';
import type {ImmutableArray} from '../../../shared/src/immutable.ts';
import {refCountSymbol} from '../../../zql/src/ivm/view-apply-change.ts';

test('run', async () => {
  const schema = createSchema({
    tables: [
      table('issues')
        .columns({
          id: string(),
          value: number(),
        })
        .primaryKey('id'),
    ],
  });
  const z = zeroForTest({
    server: null,
    schema,
  });
  const builder = createBuilder(schema);
  await z.mutate.issues.insert({id: 'a', value: 1});

  const x = await z.run(builder.issues);
  expectTypeOf(x).toEqualTypeOf<
    {
      readonly id: string;
      readonly value: number;
    }[]
  >();
  expect(x).toEqual([{id: 'a', value: 1, [refCountSymbol]: 1}]);
});

test('materialize', async () => {
  const schema = createSchema({
    tables: [
      table('issues')
        .columns({
          id: string(),
          value: number(),
        })
        .primaryKey('id'),
    ],
  });
  const z = zeroForTest({
    server: null,
    schema,
  });
  const builder = createBuilder(schema);
  await z.mutate.issues.insert({id: 'a', value: 1});

  const m = z.materialize(builder.issues);
  expectTypeOf(m.data).toEqualTypeOf<
    {
      readonly id: string;
      readonly value: number;
    }[]
  >();

  let gotData: unknown;
  m.addListener(d => {
    gotData = d;
    expectTypeOf(d).toEqualTypeOf<
      ImmutableArray<{
        readonly id: string;
        readonly value: number;
      }>
    >();
  });

  expect(gotData).toEqual([{id: 'a', value: 1, [refCountSymbol]: 1}]);
});
