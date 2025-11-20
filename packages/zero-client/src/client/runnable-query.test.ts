import {expect, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {newQuery} from '../../../zql/src/query/query-impl.ts';
import {refCountSymbol} from '../../../zql/src/ivm/view-apply-change.ts';
import {zeroForTest} from './test-utils.ts';

const schema = createSchema({
  tables: [
    table('user')
      .columns({
        id: string(),
        name: string(),
        age: number(),
      })
      .primaryKey('id'),
  ],
});

test('query.run() works the same as zero.run(query)', async () => {
  const z = zeroForTest({schema});

  await z.mutate.user.insert({id: '1', name: 'Alice', age: 30});
  await z.mutate.user.insert({id: '2', name: 'Bob', age: 25});
  await z.mutate.user.insert({id: '3', name: 'Charlie', age: 35});

  // Test that query.run() returns the same results as zero.run(query)
  const query = z.query.user.where('age', '>', 25);
  const resultsViaZeroRun = await z.run(query);
  const resultsViaQueryRun = await query.run();

  expect(resultsViaQueryRun).toEqual(resultsViaZeroRun);
  expect(resultsViaQueryRun).toEqual([
    {id: '1', name: 'Alice', age: 30, [refCountSymbol]: 1},
    {id: '3', name: 'Charlie', age: 35, [refCountSymbol]: 1},
  ]);
});

test('query.run() works with method chaining', async () => {
  const z = zeroForTest({schema});

  await z.mutate.user.insert({id: '1', name: 'Alice', age: 30});
  await z.mutate.user.insert({id: '2', name: 'Bob', age: 25});
  await z.mutate.user.insert({id: '3', name: 'Charlie', age: 35});

  // Test method chaining preserves the runnable type
  const results = await z.query.user
    .where('age', '>', 25)
    .orderBy('age', 'asc')
    .limit(10)
    .run();

  expect(results).toEqual([
    {id: '1', name: 'Alice', age: 30, [refCountSymbol]: 1},
    {id: '3', name: 'Charlie', age: 35, [refCountSymbol]: 1},
  ]);
});

test('query.run() supports RunOptions', async () => {
  const z = zeroForTest({schema});

  await z.mutate.user.insert({id: '1', name: 'Alice', age: 30});

  // Test with type: 'unknown' option (default behavior)
  const results = await z.query.user.run({type: 'unknown'});

  expect(results).toEqual([
    {id: '1', name: 'Alice', age: 30, [refCountSymbol]: 1},
  ]);
});

test('calling run() on a query without delegate throws', () => {
  // Create a query without a delegate (not from zero.query)
  const queryWithoutDelegate = newQuery(schema, 'user');

  // QueryImpl has run() at runtime but Query interface doesn't expose it
  // We cast to any to test the runtime behavior
  expect(() => (queryWithoutDelegate as any).run()).toThrow(
    'Cannot call run() on a query without a delegate',
  );
});

test('query.run() works alongside zero.run() for one() queries', async () => {
  const z = zeroForTest({schema});

  await z.mutate.user.insert({id: '1', name: 'Alice', age: 30});

  // Note: .one() returns Query (not RunnableQuery) because it changes TReturn,
  // so we use zero.run() for .one() queries
  const result = await z.run(z.query.user.where('id', '1').one());

  expect(result).toEqual({
    id: '1',
    name: 'Alice',
    age: 30,
    [refCountSymbol]: 1,
  });

  const noResult = await z.run(z.query.user.where('id', '999').one());
  expect(noResult).toBeUndefined();
});
