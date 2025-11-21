import {expect, test} from 'vitest';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
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

test('query.run() works with related queries', async () => {
  const relatedSchema = createSchema({
    tables: [
      table('user')
        .columns({
          id: string(),
          name: string(),
        })
        .primaryKey('id'),
      table('post')
        .columns({
          id: string(),
          userId: string(),
          title: string(),
        })
        .primaryKey('id'),
    ],
    relationships: [
      relationships(
        table('user')
          .columns({
            id: string(),
            name: string(),
          })
          .primaryKey('id'),
        connect => ({
          posts: connect.many({
            sourceField: ['id'],
            destField: ['userId'],
            destSchema: table('post')
              .columns({
                id: string(),
                userId: string(),
                title: string(),
              })
              .primaryKey('id'),
          }),
        }),
      ),
    ],
  });

  const z = zeroForTest({schema: relatedSchema});

  await z.mutate.user.insert({id: '1', name: 'Alice'});
  await z.mutate.post.insert({id: 'p1', userId: '1', title: 'Post 1'});
  await z.mutate.post.insert({id: 'p2', userId: '1', title: 'Post 2'});

  // Test that .related() preserves RunnableQuery and .run() works
  const results = await z.query.user.related('posts').run();

  expect(results).toHaveLength(1);
  expect(results[0]).toMatchObject({
    id: '1',
    name: 'Alice',
  });
  expect(results[0].posts).toHaveLength(2);
  expect(results[0].posts).toEqual(
    expect.arrayContaining([
      expect.objectContaining({id: 'p1', title: 'Post 1'}),
      expect.objectContaining({id: 'p2', title: 'Post 2'}),
    ]),
  );
});
