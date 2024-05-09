import {describe, expect, test} from 'vitest';
import {makeInfiniteSourceContext} from '../context/test-context.js';
import {EntityQuery} from './entity-query.js';

/**
 * To make sure `limit` is actually `limiting` the amount of data we're processing
 * from a source, we need to test it with an infinite source.
 *
 * There are some forms of queries which are not supported with an infinite source
 * but here we test all those that we expect to work.
 */
describe('pulling from an infinite source is possible if we set a limit', () => {
  type E = {
    id: string;
  };
  const infiniteGenerator = {
    *[Symbol.iterator]() {
      let i = 0;
      while (true) {
        yield [{id: String(++i)}, 1] as const;
      }
    },
  };

  const context = makeInfiniteSourceContext(infiniteGenerator);

  test('bare select', async () => {
    const q = new EntityQuery<{e: E}>(context, 'e', 'e');
    const stmt = q.select('id').limit(2).prepare();
    const data = await stmt.exec();

    expect(data).toEqual([{id: '1'}, {id: '2'}]);

    stmt.destroy();
  });

  // TODO(mlaw): test select with alternate ordering. differing fields and same fields but differing direction
  // TODO(mlaw): test cases for when `withNewOrdering` should or should not be invoked. e.g., join should drop order rn

  test('select and where', async () => {
    const q = new EntityQuery<{e: E}>(context, 'e', 'e');
    const stmt = q.select('id').where('e.id', '>', '9').limit(2).prepare();
    const data = await stmt.exec();

    expect(data).toEqual([{id: '90'}, {id: '91'}]);

    stmt.destroy();
  });

  // test when the view is sorted by a superset of the fields used to sort the source
  test('partial overlap of order', () => {});

  // need to make join lazy
  // test('join 2 tables', () => {});
  // test('join 3 tables', () => {});

  // need the `contiguous groups` optimization
  // test('group-by', () => {});
});

describe('a limited window is correctly maintained over differences', () => {
  test('adding values above the established window', () => {});
  test('adding values below the established window', () => {});
  test('adding values inside the established window', () => {});
  test('removing values above the established window', () => {});
  test('removing values below the established window', () => {});
  test('removing values inside the established window', () => {});
});
