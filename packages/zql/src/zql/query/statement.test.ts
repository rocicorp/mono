import {expect, test} from 'vitest';
import {z} from 'zod';
import {makeTestContext} from '../context/context.js';
import {EntityQuery} from './entity-query.js';
import {makeComparator} from './statement.js';

const e1 = z.object({
  id: z.string(),
  n: z.number(),
  optStr: z.string().optional(),
});
type E1 = z.infer<typeof e1>;
test('basic materialization', () => {
  const context = makeTestContext();
  const q = new EntityQuery<{e1: E1}>(context, 'e1');

  const stmt = q.select('id', 'n').where('n', '>', 100).prepare();

  const expected: E1[] = [];
  const calledWith: (readonly E1[])[] = [];
  stmt.subscribe(data => {
    calledWith.push(data);
  });

  const items = [
    {id: 'a', n: 1},
    {id: 'b', n: 101},
    {id: 'c', n: 102},
  ] as const;

  context.getSource('e1').add(items[0]);

  expected.push(items[1]);
  context.getSource('e1').add(items[1]);

  expected.push(items[2]);
  context.getSource('e1').add(items[2]);
  expect(calledWith).toEqual([[], [items[1]], [items[1], items[2]]]);
});

test('sorted materialization', () => {
  const context = makeTestContext();
  type E1 = z.infer<typeof e1>;
  const q = new EntityQuery<{e1: E1}>(context, 'e1');
  const ascView = q.select('id').asc('n').prepare().view();
  const descView = q.select('id').desc('n').prepare().view();

  context.getSource<E1>('e1').add({
    id: 'a',
    n: 3,
  });
  context.getSource<E1>('e1').add({
    id: 'b',
    n: 2,
  });
  context.getSource<E1>('e1').add({
    id: 'c',
    n: 1,
  });

  expect(ascView.value).toEqual([
    {id: 'c', n: 1},
    {id: 'b', n: 2},
    {id: 'a', n: 3},
  ]);
  expect(descView.value).toEqual([
    {id: 'a', n: 3},
    {id: 'b', n: 2},
    {id: 'c', n: 1},
  ]);
});

test('sorting is stable via suffixing the primary key to the order', () => {
  const context = makeTestContext();
  type E1 = z.infer<typeof e1>;
  const q = new EntityQuery<{e1: E1}>(context, 'e1');

  const ascView = q.select('id').asc('n').prepare().view();
  const descView = q.select('id').desc('n').prepare().view();

  context.getSource<E1>('e1').add({
    id: 'a',
    n: 1,
  });
  context.getSource<E1>('e1').add({
    id: 'b',
    n: 1,
  });
  context.getSource<E1>('e1').add({
    id: 'c',
    n: 1,
  });
  expect(ascView.value).toEqual([
    {id: 'a', n: 1},
    {id: 'b', n: 1},
    {id: 'c', n: 1},
  ]);
  expect(descView.value).toEqual([
    {id: 'c', n: 1},
    {id: 'b', n: 1},
    {id: 'a', n: 1},
  ]);
});

test('makeComparator', () => {
  function makeObject<T extends Array<unknown>>(x: T) {
    const ret: Record<string, unknown> = {};
    for (let i = 0; i < x.length; i++) {
      ret['field' + i] = x[i];
    }
    return ret;
  }
  check([1, 2], [2, 3], -1);
  check([1, 'a'], [1, 'b'], -1);
  check([1, 'a'], [1, 'a'], 0);
  check([1, 'b'], [1, 'a'], 1);
  check([1, 2], [1, 3], -1);
  check([1, 2], [1, 2], 0);
  check([1, 3], [1, 2], 1);

  check([1], [2], -1);
  check([1], [1], 0);
  check([2], [1], 1);
  check(['a'], ['b'], -1);
  check(['a'], ['a'], 0);
  check(['b'], ['a'], 1);

  check([null], [null], 0);

  function check(values1: unknown[], values2: unknown[], expected: number) {
    expect(
      makeComparator<string[], Record<string, unknown>>(
        Array.from({length: values1.length}).map((_, i) => 'field' + i),
        'asc',
      )(makeObject(values1), makeObject(values2)),
    ).toBe(expected);
  }
});

test('destroying the statement stops updating the view', async () => {
  const context = makeTestContext();
  const q = new EntityQuery<{e1: E1}>(context, 'e1');

  const stmt = q.select('id', 'n').prepare();

  let callCount = 0;
  stmt.subscribe(_ => {
    ++callCount;
  });

  const items = [
    {id: 'a', n: 1},
    {id: 'b', n: 2},
    {id: 'c', n: 3},
  ] as const;

  context.getSource('e1').add(items[0]);
  expect(callCount).toBe(1);
  stmt.destroy();
  context.getSource('e1').add(items[1]);
  context.getSource('e1').add(items[2]);
  expect(callCount).toBe(1);
  expect(await stmt.exec()).toEqual([{id: 'a', n: 1}]);
});

//
// test:
// 1. non hydrated view and exec
// 2. hydrated and exec
