import {expect, test} from 'vitest';
import {z} from 'zod';
import {orderingProp} from '../ast-to-ivm/pipeline-builder.js';
import {makeTestContext} from '../context/context.js';
import {EntityQuery} from './entity-query.js';
import {ascComparator} from './statement.js';

const e1 = z.object({
  id: z.string(),
  n: z.number(),
  optStr: z.string().optional(),
});
type E1 = z.infer<typeof e1>;
test('basic materialization', () => {
  const context = makeTestContext();
  const q = new EntityQuery<E1>(context, 'e1');

  const stmt = q.select('id', 'n').where('n', '>', 100).prepare();

  let callCount = 0;
  stmt.subscribe(data => {
    ++callCount;
    expect(data).toEqual(expected);
  });

  const items = [
    {id: 'a', n: 1},
    {id: 'b', n: 101},
    {id: 'c', n: 102},
  ] as const;
  const expected: E1[] = [];

  context.getSource('e1').add(items[0]);

  expected.push(items[1]);
  context.getSource('e1').add(items[1]);

  expected.push(items[2]);
  context.getSource('e1').add(items[2]);
  expect(callCount).toBe(2);
});

test('sorted materialization', () => {
  const context = makeTestContext();
  type E1 = z.infer<typeof e1>;
  const q = new EntityQuery<E1>(context, 'e1');
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

  expect(ascView.value).toEqual([{id: 'c'}, {id: 'b'}, {id: 'a'}]);
  expect(descView.value).toEqual([{id: 'a'}, {id: 'b'}, {id: 'c'}]);
});

test('sorting is stable via suffixing the primary key to the order', () => {
  const context = makeTestContext();
  type E1 = z.infer<typeof e1>;
  const q = new EntityQuery<E1>(context, 'e1');

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
  expect(ascView.value).toEqual([{id: 'a'}, {id: 'b'}, {id: 'c'}]);
  expect(descView.value).toEqual([{id: 'c'}, {id: 'b'}, {id: 'a'}]);
});

test('ascComparator', () => {
  function make<T extends Array<unknown>>(x: T) {
    return {[orderingProp]: x};
  }
  expect(ascComparator(make([1, 2]), make([2, 3]))).toBeLessThan(0);
  expect(ascComparator(make([1, 'a']), make([1, 'b']))).toBeLessThan(0);
  expect(ascComparator(make([1, 'a']), make([1, 'a']))).toBe(0);
  expect(ascComparator(make([1, 'b']), make([1, 'a']))).toBeGreaterThan(0);
  expect(ascComparator(make([1, 2]), make([1, 3]))).toBeLessThan(0);
  expect(ascComparator(make([1, 2]), make([1, 2]))).toBe(0);
  expect(ascComparator(make([1, 3]), make([1, 2]))).toBeGreaterThan(0);
  // no imbalance allowed
  expect(() => ascComparator(make([1, 2]), make([1, 2, 3]))).toThrow();
  expect(() => ascComparator(make([1, 2, 3]), make([1, 2]))).toThrow();

  expect(ascComparator(make([1]), make([2]))).toBeLessThan(0);
  expect(ascComparator(make([1]), make([1]))).toBe(0);
  expect(ascComparator(make([2]), make([1]))).toBeGreaterThan(0);
  expect(ascComparator(make(['a']), make(['b']))).toBeLessThan(0);
  expect(ascComparator(make(['a']), make(['a']))).toBe(0);
  expect(ascComparator(make(['b']), make(['a']))).toBeGreaterThan(0);

  expect(ascComparator(make([null]), make([null]))).toBe(0);
});

test('destroying the statement stops updating the view', async () => {
  const context = makeTestContext();
  const q = new EntityQuery<E1>(context, 'e1');

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
