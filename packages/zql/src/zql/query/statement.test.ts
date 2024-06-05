import {assert} from 'shared/src/asserts.js';
import {expect, test} from 'vitest';
import {z} from 'zod';
import {makeTestContext} from '../context/test-context.js';
import {makeComparator} from '../ivm/compare.js';
import {EntityQuery, astForTesting as ast} from './entity-query.js';

const e1 = z.object({
  id: z.string(),
  n: z.number(),
  optStr: z.string().optional(),
});

type E1 = z.infer<typeof e1>;

test('basic materialization', async () => {
  const context = makeTestContext();
  const q = new EntityQuery<{e1: E1}>(context, 'e1');

  const stmt = q.select('id', 'n').where('n', '>', 100).prepare();

  const calledWith: (readonly E1[])[] = [];
  stmt.subscribe(data => {
    calledWith.push(data);
  }, false);
  await new Promise(resolve => setTimeout(resolve, 0));

  const items = [
    {id: 'a', n: 1},
    {id: 'b', n: 101},
    {id: 'c', n: 102},
  ] as const;

  context.getSource('e1').add(items[0]);
  context.getSource('e1').add(items[1]);
  context.getSource('e1').add(items[2]);

  expect(calledWith).toEqual([
    [{id: 'b', n: 101}],
    [
      {id: 'b', n: 101},
      {id: 'c', n: 102},
    ],
  ]);
});

test('sorted materialization', async () => {
  const context = makeTestContext();
  type E1 = z.infer<typeof e1>;
  const q = new EntityQuery<{e1: E1}>(context, 'e1');
  const ascStatement = q.select('id').orderBy('n', 'asc').prepare();
  const descStatement = q.select('id').orderBy('n', 'desc').prepare();

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

  expect(await ascStatement.exec()).toEqual([
    {id: 'c', n: 1},
    {id: 'b', n: 2},
    {id: 'a', n: 3},
  ]);
  expect(await descStatement.exec()).toEqual([
    {id: 'a', n: 3},
    {id: 'b', n: 2},
    {id: 'c', n: 1},
  ]);
});

test('sorting is stable via suffixing the primary key to the order', async () => {
  const context = makeTestContext();
  type E1 = z.infer<typeof e1>;
  const q = new EntityQuery<{e1: E1}>(context, 'e1');

  const ascStatement = q.select('id').orderBy('n', 'asc').prepare();
  const descStatement = q.select('id').orderBy('n', 'desc').prepare();

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
  expect(await ascStatement.exec()).toEqual([
    {id: 'a', n: 1},
    {id: 'b', n: 1},
    {id: 'c', n: 1},
  ]);
  expect(await descStatement.exec()).toEqual([
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
      makeComparator<Record<string, unknown>>(
        Array.from({length: values1.length}).map((_, i) => [
          ['x', 'field' + i],
          'asc',
        ]),
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
  }, false);
  await new Promise(resolve => setTimeout(resolve, 0));

  const items = [
    {id: 'a', n: 1},
    {id: 'b', n: 2},
    {id: 'c', n: 3},
  ] as const;

  context.getSource('e1').add(items[0]);
  expect(callCount).toBe(1);
  stmt.destroy();
  await new Promise(resolve => setTimeout(resolve, 0));
  context.getSource('e1').add(items[1]);
  context.getSource('e1').add(items[2]);
  expect(callCount).toBe(1);
  expect(await stmt.exec()).toEqual([{id: 'a', n: 1}]);
});

test('ensure we get callbacks when subscribing and unsubscribing', async () => {
  const context = makeTestContext();
  const q = new EntityQuery<{e1: E1}>(context, 'e1').select('id', 'n');

  const statement = q.prepare();
  const unsubscribe = statement.subscribe(_ => {
    // noop
  });
  await new Promise(resolve => setTimeout(resolve, 0));

  expect(context.subscriptionsChangedLog.length).toEqual(1);
  expect(context.subscriptionsChangedLog[0]).to.deep.include({
    type: 'added',
    ast: {
      ...ast(q),
      orderBy: [[['e1', 'id'], 'asc']],
    },
  });

  context.subscriptionsChangedLog.length = 0;
  unsubscribe();
  await new Promise(resolve => setTimeout(resolve, 0));
  expect(context.subscriptionsChangedLog).toEqual([
    {
      type: 'removed',
      ast: {
        ...ast(q),
        orderBy: [[['e1', 'id'], 'asc']],
      },
    },
  ]);
});

test('preloaded resolves to true when subscription is got', async () => {
  const context = makeTestContext();
  const q = new EntityQuery<{e1: E1}>(context, 'e1').select('id', 'n');

  const statement = q.prepare();
  const {cleanup, preloaded} = statement.preload();
  let preloadedResolved = false;
  const chainedPreloaded = preloaded.then(value => {
    preloadedResolved = true;
    return value;
  });
  await new Promise(resolve => setTimeout(resolve, 0));
  expect(context.subscriptionsChangedLog.length).toEqual(1);
  expect(context.subscriptionsChangedLog[0]).to.deep.include({
    type: 'added',
    ast: {
      ...ast(q),
      orderBy: [[['e1', 'id'], 'asc']],
    },
  });

  expect(preloadedResolved).false;

  assert(context.subscriptionsChangedLog[0].type === 'added');
  const {gotCallback} = context.subscriptionsChangedLog[0];
  expect(gotCallback).toBeDefined();

  gotCallback?.(false);
  await new Promise(resolve => setTimeout(resolve, 0));
  expect(preloadedResolved).false;

  gotCallback?.(true);
  await new Promise(resolve => setTimeout(resolve, 0));
  expect(preloadedResolved).true;

  expect(await chainedPreloaded).true;

  context.subscriptionsChangedLog.length = 0;
  cleanup();
  await new Promise(resolve => setTimeout(resolve, 0));
  expect(context.subscriptionsChangedLog).toEqual([
    {
      type: 'removed',
      ast: {
        ...ast(q),
        orderBy: [[['e1', 'id'], 'asc']],
      },
    },
  ]);
});

test('preloaded resolves to false if preload is cleanedup before query is ever got', async () => {
  const context = makeTestContext();
  const q = new EntityQuery<{e1: E1}>(context, 'e1').select('id', 'n');

  const statement = q.prepare();
  const {cleanup, preloaded} = statement.preload();
  let preloadedResolved = false;
  const chainedPreloaded = preloaded.then(value => {
    preloadedResolved = true;
    return value;
  });
  await new Promise(resolve => setTimeout(resolve, 0));
  expect(context.subscriptionsChangedLog.length).toEqual(1);
  expect(context.subscriptionsChangedLog[0]).to.deep.include({
    type: 'added',
    ast: {
      ...ast(q),
      orderBy: [[['e1', 'id'], 'asc']],
    },
  });

  expect(preloadedResolved).false;

  assert(context.subscriptionsChangedLog[0].type === 'added');
  const {gotCallback} = context.subscriptionsChangedLog[0];
  expect(gotCallback).toBeDefined();

  context.subscriptionsChangedLog.length = 0;
  cleanup();
  await new Promise(resolve => setTimeout(resolve, 0));
  expect(context.subscriptionsChangedLog).toEqual([
    {
      type: 'removed',
      ast: {
        ...ast(q),
        orderBy: [[['e1', 'id'], 'asc']],
      },
    },
  ]);
  expect(preloadedResolved).true;

  expect(await chainedPreloaded).false;
});

//
// test:
// 1. non hydrated view and exec
// 2. hydrated and exec
