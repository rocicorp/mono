import {resolver} from '@rocicorp/resolver';
import {expect, test, vi} from 'vitest';
import {number, string, table} from '../../zero-client/src/mod.ts';
import {createSchema} from '../../zero-schema/src/mod.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import type {HumanReadable, Query} from '../../zql/src/query/query.ts';
import {SolidView, solidViewFactory} from './solid-view.ts';

test('basics', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});
  ms.push({row: {a: 2, b: 'b'}, type: 'add'});

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };
  const format = {singular: false, relationships: {}};
  const onDestroy = () => {};
  const queryComplete = true;

  const view = new SolidView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    onTransactionCommit,
    format,
    onDestroy,
    queryComplete,
  );

  expect(view.data).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
  ]);

  expect(view.resultDetails).toEqual({type: 'complete'});

  ms.push({row: {a: 3, b: 'c'}, type: 'add'});
  commit();

  expect(view.data).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
    {a: 3, b: 'c'},
  ]);

  ms.push({row: {a: 2, b: 'b'}, type: 'remove'});
  ms.push({row: {a: 1, b: 'a'}, type: 'remove'});
  commit();

  expect(view.data).toEqual([{a: 3, b: 'c'}]);

  ms.push({row: {a: 3, b: 'c'}, type: 'remove'});
  commit();

  expect(view.data).toEqual([]);
});

test('single-format', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});

  let commit: () => void = () => {};
  const onTransactionCommit = (cb: () => void): void => {
    commit = cb;
  };

  const view = new SolidView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    onTransactionCommit,
    {singular: true, relationships: {}},
    () => {},
    true,
  );

  expect(view.data).toEqual({a: 1, b: 'a'});

  // trying to add another element should be an error
  // pipeline should have been configured with a limit of one
  expect(() => {
    ms.push({row: {a: 2, b: 'b'}, type: 'add'});
    commit();
  }).toThrow('single output already exists');

  ms.push({row: {a: 1, b: 'a'}, type: 'remove'});
  commit();

  expect(view.data).toEqual(undefined);
});

test('queryComplete promise', async () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});
  ms.push({row: {a: 2, b: 'b'}, type: 'add'});

  const queryCompleteResolver = resolver<true>();

  const onTransactionCommit = () => {};

  const view = new SolidView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    onTransactionCommit,
    {singular: false, relationships: {}},
    () => {},
    queryCompleteResolver.promise,
  );

  expect(view.data).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
  ]);

  expect(view.resultDetails).toEqual({type: 'unknown'});

  queryCompleteResolver.resolve(true);
  await 1;
  expect(view.resultDetails).toEqual({type: 'complete'});
});

const schema = createSchema(1, {
  tables: [
    table('test')
      .columns({
        a: number(),
        b: string(),
      })
      .primaryKey('a'),
  ],
});

type TestReturn = {
  a: number;
  b: string;
};

test('factory', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});
  ms.push({row: {a: 2, b: 'b'}, type: 'add'});

  const onDestroy = vi.fn();
  const onTransactionCommit = vi.fn();

  const view: SolidView<HumanReadable<TestReturn>> = solidViewFactory(
    undefined as unknown as Query<typeof schema, 'test', TestReturn>,
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    {singular: false, relationships: {}},
    onDestroy,
    onTransactionCommit,
    true,
  );

  expect(onTransactionCommit).toHaveBeenCalledTimes(1);
  expect(view).toBeDefined();
  expect(onDestroy).not.toHaveBeenCalled();
  view.destroy();
  expect(onDestroy).toHaveBeenCalledTimes(1);
});
