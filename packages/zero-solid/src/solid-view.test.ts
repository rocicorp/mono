import {resolver} from '@rocicorp/resolver';
import {expect, test} from 'vitest';
import {MemorySource} from '../../zql/src/ivm/memory-source.js';
import type {HumanReadable, Query} from '../../zql/src/query/query.js';
import {SolidView, solidViewFactory} from './solid-view.js';
import {createSchema} from '../../zero-schema/src/mod.js';
import {number, string, table} from '../../zero-client/src/mod.js';

test('basics', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});
  ms.push({row: {a: 2, b: 'b'}, type: 'add'});

  const view = new SolidView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
  );

  expect(view.data).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
  ]);

  expect(view.resultDetails).toEqual({type: 'complete'});

  ms.push({row: {a: 3, b: 'c'}, type: 'add'});

  expect(view.data).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
    {a: 3, b: 'c'},
  ]);

  ms.push({row: {a: 2, b: 'b'}, type: 'remove'});
  ms.push({row: {a: 1, b: 'a'}, type: 'remove'});

  expect(view.data).toEqual([{a: 3, b: 'c'}]);

  ms.push({row: {a: 3, b: 'c'}, type: 'remove'});

  expect(view.data).toEqual([]);
});

test('single-format', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});

  const view = new SolidView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    {singular: true, relationships: {}},
  );

  expect(view.data).toEqual({a: 1, b: 'a'});

  // trying to add another element should be an error
  // pipeline should have been configured with a limit of one
  expect(() => ms.push({row: {a: 2, b: 'b'}, type: 'add'})).toThrow(
    'single output already exists',
  );

  ms.push({row: {a: 1, b: 'a'}, type: 'remove'});

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

  const view = new SolidView(
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    undefined,
    undefined,
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

  let onDestroyCalled = false;
  const onDestroy = () => {
    onDestroyCalled = true;
  };

  const view: SolidView<HumanReadable<TestReturn>> = solidViewFactory(
    undefined as unknown as Query<typeof schema, 'test', TestReturn>,
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    {singular: false, relationships: {}},
    onDestroy,
    () => undefined,
    true,
  );
  expect(view).toBeDefined();
  expect(onDestroyCalled).false;
  view.destroy();
  expect(onDestroyCalled).true;
});
