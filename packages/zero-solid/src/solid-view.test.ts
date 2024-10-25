import {expect, test} from 'vitest';
import {MemorySource} from '../../zql/src/zql/ivm/memory-source.js';
import {SolidView, solidViewFactory} from './solid-view.js';
import type {Query, Smash} from '../../zql/src/zql/query/query.js';

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

type TestSchema = {
  tableName: 'test';
  columns: {
    a: {type: 'number'};
    b: {type: 'string'};
  };
  primaryKey: ['a'];
  /* eslint-disable-next-line @typescript-eslint/ban-types */
  relationships: {};
};

type TestReturn = {
  row: {
    a: number;
    b: string;
  };
  /* eslint-disable-next-line @typescript-eslint/ban-types */
  related: {};
  singular: false;
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

  const view: SolidView<Smash<TestReturn>> = solidViewFactory(
    undefined as unknown as Query<TestSchema, TestReturn>,
    ms.connect([
      ['b', 'asc'],
      ['a', 'asc'],
    ]),
    {singular: false, relationships: {}},
    onDestroy,
  );
  expect(view).toBeDefined();
  expect(onDestroyCalled).false;
  view.destroy();
  expect(onDestroyCalled).true;
});
