import {assert} from 'shared/src/asserts.js';
import {must} from 'shared/src/must.js';
import {describe, expect, test} from 'vitest';
import {Materialite} from '../ivm/materialite.js';
import type {SetSource} from '../ivm/source/set-source.js';
import {AddWatch, WatchCallback, ZeroContext} from './zero-context.js';

type E1 = {
  id: string;
  str: string;
  optStr?: string | undefined;
};

test('getSource - no ordering', () => {
  const materialite = new Materialite();
  let callback: WatchCallback | undefined;
  const addWatch: AddWatch = (name, cb) => {
    expect(name).toBe('e1');
    callback = cb;
  };

  const context = new ZeroContext(materialite, addWatch, {
    subscriptionAdded() {},
    subscriptionRemoved() {},
  });
  const source = context.getSource('e1', undefined);
  expect(source).toBeDefined();
  expect(callback).toBeDefined();
  assert(callback);

  callback([
    {op: 'add', key: 'e1/1', newValue: {id: '1', str: 'a'}},
    {op: 'add', key: 'e1/2', newValue: {id: '2', str: 'a'}},
    {op: 'add', key: 'e1/3', newValue: {id: '3', str: 'a'}},
  ]);

  // source is ordered by id
  expect([...(source as unknown as SetSource<E1>).value]).toEqual([
    {id: '1', str: 'a'},
    {id: '2', str: 'a'},
    {id: '3', str: 'a'},
  ]);

  callback([{op: 'del', key: 'e1/1', oldValue: {id: '1', str: 'a'}}]);

  expect([...(source as unknown as SetSource<E1>).value]).toEqual([
    {id: '2', str: 'a'},
    {id: '3', str: 'a'},
  ]);

  callback([
    {
      op: 'change',
      key: 'e1/3',
      oldValue: {id: '3', str: 'a'},
      newValue: {id: '3', str: 'z'},
    },
  ]);

  expect([...(source as unknown as SetSource<E1>).value]).toEqual([
    {id: '2', str: 'a'},
    {id: '3', str: 'z'},
  ]);
});

describe('sort source by some alternate ordering', () => {
  const materialite = new Materialite();
  let callback: WatchCallback | undefined;
  const addWatch: AddWatch = (name, cb) => {
    expect(name).toBe('e1');
    callback = cb;
  };

  const context = new ZeroContext(materialite, addWatch, {
    subscriptionAdded() {},
    subscriptionRemoved() {},
  });
  const source = context.getSource('e1', undefined);
  expect(source).toBeDefined();
  expect(callback).toBeDefined();
  assert(callback);

  callback([
    {op: 'add', key: 'e1/1', newValue: {id: '1', str: 'c'}},
    {op: 'add', key: 'e1/2', newValue: {id: '2', str: 'b'}},
    {op: 'add', key: 'e1/3', newValue: {id: '3', str: 'a'}},
  ]);

  test('it gets populated with the same data the initial source had and is in the new ordering', () => {
    const alternateSource = context.getSource('e1', [['str', 'id'], 'asc']);
    expect([...(alternateSource as unknown as SetSource<E1>).value]).toEqual([
      {id: '3', str: 'a'},
      {id: '2', str: 'b'},
      {id: '1', str: 'c'},
    ]);
  });

  test('once a sort is created, that same sort is returned', () => {
    expect(context.getSource('e1', [['str', 'id'], 'asc'])).toBe(
      context.getSource('e1', [['str', 'id'], 'asc']),
    );
    expect(context.getSource('e1', undefined)).toBe(
      context.getSource('e1', undefined),
    );
  });

  test('it is maintained as the source changes', () => {
    const alternateSource = context.getSource('e1', [['str', 'id'], 'asc']);

    must(callback)([
      {op: 'add', key: 'e1/4', newValue: {id: '4', str: 'd'}},
      {op: 'add', key: 'e1/5', newValue: {id: '5', str: 'e'}},
    ]);

    expect([...(alternateSource as unknown as SetSource<E1>).value]).toEqual([
      {id: '3', str: 'a'},
      {id: '2', str: 'b'},
      {id: '1', str: 'c'},
      {id: '4', str: 'd'},
      {id: '5', str: 'e'},
    ]);
  });
});
