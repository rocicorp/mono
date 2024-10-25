import {expect, test} from 'vitest';
import {MemorySource} from '../../zql/src/zql/ivm/memory-source.js';
import {SolidView} from './solid-view.js';

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

  expect(view.data).toEqual([{a: 3, b: 'c'}]);
});
