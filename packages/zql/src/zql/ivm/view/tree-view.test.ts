import fc from 'fast-check';
import {expect, test} from 'vitest';
import {makeTestContext} from '../../context/test-context.js';
import {makeComparator} from '../../query/statement.js';
import type {Entity} from '../../schema/entity-schema.js';
import type {Comparator} from '../types.js';
import {TreeView} from './tree-view.js';

const numberComparator = (l: number, r: number) => l - r;
const ordering = [[['x', 'id']], 'asc'] as const;

type Selected = {id: string};
test('asc and descComparator on Entities', () => {
  const context = makeTestContext();
  const {materialite} = context;
  const s = materialite.newSetSource<Entity>(
    (l, r) => l.id.localeCompare(r.id),
    ordering,
    'x',
  );
  const orderBy = [
    [
      ['x', 'n'],
      ['x', 'id'],
    ],
    'asc',
  ] as const;
  const view = new TreeView<Selected>(
    context,
    s.stream,
    makeComparator(
      [
        ['x', 'n'],
        ['x', 'id'],
      ],
      'asc',
    ),
    orderBy,
  );

  const orderBy2 = [
    [
      ['x', 'n'],
      ['x', 'id'],
    ],
    'desc',
  ] as const;
  const descView = new TreeView<Selected>(
    context,
    s.stream,
    makeComparator(
      [
        ['x', 'n'],
        ['x', 'id'],
      ],
      'desc',
    ),
    orderBy2,
  );

  const items = [
    {id: 'a', n: 1},
    {id: 'b', n: 1},
    {id: 'c', n: 1},
  ] as const;

  s.add(items[0]);
  s.add(items[1]);
  s.add(items[2]);

  expect(view.value).toEqual([
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

test('add & remove', () => {
  fc.assert(
    fc.property(fc.uniqueArray(fc.integer()), arr => {
      const context = makeTestContext();
      const {materialite} = context;
      const source = materialite.newSetSource<{x: number}>(
        (l, r) => l.x - r.x,
        [[['test', 'x']], 'asc'] as const,
        'test',
      );
      const view = new TreeView(
        context,
        source.stream,
        (l, r) => l.x - r.x,
        undefined,
      );

      materialite.tx(() => {
        arr.forEach(x => source.add({x}));
      });
      expect(view.value).toEqual(arr.sort(numberComparator).map(x => ({x})));

      materialite.tx(() => {
        arr.forEach(x => source.delete({x}));
      });
      expect(view.value).toEqual([]);
    }),
  );
});

test('replace', () => {
  fc.assert(
    fc.property(fc.uniqueArray(fc.integer()), arr => {
      const context = makeTestContext();
      const {materialite} = context;
      const orderBy = [[['test', 'x']], 'asc'] as const;
      const source = materialite.newSetSource<{x: number}>(
        (l, r) => l.x - r.x,
        orderBy,
        'test',
      );
      const view = new TreeView(
        context,
        source.stream,
        (l, r) => l.x - r.x,
        orderBy,
      );

      materialite.tx(() => {
        arr.forEach(x => source.add({x}));
      });
      expect(view.value).toEqual(arr.sort(numberComparator).map(x => ({x})));
      materialite.tx(() => {
        arr.forEach(x => {
          // We have special affordances for deletes immediately followed by adds
          // As those are really replaces / updates.
          // Check that the source handles this correctly.
          source.delete({x});
          source.add({x});
        });
      });
      expect(view.value).toEqual(arr.sort(numberComparator).map(x => ({x})));

      materialite.tx(() => {
        arr.forEach(x => source.delete({x}));
      });
      expect(view.value).toEqual([]);
    }),
  );
});

test('replace outside viewport', () => {
  type Item = {id: number; s: string};
  const context = makeTestContext();
  const {materialite} = context;
  const orderBy = [[['test', 'x']], 'asc'] as const;
  const comparator: Comparator<Item> = (l, r) => l.id - r.id;
  const source = materialite.newSetSource<Item>(comparator, orderBy, 'test');
  const view = new TreeView(context, source.stream, comparator, orderBy, 5);

  materialite.tx(() => {
    for (let i = 0; i < 5; i++) {
      source.add({id: i, s: String(i)});
    }
  });
  expect(view.value).toEqual([
    {id: 0, s: '0'},
    {id: 1, s: '1'},
    {id: 2, s: '2'},
    {id: 3, s: '3'},
    {id: 4, s: '4'},
  ]);

  // add outside of viewport
  materialite.tx(() => {
    source.add({id: 10, s: '10'});
  });
  expect(view.value).toEqual([
    {id: 0, s: '0'},
    {id: 1, s: '1'},
    {id: 2, s: '2'},
    {id: 3, s: '3'},
    {id: 4, s: '4'},
  ]);

  // change outside of viewport
  materialite.tx(() => {
    source.delete({id: 10, s: '10'});
    source.add({id: 10, s: '11'});
  });
  expect(view.value).toEqual([
    {id: 0, s: '0'},
    {id: 1, s: '1'},
    {id: 2, s: '2'},
    {id: 3, s: '3'},
    {id: 4, s: '4'},
  ]);
});
