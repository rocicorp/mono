import fc from 'fast-check';
import {expect, test} from 'vitest';
import type {Entity} from '../../../entity.js';
import {makeTestContext} from '../../context/test-context.js';
import {makeComparator} from '../../query/statement.js';
import {TreeView} from './tree-view.js';

const numberComparator = (l: number, r: number) => l - r;
const ordering = [['id'], 'asc'] as const;

type Selected = {id: string};
test('asc and descComparator on Entities', () => {
  const context = makeTestContext();
  const {materialite} = context;
  const s = materialite.newSetSource<Entity>(
    (l, r) => l.id.localeCompare(r.id),
    ordering,
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

test('add & remove', async () => {
  await fc.assert(
    fc.asyncProperty(fc.uniqueArray(fc.integer()), async arr => {
      const context = makeTestContext();
      const {materialite} = context;
      const source = materialite.newSetSource<{x: number}>(
        (l, r) => l.x - r.x,
        [['x'], 'asc'] as const,
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
      await Promise.resolve();
      expect(view.value).toEqual(arr.sort(numberComparator).map(x => ({x})));

      materialite.tx(() => {
        arr.forEach(x => source.delete({x}));
      });
      await Promise.resolve();
      expect(view.value).toEqual([]);
    }),
  );
});

test('replace', async () => {
  await fc.assert(
    fc.asyncProperty(fc.uniqueArray(fc.integer()), async arr => {
      const context = makeTestContext();
      const {materialite} = context;
      const orderBy = [[['x', 'id']], 'asc'] as const;
      const source = materialite.newSetSource<{x: number}>(
        (l, r) => l.x - r.x,
        orderBy,
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
      await Promise.resolve();
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
      await Promise.resolve();
      expect(view.value).toEqual(arr.sort(numberComparator).map(x => ({x})));

      materialite.tx(() => {
        arr.forEach(x => source.delete({x}));
      });
      await Promise.resolve();
      expect(view.value).toEqual([]);
    }),
  );
});
