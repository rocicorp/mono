import fc from 'fast-check';
import {expect, test} from 'vitest';
import type {Entity} from '../../../entity.js';
import {makeTestContext} from '../../context/context.js';
import {makeComparator} from '../../query/statement.js';
import {MutableTreeView} from './tree-view.js';

const numberComparator = (l: number, r: number) => l - r;

type Selected = {id: string};
test('asc and descComparator on Entities', () => {
  const context = makeTestContext();
  const {materialite} = context;
  const s = materialite.newSetSource<Entity>((l, r) =>
    l.id.localeCompare(r.id),
  );
  const orderBy = [['n', 'id'], 'asc'] as const;
  const view = new MutableTreeView<Selected>(
    context,
    {table: 'issue', orderBy},
    s.stream,
    // eh... the comparator operates on the base type rather than the mapped
    // type. So there's a disconnect between the type of the comparator and the
    // type of the view.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    makeComparator(['n', 'id'] as any, 'asc'),
    orderBy,
  );

  const orderBy2 = [['n', 'id'], 'desc'] as const;
  const descView = new MutableTreeView<Selected>(
    context,
    {table: 'issue', orderBy: orderBy2},
    s.stream,
    // see above for why this is any
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    makeComparator(['n', 'id'] as any, 'desc'),
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
      const source = materialite.newSetSource<{x: number}>((l, r) => l.x - r.x);
      const view = new MutableTreeView(
        context,
        {table: 'issue', orderBy: [['x'], 'asc']},
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
      const source = materialite.newSetSource<{x: number}>((l, r) => l.x - r.x);
      const orderBy = [['id'], 'asc'] as const;
      const view = new MutableTreeView(
        context,
        {table: 'issue', orderBy},
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
