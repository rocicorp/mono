import {expect, test} from 'vitest';
import {Materialite} from '../materialite.js';
import {MutableTreeView} from './tree-view.js';
import fc from 'fast-check';
import type {Entity} from '../../../entity.js';
import {makeComparator} from '../../query/statement.js';

const numberComparator = (l: number, r: number) => l - r;

type Selected = {id: string};
test('asc and descComparator on Entities', () => {
  const m = new Materialite();
  const s = m.newSetSource<Entity>((l, r) => l.id.localeCompare(r.id));

  const view = new MutableTreeView<Selected>(
    m,
    s.stream,
    // eh... the comparator operates on the base type rather than the mapped
    // type. So there's a disconnect between the type of the comparator and the
    // type of the view.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    makeComparator(['n', 'id'] as any, 'asc'),
    [['n', 'id'], 'asc'],
  );

  const descView = new MutableTreeView<Selected>(
    m,
    s.stream,
    // see above for why this is any
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    makeComparator(['n', 'id'] as any, 'desc'),
    [['n', 'id'], 'desc'],
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
      const m = new Materialite();
      const source = m.newSetSource<{x: number}>((l, r) => l.x - r.x);
      const view = new MutableTreeView(
        m,
        source.stream,
        (l, r) => l.x - r.x,
        undefined,
      );

      m.tx(() => {
        arr.forEach(x => source.add({x}));
      });
      expect(view.value).toEqual(arr.sort(numberComparator).map(x => ({x})));

      m.tx(() => {
        arr.forEach(x => source.delete({x}));
      });
      expect(view.value).toEqual([]);
    }),
  );
});

test('replace', () => {
  fc.assert(
    fc.property(fc.uniqueArray(fc.integer()), arr => {
      const m = new Materialite();
      const source = m.newSetSource<{x: number}>((l, r) => l.x - r.x);
      const view = new MutableTreeView(m, source.stream, (l, r) => l.x - r.x, [
        ['id'],
        'asc',
      ]);

      m.tx(() => {
        arr.forEach(x => source.add({x}));
      });
      expect(view.value).toEqual(arr.sort(numberComparator).map(x => ({x})));
      m.tx(() => {
        arr.forEach(x => {
          // We have special affordances for deletes immediately followed by adds
          // As those are really replaces / updates.
          // Check that the source handles this correctly.
          source.delete({x});
          source.add({x});
        });
      });
      expect(view.value).toEqual(arr.sort(numberComparator).map(x => ({x})));

      m.tx(() => {
        arr.forEach(x => source.delete({x}));
      });
      expect(view.value).toEqual([]);
    }),
  );
});
