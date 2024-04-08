import {expect, test} from 'vitest';
import {Materialite} from '../materialite.js';
import {applySelect, orderingProp} from '../../ast-to-ivm/pipeline-builder.js';
import {MutableTreeView} from './tree-view.js';
import {ascComparator, descComparator} from '../../query/statement.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import fc from 'fast-check';
import {Primitive} from '../../ast/ast.js';
import {Entity} from '../../../generate.js';

const numberComparator = (l: number, r: number) => l - r;

type Selected = {id: string; [orderingProp]: Primitive[]};
test('asc and descComparator on Entities', () => {
  const m = new Materialite();
  const s = m.newSetSource<Entity>((l, r) => l.id.localeCompare(r.id));

  const updatedStream = applySelect(
    s.stream,
    ['id'],
    [['n', 'id'], 'asc'],
  ) as unknown as DifferenceStream<Selected>;

  const view = new MutableTreeView<Selected>(m, updatedStream, ascComparator, [
    ['n', 'id'],
    'asc',
  ]);
  const descView = new MutableTreeView<Selected>(
    m,
    applySelect(
      s.stream as unknown as DifferenceStream<Entity>,
      ['id'],
      [['n', 'id'], 'desc'],
    ) as unknown as DifferenceStream<Selected>,
    descComparator,
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

  expect(view.value).toEqual([{id: 'a'}, {id: 'b'}, {id: 'c'}]);
  expect(descView.value).toEqual([{id: 'c'}, {id: 'b'}, {id: 'a'}]);
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
