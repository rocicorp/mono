import fc from 'fast-check';
import {expect, test} from 'vitest';
import {makeTestContext} from '../../context/test-context.js';
import type {Entity} from '../../schema/entity-schema.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import {TreeView} from './tree-view.js';

const numberComparator = (l: number, r: number) => l - r;
const ordering = [[['x', 'id'], 'asc']] as const;

type Selected = {id: string};
test('asc and descComparator on Entities', () => {
  const context = makeTestContext();
  const {materialite} = context;
  const s = materialite.newSetSource<Entity>(ordering, 'x');
  const orderBy = [
    [['x', 'n'], 'asc'],
    [['x', 'id'], 'asc'],
  ] as const;
  const view = new TreeView<Selected>(context, s.stream, orderBy);

  const orderBy2 = [
    [['x', 'n'], 'desc'],
    [['x', 'id'], 'desc'],
  ] as const;
  const descView = new TreeView<Selected>(context, s.stream, orderBy2);

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
      const order = [[['test', 'x'], 'asc']] as const;
      const source = materialite.newSetSource<{x: number}>(order, 'test');
      const view = new TreeView(context, source.stream, order);

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
      const orderBy = [[['test', 'x'], 'asc']] as const;
      const source = materialite.newSetSource<{x: number}>(orderBy, 'test');
      const view = new TreeView(context, source.stream, orderBy);

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
  const orderBy = [[['test', 'id'], 'asc']] as const;
  const source = materialite.newSetSource<Item>(orderBy, 'test');
  const view = new TreeView(context, source.stream, orderBy, 5);

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

test('iterator passed to the view is correctly returned', () => {
  const context = makeTestContext();
  const orderBy = [[['test', 'x'], 'asc']] as const;

  const stream = new DifferenceStream<{x: number}>();

  const view = new TreeView(context, stream, orderBy, 2);

  const items = [1, 2, 3, 4, 5].map(x => ({x}));

  let returned = false;

  class Iterator {
    #i = 0;

    next() {
      if (this.#i === items.length) {
        return {done: true, value: undefined} as const;
      }
      return {done: false, value: [items[this.#i++], 1] as const} as const;
    }

    return() {
      returned = true;
      return {done: true, value: undefined} as const;
    }

    throw() {
      return {done: true, value: undefined} as const;
    }
  }

  view.pullHistoricalData();

  stream.newDifference(
    1,
    {
      [Symbol.iterator]: () => new Iterator(),
    },
    {
      replyingTo: 0,
      type: 'pullResponse',
      sourceName: 'test',
      contiguousGroup: [],
      order: orderBy,
    },
  );

  expect(view.value).toEqual([{x: 1}, {x: 2}]);
  expect(returned).toBe(true);
});
