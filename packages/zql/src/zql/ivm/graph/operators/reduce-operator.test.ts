import {expect, test} from 'vitest';
import {DifferenceStream} from '../difference-stream.js';

type Thing = {
  id: string;
  value: number;
  groupKey: string;
};

type Reduction = {
  id: string;
  sum: number;
};

test('collects all things with the same key', () => {
  const input = new DifferenceStream<Thing>();
  let version = 0;
  function getGroupKey(t: Thing) {
    return t.groupKey;
  }
  function getValueIdentity(t: Thing) {
    return t.id;
  }
  const output = input.reduce(
    getGroupKey,
    getValueIdentity,
    (group: Iterable<Thing>) => {
      let sum = 0;
      let id = '';
      for (const item of group) {
        id = item.groupKey;
        sum += item.value;
      }

      return {
        id,
        sum,
      };
    },
  );

  const items: [Reduction, number][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  input.newDifference(1, [
    [
      {
        id: 'a',
        value: 1,
        groupKey: 'x',
      },
      1,
    ],
    [
      {
        id: 'b',
        value: 2,
        groupKey: 'x',
      },
      2,
    ],
  ]);
  check([[{id: 'x', sum: 5}, 1]]);

  // retract an item
  input.newDifference(1, [
    [
      {
        id: 'a',
        value: 1,
        groupKey: 'x',
      },
      -1,
    ],
  ]);
  check([
    [{id: 'x', sum: 5}, -1],
    [{id: 'x', sum: 4}, 1],
  ]);

  // fully retract items that constitute a grouping
  input.newDifference(1, [
    [
      {
        id: 'b',
        value: 2,
        groupKey: 'x',
      },
      -2,
    ],
  ]);
  check([[{id: 'x', sum: 4}, -1]]);

  // add more entries
  input.newDifference(1, [
    [
      {
        id: 'a',
        value: 1,
        groupKey: 'c',
      },
      1,
    ],
  ]);
  check([[{id: 'c', sum: 1}, 1]]);
  input.newDifference(1, [
    [
      {
        id: 'b',
        value: 2,
        groupKey: 'c',
      },
      1,
    ],
  ]);
  check([
    [{id: 'c', sum: 1}, -1],
    [{id: 'c', sum: 3}, 1],
  ]);

  input.newDifference(1, [
    [
      {
        id: 'a',
        value: 1,
        groupKey: 'c',
      },
      -1,
    ],
    [
      {
        id: 'a',
        value: 2,
        groupKey: 'c',
      },
      1,
    ],
  ]);
  check([
    [{id: 'c', sum: 3}, -1],
    [{id: 'c', sum: 4}, 1],
  ]);

  function check(expected: [Reduction, number][]) {
    input.commit(++version);
    expect(items).toEqual(expected);
    items.length = 0;
  }
});
