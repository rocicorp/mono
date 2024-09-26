import {expect} from 'chai';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import {computeSplices, type Splice} from './splice.js';

type Entry<V> = readonly [string, V];

test('splice', () => {
  const t = <T extends ReadonlyJSONValue>(
    previous: Entry<T>[],
    current: Entry<T>[],
    expected: Splice[],
  ) => {
    expect([...computeSplices(previous, current)]).to.deep.equal(expected);
  };

  t([['a', 0]], [['a', 0]], []);
  t([['a', 0]], [['a', 1]], [[0, 1, 1, 0]]);
  t([['a', 0]], [], [[0, 1, 0, 0]]);
  t([], [['a', 0]], [[0, 0, 1, 0]]);

  t([['a', 0]], [['b', 1]], [[0, 1, 1, 0]]);

  t(
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
      ['d', 3],
      ['e', 4],
    ],
    [
      ['a', 0],
      ['b', 1],
      ['c', 22],
      ['d', 3],
      ['e', 4],
    ],
    [[2, 1, 1, 2]],
  );

  t(
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
      ['d', 3],
      ['e', 4],
    ],
    [
      ['a', 0],
      ['b', 1],
      ['d', 3],
      ['e', 4],
    ],
    [[2, 1, 0, 0]],
  );

  t(
    [
      ['a', 0],
      ['b', 1],
      ['d', 3],
      ['e', 4],
    ],
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
      ['d', 3],
      ['e', 4],
    ],
    [[2, 0, 1, 2]],
  );

  t(
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    [
      ['a', 0],
      ['b', 1],
    ],
    [[2, 1, 0, 0]],
  );

  t(
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    [
      ['b', 1],
      ['c', 2],
    ],
    [[0, 1, 0, 0]],
  );

  t(
    [
      ['a', 0],
      ['b', 1],
    ],
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    [[2, 0, 1, 2]],
  );

  t(
    [
      ['b', 1],
      ['c', 2],
    ],
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    [[0, 0, 1, 0]],
  );

  t(
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    [
      ['a', 10],
      ['b', 11],
      ['c', 12],
    ],
    [[0, 3, 3, 0]],
  );

  t(
    [['b', 1]],
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    [
      [0, 0, 1, 0],
      [1, 0, 1, 2],
    ],
  );

  t(
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    [['b', 1]],
    [
      [0, 1, 0, 0],
      [2, 1, 0, 0],
    ],
  );

  t(
    [
      ['b', 1],
      ['d', 3],
    ],
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    [
      [0, 0, 1, 0],
      [1, 1, 1, 2],
    ],
  );

  t(
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    [
      ['b', 1],
      ['d', 3],
    ],
    [
      [0, 1, 0, 0],
      [2, 1, 1, 1],
    ],
  );

  t(
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    [
      ['b', 1],
      ['d', 3],
      ['e', 4],
    ],
    [
      [0, 1, 0, 0],
      [2, 1, 2, 1],
    ],
  );

  t(
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    [
      ['b', 3],
      ['c', 4],
    ],
    [[0, 3, 2, 0]],
  );

  // Test with objects to ensure deep equality is used.
  t(
    [
      ['a', {x: 0}],
      ['b', {x: 1}],
      ['c', {x: 2}],
    ],
    [
      ['b', {x: 1}],
      ['d', {x: 3}],
      ['e', {x: 4}],
    ],
    [
      [0, 1, 0, 0],
      [2, 1, 2, 1],
    ],
  );
});

test('splice roundtrip', () => {
  const t = <T>(
    arr: readonly Entry<T>[],
    start: number,
    deleteCount: number,
    ...items: readonly Entry<T>[]
  ) => {
    const current = arr.slice();
    current.splice(start, deleteCount, ...items);
    const res = [...computeSplices(arr, current)];
    const expected: Splice[] = [
      [start, deleteCount, items.length, items.length === 0 ? 0 : start],
    ];
    expect(res).to.deep.equal(expected);
  };

  t(
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    1,
    1,
  );
  t(
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    1,
    1,
    ['b', 3],
  );
  t(
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    1,
    1,
    ['b', 3],
    ['b2', 33],
  );
  t(
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    1,
    0,
    ['a2', 3],
  );
  t(
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
    ],
    1,
    0,
    ['a2', 3],
    ['a3', 4],
  );
});
