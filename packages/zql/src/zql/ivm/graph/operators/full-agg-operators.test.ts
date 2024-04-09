import {expect, test} from 'vitest';
import {DifferenceStream} from '../difference-stream.js';

test('count', () => {
  const input = new DifferenceStream<{x: string}>();
  const output = input.count('count');
  const items: [{x: string}, number][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  // does not count things that do not exist
  input.newDifference(1, [
    [
      {
        x: 'foo',
      },
      0,
    ],
  ]);
  check(1, [[{x: 'foo', count: 0}, 1]]);

  // counts multiplicity of 1
  input.newDifference(2, [
    [
      {
        x: 'foo',
      },
      1,
    ],
  ]);
  check(2, [
    [{x: 'foo', count: 0}, -1],
    [{x: 'foo', count: 1}, 1],
  ]);

  // decrements if an item is removed
  input.newDifference(3, [
    [
      {
        x: 'foo',
      },
      -1,
    ],
  ]);
  check(3, [
    [{x: 'foo', count: 1}, -1],
    [{x: 'foo', count: 0}, 1],
  ]);

  // double counts doubly present items
  input.newDifference(4, [
    [
      {
        x: 'foo',
      },
      2,
    ],
  ]);
  check(4, [
    [{x: 'foo', count: 0}, -1],
    [{x: 'foo', count: 2}, 1],
  ]);

  function check(
    version: number,
    expected: [{x: string; count: number}, number][],
  ) {
    items.length = 0;
    input.commit(version);
    expect(items).toEqual(expected);
  }
});

test('average', () => {
  const input = new DifferenceStream<{x: number}>();
  const output = input.average('x', 'x');
  const items: [{x: number}, number][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  // does not avg things that do not exist
  input.newDifference(1, [
    [
      {
        x: 1,
      },
      0,
    ],
  ]);
  check(1, [[{x: 0}, 1]]);

  // averages things that exist
  input.newDifference(1, [
    [
      {
        x: 1,
      },
      1,
    ],
    [
      {
        x: 2,
      },
      1,
    ],
    [
      {
        x: 3,
      },
      1,
    ],
  ]);
  check(2, [
    [{x: 0}, -1],
    [{x: 2}, 1],
  ]);

  // updates the average when new items enter
  input.newDifference(1, [
    [
      {
        x: 4,
      },
      1,
    ],
    [
      {
        x: 5,
      },
      1,
    ],
  ]);
  check(3, [
    [{x: 2}, -1],
    [{x: 3}, 1],
  ]);

  // updates the average when items leave
  input.newDifference(1, [
    [
      {
        x: 4,
      },
      -1,
    ],
    [
      {
        x: 5,
      },
      -1,
    ],
  ]);
  check(4, [
    [{x: 3}, -1],
    [{x: 2}, 1],
  ]);

  function check(version: number, expected: [{x: number}, number][]) {
    items.length = 0;
    input.commit(version);
    expect(items).toEqual(expected);
  }
});

test('sum', () => {
  const input = new DifferenceStream<{x: number}>();
  const output = input.sum('x', 'x');
  const items: [{x: number}, number][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  // does not sum things that do not exist
  input.newDifference(1, [
    [
      {
        x: 1,
      },
      0,
    ],
  ]);
  check(1, [[{x: 0}, 1]]);

  // sums things that exist
  input.newDifference(1, [
    [
      {
        x: 1,
      },
      1,
    ],
    [
      {
        x: 2,
      },
      1,
    ],
    [
      {
        x: 3,
      },
      1,
    ],
  ]);
  check(2, [
    [{x: 0}, -1],
    [{x: 6}, 1],
  ]);

  // updates the sum when new items enter
  input.newDifference(1, [
    [
      {
        x: 4,
      },
      1,
    ],
    [
      {
        x: 5,
      },
      1,
    ],
  ]);
  check(3, [
    [{x: 6}, -1],
    [{x: 15}, 1],
  ]);

  // updates the sum when items leave
  input.newDifference(1, [
    [
      {
        x: 4,
      },
      -1,
    ],
    [
      {
        x: 5,
      },
      -1,
    ],
  ]);
  check(4, [
    [{x: 15}, -1],
    [{x: 6}, 1],
  ]);

  function check(version: number, expected: [{x: number}, number][]) {
    items.length = 0;
    input.commit(version);
    expect(items).toEqual(expected);
  }
});
