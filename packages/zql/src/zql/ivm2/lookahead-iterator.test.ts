import {expect, test} from 'vitest';
import {LookaheadIterator} from './lookahead-iterator.js';

test('basics', () => {
  const cases: {
    input: number[];
    size: number;
    expected: (number | undefined)[][];
  }[] = [
    {
      input: [],
      size: 2,
      expected: [],
    },
    {
      input: [1],
      size: 2,
      expected: [[1, undefined]],
    },
    {
      input: [1, 2],
      size: 2,
      expected: [
        [1, 2],
        [2, undefined],
      ],
    },
    {
      input: [1, 2, 3],
      size: 2,
      expected: [
        [1, 2],
        [2, 3],
        [3, undefined],
      ],
    },
    {
      input: [1, 2, 3, 4],
      size: 2,
      expected: [
        [1, 2],
        [2, 3],
        [3, 4],
        [4, undefined],
      ],
    },
    {
      input: [],
      size: 3,
      expected: [],
    },
    {
      input: [1],
      size: 3,
      expected: [[1, undefined, undefined]],
    },
    {
      input: [1, 2],
      size: 3,
      expected: [
        [1, 2, undefined],
        [2, undefined, undefined],
      ],
    },
    {
      input: [1, 2, 3],
      size: 3,
      expected: [
        [1, 2, 3],
        [2, 3, undefined],
        [3, undefined, undefined],
      ],
    },
    {
      input: [1, 2, 3, 4],
      size: 3,
      expected: [
        [1, 2, 3],
        [2, 3, 4],
        [3, 4, undefined],
        [4, undefined, undefined],
      ],
    },
  ];

  for (const {input, size, expected} of cases) {
    const cursor = new LookaheadIterator(input[Symbol.iterator](), size);
    const actual: (number | undefined)[][] = [];
    for (const [curr, ...next] of cursor) {
      actual.push([curr, ...next]);
    }
    expect(actual, JSON.stringify({input, size, expected})).toEqual(expected);
  }
});
