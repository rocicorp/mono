import {describe, expect, test} from 'vitest';
import {Multiset, normalize} from './multiset.js';

describe('normalize', () => {
  test.each([
    {
      trial: [
        [1, 1],
        [1, -1],
      ],
      expected: [],
    },
    {
      trial: [[1, 1]],
      expected: [[1, 1]],
    },
    {
      trial: [
        [1, 3],
        [1, -1],
      ],
      expected: [[1, 2]],
    },
    {
      trial: [
        [1, 3],
        [1, -4],
      ],
      expected: [[1, -1]],
    },
    {
      trial: [
        [1, 1],
        [1, -1],
        [2, 2],
      ],
      expected: [[2, 2]],
    },
  ] satisfies {
    trial: Multiset<number>;
    expected: Multiset<number>;
  }[])('$trial', ({trial, expected}) => {
    expect([...normalize(trial, x => x)]).toEqual(expected);
  });
});
