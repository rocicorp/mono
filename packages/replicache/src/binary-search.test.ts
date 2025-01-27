import {expect, test} from 'vitest';
import {binarySearch} from './binary-search.ts';

test('binarySearch', () => {
  // expect(binarySearch(0, () => -1)).to.equal(0);

  const t = (needle: number, haystack: number[], expected: number) => {
    expect(binarySearch(haystack.length, i => needle - haystack[i])).to.equal(
      expected,
    );
  };

  t(0, [], 0);

  t(-1, [0], 0);
  t(0, [0], 0);
  t(1, [0], 1);

  t(-1, [0, 1], 0);
  t(0, [0, 1], 0);
  t(0.5, [0, 1], 1);
  t(1, [0, 1], 1);
  t(2, [0, 1], 2);

  t(-1, [0, 1, 2], 0);
  t(0, [0, 1, 2], 0);
  t(0.5, [0, 1, 2], 1);
  t(1, [0, 1, 2], 1);
  t(1.5, [0, 1, 2], 2);
  t(2, [0, 1, 2], 2);
  t(3, [0, 1, 2], 3);

  t(-1, [0, 1, 2, 3], 0);
  t(0, [0, 1, 2, 3], 0);
  t(0.5, [0, 1, 2, 3], 1);
  t(1, [0, 1, 2, 3], 1);
  t(1.5, [0, 1, 2, 3], 2);
  t(2, [0, 1, 2, 3], 2);
  t(2.5, [0, 1, 2, 3], 3);
  t(3, [0, 1, 2, 3], 3);
  t(4, [0, 1, 2, 3], 4);
});
