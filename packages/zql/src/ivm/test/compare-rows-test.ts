import {expect} from 'vitest';
import type {Row} from '../../../../zql/src/ivm/data.ts';
import type {Ordering} from '../constraint.ts';

export function compareRowsTest(
  makeComparator: (order: Ordering) => (r1: Row, r2: Row) => number,
) {
  const cases: {
    order: Ordering;
    r1: Row;
    r2: Row;
    expected: number | string;
  }[] = [
    {
      order: [['a', 'asc']],
      r1: {a: 1},
      r2: {a: 2},
      expected: -1,
    },
    {
      order: [['a', 'desc']],
      r1: {a: 1},
      r2: {a: 2},
      expected: 1,
    },
    {
      order: [['a', 'asc']],
      r1: {a: 2},
      r2: {a: 1},
      expected: 1,
    },
    {
      order: [['a', 'desc']],
      r1: {a: 1},
      r2: {a: 2},
      expected: 1,
    },
    {
      order: [
        ['a', 'asc'],
        ['b', 'asc'],
      ],
      r1: {a: 1, b: ''},
      r2: {a: 1, b: ''},
      expected: 0,
    },
    {
      order: [
        ['a', 'asc'],
        ['b', 'asc'],
      ],
      r1: {a: 1, b: ''},
      r2: {a: 1, b: 'foo'},
      expected: -1,
    },
    {
      order: [
        ['a', 'asc'],
        ['b', 'asc'],
      ],
      r1: {a: 1, b: 'foo'},
      r2: {a: 1, b: ''},
      expected: 1,
    },
    {
      order: [
        ['a', 'asc'],
        ['b', 'asc'],
      ],
      r1: {a: 1, b: 'foo'},
      r2: {a: 1, b: 'bar'},
      expected: 1,
    },
    {
      order: [['a', 'asc']],
      r1: {a: 1},
      r2: {a: 'foo'},
      expected: 'expected number',
    },
  ];

  for (const c of cases) {
    if (c.expected === 0) {
      expect(makeComparator(c.order)(c.r1, c.r2), JSON.stringify(c)).toBe(0);
    } else if (c.expected === 1) {
      expect(
        makeComparator(c.order)(c.r1, c.r2),
        JSON.stringify(c),
      ).toBeGreaterThan(0);
    } else if (c.expected === -1) {
      expect(
        makeComparator(c.order)(c.r1, c.r2),
        JSON.stringify(c),
      ).toBeLessThan(0);
    } else if (typeof c.expected === 'string') {
      expect(
        () => makeComparator(c.order)(c.r1, c.r2),
        JSON.stringify(c),
      ).toThrow(c.expected);
    } else {
      throw new Error('unreachable');
    }
  }
}
