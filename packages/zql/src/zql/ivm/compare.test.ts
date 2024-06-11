import {expect, test} from 'vitest';
import fc from 'fast-check';
import {makeComparator} from './compare.js';

const objArbitrary = fc.record({
  id: fc.string(),
  title: fc.string(),
  albumId: fc.string(),
  length: fc.integer(),
});

// generate Ordering objects with random sets of properties
const orderingArbitrary = fc.array(
  fc.tuple(
    fc.oneof(
      fc.constant('id' as const),
      fc.constant('title' as const),
      fc.constant('albumId' as const),
      fc.constant('length' as const),
    ),
    fc.oneof(fc.constant('asc' as const), fc.constant('desc' as const)),
  ),
  {minLength: 1},
);

test('makeComparator', () => {
  fc.assert(
    fc.property(
      orderingArbitrary,
      fc.array(objArbitrary, {minLength: 1}),
      (order, tracks) => {
        const orderViaSelectors = order.map(
          ([field, direction]) => [['track', field], direction] as const,
        );
        const comparator = makeComparator(orderViaSelectors);

        const result = tracks.concat().sort(comparator);
        const expected = tracks.concat().sort((l, r) => {
          for (const [field, direction] of order) {
            const lVal = l[field];
            const rVal = r[field];
            if (lVal === rVal) {
              continue;
            }
            if (lVal === null || lVal === undefined) {
              return direction === 'asc' ? -1 : 1;
            }
            if (rVal === null || rVal === undefined) {
              return direction === 'asc' ? 1 : -1;
            }
            if (lVal < rVal) {
              return direction === 'asc' ? -1 : 1;
            }
            if (lVal > rVal) {
              return direction === 'asc' ? 1 : -1;
            }
            throw new Error('unreachable');
          }
          return 0;
        });

        expect(result).toEqual(expected);
      },
    ),
  );
});
