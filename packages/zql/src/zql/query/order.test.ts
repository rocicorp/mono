import {expect, test} from 'vitest';
import fc from 'fast-check';
import {TestContext} from '../context/test-context.js';
import {makeComparator} from '../ivm/compare.js';
import {EntityQuery} from './entity-query.js';

type Track = {
  id: string;
  title: string;
  albumId: string;
  length: number;
};

const objArbitrary = fc.record({
  id: fc.uuid(),
  title: fc.string(),
  albumId: fc.string(),
  length: fc.integer(),
});

// generate Ordering objects with random sets of properties
const orderingArbitrary = fc.array(
  fc.oneof(
    fc.constant('id' as const),
    fc.constant('title' as const),
    fc.constant('albumId' as const),
    fc.constant('length' as const),
  ),
  {minLength: 1},
);

test('select query with order by', async () => {
  await fc.assert(
    fc.asyncProperty(
      orderingArbitrary,
      // TODO: direction is split out since we don't support different directions for different fields yet
      fc.oneof(fc.constant('asc' as const), fc.constant('desc' as const)),
      fc.array(objArbitrary, {minLength: 1}),
      async (order, direction, tracks) => {
        const context = new TestContext();
        const trackSource = context.getSource('track');
        context.materialite.tx(() => {
          for (const track of tracks) {
            trackSource.add(track);
          }
        });

        const comparator = makeComparatorForExpectedResults(
          order.map(field => [field, direction]),
          direction,
        );
        let query = new EntityQuery<{track: Track}>(context, 'track').select(
          '*',
        );
        for (const field of order) {
          query = query.orderBy(field, direction);
        }
        const stmt = query.prepare();
        const rows = await stmt.exec();
        const expected = tracks.concat().sort(comparator);
        stmt.destroy();

        expect(rows).toEqual(expected);
      },
    ),
  );
});

function makeComparatorForExpectedResults(
  order: [string, 'asc' | 'desc'][],
  defaultDirection: 'asc' | 'desc',
) {
  const orderViaSelectors = order.map(
    ([field, direction]) => [['track', field], direction] as const,
  );
  if (!order.find(([field]) => field === 'id')) {
    orderViaSelectors.push([['track', 'id'], defaultDirection]);
  }
  return makeComparator(orderViaSelectors);
}
