// cases with a controlled cost model
import {describe, expect, test} from 'vitest';
import {planQuery} from '../../../zql/src/planner/planner-builder.ts';
import {builder} from './schema.ts';
import {pick} from '../helpers/planner.ts';
import type {PlannerConstraint} from '../../../zql/src/planner/planner-constraint.ts';
import type {Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import {must} from '../../../shared/src/must.ts';

describe('one join', () => {
  test('no changes in cost', () => {
    const costModel = () => 10;
    const unplanned = builder.track.whereExists('album').ast;
    const planned = planQuery(unplanned, costModel);

    // All plans are same cost, use original order
    expect(planned).toEqual(unplanned);
  });

  test('track.exists(album): track is more expensive', () => {
    const costModel = makeCostModel({track: 5000, album: 100});
    const planned = planQuery(
      builder.track.whereExists('album').ast,
      costModel,
    );
    expect(pick(planned, ['where', 'flip'])).toBe(true);
  });

  test('track.exists(album): album is more expensive', () => {
    const costModel = makeCostModel({track: 100, album: 5000});
    const planned = planQuery(
      builder.track.whereExists('album').ast,
      costModel,
    );
    expect(pick(planned, ['where', 'flip'])).toBe(false);
  });
});

describe('two joins via and', () => {
  test('track.exists(album).exists(genre): track > album > genre', () => {
    const costModel = makeCostModel({track: 5000, album: 100, genre: 10});
    const planned = planQuery(
      builder.track.whereExists('album').whereExists('genre').ast,
      costModel,
    );

    // Genre gets flipped to the root
    // Cost 10 -> Cost 1 -> Cost 1
    // TODO: we need some tracing mechanism to check what constraints were chosen
    expect(pick(planned, ['where', 'conditions', 0, 'flip'])).toBe(false);
    expect(pick(planned, ['where', 'conditions', 1, 'flip'])).toBe(true);
    expect(
      pick(planned, ['where', 'conditions', 1, 'related', 'subquery', 'table']),
    ).toBe('genre');
  });

  test('track.exists(album).exists(genre): track > genre > album', () => {
    const costModel = makeCostModel({track: 5000, album: 10, genre: 100});
    const planned = planQuery(
      builder.track.whereExists('album').whereExists('genre').ast,
      costModel,
    );

    expect(pick(planned, ['where', 'conditions', 0, 'flip'])).toBe(true);
    expect(pick(planned, ['where', 'conditions', 1, 'flip'])).toBe(false);
    expect(
      pick(planned, ['where', 'conditions', 0, 'related', 'subquery', 'table']),
    ).toBe('album');
  });
});

// TODO:
// I need to re-do how we compute costs over the graph.
// 1. ANDs should be added
// 2. ORs should be added with UFO multiplying cost of parent input
// 3. Joins multiplies parent * child
// describe('two joins via or', () => {
//   test.only('track.exists(album).or.exists(genre): track > album > genre', () => {
//     const costModel = makeCostModel({track: 500000, album: 10, genre: 10});
//     const planned = planQuery(
//       builder.track.where(({or, exists}) =>
//         or(exists('album'), exists('genre')),
//       ).ast,
//       costModel,
//     );

//     // TODO: I would expect `album` and `genre` to be flipped. We must have some bug in `or` planning?
//     console.log(JSON.stringify(planned, null, 2));
//   });
// });

function makeCostModel(costs: Record<string, number>) {
  return (
    table: string,
    _sort: Ordering,
    _filters: Condition | undefined,
    constraint: PlannerConstraint | undefined,
  ) => {
    constraint = constraint ?? {};
    if ('id' in constraint) {
      // Primary key constraint, very fast
      return 1;
    }

    const ret =
      must(costs[table]) / (Object.keys(constraint).length * 100 || 1);
    return ret;
  };
}
