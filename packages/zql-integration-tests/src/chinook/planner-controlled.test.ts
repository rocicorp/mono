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

describe('two joins via or', () => {
  test('track.exists(album).or.exists(genre): track > album > genre', () => {
    const costModel = makeCostModel({track: 500000, album: 10, genre: 10});
    const planned = planQuery(
      builder.track.where(({or, exists}) =>
        or(exists('album'), exists('genre')),
      ).ast,
      costModel,
    );

    expect(pick(planned, ['where', 'conditions', 0, 'flip'])).toBe(true);
    expect(pick(planned, ['where', 'conditions', 1, 'flip'])).toBe(true);
  });

  // TODO: trace planner.
  test('track.exists(album).or.exists(genre): track < invoiceLines > album', () => {
    const costModel = makeCostModel({
      track: 10_000,
      album: 10,
      invoiceLine: 1_000_000,
    });
    const planned = planQuery(
      builder.track.where(({or, exists}) =>
        or(exists('album'), exists('invoiceLines')),
      ).ast,
      costModel,
    );

    expect(pick(planned, ['where', 'conditions', 0, 'flip'])).toBe(true);
    expect(pick(planned, ['where', 'conditions', 1, 'flip'])).toBe(false);
  });
});

describe('double nested exists', () => {
  test('track.exists(album.exists(artist)): track > album > artist', () => {
    const costModel = makeCostModel({track: 5000, album: 100, artist: 10});
    const planned = planQuery(
      builder.track.where(({exists}) =>
        exists('album', q => q.whereExists('artist')),
      ).ast,
      costModel,
    );

    // Artist should be flipped to root since it's cheapest
    // The nested structure should be optimized
    // Nested EXISTS creates a single correlatedSubquery, not an 'and' with conditions
    expect(pick(planned, ['where', 'flip'])).toBe(true);
  });

  test('track.exists(album.exists(artist)): artist > album > track', () => {
    const costModel = makeCostModel({track: 10, album: 100, artist: 5000});
    const planned = planQuery(
      builder.track.where(({exists}) =>
        exists('album', q => q.whereExists('artist')),
      ).ast,
      costModel,
    );

    // With track being cheapest, no flip needed at top level
    // Nested EXISTS creates a single correlatedSubquery, not an 'and' with conditions
    expect(pick(planned, ['where', 'flip'])).toBe(false);
  });

  test('track.exists(album.exists(artist)): album > track > artist', () => {
    const costModel = makeCostModel({track: 100, album: 5000, artist: 10});
    const planned = planQuery(
      builder.track.where(({exists}) =>
        exists('album', q => q.whereExists('artist')),
      ).ast,
      costModel,
    );

    // Complex case: artist is cheapest unconstrained (10) but track with constraints is better
    // Starting from track: track(100) -> album(1 with semi) = 101 total
    // Starting from artist: artist(10) -> album(50) -> track(1) = 500+ total (flips expensive)
    // Planner correctly chooses no flip (start from track)
    // Nested EXISTS creates a single correlatedSubquery, not an 'and' with conditions
    expect(pick(planned, ['where', 'flip'])).toBe(false);
  });
});

describe('triple nested exists', () => {
  test('invoiceLine.exists(invoice.exists(customer.exists(supportRep))): costs descending', () => {
    const costModel = makeCostModel({
      invoiceLine: 10000,
      invoice: 1000,
      customer: 100,
      employee: 10,
    });
    const planned = planQuery(
      builder.invoiceLine.where(({exists}) =>
        exists('invoice', q =>
          q.where(({exists: e2}) =>
            e2('customer', q2 => q2.whereExists('supportRep')),
          ),
        ),
      ).ast,
      costModel,
    );

    // Employee (supportRep) is cheapest, should be flipped to root
    // Nested EXISTS creates a single correlatedSubquery, not an 'and' with conditions
    expect(pick(planned, ['where', 'flip'])).toBe(true);
  });

  test('invoiceLine.exists(invoice.exists(customer.exists(supportRep))): costs ascending', () => {
    const costModel = makeCostModel({
      invoiceLine: 10,
      invoice: 100,
      customer: 1000,
      employee: 10000,
    });
    const planned = planQuery(
      builder.invoiceLine.where(({exists}) =>
        exists('invoice', q =>
          q.where(({exists: e2}) =>
            e2('customer', q2 => q2.whereExists('supportRep')),
          ),
        ),
      ).ast,
      costModel,
    );

    // InvoiceLine is cheapest, no flip at root level
    // Nested EXISTS creates a single correlatedSubquery, not an 'and' with conditions
    expect(pick(planned, ['where', 'flip'])).toBe(false);
  });

  test('invoiceLine.exists(invoice.exists(customer.exists(supportRep))): middle is cheapest', () => {
    const costModel = makeCostModel({
      invoiceLine: 5000,
      invoice: 100,
      customer: 10,
      employee: 5000,
    });
    const planned = planQuery(
      builder.invoiceLine.where(({exists}) =>
        exists('invoice', q =>
          q.where(({exists: e2}) =>
            e2('customer', q2 => q2.whereExists('supportRep')),
          ),
        ),
      ).ast,
      costModel,
    );

    // Customer is cheapest in the middle, should optimize access path
    // Nested EXISTS creates a single correlatedSubquery, not an 'and' with conditions
    expect(pick(planned, ['where', 'flip'])).toBe(true);
  });
});

function makeCostModel(costs: Record<string, number>) {
  return (
    table: string,
    _sort: Ordering,
    _filters: Condition | undefined,
    constraint: PlannerConstraint | undefined,
  ) => {
    if (!constraint) {
      return must(costs[table]);
    }

    const fields = constraint.fields;
    if ('id' in fields) {
      // Primary key constraint, very fast
      return 1;
    }

    const fieldCount = Object.keys(fields).length;
    const baseCost = must(costs[table]) / (fieldCount * 100 || 1);

    // Apply 10x discount for semi-joins (early termination)
    return constraint.isSemiJoin ? baseCost / 10 : baseCost;
  };
}
