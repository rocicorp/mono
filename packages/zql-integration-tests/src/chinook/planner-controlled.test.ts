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

  test('track.exists(album).or.exists(genre): track < invoiceLines > album', () => {
    const costModel = makeCostModel({
      track: 10_000, // gets decimated to 10 when fetched by album⨝track join
      album: 1_000, // gets decimated to 5 from the `title` filter
      invoiceLine: 1_000_000, // gets decimated to 100 when fetched by track⨝invoiceLine join
    });
    const planned = planQuery(
      builder.track.where(({or, exists}) =>
        or(
          exists('album', q => q.where('title', 'Outlaw Blues')),
          exists('invoiceLines'),
        ),
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

    // Artist should be flipped which forces all others to flip too
    expect(pick(planned, ['where', 'flip'])).toBe(true);
    expect(
      pick(planned, ['where', 'related', 'subquery', 'where', 'flip']),
    ).toBe(true);
  });

  test('track.exists(album.exists(artist)): artist > album > track', () => {
    const costModel = makeCostModel({track: 10, album: 100, artist: 5000});
    const planned = planQuery(
      builder.track.where(({exists}) =>
        exists('album', q => q.whereExists('artist')),
      ).ast,
      costModel,
    );

    // No flips
    expect(pick(planned, ['where', 'flip'])).toBe(false);
    expect(
      pick(planned, ['where', 'related', 'subquery', 'where', 'flip']),
    ).toBe(false);
  });

  test('track.exists(album.exists(artist)): track > artist > album', () => {
    const costModel = makeCostModel({track: 1000, album: 10, artist: 100});
    const planned = planQuery(
      builder.track.where(({exists}) =>
        exists('album', q => q.whereExists('artist')),
      ).ast,
      costModel,
    );

    // join order: album -> artist -> track
    expect(pick(planned, ['where', 'flip'])).toBe(true);
    expect(
      pick(planned, ['where', 'related', 'subquery', 'where', 'flip']),
    ).toBe(false);
  });
});

describe('no exists', () => {
  test('simple', () => {
    const costModel = makeCostModel({track: 1000, album: 10, artist: 100});
    const unplanned = builder.track.where('name', 'Outlaw Blues').ast;
    const planned = planQuery(unplanned, costModel);

    // No joins to plan, should be unchanged
    expect(planned).toEqual(unplanned);
  });

  test('with related', () => {
    const costModel = makeCostModel({track: 1000, album: 10, artist: 100});
    const unplanned = builder.track
      .where('name', 'Outlaw Blues')
      .related('album', q => q.where('title', 'Outlaw Blues')).ast;
    const planned = planQuery(unplanned, costModel);
    // No joins to plan, should be unchanged
    expect(planned).toEqual(unplanned);
  });

  test('with or', () => {
    const costModel = makeCostModel({track: 1000, album: 10, artist: 100});
    const unplanned = builder.track.where(({or, cmp}) =>
      or(cmp('name', 'Outlaw Blues'), cmp('composer', 'foo')),
    ).ast;
    const planned = planQuery(unplanned, costModel);
    // No joins to plan, should be unchanged
    expect(planned).toEqual(unplanned);
  });
});

describe('related calls get plans', () => {
  test('related with exists inside', () => {
    const costModel = makeCostModel({track: 1000, album: 10, artist: 2});
    const unplanned = builder.track
      .where('name', 'Outlaw Blues')
      // TODO: this is wrong. album should have cost 1 when coming from a related call
      // because it is pinned to the given track!
      .related('album', q => q.whereExists('artist')).ast;
    const planned = planQuery(unplanned, costModel);

    console.log(JSON.stringify(planned, null, 2));
  });
});

function makeCostModel(costs: Record<string, number>) {
  return (
    table: string,
    _sort: Ordering,
    filters: Condition | undefined,
    constraint: PlannerConstraint | undefined,
  ) => {
    constraint = constraint ?? {};
    if ('id' in constraint) {
      // Primary key constraint, very fast
      return 1;
    }

    if (table === 'invoiceLine' && 'trackId' in constraint) {
      // not many invoices lines per track
      return 100;
    }

    if (table === 'track' && 'albumId' in constraint) {
      // not many tracks per album
      return 10;
    }

    if (
      table === 'album' &&
      filters?.type === 'simple' &&
      filters.left.type === 'column' &&
      filters.left.name === 'title'
    ) {
      // not many albums with same title
      return 5;
    }

    const ret =
      must(costs[table]) / (Object.keys(constraint).length * 100 || 1);
    return ret;
  };
}
