// cases with a controlled cost model
import {describe, expect, test} from 'vitest';
import {planQuery} from '../../../zql/src/planner/planner-builder.ts';
import {builder} from './schema.ts';
import {pick} from '../helpers/planner.ts';
import type {PlannerConstraint} from '../../../zql/src/planner/planner-constraint.ts';
import type {Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import {must} from '../../../shared/src/must.ts';
import {assert} from '../../../shared/src/asserts.ts';
import type {CostModelCost} from '../../../zql/src/planner/planner-connection.ts';

describe('one join', () => {
  test('no changes in cost', () => {
    const costModel = () => ({startupCost: 0, rows: 10});
    const unplanned = builder.track.whereExists('album').ast;
    const planned = planQuery(unplanned, costModel);

    // With semi-join overhead, planner now prefers flipped joins even when base costs are equal
    // This is expected: flipped joins are more efficient than semi-joins for equal row counts
    expect(pick(planned, ['where', 'flip'])).toBe(true);
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

  test('track.exists(album).or.exists(invoiceLines): track < invoiceLines > album', () => {
    const costModel = (
      table: string,
      _sort: Ordering,
      _filters: Condition | undefined,
      constraint: PlannerConstraint | undefined,
    ): CostModelCost => {
      if (table === 'album') {
        if (constraint !== undefined) {
          // fetching album by id
          assert(
            constraint.hasOwnProperty('id'),
            'Expected constraint to have id',
          );
          return {startupCost: 0, rows: 1};
        }
        return {startupCost: 0, rows: 2}; // only 2 albums with the name 'Outlaw Blues'
      }

      if (table === 'invoiceLine') {
        if (constraint !== undefined) {
          // fetching invoiceLines by trackId
          assert(
            constraint.hasOwnProperty('trackId'),
            'Expected constraint to have trackId',
          );
          // TODO: We cannot get this to flip one and not the other without incorporating
          // limits and selectivity into the cost model. For now, just return a low cost to
          // simulate the track quickly matching invoices and returning early.
          return {startupCost: 0, rows: 0.1};
        }

        return {startupCost: 0, rows: 10_000};
      }

      if (table === 'track') {
        if (constraint !== undefined) {
          if (constraint.hasOwnProperty('id')) {
            return {startupCost: 0, rows: 1};
          }
          if (constraint.hasOwnProperty('albumId')) {
            return {startupCost: 0, rows: 10};
          }
          throw new Error('Unexpected constraint on track');
        }
        return {startupCost: 0, rows: 1_000};
      }

      throw new Error(`Unexpected table: ${table}`);
    };

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

    // join order: artist -> album -> track
    // With semi-join overhead, planner now flips both joins to avoid overhead
    expect(pick(planned, ['where', 'flip'])).toBe(true);
    expect(
      pick(planned, ['where', 'related', 'subquery', 'where', 'flip']),
    ).toBe(true);
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
  test('1:1 will not flip since it is anchored by primary key', () => {
    // album cost is decimated to 1 during the `related` transition since we are related by `albumId -> id`
    const costModel = makeCostModel({track: 1000, album: 100000, artist: 2});
    const unplanned = builder.track
      .where('name', 'Outlaw Blues')
      .related('album', q => q.whereExists('artist')).ast;
    const planned = planQuery(unplanned, costModel);

    expect(pick(planned, ['related', 0, 'subquery', 'where', 'flip'])).toBe(
      false,
    );
  });

  test('1:many may flip', () => {
    const unplanned = builder.album.related('tracks', q =>
      q.whereExists('genre', q => q.where('name', 'Foo')),
    ).ast;
    const costModel = (
      table: string,
      _sort: Ordering,
      _filters: Condition | undefined,
      constraint: PlannerConstraint | undefined,
    ): CostModelCost => {
      // Force `.related('tracks')` to be more expensive
      if (table === 'track') {
        assert(
          constraint?.hasOwnProperty('albumId'),
          'Expected constraint to have albumId',
        );
        // if we flip to do genre, we can reduce the cost.
        if (constraint?.hasOwnProperty('genreId')) {
          return {
            rows: 1,
            startupCost: 0,
          };
        }
        return {
          rows: 10_000,
          startupCost: 0,
        };
      }
      return {
        rows: 10,
        startupCost: 0,
      };
    };

    const planned = planQuery(unplanned, costModel);
    expect(pick(planned, ['related', 0, 'subquery', 'where', 'flip'])).toBe(
      true,
    );
  });
});

describe('junction edge', () => {
  test('playlist -> track', () => {
    // should incur no flips since fewer playlists than tracks
    const costModel = makeCostModel({
      playlist: 100,
      playlistTrack: 1000,
      track: 10000,
    });
    const planned = planQuery(
      builder.playlist.whereExists('tracks').ast,
      costModel,
    );

    // No flip: playlist (100) -> playlistTrack -> track is cheaper than flipping
    expect(pick(planned, ['where', 'flip'])).toBe(false);
  });

  test('track -> playlist', () => {
    // should flip since fewer playlists than tracks
    const costModel = makeCostModel({
      playlist: 100,
      playlistTrack: 1000,
      track: 10000,
    });
    const planned = planQuery(
      builder.track.whereExists('playlists').ast,
      costModel,
    );

    // Flip: start from playlist (100) instead of track (10000)
    expect(pick(planned, ['where', 'flip'])).toBe(true);
  });
});

test('ors anded one after the other', () => {
  // (A or B) and (C or D)
  const ast = builder.track
    .where(({or, exists}) => or(exists('album'), exists('genre')))
    .where(({or, exists}) =>
      or(exists('invoiceLines'), exists('mediaType')),
    ).ast;

  const costModel = makeCostModel({
    track: 10000,
    album: 10000,
    genre: 10000,
    invoiceLine: 10000,
    mediaType: 10000,
  });

  const planned = planQuery(ast, costModel);

  // With uniform costs, planner should keep original order (no flips)
  // Check first OR: album and genre
  expect(
    pick(planned, ['where', 'conditions', 0, 'conditions', 0, 'flip']),
  ).toBe(false);
  expect(
    pick(planned, ['where', 'conditions', 0, 'conditions', 1, 'flip']),
  ).toBe(false);

  // Check second OR: invoiceLines and mediaType
  // flipping invoice lines is actually cheaper due to the FK from invoiceLine -> track
  expect(
    pick(planned, ['where', 'conditions', 1, 'conditions', 0, 'flip']),
  ).toBe(true);
  expect(
    pick(planned, ['where', 'conditions', 1, 'conditions', 1, 'flip']),
  ).toBe(false);
});

function makeCostModel(costs: Record<string, number>) {
  return (
    table: string,
    _sort: Ordering,
    _filters: Condition | undefined,
    constraint: PlannerConstraint | undefined,
  ): CostModelCost => {
    constraint = constraint ?? {};
    if ('id' in constraint) {
      // Primary key constraint, very fast
      return {
        startupCost: 0,
        rows: 1,
      };
    }

    if (table === 'invoiceLine' && 'trackId' in constraint) {
      // not many invoices lines per track
      return {
        startupCost: 0,
        rows: 100,
      };
    }

    if (table === 'track' && 'albumId' in constraint) {
      // not many tracks per album
      return {
        startupCost: 0,
        rows: 10,
      };
    }

    const ret =
      must(costs[table]) / (Object.keys(constraint).length * 100 || 1);
    return {
      startupCost: 0,
      rows: ret,
    };
  };
}
