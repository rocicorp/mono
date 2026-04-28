// cases with a controlled cost model
import {describe, expect, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {AST, Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import {transformFilters} from '../../../zql/src/builder/filter.ts';
import {planQuery} from '../../../zql/src/planner/planner-builder.ts';
import type {CostModelCost} from '../../../zql/src/planner/planner-connection.ts';
import type {PlannerConstraint} from '../../../zql/src/planner/planner-constraint.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {pick} from '../helpers/planner.ts';
import {builder} from './schema.ts';

function ast(q: AnyQuery) {
  return asQueryInternals(q).ast;
}

/**
 * Normalize an AST by setting all undefined flip flags to false.
 * This matches what the planner outputs after planning.
 */
function normalizeFlipFlags(ast: AST): AST {
  return {
    ...ast,
    where: ast.where ? normalizeConditionFlipFlags(ast.where) : undefined,
    related: ast.related?.map(r => ({
      ...r,
      subquery: normalizeFlipFlags(r.subquery),
    })),
  };
}

function normalizeConditionFlipFlags(condition: Condition): Condition {
  if (condition.type === 'simple') {
    return condition;
  }

  if (condition.type === 'correlatedSubquery') {
    return {
      ...condition,
      flip: condition.flip ?? false,
      related: {
        ...condition.related,
        subquery: normalizeFlipFlags(condition.related.subquery),
      },
    };
  }

  return {
    ...condition,
    conditions: condition.conditions.map(normalizeConditionFlipFlags),
  };
}

describe('one join', () => {
  test('no changes in cost', () => {
    const costModel = () => ({
      startupCost: 0,
      rows: 10,
      fanout: () => ({fanout: 3, confidence: 'none' as const}),
    });
    const unplanned = ast(builder.track.whereExists('album'));
    const planned = planQuery(unplanned, costModel);

    // All plans are same cost, planner chooses not to flip (keeps original order)
    expect(planned).toEqual(normalizeFlipFlags(unplanned));
  });

  test('track.exists(album): track is more expensive', () => {
    const costModel = makeCostModel({track: 5000, album: 100});
    const planned = planQuery(
      ast(builder.track.whereExists('album')),
      costModel,
    );
    expect(pick(planned, ['where', 'flip'])).toBe(true);
  });

  test('track.exists(album): album is more expensive', () => {
    const costModel = makeCostModel({track: 100, album: 5000});
    const planned = planQuery(
      ast(builder.track.whereExists('album')),
      costModel,
    );
    expect(pick(planned, ['where', 'flip'])).toBe(false);
  });
});

describe('two joins via and', () => {
  test('track.exists(album).exists(genre): track > album > genre', () => {
    const costModel = makeCostModel({track: 5000, album: 100, genre: 10});
    const planned = planQuery(
      ast(builder.track.whereExists('album').whereExists('genre')),
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
      ast(builder.track.whereExists('album').whereExists('genre')),
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
      ast(
        builder.track.where(({or, exists}) =>
          or(exists('album'), exists('genre')),
        ),
      ),
      costModel,
    );

    expect(pick(planned, ['where', 'conditions', 0, 'flip'])).toBe(true);
    expect(pick(planned, ['where', 'conditions', 1, 'flip'])).toBe(true);
  });

  test('track.exists(album).or.exists(invoiceLines): track < invoiceLines > album', () => {
    const defaultFanout = () => ({fanout: 3, confidence: 'none' as const});
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
            Object.hasOwn(constraint, 'id'),
            'Expected constraint to have id',
          );
          return {startupCost: 0, rows: 1, fanout: defaultFanout};
        }
        return {startupCost: 0, rows: 2, fanout: defaultFanout}; // only 2 albums with the name 'Outlaw Blues'
      }

      if (table === 'invoiceLine') {
        if (constraint !== undefined) {
          // fetching invoiceLines by trackId
          assert(
            Object.hasOwn(constraint, 'trackId'),
            'Expected constraint to have trackId',
          );
          // TODO: We cannot get this to flip one and not the other without incorporating
          // limits and selectivity into the cost model. For now, just return a low cost to
          // simulate the track quickly matching invoices and returning early.
          return {startupCost: 0, rows: 0.1, fanout: defaultFanout};
        }

        return {startupCost: 0, rows: 10_000, fanout: defaultFanout};
      }

      if (table === 'track') {
        if (constraint !== undefined) {
          if (Object.hasOwn(constraint, 'id')) {
            return {startupCost: 0, rows: 1, fanout: defaultFanout};
          }
          if (Object.hasOwn(constraint, 'albumId')) {
            return {startupCost: 0, rows: 10, fanout: defaultFanout};
          }
          throw new Error('Unexpected constraint on track');
        }
        return {startupCost: 0, rows: 1_000, fanout: defaultFanout};
      }

      throw new Error(`Unexpected table: ${table}`);
    };

    const planned = planQuery(
      ast(
        builder.track.where(({or, exists}) =>
          or(
            exists('album', q => q.where('title', 'Outlaw Blues')),
            exists('invoiceLines'),
          ),
        ),
      ),
      costModel,
    );

    expect(pick(planned, ['where', 'conditions', 0, 'flip'])).toBe(true);
    expect(pick(planned, ['where', 'conditions', 1, 'flip'])).toBe(false);
  });
});

describe('or(simple, exists) — predicate-pushdown planning', () => {
  /**
   * Cost model that mimics the production SQLite cost model: it
   * (a) honors filter selectivity (PK-equality on `id` collapses to 1 row),
   * (b) strips CSQs from the incoming filter via `transformFilters` (the
   *     same function the runtime uses to decide what to push to the
   *     source at connection time),
   * (c) honors constraint selectivity for known FK columns.
   *
   * Previously, the production cost model used `removeCorrelatedSubqueries`
   * which had wrong OR semantics (it returned just the simple branch
   * instead of `undefined`). That bias caused the planner to think the
   * parent connection was a 1-row PK lookup when it was actually a full
   * table scan, biasing plan selection toward semi-join. See
   * `packages/zqlite/src/sqlite-cost-model.ts` history.
   */
  function makeRealisticCostModel(tableSizes: Record<string, number>) {
    const defaultFanout = () => ({fanout: 3, confidence: 'none' as const});

    return (
      table: string,
      _sort: Ordering,
      filters: Condition | undefined,
      constraint: PlannerConstraint | undefined,
    ): CostModelCost => {
      const stripped = transformFilters(filters).filters;

      // PK constraint from a parent join → 1-row PK lookup.
      if (constraint && 'id' in constraint) {
        return {startupCost: 0, rows: 1, fanout: defaultFanout};
      }

      // FK constraints we know about (low fanout per parent).
      if (constraint && 'trackId' in constraint) {
        return {startupCost: 0, rows: 5, fanout: defaultFanout};
      }
      if (constraint && 'albumId' in constraint) {
        return {startupCost: 0, rows: 10, fanout: defaultFanout};
      }

      // Recognize a PK-equality filter (e.g. `id = ?`) and treat as 1 row.
      if (
        stripped &&
        stripped.type === 'simple' &&
        stripped.op === '=' &&
        stripped.left.type === 'column' &&
        stripped.left.name === 'id'
      ) {
        return {startupCost: 0, rows: 1, fanout: defaultFanout};
      }

      return {
        startupCost: 0,
        rows: must(tableSizes[table]),
        fanout: defaultFanout,
      };
    };
  }

  /**
   * Regression: `track.where(or(cmp('id', X), exists('invoiceLines')))`.
   *
   * Before the fix to `removeCorrelatedSubqueries`, the cost model received
   * `cmp('id', X)` for the parent connection (because the buggy OR
   * simplification dropped the CSQ branch) and reported it as a 1-row PK
   * lookup. The planner therefore preferred semi-join even though semi
   * scans the full track table at runtime.
   *
   * After the fix, the cost model receives `undefined` for the parent
   * (transformFilters returns undefined for OR-with-CSQ), reflecting the
   * runtime's actual behavior. The planner then sees:
   *   - semi cost = 1M (full track) × 5 (invoiceLines via trackId) = 5M
   *   - flipped cost = 100 (invoiceLines) × 1 (track by PK) = 100
   * and correctly picks flipped.
   */
  test('flips EXISTS for or(cmp("id", X), exists)', () => {
    const costModel = makeRealisticCostModel({
      track: 1_000_000,
      invoiceLine: 100,
    });

    const planned = planQuery(
      ast(
        builder.track.where(({or, cmp, exists}) =>
          or(cmp('id', 1), exists('invoiceLines')),
        ),
      ),
      costModel,
    );

    expect(pick(planned, ['where', 'conditions', 1, 'flip'])).toBe(true);
  });

  /**
   * Same shape with a non-PK simple branch. The buggy simplification didn't
   * mis-credit cardinality here (track scan stays at 1M) so this case
   * already passed before the fix — keeping it as a sanity check.
   */
  test('flips EXISTS for or(cmp(non-PK, X), exists)', () => {
    const costModel = makeRealisticCostModel({
      track: 1_000_000,
      invoiceLine: 100,
    });

    const planned = planQuery(
      ast(
        builder.track.where(({or, cmp, exists}) =>
          or(cmp('name', 'Outlaw Blues'), exists('invoiceLines')),
        ),
      ),
      costModel,
    );

    expect(pick(planned, ['where', 'conditions', 1, 'flip'])).toBe(true);
  });

  /**
   * AND with CSQ: `track.where(cmp).whereExists('invoiceLines')`.
   *
   * Before AND the fix, transformFilters and removeCorrelatedSubqueries
   * agreed for AND shapes — both kept the simple conjuncts and dropped
   * CSQs. This test guards against regression in the AND path.
   *
   * The simple `cmp('id', X)` is a PK lookup → cost model reports 1 row
   * for the parent. With only 1 parent row to scan, semi is much cheaper
   * than flipped (1 × 5 = 5 vs 100 × 1 = 100). Planner should keep semi.
   */
  test('AND with PK simple branch: cost model still sees the simple filter', () => {
    const costModel = makeRealisticCostModel({
      track: 1_000_000,
      invoiceLine: 100,
    });

    const planned = planQuery(
      ast(builder.track.where('id', 1).whereExists('invoiceLines')),
      costModel,
    );

    // `where(...).whereExists(...)` ⇒ AST shape `and(cmp, exists)`.
    // The exists is the second conjunct. Should NOT flip — semi is cheaper.
    expect(pick(planned, ['where', 'conditions', 1, 'flip'])).toBe(false);
  });
});

describe('double nested exists', () => {
  test('track.exists(album.exists(artist)): track > album > artist', () => {
    const costModel = makeCostModel({track: 5000, album: 100, artist: 10});
    const planned = planQuery(
      ast(
        builder.track.where(({exists}) =>
          exists('album', q => q.whereExists('artist')),
        ),
      ),
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
      ast(
        builder.track.where(({exists}) =>
          exists('album', q => q.whereExists('artist')),
        ),
      ),
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
      ast(
        builder.track.where(({exists}) =>
          exists('album', q => q.whereExists('artist')),
        ),
      ),
      costModel,
    );

    // join order: artist -> album -> track
    expect(pick(planned, ['where', 'flip'])).toBe(true);
    expect(
      pick(planned, ['where', 'related', 'subquery', 'where', 'flip']),
    ).toBe(false);
  });
});

describe('no exists', () => {
  test('simple', () => {
    const costModel = makeCostModel({track: 1000, album: 10, artist: 100});
    const unplanned = ast(builder.track.where('name', 'Outlaw Blues'));
    const planned = planQuery(unplanned, costModel);

    // No joins to plan, should be unchanged
    expect(planned).toEqual(normalizeFlipFlags(unplanned));
  });

  test('with related', () => {
    const costModel = makeCostModel({track: 1000, album: 10, artist: 100});
    const unplanned = ast(
      builder.track
        .where('name', 'Outlaw Blues')
        .related('album', q => q.where('title', 'Outlaw Blues')),
    );
    const planned = planQuery(unplanned, costModel);
    // No joins to plan, should be unchanged
    expect(planned).toEqual(normalizeFlipFlags(unplanned));
  });

  test('with or', () => {
    const costModel = makeCostModel({track: 1000, album: 10, artist: 100});
    const unplanned = ast(
      builder.track.where(({or, cmp}) =>
        or(cmp('name', 'Outlaw Blues'), cmp('composer', 'foo')),
      ),
    );
    const planned = planQuery(unplanned, costModel);
    // No joins to plan, should be unchanged
    expect(planned).toEqual(normalizeFlipFlags(unplanned));
  });
});

describe('related calls get plans', () => {
  test('1:1 will not flip since it is anchored by primary key', () => {
    // album cost is decimated to 1 during the `related` transition since we are related by `albumId -> id`
    const costModel = makeCostModel({track: 1000, album: 100000, artist: 2});
    const unplanned = ast(
      builder.track
        .where('name', 'Outlaw Blues')
        .related('album', q => q.whereExists('artist')),
    );
    const planned = planQuery(unplanned, costModel);

    expect(pick(planned, ['related', 0, 'subquery', 'where', 'flip'])).toBe(
      false,
    );
  });

  test('1:many may flip', () => {
    const defaultFanout = () => ({fanout: 3, confidence: 'none' as const});
    const unplanned = ast(
      builder.album.related('tracks', q =>
        q.whereExists('genre', q => q.where('name', 'Foo')),
      ),
    );
    const costModel = (
      table: string,
      _sort: Ordering,
      _filters: Condition | undefined,
      constraint: PlannerConstraint | undefined,
    ): CostModelCost => {
      // Force `.related('tracks')` to be more expensive
      if (table === 'track') {
        assert(
          constraint && Object.hasOwn(constraint, 'albumId'),
          'Expected constraint to have albumId',
        );
        // if we flip to do genre, we can reduce the cost.
        if (Object.hasOwn(constraint, 'genreId')) {
          return {
            rows: 1,
            startupCost: 0,
            fanout: defaultFanout,
          };
        }
        return {
          rows: 10_000,
          startupCost: 0,
          fanout: defaultFanout,
        };
      }
      return {
        rows: 10,
        startupCost: 0,
        fanout: defaultFanout,
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
      ast(builder.playlist.whereExists('tracks')),
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
      ast(builder.track.whereExists('playlists')),
      costModel,
    );

    // Flip: start from playlist (100) instead of track (10000)
    expect(pick(planned, ['where', 'flip'])).toBe(true);
  });
});

test('ors anded one after the other', () => {
  // (A or B) and (C or D)
  const astResult = ast(
    builder.track
      .where(({or, exists}) => or(exists('album'), exists('genre')))
      .where(({or, exists}) => or(exists('invoiceLines'), exists('mediaType'))),
  );

  const costModel = makeCostModel({
    track: 10000,
    album: 10000,
    genre: 10000,
    invoiceLine: 10000,
    mediaType: 10000,
  });

  const planned = planQuery(astResult, costModel);

  // With uniform costs, planner should keep original order (no flips)
  // Check first OR: album and genre
  expect(
    pick(planned, ['where', 'conditions', 0, 'conditions', 0, 'flip']),
  ).toBe(false);
  expect(
    pick(planned, ['where', 'conditions', 0, 'conditions', 1, 'flip']),
  ).toBe(false);

  // Check second OR: invoiceLines and mediaType
  // In the new cost model, planner doesn't flip when costs are uniform
  expect(
    pick(planned, ['where', 'conditions', 1, 'conditions', 0, 'flip']),
  ).toBe(false);
  expect(
    pick(planned, ['where', 'conditions', 1, 'conditions', 1, 'flip']),
  ).toBe(false);
});

function makeCostModel(costs: Record<string, number>) {
  const defaultFanout = () => ({fanout: 3, confidence: 'none' as const});

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
        fanout: defaultFanout,
      };
    }

    if (table === 'invoiceLine' && 'trackId' in constraint) {
      // not many invoices lines per track
      return {
        startupCost: 0,
        rows: 100,
        fanout: defaultFanout,
      };
    }

    if (table === 'track' && 'albumId' in constraint) {
      // not many tracks per album
      return {
        startupCost: 0,
        rows: 10,
        fanout: defaultFanout,
      };
    }

    const ret =
      must(costs[table]) / (Object.keys(constraint).length * 100 || 1);
    return {
      startupCost: 0,
      rows: ret,
      fanout: defaultFanout,
    };
  };
}
