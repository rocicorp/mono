import {beforeAll, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../../zero-cache/src/db/specs.ts';
import type {NameMapper} from '../../../zero-schema/src/name-mapper.ts';
import {clientToServer} from '../../../zero-schema/src/name-mapper.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {makeGetPlanAST, pick} from '../helpers/planner.ts';
import {bootstrap} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';

const pgContent = await getChinook();

const {dbs, queries} = await bootstrap({
  suiteName: 'chinook_planner',
  pgContent,
  zqlSchema: schema,
});

let costModel: ReturnType<typeof createSQLiteCostModel>;
let mapper: NameMapper;
let getPlanAST: ReturnType<typeof makeGetPlanAST>;
describe('Chinook planner tests', () => {
  beforeAll(() => {
    mapper = clientToServer(schema.tables);
    dbs.sqlite.exec('ANALYZE;');

    // Get table specs using computeZqlSpecs
    const tableSpecs = new Map<string, LiteAndZqlSpec>();
    computeZqlSpecs(
      createSilentLogContext(),
      dbs.sqlite,
      {includeBackfillingColumns: false},
      tableSpecs,
    );

    costModel = createSQLiteCostModel(dbs.sqlite, tableSpecs);

    getPlanAST = makeGetPlanAST(schema, mapper, costModel);
  });

  test('tracks for a given album', () => {
    const ast = getPlanAST(
      queries.track.whereExists('album', q => q.where('title', 'Big Ones')),
    );

    expect(pick(ast, ['where', 'flip'])).toBe(true);
  });

  test('has album and artist', () => {
    const ast = getPlanAST(
      queries.track
        .whereExists('album', q => q.where('title', 'Big Ones'))
        .whereExists('genre', q => q.where('name', 'Rock')),
    );

    expect(pick(ast, ['where', 'conditions', 0, 'flip'])).toBe(true);
    expect(pick(ast, ['where', 'conditions', 1, 'flip'])).toBe(false);
  });

  test('playlist with track', () => {
    const ast = getPlanAST(queries.playlist.whereExists('tracks'));
    // No flip because:
    // all playlists have a track so we must scan all playlists!
    expect(pick(ast, ['where', 'flip'])).toBe(false);
    expect(pick(ast, ['where', 'related', 'subquery', 'where', 'flip'])).toBe(
      false,
    );
  });

  test('tracks with playlist', () => {
    const ast = getPlanAST(queries.track.whereExists('playlists'));
    // TODO: will address after we fix the join cost computation
    expect(pick(ast, ['where', 'flip'])).toBe(false);
  });

  test('has album a or album b', () => {
    const query = queries.track
      .where(({or, exists}) =>
        or(
          exists('album', q => q.where('title', 'Big Ones')),
          exists('album', q => q.where('title', 'Greatest Hits')),
        ),
      )
      .limit(10);

    const ast = getPlanAST(query);

    /*
    For album with title="Big Ones":
    - costWithoutFilters = cost of all albums = ~347
    - costWithFilters = cost of albums matching the filter

    If selectivity = 0.250, then:
    costWithFilters = 0.250 × 347 ≈ 87

    So the cost model is estimating that ~87 albums match the title filter, when in reality probably only 1-2 do.

    If only 1-2 albums out of 347 match (selectivity ~0.3%):
    scanEst = 10 / 0.003 = 3,333 tracks

    In the flipped plan (Attempts 1-2):
      - Album connections cost 80 each currently (with selectivity=0.25)
      - If selectivity was correct (~0.003), album cost would be ~1-2
      - Track connection (child) costs 44 (with its limit applied somehow)
      - Join cost: albumRows * trackCost = 1 * 44 = 44 per branch
      - Total via UFI: 44 + 44 = 88


    The SQLite Cost Model is off because we are missing an index on the name column!
    */
    expect(pick(ast, ['where', 'conditions', 0, 'flip'])).toBe(false);
    expect(pick(ast, ['where', 'conditions', 1, 'flip'])).toBe(false);
  });
});
