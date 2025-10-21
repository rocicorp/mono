import {beforeAll, describe, expect, test} from 'vitest';
import {bootstrap} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {
  clientToServer,
  NameMapper,
} from '../../../zero-schema/src/name-mapper.ts';
import {makeGetPlanAST, pick} from '../helpers/planner.ts';

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

    costModel = createSQLiteCostModel(
      dbs.sqlite,
      Object.fromEntries(
        Object.entries(schema.tables).map(([k, v]) => [
          'serverName' in v ? v.serverName : k,
          {
            columns: Object.fromEntries(
              Object.entries(v.columns).map(([colName, col]) => [
                'serverName' in col ? col.serverName : colName,
                {
                  ...col,
                },
              ]),
            ),
            primaryKey: v.primaryKey,
          },
        ]),
      ),
    );

    getPlanAST = makeGetPlanAST(mapper, costModel);
  });

  test('tracks for a given album', () => {
    const ast = getPlanAST(
      queries.sqlite.track.whereExists('album', q =>
        q.where('title', 'Big Ones'),
      ),
    );

    expect(pick(ast, ['where', 'flip'])).toBe(true);
  });

  test('has album and artist', () => {
    const ast = getPlanAST(
      queries.sqlite.track
        .whereExists('album', q => q.where('title', 'Big Ones'))
        .whereExists('genre', q => q.where('name', 'Rock')),
    );

    expect(pick(ast, ['where', 'conditions', 0, 'flip'])).toBe(true);
    expect(pick(ast, ['where', 'conditions', 1, 'flip'])).toBe(false);
  });

  test('playlist with track', () => {
    const ast = getPlanAST(queries.sqlite.playlist.whereExists('tracks'));
    expect(pick(ast, ['where', 'flip'])).toBe(true);
    expect(pick(ast, ['where', 'related', 'subquery', 'where', 'flip'])).toBe(
      false,
    );
  });

  test('tracks with playlist', () => {
    const ast = getPlanAST(queries.sqlite.track.whereExists('playlists'));

    // playlist table is smaller. Should be flipped.
    expect(pick(ast, ['where', 'flip'])).toBe(true);
  });

  test('has album a or album b', () => {
    const ast = getPlanAST(
      queries.sqlite.track.where(({or, exists}) =>
        or(
          exists('album', q => q.where('title', 'Big Ones')),
          exists('album', q => q.where('title', 'Greatest Hits')),
        ),
      ),
    );

    expect(pick(ast, ['where', 'conditions', 0, 'flip'])).toBe(true);
    expect(pick(ast, ['where', 'conditions', 1, 'flip'])).toBe(true);
  });
});
