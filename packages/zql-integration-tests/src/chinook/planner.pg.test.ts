import {beforeAll, describe, expect, test} from 'vitest';
import {bootstrap} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';
import {planQuery} from '../../../zql/src/planner/planner-builder.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {mapAST} from '../../../zero-protocol/src/ast.ts';
import {
  clientToServer,
  NameMapper,
} from '../../../zero-schema/src/name-mapper.ts';
import type {AnyQuery} from '../../../zql/src/query/query-impl.ts';

const pgContent = await getChinook();

const {dbs, queries} = await bootstrap({
  suiteName: 'chinook_planner',
  pgContent,
  zqlSchema: schema,
});

let costModel: ReturnType<typeof createSQLiteCostModel>;
let mapper: NameMapper;
describe('Chinook planner tests', () => {
  beforeAll(() => {
    dbs.sqlite.exec('ANALYZE;');
    costModel = createSQLiteCostModel(
      dbs.sqlite,
      Object.fromEntries(
        Object.entries(schema.tables).map(([k, v]) => [
          k,
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
    mapper = clientToServer(schema.tables);
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

  // test('has album a or album b', () => {
  //   const ast = getPlanAST(
  //     queries.sqlite.track.where(({or, exists}) =>
  //       or(
  //         exists('album', q => q.where('title', 'Big Ones')),
  //         exists('album', q => q.where('title', 'Greatest Hits')),
  //       ),
  //     ),
  //   );

  //   expect(pick(ast, ['where', 'conditions', 0, 'flip'])).toBe(true);
  //   expect(pick(ast, ['where', 'conditions', 1, 'flip'])).toBe(true);
  // });
});

function getPlanAST(q: AnyQuery) {
  return planQuery(mapAST(q.ast, mapper), costModel);
}

// oxlint-disable-next-line no-explicit-any
function pick(node: any, path: (string | number)[]) {
  let cur = node;
  for (const p of path) {
    cur = cur[p];
    if (cur === undefined) return undefined;
  }
  return cur;
}
