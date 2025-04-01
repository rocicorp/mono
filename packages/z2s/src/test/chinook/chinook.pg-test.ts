/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/naming-convention */
/**
 * Test suite that
 * 1. Downloads the chinook dataset
 * 2. Allows comparing manually crafted ZQL queries with Postgres output
 *
 * The ZQL will be run from scratch and via
 * diffs. The diffs applied are randomly generated.
 * The seed used for the random generation will be output
 * so that the test can be reproduced.
 */

import {beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {wrapIterable} from '../../../../shared/src/iterables.ts';
import type {
  JSONValue,
  ReadonlyJSONValue,
} from '../../../../shared/src/json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../shared/src/must.ts';
import type {Writable} from '../../../../shared/src/writable.ts';
import {testDBs} from '../../../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../../../zero-cache/src/types/pg.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import {
  clientToServer,
  NameMapper,
} from '../../../../zero-schema/src/name-mapper.ts';
import type {TableSchema} from '../../../../zero-schema/src/table-schema.ts';
import type {Change} from '../../../../zql/src/ivm/change.ts';
import type {Node} from '../../../../zql/src/ivm/data.ts';
import {MemorySource} from '../../../../zql/src/ivm/memory-source.ts';
import type {Input} from '../../../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../../../zql/src/ivm/schema.ts';
import type {Format} from '../../../../zql/src/ivm/view.ts';
import type {ExpressionBuilder} from '../../../../zql/src/query/expression.ts';
import {
  astForTestingSymbol,
  completedAstSymbol,
  newQuery,
  QueryImpl,
  type QueryDelegate,
} from '../../../../zql/src/query/query-impl.ts';
import type {Operator, Query} from '../../../../zql/src/query/query.ts';
import {QueryDelegateImpl as TestMemoryQueryDelegate} from '../../../../zql/src/query/test/query-delegate.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {
  mapResultToClientNames,
  newQueryDelegate,
} from '../../../../zqlite/src/test/source-factory.ts';
import {compile, extractZqlResult} from '../../compiler.ts';
import '../comparePg.ts';
import {writeChinook} from './get-deps.ts';
import {schema} from './schema.ts';
import {formatPgInternalConvert} from '../../sql.ts';
import type {ServerSchema} from '../../schema.ts';
import {
  StaticQuery,
  staticQuery,
} from '../../../../zql/src/query/static-query.ts';

// TODO: Ideally z2s wouldn't depend on zero-pg (even in tests).  These
// chinook tests should move to their own package.
import {Transaction} from '../../../../zero-pg/src/test/util.ts';
import {getServerSchema} from '../../../../zero-pg/src/schema.ts';

let pg: PostgresDB;
let sqlite: Database;
let serverSchema: ServerSchema;
let zqliteQueryDelegate: QueryDelegate;
let memoryQueryDelegate: QueryDelegate;
type AnyQuery = Query<any, any, any>;

type Schema = typeof schema;
type Queries = {
  album: Query<Schema, 'album'>;
  artist: Query<Schema, 'artist'>;
  customer: Query<Schema, 'customer'>;
  employee: Query<Schema, 'employee'>;
  genre: Query<Schema, 'genre'>;
  mediaType: Query<Schema, 'mediaType'>;
  playlist: Query<Schema, 'playlist'>;
  playlistTrack: Query<Schema, 'playlistTrack'>;
  invoice: Query<Schema, 'invoice'>;
  invoiceLine: Query<Schema, 'invoiceLine'>;
  track: Query<Schema, 'track'>;
};
const zqliteQueries: Queries = {
  album: null,
  artist: null,
  customer: null,
  employee: null,
  genre: null,
  mediaType: null,
  playlist: null,
  playlistTrack: null,
  invoice: null,
  invoiceLine: null,
  track: null,
} as any;
const memoryQueries: Queries = {...zqliteQueries} as any;
const tables = Object.keys(zqliteQueries) as (keyof typeof zqliteQueries)[];

const lc = createSilentLogContext();

function makeMemorySources() {
  return Object.fromEntries(
    Object.entries(schema.tables).map(([key, tableSchema]) => [
      key,
      new MemorySource(
        tableSchema.name,
        tableSchema.columns,
        tableSchema.primaryKey,
      ),
    ]),
  );
}

const data: Map<string, Row[]> = new Map();

beforeEach(async () => {
  pg = await testDBs.create('chinook', undefined, false);
  sqlite = new Database(lc, ':memory:');
  const memorySources = makeMemorySources();
  await writeChinook(pg, sqlite);

  serverSchema = await pg.begin(tx =>
    getServerSchema(new Transaction(tx), schema),
  );

  zqliteQueryDelegate = newQueryDelegate(lc, testLogConfig, sqlite, schema);
  memoryQueryDelegate = new TestMemoryQueryDelegate(memorySources);

  tables.forEach(table => {
    zqliteQueries[table] = newQuery(zqliteQueryDelegate, schema, table) as any;
    memoryQueries[table] = newQuery(memoryQueryDelegate, schema, table) as any;
  });

  await Promise.all(
    tables.map(async table => {
      const rows = mapResultToClientNames<Row[], typeof schema>(
        await zqliteQueries[table].run(),
        schema,
        table,
      );
      data.set(table, rows);
      for (const row of rows) {
        memorySources[table].push({
          type: 'add',
          row,
        });
      }
    }),
  );
});

test('limited junction edge', () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-explicit-any
  function getSQL(q: Query<any, any, any>) {
    return formatPgInternalConvert(
      compile(
        (q as StaticQuery<Schema, keyof Schema['tables']>).ast,
        schema.tables,
        serverSchema,
        q.format,
      ),
    ).text;
  }

  const q = staticQuery(schema, 'playlist').related('tracks', q => q.limit(10));
  expect(getSQL(q)).toMatchInlineSnapshot(`
    "SELECT COALESCE(json_agg(row_to_json("root")) , '[]'::json)::TEXT as "zql_result" FROM (SELECT (
            SELECT COALESCE(json_agg(row_to_json("inner_tracks")) , '[]'::json) FROM (SELECT "table_1"."track_id" as "id","table_1"."name","table_1"."album_id" as "albumId","table_1"."media_type_id" as "mediaTypeId","table_1"."genre_id" as "genreId","table_1"."composer","table_1"."milliseconds","table_1"."bytes","table_1"."unit_price" as "unitPrice" FROM "playlist_track" as "playlistTrack" JOIN "track" as "table_1" ON "playlistTrack"."track_id" = "table_1"."track_id" WHERE ("playlist"."playlist_id" = "playlistTrack"."playlist_id")  ORDER BY "playlistTrack"."playlist_id" ASC, "playlistTrack"."track_id" ASC LIMIT $1::text::numeric ) "inner_tracks"
          ) as "tracks","playlist"."playlist_id" as "id","playlist"."name" FROM "playlist"   ORDER BY "playlist"."playlist_id" ASC )"root""
  `);
});

describe('basic select', () => {
  test.each(tables.map(table => [table]))(
    'select * from %s',
    async table => {
      await checkZqlAndSql(pg, zqliteQueries[table], memoryQueries[table]);
    },
    20_000,
  );

  test.each(tables.map(table => [table]))(
    'select * from %s limit 100',
    async table => {
      await checkZqlAndSql(
        pg,
        zqliteQueries[table].limit(100),
        memoryQueries[table].limit(100),
      );
    },
  );
});

describe('1 level related', () => {
  const brokenRelationships = [
    // Bad type conversion. We need to convert types when doing JSON aggregation
    // as `postgresTypeConfig` does.
    'supportRep',
    'reportsTo',
  ];
  function getQueriesAndRelationships(table: keyof Schema['tables']) {
    const zqliteQuery = zqliteQueries[table] as AnyQuery;
    const memoryQuery = memoryQueries[table] as AnyQuery;
    const relationships = Object.keys(
      (schema.relationships as Record<string, Record<string, unknown>>)[
        table
      ] ?? {},
    );
    return {zqliteQuery, memoryQuery, relationships};
  }

  test.each(tables.map(table => [table]))('%s w/ related', async table => {
    const {zqliteQuery, memoryQuery, relationships} =
      getQueriesAndRelationships(table);

    for (const r of relationships) {
      if (brokenRelationships.includes(r)) {
        continue;
      }
      await checkZqlAndSql(
        pg,
        zqliteQuery.related(r),
        memoryQuery.related(r),
        false,
      );
    }
  });

  test.each(tables.map(table => [table]))(
    '%s w/ related limit 100',
    async table => {
      const {zqliteQuery, memoryQuery, relationships} =
        getQueriesAndRelationships(table);

      // Junction edges do not correctly handle limits
      // in ZQL 😬
      const brokenLimits = ['tracks'];
      for (const r of relationships) {
        if (brokenRelationships.includes(r) || brokenLimits.includes(r)) {
          continue;
        }
        await checkZqlAndSql(
          pg,
          zqliteQuery.related(r, q => q.limit(100)).limit(100),
          memoryQuery.related(r, q => q.limit(100)).limit(100),
        );
      }
    },
  );
});

function randomRowAndColumn(table: string) {
  const rows = must(data.get(table));
  const randomRow = rows[Math.floor(Math.random() * rows.length)];
  const columns = Object.keys(randomRow);
  const columnIndex = Math.floor(Math.random() * columns.length);
  const randomColumn = columns[columnIndex];
  return {randomRow, randomColumn};
}

function randomOperator(): Operator {
  const operators = ['=', '!=', '>', '>=', '<', '<='] as const;
  return operators[Math.floor(Math.random() * operators.length)];
}

describe('or', () => {
  // This is currently unsupported in z2s
  // test.each(tables.map(table => [table]))('0-branches %s', async table => {
  //   await checkZqlAndSql(
  //     pg,
  //     (zqliteQueries[table] as AnyQuery).where(({or}) => or()),
  //     (memoryQueries[table] as AnyQuery).where(({or}) => or()),
  //   );
  // });

  test.each(tables.map(table => [table]))('1-branch %s', async table => {
    const {randomRow, randomColumn} = randomRowAndColumn(table);
    await checkZqlAndSql(
      pg,
      (zqliteQueries[table] as AnyQuery).where(({or, cmp}) =>
        or(cmp(randomColumn as any, '=', randomRow[randomColumn])),
      ),
      (memoryQueries[table] as AnyQuery).where(({or, cmp}) =>
        or(cmp(randomColumn as any, '=', randomRow[randomColumn])),
      ),
    );
  });

  test.each(tables.map(table => [table]))('N-branches %s', async table => {
    const n = 5;
    const rowsAndColumns = Array.from({length: n}, () =>
      randomRowAndColumn(table),
    );
    const operators = Array.from({length: n}, () => randomOperator());
    function q({or, cmp}: ExpressionBuilder<any, any>) {
      return or(
        ...rowsAndColumns.map(({randomRow, randomColumn}, i) =>
          cmp(randomColumn as any, operators[i], randomRow[randomColumn]),
        ),
      );
    }
    await checkZqlAndSql(
      pg,
      (zqliteQueries[table] as AnyQuery).where(q),
      (memoryQueries[table] as AnyQuery).where(q),
    );
  });

  // This checks the short-circuit case of edit
  // that previously broke us. See discord-repro.pg-test.ts
  test.each(tables.map(table => [table]))(
    'contradictory-branches %s',
    async table => {
      const {randomRow, randomColumn} = randomRowAndColumn(table);
      function q({or, cmp}: ExpressionBuilder<any, any>) {
        return or(
          cmp(randomColumn as any, '=', randomRow[randomColumn]),
          cmp(randomColumn as any, '!=', randomRow[randomColumn]),
        );
      }
      await checkZqlAndSql(
        pg,
        (zqliteQueries[table] as AnyQuery).where(q),
        (memoryQueries[table] as AnyQuery).where(q),
        true,
        [[table, randomRow]],
      );
    },
  );

  test('exists in a branch', async () => {
    for (let i = 0; i < 4; ++i) {
      const {randomRow} = randomRowAndColumn('invoice');
      const q = ({or, cmp, exists}: ExpressionBuilder<Schema, 'invoice'>) =>
        or(
          cmp('customerId', '=', randomRow.customerId as number),
          exists('lines'),
        );
      await checkZqlAndSql(
        pg,
        zqliteQueries.invoice.where(q),
        memoryQueries.invoice.where(q),
        true,
        [['invoice', randomRow]],
      );
    }
  });
});

async function checkZqlAndSql(
  pg: PostgresDB,
  zqliteQuery: Query<Schema, keyof Schema['tables']>,
  memoryQuery: Query<Schema, keyof Schema['tables']>,
  // flag to disable push checking.
  // There are some perf issues to debug where some
  // tests take too long to run push checks.
  shouldCheckPush = true,
  mustEditRows?: [table: string, row: Row][],
) {
  const pgResult = await runZqlAsSql(pg, zqliteQuery);
  const zqliteResult = await zqliteQuery.run();
  const zqlMemResult = await memoryQuery.run();
  const ast = (zqliteQuery as QueryImpl<Schema, any>)[astForTestingSymbol];
  // In failure output:
  // `-` is PG
  // `+` is ZQL
  expect(
    mapResultToClientNames(
      zqliteResult,
      schema,
      ast.table as keyof Schema['tables'],
    ),
  ).toEqualPg(pgResult);
  expect(zqlMemResult).toEqualPg(pgResult);

  // now check pushes
  if (shouldCheckPush) {
    await checkPush(pg, zqliteQuery, memoryQuery, mustEditRows);
  }
}

async function checkPush(
  pg: PostgresDB,
  zqliteQuery: Query<Schema, keyof Schema['tables']>,
  memoryQuery: Query<Schema, keyof Schema['tables']>,
  mustEditRows?: [table: string, row: Row][],
) {
  const queryRows = gatherRows(memoryQuery);

  function copyRows() {
    return new Map(
      wrapIterable(queryRows.entries()).map(([table, rows]) => [
        table,
        [...rows.values()],
      ]),
    );
  }

  const totalNumRows = [...queryRows.values()].reduce(
    (acc, rows) => acc + rows.size,
    0,
  );

  const interval = Math.floor(totalNumRows / 10);
  const removedRows = await checkRemove(
    interval,
    copyRows(),
    pg,
    zqliteQuery,
    memoryQuery,
    mustEditRows,
  );
  await checkAddBack(removedRows, pg, zqliteQuery, memoryQuery);
  const editedRows = await checkEditToRandom(
    interval,
    copyRows(),
    pg,
    zqliteQuery,
    memoryQuery,
    mustEditRows,
  );
  await checkEditToMatch(editedRows, pg, zqliteQuery, memoryQuery);
}

// Removes all rows that are in the result set
// one at a time till there are no rows left.
// Randomly selects which table to remove a row from on each iteration.
async function checkRemove(
  removalInterval: number,
  queryRows: Map<string, Row[]>,
  sql: PostgresDB,
  zqliteQuery: Query<Schema, keyof Schema['tables']>,
  memoryQuery: Query<Schema, keyof Schema['tables']>,
  mustEditRows: [table: string, row: Row][] = [],
): Promise<[table: string, row: Row][]> {
  const tables = [...queryRows.keys()];

  const zqliteMaterialized = zqliteQuery.materialize();
  const zqlMaterialized = memoryQuery.materialize();

  let numOps = 0;
  const removedRows: [string, Row][] = [];
  const seen = new Set<string>();
  while (tables.length > 0) {
    ++numOps;
    const tableIndex = Math.floor(Math.random() * tables.length);
    const table = tables[tableIndex];
    const rows = must(queryRows.get(table));
    const rowIndex = Math.floor(Math.random() * rows.length);
    const row = must(rows[rowIndex]);
    rows.splice(rowIndex, 1);

    if (rows.length === 0) {
      tables.splice(tableIndex, 1);
    }

    // doing this for all rows of a large table
    // is too slow we only do it every `removalInterval`
    if (numOps % removalInterval === 0) {
      await run(table, row);
    }
  }

  for (const [table, row] of mustEditRows) {
    if (!seen.has(pullPrimaryKey(table, row))) {
      await run(table, row);
    }
  }

  async function run(table: string, row: Row) {
    seen.add(pullPrimaryKey(table, row));
    removedRows.push([table, row]);
    const {primaryKey} = schema.tables[table as keyof Schema['tables']];
    const mappedRow = mapRow(row, table, clientToServerMapper);
    await sql`DELETE FROM ${sql(
      clientToServerMapper.tableName(table),
    )} WHERE ${primaryKey
      .map(
        col =>
          sql`${sql(clientToServerMapper.columnName(table, col))} = ${
            row[col] ?? null
          }`,
      )
      .reduce((l, r) => sql`${l} AND ${r}`)}`;

    must(
      zqliteQueryDelegate.getSource(clientToServerMapper.tableName(table)),
    ).push({
      type: 'remove',
      row: mappedRow,
    });
    must(memoryQueryDelegate.getSource(table)).push({
      type: 'remove',
      row,
    });

    const pgResult = await runZqlAsSql(pg, zqliteQuery);
    // TODO: empty single relationships return `undefined` from ZQL and `null` from PG
    expect(
      mapResultToClientNames(
        zqliteMaterialized.data,
        schema,
        (zqliteQuery as QueryImpl<Schema, any>)[astForTestingSymbol]
          .table as keyof Schema['tables'],
      ),
    ).toEqualPg(pgResult);
    expect(zqlMaterialized.data).toEqualPg(pgResult);
  }

  zqliteMaterialized.destroy();
  zqlMaterialized.destroy();

  return removedRows;
}

function pullPrimaryKey(table: string, row: Row): string {
  const {primaryKey} = schema.tables[table as keyof Schema['tables']];
  return primaryKey.map(col => row[col] ?? '').join('-');
}

async function checkAddBack(
  rowsToAdd: [string, Row][],
  sql: PostgresDB,
  zqliteQuery: Query<Schema, keyof Schema['tables']>,
  memoryQuery: Query<Schema, keyof Schema['tables']>,
) {
  const zqliteMaterialized = zqliteQuery.materialize();
  const zqlMaterialized = memoryQuery.materialize();
  const mapper = clientToServer(schema.tables);

  for (const [table, row] of rowsToAdd) {
    const mappedRow = mapRow(row, table, clientToServerMapper);
    await sql`INSERT INTO ${sql(mapper.tableName(table))} ${sql(mappedRow)}`;

    must(
      zqliteQueryDelegate.getSource(clientToServerMapper.tableName(table)),
    ).push({
      type: 'add',
      row: mappedRow,
    });
    must(memoryQueryDelegate.getSource(table)).push({
      type: 'add',
      row,
    });

    const pgResult = await runZqlAsSql(pg, zqliteQuery);
    expect(
      mapResultToClientNames(
        zqliteMaterialized.data,
        schema,
        (zqliteQuery as QueryImpl<Schema, any>)[astForTestingSymbol]
          .table as keyof Schema['tables'],
      ),
    ).toEqualPg(pgResult);
    expect(zqlMaterialized.data).toEqualPg(pgResult);
  }

  zqlMaterialized.destroy();
  zqliteMaterialized.destroy();
}

// TODO: we should handle foreign keys more intelligently
async function checkEditToRandom(
  removalInterval: number,
  queryRows: Map<string, Row[]>,
  sql: PostgresDB,
  zqliteQuery: Query<Schema, keyof Schema['tables']>,
  memoryQuery: Query<Schema, keyof Schema['tables']>,
  mustEditRows: [table: string, row: Row][] = [],
): Promise<[table: string, [original: Row, edited: Row]][]> {
  const tables = [...queryRows.keys()];

  const zqliteMaterialized = zqliteQuery.materialize();
  const zqlMaterialized = memoryQuery.materialize();

  let numOps = 0;
  const editedRows: [string, [original: Row, edited: Row]][] = [];
  const seen = new Set<string>();
  while (tables.length > 0) {
    ++numOps;
    const tableIndex = Math.floor(Math.random() * tables.length);
    const table = tables[tableIndex];
    const rows = must(queryRows.get(table));
    const rowIndex = Math.floor(Math.random() * rows.length);
    const row = must(rows[rowIndex]);
    rows.splice(rowIndex, 1);

    if (rows.length === 0) {
      tables.splice(tableIndex, 1);
    }

    if (numOps % removalInterval === 0) {
      await run(table, row);
    }
  }

  for (const [table, row] of mustEditRows) {
    if (!seen.has(pullPrimaryKey(table, row))) {
      await run(table, row);
    }
  }

  async function run(table: string, row: Row) {
    seen.add(pullPrimaryKey(table, row));
    const tableSchema = schema.tables[table as keyof Schema['tables']];
    const {primaryKey} = tableSchema;
    const editedRow = assignRandomValues(tableSchema, row);
    editedRows.push([table, [row, editedRow]]);
    const mappedRow = mapRow(row, table, clientToServerMapper);
    const mappedEditedRow = mapRow(editedRow, table, clientToServerMapper);

    await sql`UPDATE ${sql(clientToServerMapper.tableName(table))} SET ${sql(
      mappedEditedRow,
    )} WHERE ${primaryKey
      .map(
        col =>
          sql`${sql(clientToServerMapper.columnName(table, col))} = ${
            row[col] ?? null
          }`,
      )
      .reduce((l, r) => sql`${l} AND ${r}`)}`;

    must(
      zqliteQueryDelegate.getSource(clientToServerMapper.tableName(table)),
    ).push({
      type: 'edit',
      oldRow: mappedRow,
      row: mappedEditedRow,
    });
    must(memoryQueryDelegate.getSource(table)).push({
      type: 'edit',
      oldRow: row,
      row: editedRow,
    });

    const pgResult = await runZqlAsSql(pg, zqliteQuery);
    // TODO: relationships return `undefined` from ZQL and `null` from PG
    expect(
      mapResultToClientNames(
        zqliteMaterialized.data,
        schema,
        (zqliteQuery as QueryImpl<Schema, any>)[astForTestingSymbol]
          .table as keyof Schema['tables'],
      ),
    ).toEqualPg(pgResult);
    expect(zqlMaterialized.data).toEqualPg(pgResult);
  }

  zqliteMaterialized.destroy();
  zqlMaterialized.destroy();

  return editedRows;
}

function assignRandomValues(schema: TableSchema, row: Row): Row {
  const newRow: Record<string, ReadonlyJSONValue | undefined> = {...row};
  for (const [col, colSchema] of Object.entries(schema.columns)) {
    if (schema.primaryKey.includes(col)) {
      continue;
    }
    switch (colSchema.type) {
      case 'boolean':
        newRow[col] = Math.random() > 0.5;
        break;
      case 'number':
        newRow[col] = Math.floor(Math.random() * 100);
        break;
      case 'string':
        newRow[col] = Math.random().toString(36).substring(7);
        break;
      case 'json':
        newRow[col] = {random: Math.random()};
        break;
      case 'null':
        newRow[col] = null;
        break;
    }
  }
  return newRow;
}

async function checkEditToMatch(
  rowsToEdit: [string, [original: Row, edited: Row]][],
  sql: PostgresDB,
  zqliteQuery: Query<Schema, keyof Schema['tables']>,
  memoryQuery: Query<Schema, keyof Schema['tables']>,
) {
  const zqliteMaterialized = zqliteQuery.materialize();
  const zqlMaterialized = memoryQuery.materialize();

  for (const [table, [original, edited]] of rowsToEdit) {
    const tableSchema = schema.tables[table as keyof Schema['tables']];
    const {primaryKey} = tableSchema;
    const mappedOriginal = mapRow(original, table, clientToServerMapper);
    const mappedEdited = mapRow(edited, table, clientToServerMapper);
    await sql`UPDATE ${sql(clientToServerMapper.tableName(table))} SET ${sql(
      mappedOriginal,
    )} WHERE ${primaryKey
      .map(
        col =>
          sql`${sql(clientToServerMapper.columnName(table, col))} = ${
            original[col] ?? null
          }`,
      )
      .reduce((l, r) => sql`${l} AND ${r}`)}`;

    must(
      zqliteQueryDelegate.getSource(clientToServerMapper.tableName(table)),
    ).push({
      type: 'edit',
      oldRow: mappedEdited,
      row: mappedOriginal,
    });
    must(memoryQueryDelegate.getSource(table)).push({
      type: 'edit',
      oldRow: edited,
      row: original,
    });

    const pgResult = await runZqlAsSql(pg, zqliteQuery);
    expect(
      mapResultToClientNames(
        zqliteMaterialized.data,
        schema,
        (zqliteQuery as QueryImpl<Schema, any>)[astForTestingSymbol]
          .table as keyof Schema['tables'],
      ),
    ).toEqualPg(pgResult);
    expect(zqlMaterialized.data).toEqualPg(pgResult);
  }

  zqlMaterialized.destroy();
  zqliteMaterialized.destroy();
}

const clientToServerMapper = clientToServer(schema.tables);
function gatherRows(q: AnyQuery): Map<string, Map<string, Row>> {
  const rows = new Map<string, Map<string, Row>>();

  const view = q.materialize(
    (
      _query: AnyQuery,
      input: Input,
      _format: Format,
      onDestroy: () => void,
      _onTransactionCommit: (cb: () => void) => void,
      _queryComplete: true | Promise<true>,
    ) => {
      const schema = input.getSchema();
      for (const node of input.fetch({})) {
        processNode(schema, node);
      }

      return {
        push: (_change: Change) => {
          throw new Error('should not receive a push');
        },
        destroy() {
          onDestroy();
        },
      } as const;
    },
  );

  function processNode(schema: SourceSchema, node: Node) {
    const {tableName: table} = schema;
    let rowsForTable = rows.get(table);
    if (rowsForTable === undefined) {
      rowsForTable = new Map();
      rows.set(table, rowsForTable);
    }
    rowsForTable.set(pullPrimaryKey(table, node.row), node.row);
    for (const [relationship, getChildren] of Object.entries(
      node.relationships,
    )) {
      const childSchema = must(schema.relationships[relationship]);
      for (const child of getChildren()) {
        processNode(childSchema, child);
      }
    }
  }

  function pullPrimaryKey(table: string, row: Row): string {
    const {primaryKey} = schema.tables[table as keyof Schema['tables']];
    return primaryKey.map(col => row[col] ?? '').join('-');
  }

  view.destroy();
  return rows;
}

async function runZqlAsSql(
  pg: PostgresDB,
  query: Query<Schema, keyof Schema['tables']>,
) {
  const sqlQuery = formatPgInternalConvert(
    compile(ast(query), schema.tables, serverSchema, query.format),
  );
  return extractZqlResult(
    await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
  );
}

function ast(q: Query<Schema, keyof Schema['tables']>) {
  return (q as QueryImpl<Schema, keyof Schema['tables']>)[completedAstSymbol];
}

function mapRow(row: Row, table: string, mapper: NameMapper): Row {
  const newRow: Writable<Row> = {};
  for (const [column, value] of Object.entries(row)) {
    newRow[mapper.columnName(table, column)] = value;
  }
  return newRow;
}
