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

import '../nullish.ts';
import {beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../../../../zero-cache/src/test/db.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import type {PostgresDB} from '../../../../zero-cache/src/types/pg.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {writeChinook} from './get-deps.ts';
import {
  completedAstSymbol,
  newQuery,
  QueryImpl,
  type QueryDelegate,
} from '../../../../zql/src/query/query-impl.ts';
import {newQueryDelegate} from '../../../../zqlite/src/test/source-factory.ts';
import type {LogConfig} from '../../../../otel/src/log-options.ts';
import {schema} from './schema.ts';
import type {Query} from '../../../../zql/src/query/query.ts';
import {formatPg} from '../../sql.ts';
import {compile} from '../../compiler.ts';
import type {
  JSONValue,
  ReadonlyJSONValue,
} from '../../../../shared/src/json.ts';
import {MemorySource} from '../../../../zql/src/ivm/memory-source.ts';
import {QueryDelegateImpl as TestMemoryQueryDelegate} from '../../../../zql/src/query/test/query-delegate.ts';
import type {AdvancedQuery} from '../../../../zql/src/query/query-internal.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {Input} from '../../../../zql/src/ivm/operator.ts';
import type {Format} from '../../../../zql/src/ivm/view.ts';
import type {SourceSchema} from '../../../../zql/src/ivm/schema.ts';
import type {Change} from '../../../../zql/src/ivm/change.ts';
import {must} from '../../../../shared/src/must.ts';
import type {Node} from '../../../../zql/src/ivm/data.ts';
import {wrapIterable} from '../../../../shared/src/iterables.ts';
import type {TableSchema} from '../../../../zero-schema/src/table-schema.ts';

let pg: PostgresDB;
let sqlite: Database;
let zqliteQueryDelegate: QueryDelegate;
let memoryQueryDelegate: QueryDelegate;
type AnyQuery = Query<any, any, any>;
type AnyAdvancedQuery = AdvancedQuery<any, any, any>;

type Schema = typeof schema;
type Queries = {
  album: Query<Schema, 'album'>;
  artist: Query<Schema, 'artist'>;
  customer: Query<Schema, 'customer'>;
  employee: Query<Schema, 'employee'>;
  genre: Query<Schema, 'genre'>;
  media_type: Query<Schema, 'media_type'>;
  playlist: Query<Schema, 'playlist'>;
  playlist_track: Query<Schema, 'playlist_track'>;
  invoice: Query<Schema, 'invoice'>;
  invoice_line: Query<Schema, 'invoice_line'>;
  track: Query<Schema, 'track'>;
};
const zqliteQueries: Queries = {
  album: null,
  artist: null,
  customer: null,
  employee: null,
  genre: null,
  media_type: null,
  playlist: null,
  playlist_track: null,
  invoice: null,
  invoice_line: null,
  track: null,
} as any;
const memoryQueries: Queries = {...zqliteQueries} as any;
const tables = Object.keys(zqliteQueries) as (keyof typeof zqliteQueries)[];

const lc = createSilentLogContext();
const logConfig: LogConfig = {
  format: 'text',
  level: 'debug',
  ivmSampling: 0,
  slowRowThreshold: 0,
};

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

beforeEach(async () => {
  pg = await testDBs.create('chinook');
  sqlite = new Database(lc, ':memory:');
  const memorySources = makeMemorySources();
  await writeChinook(pg, sqlite);

  zqliteQueryDelegate = newQueryDelegate(lc, logConfig, sqlite, schema);
  memoryQueryDelegate = new TestMemoryQueryDelegate(memorySources);

  tables.forEach(table => {
    zqliteQueries[table] = newQuery(zqliteQueryDelegate, schema, table) as any;
    memoryQueries[table] = newQuery(memoryQueryDelegate, schema, table) as any;
  });

  await Promise.all(
    tables.map(async table => {
      const rows = await zqliteQueries[table].run();
      for (const row of rows) {
        memorySources[table].push({
          type: 'add',
          row,
        });
      }
    }),
  );
});

describe('basic select', () => {
  test.each(tables.map(table => [table]))('select * from %s', async table => {
    await checkZqlAndSql(pg, zqliteQueries[table], memoryQueries[table]);
  });

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

async function checkZqlAndSql(
  pg: PostgresDB,
  zqliteQuery: Query<Schema, keyof Schema['tables']>,
  memoryQuery: Query<Schema, keyof Schema['tables']>,
  // flag to disable push checking.
  // There are some perf issues to debug where some
  // tests take too long to run push checks.
  shouldCheckPush = true,
) {
  const pgResult = await runZqlAsSql(pg, zqliteQuery);
  const zqliteResult = await zqliteQuery.run();
  const zqlMemResult = await memoryQuery.run();
  // In failure output:
  // `-` is PG
  // `+` is ZQLite
  expect(zqliteResult).toEqual(pgResult);
  expect(zqlMemResult).toEqual(pgResult);

  // now check pushes
  if (shouldCheckPush) {
    await checkPush(pg, zqliteQuery, memoryQuery);
  }
}

async function checkPush(
  pg: PostgresDB,
  zqliteQuery: Query<Schema, keyof Schema['tables']>,
  memoryQuery: Query<Schema, keyof Schema['tables']>,
) {
  const queryRows = gatherRows(memoryQuery as unknown as AnyAdvancedQuery);

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
  );
  await checkAddBack(removedRows, pg, zqliteQuery, memoryQuery);
  const editedRows = await checkEditToRandom(
    interval,
    copyRows(),
    pg,
    zqliteQuery,
    memoryQuery,
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
): Promise<[table: string, row: Row][]> {
  const tables = [...queryRows.keys()];

  const zqliteMaterialized = zqliteQuery.materialize();
  const zqlMaterialized = memoryQuery.materialize();
  const sqlQuery = formatPg(compile(ast(zqliteQuery), format(zqliteQuery)));

  let numOps = 0;
  const removedRows: [string, Row][] = [];
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
      removedRows.push([table, row]);
      const {primaryKey} = schema.tables[table as keyof Schema['tables']];
      await sql`DELETE FROM ${sql(table)} WHERE ${primaryKey
        .map(col => sql`${sql(col)} = ${row[col] ?? null}`)
        .reduce((l, r) => sql`${l} AND ${r}`)}`;

      must(zqliteQueryDelegate.getSource(table)).push({
        type: 'remove',
        row,
      });
      must(memoryQueryDelegate.getSource(table)).push({
        type: 'remove',
        row,
      });

      const pgResult = await sql.unsafe(
        sqlQuery.text,
        sqlQuery.values as JSONValue[],
      );
      // TODO: relationships return `undefined` from ZQL and `null` from PG
      expect(zqliteMaterialized.data).toEqualNullish(pgResult);
      expect(zqlMaterialized.data).toEqualNullish(pgResult);
    }
  }

  zqliteMaterialized.destroy();
  zqlMaterialized.destroy();

  return removedRows;
}

async function checkAddBack(
  rowsToAdd: [string, Row][],
  sql: PostgresDB,
  zqliteQuery: Query<Schema, keyof Schema['tables']>,
  memoryQuery: Query<Schema, keyof Schema['tables']>,
) {
  const zqliteMaterialized = zqliteQuery.materialize();
  const zqlMaterialized = memoryQuery.materialize();
  const sqlQuery = formatPg(compile(ast(zqliteQuery), format(zqliteQuery)));

  for (const [table, row] of rowsToAdd) {
    await sql`INSERT INTO ${sql(table)} ${sql(row)}`;

    must(zqliteQueryDelegate.getSource(table)).push({
      type: 'add',
      row,
    });
    must(memoryQueryDelegate.getSource(table)).push({
      type: 'add',
      row,
    });

    const pgResult = await pg.unsafe(
      sqlQuery.text,
      sqlQuery.values as JSONValue[],
    );
    expect(zqliteMaterialized.data).toEqualNullish(pgResult);
    expect(zqlMaterialized.data).toEqualNullish(pgResult);
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
): Promise<[table: string, [original: Row, edited: Row]][]> {
  const tables = [...queryRows.keys()];

  const zqliteMaterialized = zqliteQuery.materialize();
  const zqlMaterialized = memoryQuery.materialize();
  const sqlQuery = formatPg(compile(ast(zqliteQuery), format(zqliteQuery)));

  let numOps = 0;
  const editedRows: [string, [original: Row, edited: Row]][] = [];
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
      const tableSchema = schema.tables[table as keyof Schema['tables']];
      const {primaryKey} = tableSchema;
      const editedRow = assignRandomValues(tableSchema, row);
      editedRows.push([table, [row, editedRow]]);

      await sql`UPDATE ${sql(table)} SET ${sql(editedRow)} WHERE ${primaryKey
        .map(col => sql`${sql(col)} = ${row[col] ?? null}`)
        .reduce((l, r) => sql`${l} AND ${r}`)}`;

      must(zqliteQueryDelegate.getSource(table)).push({
        type: 'edit',
        oldRow: row,
        row: editedRow,
      });
      must(memoryQueryDelegate.getSource(table)).push({
        type: 'edit',
        oldRow: row,
        row: editedRow,
      });

      const pgResult = await sql.unsafe(
        sqlQuery.text,
        sqlQuery.values as JSONValue[],
      );
      // TODO: relationships return `undefined` from ZQL and `null` from PG
      expect(zqliteMaterialized.data).toEqualNullish(pgResult);
      expect(zqlMaterialized.data).toEqualNullish(pgResult);
    }
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
  const sqlQuery = formatPg(compile(ast(zqliteQuery), format(zqliteQuery)));

  for (const [table, [original, edited]] of rowsToEdit) {
    const tableSchema = schema.tables[table as keyof Schema['tables']];
    const {primaryKey} = tableSchema;
    await sql`UPDATE ${sql(table)} SET ${sql(original)} WHERE ${primaryKey
      .map(col => sql`${sql(col)} = ${original[col] ?? null}`)
      .reduce((l, r) => sql`${l} AND ${r}`)}`;

    must(zqliteQueryDelegate.getSource(table)).push({
      type: 'edit',
      oldRow: edited,
      row: original,
    });
    must(memoryQueryDelegate.getSource(table)).push({
      type: 'edit',
      oldRow: edited,
      row: original,
    });

    const pgResult = await pg.unsafe(
      sqlQuery.text,
      sqlQuery.values as JSONValue[],
    );
    expect(zqliteMaterialized.data).toEqualNullish(pgResult);
    expect(zqlMaterialized.data).toEqualNullish(pgResult);
  }

  zqlMaterialized.destroy();
  zqliteMaterialized.destroy();
}

function gatherRows(q: AnyAdvancedQuery): Map<string, Map<string, Row>> {
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

function runZqlAsSql(
  pg: PostgresDB,
  query: Query<Schema, keyof Schema['tables']>,
) {
  const sqlQuery = formatPg(compile(ast(query), format(query)));
  return pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]);
}

function ast(q: Query<Schema, keyof Schema['tables']>) {
  return (q as QueryImpl<Schema, keyof Schema['tables']>)[completedAstSymbol];
}

function format(q: Query<Schema, keyof Schema['tables']>) {
  return (q as QueryImpl<Schema, keyof Schema['tables']>).format;
}
