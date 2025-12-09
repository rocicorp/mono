import {bench, run, summary} from 'mitata';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {afterAll, expect} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {unreachable} from '../../../shared/src/asserts.ts';
import {wrapIterable} from '../../../shared/src/iterables.ts';
import type {JSONValue, ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {Writable} from '../../../shared/src/writable.ts';
import {compile, extractZqlResult} from '../../../z2s/src/compiler.ts';
import {formatPgInternalConvert} from '../../../z2s/src/sql.ts';
import {initialSync} from '../../../zero-cache/src/services/change-source/pg/initial-sync.ts';
import {getConnectionURI, testDBs} from '../../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../../zero-cache/src/types/pg.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {
  clientToServer,
  type NameMapper,
} from '../../../zero-schema/src/name-mapper.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import {makeSchemaCRUD} from '../../../zero-server/src/custom.ts';
import {executePostgresQuery} from '../../../zero-server/src/pg-query-executor.ts';
import {getServerSchema} from '../../../zero-server/src/schema.ts';
import {Transaction} from '../../../zero-server/src/test/util.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {ServerSchema} from '../../../zero-types/src/server-schema.ts';
import type {Change} from '../../../zql/src/ivm/change.ts';
import type {Node} from '../../../zql/src/ivm/data.ts';
import {
  defaultFormat,
  type Format,
} from '../../../zql/src/ivm/default-format.ts';
import {MemorySource} from '../../../zql/src/ivm/memory-source.ts';
import {skipYields} from '../../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../../zql/src/ivm/schema.ts';
import type {Source, SourceChange} from '../../../zql/src/ivm/source.ts';
import {consume} from '../../../zql/src/ivm/stream.ts';
import type {DBTransaction} from '../../../zql/src/mutate/custom.ts';
import {QueryDelegateBase} from '../../../zql/src/query/query-delegate-base.ts';
import type {QueryDelegate} from '../../../zql/src/query/query-delegate.ts';
import {QueryImpl} from '../../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {
  AnyQuery,
  HumanReadable,
  Query,
  RunOptions,
} from '../../../zql/src/query/query.ts';
import {QueryDelegateImpl as TestMemoryQueryDelegate} from '../../../zql/src/query/test/query-delegate.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {
  mapResultToClientNames,
  newQueryDelegate,
} from '../../../zqlite/src/test/source-factory.ts';
import '../helpers/comparePg.ts';

const lc = createSilentLogContext();

type DBs<TSchema extends Schema> = {
  pg: PostgresDB;
  sqlite: Database;
  memory: Record<keyof TSchema['tables'], MemorySource>;
  raw: ReadonlyMap<keyof TSchema['tables'], readonly Row[]>;
  pgSchema: ServerSchema;
  sqliteFile: string;
};

type Delegates = {
  pg: TestPGQueryDelegate;
  sqlite: QueryDelegate;
  memory: QueryDelegate;
  mapper: NameMapper;
};

type QueryBuilders<TSchema extends Schema> = {
  [K in keyof TSchema['tables'] & string]: Query<K, TSchema>;
};

let tempDir: string | undefined;
afterAll(async () => {
  if (tempDir) {
    await fs.rm(tempDir, {recursive: true, force: true});
  }
  tempDir = undefined;
});

async function makeDatabases<TSchema extends Schema>(
  suiteName: string,
  schema: TSchema,
  pgContent: string,
  // Test data must be in client format
  testData?: (serverSchema: ServerSchema) => Record<string, Row[]>,
): Promise<DBs<TSchema>> {
  const pg = await testDBs.create(suiteName, undefined, false);
  await pg.unsafe(pgContent);

  const serverSchema = await pg.begin(tx =>
    getServerSchema(new Transaction(tx), schema),
  );

  // If there is test data it is assumed to be in ZQL format.
  // We insert via schemaCRUD which is good since this will flex
  // custom mutator insertion code.
  if (testData) {
    await pg.begin(async tx => {
      const crud = makeSchemaCRUD(schema)(new Transaction(tx), serverSchema);

      for (const [table, rows] of Object.entries(testData(serverSchema))) {
        // oxlint-disable-next-line @typescript-eslint/no-explicit-any
        await Promise.all(rows.map(row => crud[table].insert(row as any)));
      }
    });
  }

  tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'zero-integration-tests'));
  const tempFile = path.join(tempDir, `${suiteName}.db`);
  const sqlite = new Database(lc, tempFile);
  sqlite.pragma('journal_mode = WAL2');

  await initialSync(
    lc,
    {appID: suiteName, shardNum: 0, publications: []},
    sqlite,
    getConnectionURI(pg),
    {tableCopyWorkers: 1},
  );

  const memory = Object.fromEntries(
    Object.entries(schema.tables).map(([key, tableSchema]) => [
      key,
      new MemorySource(
        tableSchema.name,
        tableSchema.columns,
        tableSchema.primaryKey,
      ),
    ]),
  ) as Record<keyof TSchema['tables'], MemorySource>;

  const raw = new Map<keyof TSchema['tables'], Row[]>();

  // We fill the memory sources with the data from the pg database
  // since the pg database could have had insert statements applied in pgContent.
  // This is especially true of pre-canned datasets like Chinook.
  await Promise.all(
    Object.values(schema.tables).map(async table => {
      const sqlQuery = formatPgInternalConvert(
        compile(serverSchema, schema, {
          table: table.name,
        }),
      );
      const rows = extractZqlResult(
        await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
      ) as Row[];
      raw.set(table.name, rows);
      for (const row of rows) {
        consume(
          memory[table.name].push({
            type: 'add',
            row,
          }),
        );
      }
    }),
  );

  return {
    pg,
    sqlite,
    memory,
    raw,
    pgSchema: serverSchema,
    sqliteFile: tempFile,
  };
}

function makeDelegates<TSchema extends Schema>(
  dbs: DBs<TSchema>,
  schema: TSchema,
): Delegates {
  return {
    pg: new TestPGQueryDelegate(dbs.pg, schema, dbs.pgSchema),
    sqlite: newQueryDelegate(lc, testLogConfig, dbs.sqlite, schema),
    memory: new TestMemoryQueryDelegate({sources: dbs.memory}),
    mapper: clientToServer(schema.tables),
  };
}

function makeQueries<TSchema extends Schema>(
  schema: TSchema,
): QueryBuilders<TSchema> {
  const ret: Record<string, Query<string, TSchema>> = {};

  for (const table of Object.keys(schema.tables)) {
    ret[table] = new QueryImpl(schema, table, {table}, defaultFormat, 'test');
  }

  return ret as QueryBuilders<TSchema>;
}

type Options<TSchema extends Schema> = {
  suiteName: string;
  zqlSchema: TSchema;
  only?: string;
  // pg schema and, optionally, data to insert.
  pgContent: string;
  // Optional test data to insert (using client names).
  // You may also run insert statements in `pgContent`.
  testData?: (serverSchema: ServerSchema) => Record<string, Row[]>;
  push?: number;
  setRawData?: (
    raw: ReadonlyMap<keyof TSchema['tables'], readonly Row[]>,
  ) => void;
};

type BenchOptionsBase<TSchema extends Schema> = {
  suiteName: string;
  zqlSchema: TSchema;
  only?: string;
  pgContent: string;
  setRawData?: (
    raw: ReadonlyMap<keyof TSchema['tables'], readonly Row[]>,
  ) => void;
};

type BenchOptions<TSchema extends Schema> =
  | HydrationOptions<TSchema>
  | PushOptions<TSchema>;

type HydrationOptions<TSchema extends Schema> = {
  type: 'hydration';
} & BenchOptionsBase<TSchema>;

type PushOptions<TSchema extends Schema> = {
  type: 'push';
} & BenchOptionsBase<TSchema>;

export async function createVitests<TSchema extends Schema>(
  {
    suiteName,
    zqlSchema,
    pgContent,
    only,
    testData,
    push: pushEvery,
    setRawData,
  }: Options<TSchema>,
  ...testSpecs: (readonly {
    name: string;
    createQuery: (
      q: QueryBuilders<TSchema>,
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    ) => Query<string, TSchema, any>;
    manualVerification?: unknown;
  }[])[]
) {
  const dbs = await makeDatabases(suiteName, zqlSchema, pgContent, testData);
  const delegates = makeDelegates(dbs, zqlSchema);
  if (setRawData) {
    setRawData(dbs.raw);
  }

  return testSpecs
    .flat()
    .filter(t => (only ? t.name.includes(only) : true))
    .map(({name, createQuery, manualVerification}) => ({
      name,
      fn: makeTest(
        zqlSchema,
        dbs,
        delegates,
        createQuery,
        pushEvery ?? 1,
        manualVerification,
      ),
    }));
}

export type PushGenerator = (
  iteration: number,
) => [source: string, change: SourceChange][];

export async function runBenchmarks<TSchema extends Schema>(
  {suiteName, type, zqlSchema, pgContent, only}: HydrationOptions<TSchema>,
  ...benchSpecs: (readonly {
    name: string;
    createQuery: (q: QueryBuilders<TSchema>) => Query<string, TSchema>;
  }[])[]
): Promise<void>;
export async function runBenchmarks<TSchema extends Schema>(
  {suiteName, type, zqlSchema, pgContent, only}: PushOptions<TSchema>,
  ...benchSpecs: (readonly {
    name: string;
    createQuery: (q: QueryBuilders<TSchema>) => Query<string, TSchema>;
    generatePush: PushGenerator;
  }[])[]
): Promise<void>;
export async function runBenchmarks<TSchema extends Schema>(
  {
    suiteName,
    type,
    zqlSchema,
    pgContent,
    only,
    setRawData,
  }: BenchOptions<TSchema>,
  ...benchSpecs: (readonly {
    name: string;
    createQuery: (q: QueryBuilders<TSchema>) => Query<string, TSchema>;
    generatePush?: PushGenerator;
  }[])[]
): Promise<void> {
  const {dbs, delegates, queries} = await bootstrap({
    suiteName,
    zqlSchema,
    pgContent,
  });
  if (setRawData) {
    setRawData(dbs.raw);
  }

  benchSpecs
    .flat()
    .filter(t => (only ? only.includes(t.name) : true))
    .map(({name, createQuery, generatePush}) =>
      summary(() => {
        makeBenchmark({
          name,
          zqlSchema,
          delegates,
          dbs,
          createQuery,
          type,
          queries,
          generatePush,
        });
      }),
    );

  // Check if JSON output is requested via environment variable
  const format = process.env.BENCH_OUTPUT_FORMAT;

  if (format === 'json') {
    // Output JSON without samples for smaller, cleaner output
    await run({
      format: {
        json: {
          samples: false,
          debug: false,
        },
      },
    });
  } else {
    await run();
  }
}

export async function bootstrap<TSchema extends Schema>({
  suiteName,
  zqlSchema,
  pgContent,
}: {
  suiteName: string;
  zqlSchema: TSchema;
  pgContent: string;
}) {
  const dbs = await makeDatabases(suiteName, zqlSchema, pgContent);
  const delegates = makeDelegates(dbs, zqlSchema);
  const queries = makeQueries(zqlSchema);

  async function transact(cb: (delegates: Delegates) => Promise<void>) {
    await dbs.pg.begin(async tx => {
      const scopedDelegates: Delegates = {
        ...delegates,
        pg: new TestPGQueryDelegate(tx, zqlSchema, dbs.pgSchema),
        memory: new TestMemoryQueryDelegate({
          sources: Object.fromEntries(
            Object.entries(dbs.memory).map(([key, source]) => [
              key,
              source.fork(),
            ]),
          ),
        }),
        sqlite: newQueryDelegate(
          lc,
          testLogConfig,
          (() => {
            const db = new Database(lc, dbs.sqliteFile);
            db.exec('BEGIN CONCURRENT');
            return db;
          })(),
          zqlSchema,
        ),
      };
      await cb(scopedDelegates);
    });
  }

  return {
    dbs,
    delegates,
    queries,
    transact,
  };
}

function makeBenchmark<TSchema extends Schema>({
  name,
  zqlSchema,
  createQuery,
  type,
  queries,
  delegates,
  dbs,
  generatePush,
}: {
  name: string;
  zqlSchema: TSchema;
  createQuery: (q: QueryBuilders<TSchema>) => Query<string, TSchema>;
  type: 'hydration' | 'push';
  queries: QueryBuilders<TSchema>;
  delegates: Delegates;
  dbs: DBs<TSchema>;
  generatePush: PushGenerator | undefined;
}) {
  switch (type) {
    case 'push': {
      // isolate delegates to own transactions
      // so push benchmarks are isolated from each other.
      const newDelegates = {
        ...delegates,
        memory: new TestMemoryQueryDelegate({
          sources: Object.fromEntries(
            Object.entries(dbs.memory).map(([key, source]) => [
              key,
              source.fork(),
            ]),
          ),
        }),
        sqlite: newQueryDelegate(
          lc,
          testLogConfig,
          (() => {
            const db = new Database(lc, dbs.sqliteFile);
            db.exec('BEGIN CONCURRENT');
            return db;
          })(),
          zqlSchema,
        ),
      };
      const queries = makeQueries(zqlSchema);
      const query = createQuery(queries);

      // materialize before starting the benchmark. We only want to time push
      // and not the initial hydration.
      benchPush(name, 'zql', query, newDelegates, must(generatePush));
      benchPush(name, 'zqlite', query, newDelegates, must(generatePush));

      break;
    }
    case 'hydration': {
      const q = createQuery(queries);
      benchHydration(`zql: ${name}`, delegates.memory, q);
      benchHydration(`zqlite: ${name}`, delegates.sqlite, q);
      benchHydration(`zpg: ${name}`, delegates.pg, q);
      break;
    }
  }
}

function benchHydration(name: string, delegate: QueryDelegate, q: AnyQuery) {
  bench(name, async () => {
    await delegate.run(q);
  });
}

function benchPush(
  name: string,
  type: 'zql' | 'zqlite',
  query: AnyQuery,
  delegates: Delegates,
  pushGenerator: PushGenerator,
) {
  let iteration = 0;
  bench(`${type}: ${name}`, function* () {
    // setup
    const delegate = type === 'zqlite' ? delegates.sqlite : delegates.memory;
    const view = delegate.materialize(query);

    // benchmark
    yield () => {
      let changes = pushGenerator(iteration++);
      let sourceGetter;
      if (type === 'zqlite') {
        changes = changes.map(([source, change]): [string, SourceChange] => {
          switch (change.type) {
            case 'add':
              return [
                source,
                {
                  ...change,
                  row: mapRow(change.row, source, delegates.mapper),
                },
              ];
            case 'edit':
              return [
                source,
                {
                  ...change,
                  oldRow: mapRow(change.oldRow, source, delegates.mapper),
                  row: mapRow(change.row, source, delegates.mapper),
                },
              ];
            case 'remove':
              return [
                source,
                {
                  ...change,
                  row: mapRow(change.row, source, delegates.mapper),
                },
              ];
          }
        });

        sourceGetter = (source: string) =>
          must(delegates.sqlite.getSource(delegates.mapper.tableName(source)));
      } else {
        sourceGetter = (source: string) =>
          must(delegates.memory.getSource(source));
      }

      for (const change of changes) {
        const [source, changeData] = change;
        const sourceInstance = sourceGetter(source);
        consume(sourceInstance.push(changeData));
      }
    };

    // tear down
    view.destroy();
  });
}

function makeTest<TSchema extends Schema>(
  zqlSchema: TSchema,
  // we could open a separate transaction for each test so we
  // have complete isolation. Hence why `dbs` is here (as a reminder for future improvement).
  // ZPG supports transactions. ZQLite wouldn't be much more work to add it.
  // Memory can do it by forking the sources as we do in custom mutators on rebase.
  dbs: DBs<TSchema>,
  delegates: Delegates,
  createQuery: (q: QueryBuilders<TSchema>) => Query<string, TSchema>,
  pushEvery: number,
  manualVerification?: unknown,
) {
  return async () => {
    await dbs.pg.begin(async tx => {
      // Isolate the test from other tests by putting each test in its own transaction.
      delegates = {
        ...delegates,
        pg: new TestPGQueryDelegate(tx, zqlSchema, dbs.pgSchema),
        memory: new TestMemoryQueryDelegate({
          sources: Object.fromEntries(
            Object.entries(dbs.memory).map(([key, source]) => [
              key,
              source.fork(),
            ]),
          ),
        }),
        sqlite: newQueryDelegate(
          lc,
          testLogConfig,
          (() => {
            const db = new Database(lc, dbs.sqliteFile);
            db.exec('BEGIN CONCURRENT');
            return db;
          })(),
          zqlSchema,
        ),
      };

      const queryBuilders = makeQueries(zqlSchema);
      const query = createQuery(queryBuilders);

      await runAndCompare(zqlSchema, delegates, query, manualVerification);

      if (pushEvery > 0) {
        await checkPush(zqlSchema, delegates, query, pushEvery);
      }
    });
  };
}

export async function runAndCompare(
  zqlSchema: Schema,
  delegates: Delegates,
  query: AnyQuery,
  manualVerification: unknown,
) {
  const pgResult = await delegates.pg.run(query);
  // Might we worth being able to configure ZQLite to return client vs server names
  const sqliteResult = mapResultToClientNames(
    await delegates.sqlite.run(query),
    zqlSchema,
    asQueryInternals(query).ast.table,
  );
  const memoryResult = await delegates.memory.run(query);

  // - is PG
  // + is SQLite / Memory
  expect(memoryResult).toEqualPg(pgResult);
  expect(sqliteResult).toEqualPg(pgResult);
  if (manualVerification) {
    expect(manualVerification).toEqualPg(pgResult);
  }
}

async function checkPush(
  zqlSchema: Schema,
  delegates: Delegates,
  query: AnyQuery,
  pushEvery: number,
  mustEditRows?: [table: string, row: Row][],
) {
  const queryRows = gatherRows(zqlSchema, query, delegates.memory);

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

  const interval = Math.floor(totalNumRows / pushEvery);
  const removedRows = await checkRemove(
    zqlSchema,
    delegates,
    interval,
    copyRows(),
    query,
    mustEditRows,
  );
  await checkAddBack(zqlSchema, delegates, removedRows, query);
  const editedRows = await checkEditToRandom(
    zqlSchema,
    delegates,
    interval,
    copyRows(),
    query,
    mustEditRows,
  );
  await checkEditToMatch(zqlSchema, delegates, editedRows, query);
}

function gatherRows(
  zqlSchema: Schema,
  q: AnyQuery,
  queryDelegate: QueryDelegate,
): Map<string, Map<string, Row>> {
  const rows = new Map<string, Map<string, Row>>();

  const view = queryDelegate.materialize(
    q,
    (
      _query,
      input,
      _format,
      onDestroy,
      _onTransactionCommit,
      _queryComplete,
    ) => {
      const schema = input.getSchema();
      for (const node of skipYields(input.fetch({}))) {
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
    rowsForTable.set(pullPrimaryKey(zqlSchema, table, node.row), node.row);
    for (const [relationship, getChildren] of Object.entries(
      node.relationships,
    )) {
      const childSchema = must(schema.relationships[relationship]);
      for (const child of skipYields(getChildren())) {
        processNode(childSchema, child);
      }
    }
  }

  view.destroy();
  return rows;
}

function pullPrimaryKey(zqlSchema: Schema, table: string, row: Row): string {
  const {primaryKey} = zqlSchema.tables[table];
  return primaryKey.map(col => row[col] ?? '').join('-');
}

// Removes all rows that are in the result set
// one at a time till there are no rows left.
// Randomly selects which table to remove a row from on each iteration.
async function checkRemove(
  zqlSchema: Schema,
  delegates: Delegates,
  removalInterval: number,
  queryRows: Map<string, Row[]>,
  query: AnyQuery,
  mustEditRows: [table: string, row: Row][] = [],
): Promise<[table: string, row: Row][]> {
  const tables = [...queryRows.keys()];

  const zqliteMaterialized = delegates.sqlite.materialize(query);
  const zqlMaterialized = delegates.memory.materialize(query);

  let numOps = 0;
  const removedRows: [string, Row][] = [];
  const seen = new Set<string>();

  const crud = makeSchemaCRUD(zqlSchema)(
    delegates.pg.transaction,
    delegates.pg.serverSchema,
  );
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
    if (!seen.has(pullPrimaryKey(zqlSchema, table, row))) {
      await run(table, row);
    }
  }

  async function run(table: string, row: Row) {
    seen.add(pullPrimaryKey(zqlSchema, table, row));
    removedRows.push([table, row]);
    const mappedRow = mapRow(row, table, delegates.mapper);

    await crud[table].delete(row);

    consume(
      must(delegates.sqlite.getSource(delegates.mapper.tableName(table))).push({
        type: 'remove',
        row: mappedRow,
      }),
    );
    consume(
      must(delegates.memory.getSource(table)).push({
        type: 'remove',
        row,
      }),
    );

    // pg cannot be materialized.
    const pgResult = await delegates.pg.run(query);
    expect(
      mapResultToClientNames(
        zqliteMaterialized.data,
        zqlSchema,
        asQueryInternals(query).ast.table,
      ),
    ).toEqualPg(pgResult);
    expect(zqlMaterialized.data).toEqualPg(pgResult);
  }

  zqliteMaterialized.destroy();
  zqlMaterialized.destroy();

  return removedRows;
}

function mapRow(row: Row, table: string, mapper: NameMapper): Row {
  const newRow: Writable<Row> = {};
  for (const [column, value] of Object.entries(row)) {
    newRow[mapper.columnName(table, column)] = value;
  }
  return newRow;
}

async function checkAddBack(
  zqlSchema: Schema,
  delegates: Delegates,
  rowsToAdd: [string, Row][],
  query: AnyQuery,
) {
  const zqliteMaterialized = delegates.sqlite.materialize(query);
  const zqlMaterialized = delegates.memory.materialize(query);

  const crud = makeSchemaCRUD(zqlSchema)(
    delegates.pg.transaction,
    delegates.pg.serverSchema,
  );
  for (const [table, row] of rowsToAdd) {
    const mappedRow = mapRow(row, table, delegates.mapper);
    await crud[table].insert(row);

    consume(
      must(delegates.sqlite.getSource(delegates.mapper.tableName(table))).push({
        type: 'add',
        row: mappedRow,
      }),
    );
    consume(
      must(delegates.memory.getSource(table)).push({
        type: 'add',
        row,
      }),
    );

    const pgResult = await delegates.pg.run(query);
    expect(
      mapResultToClientNames(
        zqliteMaterialized.data,
        zqlSchema,
        asQueryInternals(query).ast.table,
      ),
    ).toEqualPg(pgResult);
    expect(zqlMaterialized.data).toEqualPg(pgResult);
  }

  zqlMaterialized.destroy();
  zqliteMaterialized.destroy();
}

// TODO: we should handle foreign keys more intelligently
async function checkEditToRandom(
  zqlSchema: Schema,
  delegates: Delegates,
  removalInterval: number,
  queryRows: Map<string, Row[]>,
  query: AnyQuery,
  mustEditRows: [table: string, row: Row][] = [],
): Promise<[table: string, [original: Row, edited: Row]][]> {
  const tables = [...queryRows.keys()];

  const zqliteMaterialized = delegates.sqlite.materialize(query);
  const zqlMaterialized = delegates.memory.materialize(query);

  let numOps = 0;
  const editedRows: [string, [original: Row, edited: Row]][] = [];
  const seen = new Set<string>();

  const crud = makeSchemaCRUD(zqlSchema)(
    delegates.pg.transaction,
    delegates.pg.serverSchema,
  );
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
    if (!seen.has(pullPrimaryKey(zqlSchema, table, row))) {
      await run(table, row);
    }
  }

  async function run(table: string, row: Row) {
    seen.add(pullPrimaryKey(zqlSchema, table, row));
    const tableSchema = zqlSchema.tables[table];
    const editedRow = assignRandomValues(tableSchema, row);
    editedRows.push([table, [row, editedRow]]);
    const mappedRow = mapRow(row, table, delegates.mapper);
    const mappedEditedRow = mapRow(editedRow, table, delegates.mapper);

    await crud[table].update(editedRow);
    consume(
      must(delegates.sqlite.getSource(delegates.mapper.tableName(table))).push({
        type: 'edit',
        oldRow: mappedRow,
        row: mappedEditedRow,
      }),
    );
    consume(
      must(delegates.memory.getSource(table)).push({
        type: 'edit',
        oldRow: row,
        row: editedRow,
      }),
    );

    const pgResult = await delegates.pg.run(query);
    expect(
      mapResultToClientNames(
        zqliteMaterialized.data,
        zqlSchema,
        asQueryInternals(query).ast.table,
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
      default:
        unreachable(colSchema.type);
    }
  }
  return newRow;
}

async function checkEditToMatch(
  zqlSchema: Schema,
  delegates: Delegates,
  rowsToEdit: [string, [original: Row, edited: Row]][],
  query: AnyQuery,
) {
  const zqliteMaterialized = delegates.sqlite.materialize(query);
  const zqlMaterialized = delegates.memory.materialize(query);

  const crud = makeSchemaCRUD(zqlSchema)(
    delegates.pg.transaction,
    delegates.pg.serverSchema,
  );
  for (const [table, [original, edited]] of rowsToEdit) {
    const mappedOriginal = mapRow(original, table, delegates.mapper);
    const mappedEdited = mapRow(edited, table, delegates.mapper);
    await crud[table].update(original);

    consume(
      must(delegates.sqlite.getSource(delegates.mapper.tableName(table))).push({
        type: 'edit',
        oldRow: mappedEdited,
        row: mappedOriginal,
      }),
    );
    consume(
      must(delegates.memory.getSource(table)).push({
        type: 'edit',
        oldRow: edited,
        row: original,
      }),
    );

    const pgResult = await delegates.pg.run(query);
    expect(
      mapResultToClientNames(
        zqliteMaterialized.data,
        zqlSchema,
        asQueryInternals(query).ast.table,
      ),
    ).toEqualPg(pgResult);
    expect(zqlMaterialized.data).toEqualPg(pgResult);
  }

  zqliteMaterialized.destroy();
  zqlMaterialized.destroy();
}

class TestPGQueryDelegate extends QueryDelegateBase {
  readonly #pg: PostgresDB;
  readonly #schema: Schema;
  readonly serverSchema: ServerSchema;
  readonly transaction: DBTransaction<PostgresDB>;

  constructor(pg: PostgresDB, schema: Schema, serverSchema: ServerSchema) {
    super();
    this.#pg = pg;
    this.#schema = schema;
    this.serverSchema = serverSchema;
    this.transaction = {
      query: (query: string, args: unknown[]) =>
        pg.unsafe(query, args as JSONValue[]),
      wrappedTransaction: pg,
      runQuery<TReturn>(
        ast: AST,
        format: Format,
        schema: Schema,
        serverSchema: ServerSchema,
      ): Promise<HumanReadable<TReturn>> {
        return executePostgresQuery<TReturn>(
          this,
          ast,
          format,
          schema,
          serverSchema,
        );
      },
    };
  }

  readonly defaultQueryComplete = false;

  getSource(_tableName: string): Source | undefined {
    return undefined;
  }

  query(query: string, args: unknown[]) {
    return this.#pg.unsafe(query, args as JSONValue[]);
  }

  override async run<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn,
  >(
    query: Query<TTable, TSchema, TReturn>,
    _options?: RunOptions,
  ): Promise<HumanReadable<TReturn>> {
    const queryInternals = asQueryInternals(query);
    const sqlQuery = formatPgInternalConvert(
      compile(
        this.serverSchema,
        this.#schema,
        queryInternals.ast,
        queryInternals.format,
      ),
    );
    const result = await this.query(
      sqlQuery.text,
      sqlQuery.values as JSONValue[],
    );
    // Handle empty results for .one() queries
    if (result.length === 0 && queryInternals.format.singular) {
      return undefined as unknown as HumanReadable<TReturn>;
    }
    return extractZqlResult(result) as HumanReadable<TReturn>;
  }
}
