import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {getConnectionURI, testDBs} from '../../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../../zero-cache/src/types/pg.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {MemorySource} from '../../../zql/src/ivm/memory-source.ts';
import {
  astForTestingSymbol,
  defaultFormat,
  newQuery,
  QueryImpl,
  type QueryDelegate,
} from '../../../zql/src/query/query-impl.ts';
import type {Query} from '../../../zql/src/query/query.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {
  mapResultToClientNames,
  newQueryDelegate,
} from '../../../zqlite/src/test/source-factory.ts';
import {QueryDelegateImpl as TestMemoryQueryDelegate} from '../../../zql/src/query/test/query-delegate.ts';
import {ZPGQuery} from '../../../zero-pg/src/query.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import {Transaction} from '../../../zero-pg/src/test/util.ts';
import {getServerSchema} from '../../../zero-pg/src/schema.ts';
import type {ServerSchema} from '../../../z2s/src/schema.ts';
import type {DBTransaction} from '../../../zql/src/mutate/custom.ts';
import {initialSync} from '../../../zero-cache/src/services/change-source/pg/initial-sync.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {expect} from 'vitest';
import '../helpers/comparePg.ts';
import {compile, extractZqlResult} from '../../../z2s/src/compiler.ts';
import {formatPgInternalConvert} from '../../../z2s/src/sql.ts';
import {makeSchemaCRUD} from '../../../zero-pg/src/custom.ts';

const lc = createSilentLogContext();

type DBs<TSchema extends Schema> = {
  pg: PostgresDB;
  sqlite: Database;
  memory: Record<keyof TSchema['tables'], MemorySource>;
  raw: ReadonlyMap<keyof TSchema['tables'], readonly Row[]>;
};

type Delegates = {
  pg: {
    transaction: DBTransaction<unknown>;
    serverSchema: ServerSchema;
  };
  sqlite: QueryDelegate;
  memory: QueryDelegate;
};

type Queries<TSchema extends Schema> = {
  [K in keyof TSchema['tables'] & string]: Query<TSchema, K>;
};

type QueriesBySource<TSchema extends Schema> = {
  pg: Queries<TSchema>;
  sqlite: Queries<TSchema>;
  memory: Queries<TSchema>;
};

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
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        await Promise.all(rows.map(row => crud[table].insert(row as any)));
      }
    });
  }

  const sqlite = new Database(lc, ':memory:');

  await initialSync(
    lc,
    {appID: suiteName, shardNum: 0, publications: []},
    sqlite,
    getConnectionURI(pg),
    {tableCopyWorkers: 1, rowBatchSize: 10000},
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
        compile(
          {
            table: table.name,
          },
          schema.tables,
          serverSchema,
        ),
      );
      const rows = extractZqlResult(
        await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
      ) as Row[];
      raw.set(table.name, rows);
      for (const row of rows) {
        memory[table.name].push({
          type: 'add',
          row,
        });
      }
    }),
  );

  return {pg, sqlite, memory, raw};
}

async function makeDelegates<TSchema extends Schema>(
  dbs: DBs<TSchema>,
  schema: TSchema,
): Promise<Delegates> {
  const serverSchema = await dbs.pg.begin(tx =>
    getServerSchema(new Transaction(tx), schema),
  );
  return {
    pg: {
      transaction: {
        query(query: string, args: unknown[]) {
          return dbs.pg.unsafe(query, args as JSONValue[]);
        },
        wrappedTransaction: dbs.pg,
      },
      serverSchema,
    },
    sqlite: newQueryDelegate(lc, testLogConfig, dbs.sqlite, schema),
    memory: new TestMemoryQueryDelegate(dbs.memory),
  };
}

function makeQueries<TSchema extends Schema>(
  schema: TSchema,
  delegates: Delegates,
): QueriesBySource<TSchema> {
  const ret: {
    pg: Record<string, Query<TSchema, string>>;
    sqlite: Record<string, Query<TSchema, string>>;
    memory: Record<string, Query<TSchema, string>>;
  } = {
    pg: {},
    sqlite: {},
    memory: {},
  };

  Object.keys(schema.tables).forEach(table => {
    // Life would be nice if zpg was constructed the same as zqlite and memory.
    ret.pg[table] = new ZPGQuery(
      schema,
      delegates.pg.serverSchema,
      table,
      delegates.pg.transaction,
      {table},
      defaultFormat,
    );
    ret.memory[table] = newQuery(delegates.memory, schema, table);
    ret.sqlite[table] = newQuery(delegates.sqlite, schema, table);
  });

  return ret as QueriesBySource<TSchema>;
}

type Options<TSchema extends Schema> = {
  suiteName: string;
  zqlSchema: TSchema;
  // pg schema and, optionally, data to insert.
  pgContent: string;
  // Optional test data to insert (using client names).
  // You may also run insert statements in `pgContent`.
  testData?: (serverSchema: ServerSchema) => Record<string, Row[]>;
};

export async function createVitests<TSchema extends Schema>(
  {suiteName, zqlSchema, pgContent, testData}: Options<TSchema>,
  testSpecs: readonly {
    name: string;
    createQuery: (q: Queries<TSchema>) => Query<TSchema, string>;
    manualVerification?: unknown;
  }[],
) {
  const dbs = await makeDatabases(suiteName, zqlSchema, pgContent, testData);
  const delegates = await makeDelegates(dbs, zqlSchema);
  const queryBuilders = makeQueries(zqlSchema, delegates);

  return testSpecs.map(({name, createQuery, manualVerification}) => ({
    name,
    fn: makeTest(
      zqlSchema,
      dbs,
      queryBuilders,
      createQuery,
      manualVerification,
    ),
  }));
}

function makeTest<TSchema extends Schema>(
  zqlSchema: TSchema,
  // we could open a separate transaction for each test so we
  // have complete isolation. Hence why `dbs` is here (as a reminder for future improvement).
  // ZPG supports transactions. ZQLite wouldn't be much more work to add it.
  // Memory can do it by forking the sources as we do in custom mutators on rebase.
  _dbs: DBs<TSchema>,
  queryBuilders: QueriesBySource<TSchema>,
  createQuery: (q: Queries<TSchema>) => Query<TSchema, string>,
  manualVerification?: unknown,
) {
  return async () => {
    const queries = {
      pg: createQuery(queryBuilders.pg),
      sqlite: createQuery(queryBuilders.sqlite),
      memory: createQuery(queryBuilders.memory),
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const ast = (queries.sqlite as unknown as QueryImpl<Schema, string>)[
      astForTestingSymbol
    ];
    const pgResult = await queries.pg;
    // Might we worth being able to configure ZQLite to return client vs server names
    const sqliteResult = mapResultToClientNames(
      await queries.sqlite,
      zqlSchema,
      ast.table,
    );
    const memoryResult = await queries.memory;

    expect(memoryResult).toEqualPg(pgResult);
    expect(sqliteResult).toEqualPg(pgResult);
    if (manualVerification) {
      expect(manualVerification).toEqualPg(pgResult);
    }
  };
}
