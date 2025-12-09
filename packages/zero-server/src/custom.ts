import {assert} from '../../shared/src/asserts.ts';
import {mapValues} from '../../shared/src/objects.ts';
import {recordProxy} from '../../shared/src/record-proxy.ts';
import {
  formatPgInternalConvert,
  sql,
  sqlConvertColumnArg,
} from '../../z2s/src/sql.ts';
import type {TableSchema} from '../../zero-schema/src/table-schema.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {
  ServerColumnSchema,
  ServerSchema,
  ServerTableSchema,
} from '../../zero-types/src/server-schema.ts';
import type {
  DBTransaction,
  SchemaCRUD,
  ServerTransaction,
  TableCRUD,
} from '../../zql/src/mutate/custom.ts';
import {createRunnableBuilder} from '../../zql/src/query/create-builder.ts';
import {QueryDelegateBase} from '../../zql/src/query/query-delegate-base.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import type {
  HumanReadable,
  Query,
  RunOptions,
} from '../../zql/src/query/query.ts';
import type {SchemaQuery} from '../../zql/src/query/schema-query.ts';
import {getServerSchema} from './schema.ts';

export type CustomMutatorDefs<TDBTransaction> = {
  [namespaceOrKey: string]:
    | CustomMutatorImpl<TDBTransaction>
    | CustomMutatorDefs<TDBTransaction>;
};

export type CustomMutatorImpl<
  TDBTransaction,
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  TArgs = any,
  Context = unknown,
> = (tx: TDBTransaction, args: TArgs, ctx: Context) => Promise<void>;

/**
 * QueryDelegate implementation for server-side transactions.
 * Extends QueryDelegateBase to satisfy the QueryDelegate interface,
 * but overrides run() to execute against Postgres and throws on
 * preload()/materialize() which don't make sense server-side.
 */
class ServerTransactionQueryDelegate extends QueryDelegateBase {
  readonly #dbTransaction: DBTransaction<unknown>;
  readonly #schema: Schema;
  readonly #serverSchema: ServerSchema;

  readonly defaultQueryComplete = true;

  constructor(
    dbTransaction: DBTransaction<unknown>,
    schema: Schema,
    serverSchema: ServerSchema,
  ) {
    super();
    this.#dbTransaction = dbTransaction;
    this.#schema = schema;
    this.#serverSchema = serverSchema;
  }

  getSource(): never {
    throw new Error('not implemented');
  }

  override run<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn,
  >(
    query: Query<TTable, TSchema, TReturn>,
    _options?: RunOptions,
  ): Promise<HumanReadable<TReturn>> {
    const queryInternals = asQueryInternals(query);
    return this.#dbTransaction.runQuery<TReturn>(
      queryInternals.ast,
      queryInternals.format,
      this.#schema,
      this.#serverSchema,
    );
  }

  override preload(): never {
    throw new Error('preload() is not supported in server transactions');
  }

  override materialize(): never {
    throw new Error('materialize() is not supported in server transactions');
  }
}

export class TransactionImpl<TSchema extends Schema, TWrappedTransaction>
  implements ServerTransaction<TSchema, TWrappedTransaction>
{
  readonly location = 'server';
  readonly reason = 'authoritative';
  readonly dbTransaction: DBTransaction<TWrappedTransaction>;
  readonly clientID: string;
  readonly mutationID: number;
  readonly mutate: SchemaCRUD<TSchema>;
  readonly query: SchemaQuery<TSchema>;
  readonly #schema: TSchema;
  readonly #serverSchema: ServerSchema;

  constructor(
    dbTransaction: DBTransaction<TWrappedTransaction>,
    clientID: string,
    mutationID: number,
    mutate: SchemaCRUD<TSchema>,
    schema: TSchema,
    serverSchema: ServerSchema,
  ) {
    this.dbTransaction = dbTransaction;
    this.clientID = clientID;
    this.mutationID = mutationID;
    this.mutate = mutate;
    this.#schema = schema;
    this.#serverSchema = serverSchema;

    const delegate = new ServerTransactionQueryDelegate(
      dbTransaction,
      schema,
      serverSchema,
    );
    this.query = createRunnableBuilder(delegate, schema);
  }

  run<TTable extends keyof TSchema['tables'] & string, TReturn>(
    query: Query<TTable, TSchema, TReturn>,
    _options?: RunOptions,
  ): Promise<HumanReadable<TReturn>> {
    const queryInternals = asQueryInternals(query);

    // Execute the query using the database-specific executor
    return this.dbTransaction.runQuery<TReturn>(
      queryInternals.ast,
      queryInternals.format,
      this.#schema,
      this.#serverSchema,
    );
  }
}

const dbTxSymbol = Symbol();

const serverSchemaSymbol = Symbol();

type WithHiddenTxAndSchema = {
  [dbTxSymbol]: DBTransaction<unknown>;
  [serverSchemaSymbol]: ServerSchema;
};

export async function makeServerTransaction<
  TSchema extends Schema,
  TWrappedTransaction,
>(
  dbTransaction: DBTransaction<TWrappedTransaction>,
  clientID: string,
  mutationID: number,
  schema: TSchema,
  mutate: (
    dbTransaction: DBTransaction<TWrappedTransaction>,
    serverSchema: ServerSchema,
  ) => SchemaCRUD<TSchema>,
) {
  const serverSchema = await getServerSchema(dbTransaction, schema);
  return new TransactionImpl(
    dbTransaction,
    clientID,
    mutationID,
    mutate(dbTransaction, serverSchema),
    schema,
    serverSchema,
  );
}

export function makeSchemaCRUD<S extends Schema>(
  schema: S,
): (
  dbTransaction: DBTransaction<unknown>,
  serverSchema: ServerSchema,
) => SchemaCRUD<S> {
  /**
   * For users with very large schemas it is expensive to re-create
   * all the CRUD mutators for each transaction. Instead, we create
   * them all once up-front and then bind them to the transaction
   * as requested.
   */
  const schemaCRUDs: Record<string, TableCRUD<TableSchema>> = {};
  for (const tableSchema of Object.values(schema.tables)) {
    schemaCRUDs[tableSchema.name] = makeTableCRUD(tableSchema);
  }

  return (
    dbTransaction: DBTransaction<unknown>,
    serverSchema: ServerSchema,
  ) => {
    const txHolder: WithHiddenTxAndSchema = {
      [dbTxSymbol]: dbTransaction,
      [serverSchemaSymbol]: serverSchema,
    };
    return recordProxy(schemaCRUDs, tableCRUD =>
      mapValues(tableCRUD, method => method.bind(txHolder)),
    ) as unknown as SchemaCRUD<S>;
  };
}

function removeUndefined<T extends Record<string, unknown>>(value: T): T {
  const valueWithoutUndefined: Record<string, unknown> = {};
  for (const [key, val] of Object.entries(value)) {
    if (val !== undefined) {
      valueWithoutUndefined[key] = val;
    }
  }
  return valueWithoutUndefined as T;
}

function makeTableCRUD(schema: TableSchema): TableCRUD<TableSchema> {
  return {
    async insert(this: WithHiddenTxAndSchema, value) {
      value = removeUndefined(value);
      const serverTableSchema = this[serverSchemaSymbol][serverName(schema)];

      const targetedColumns = origAndServerNamesFor(Object.keys(value), schema);
      const stmt = formatPgInternalConvert(
        sql`INSERT INTO ${sql.ident(serverName(schema))} (${sql.join(
          targetedColumns.map(([, serverName]) => sql.ident(serverName)),
          ',',
        )}) VALUES (${sql.join(
          Object.entries(value).map(([col, v]) =>
            sqlInsertValue(v, serverTableSchema[serverNameFor(col, schema)]),
          ),
          ', ',
        )})`,
      );
      const tx = this[dbTxSymbol];
      await tx.query(stmt.text, stmt.values);
    },
    async upsert(this: WithHiddenTxAndSchema, value) {
      value = removeUndefined(value);
      const serverTableSchema = this[serverSchemaSymbol][serverName(schema)];
      const targetedColumns = origAndServerNamesFor(Object.keys(value), schema);
      const primaryKeyColumns = origAndServerNamesFor(
        schema.primaryKey,
        schema,
      );
      const stmt = formatPgInternalConvert(
        sql`INSERT INTO ${sql.ident(serverName(schema))} (${sql.join(
          targetedColumns.map(([, serverName]) => sql.ident(serverName)),
          ',',
        )}) VALUES (${sql.join(
          Object.entries(value).map(([col, val]) =>
            sqlInsertValue(val, serverTableSchema[serverNameFor(col, schema)]),
          ),
          ', ',
        )}) ON CONFLICT (${sql.join(
          primaryKeyColumns.map(([, serverName]) => sql.ident(serverName)),
          ', ',
        )}) DO UPDATE SET ${sql.join(
          Object.entries(value).map(
            ([col, val]) =>
              sql`${sql.ident(
                schema.columns[col].serverName ?? col,
              )} = ${sqlInsertValue(val, serverTableSchema[serverNameFor(col, schema)])}`,
          ),
          ', ',
        )}`,
      );
      const tx = this[dbTxSymbol];
      await tx.query(stmt.text, stmt.values);
    },
    async update(this: WithHiddenTxAndSchema, value) {
      value = removeUndefined(value);
      const serverTableSchema = this[serverSchemaSymbol][serverName(schema)];
      const targetedColumns = origAndServerNamesFor(Object.keys(value), schema);
      const stmt = formatPgInternalConvert(
        sql`UPDATE ${sql.ident(serverName(schema))} SET ${sql.join(
          targetedColumns.map(
            ([origName, serverName]) =>
              sql`${sql.ident(serverName)} = ${sqlInsertValue(value[origName], serverTableSchema[serverName])}`,
          ),
          ', ',
        )} WHERE ${primaryKeyClause(schema, serverTableSchema, value)}`,
      );
      const tx = this[dbTxSymbol];
      await tx.query(stmt.text, stmt.values);
    },
    async delete(this: WithHiddenTxAndSchema, value) {
      value = removeUndefined(value);
      const serverTableSchema = this[serverSchemaSymbol][serverName(schema)];
      const stmt = formatPgInternalConvert(
        sql`DELETE FROM ${sql.ident(
          serverName(schema),
        )} WHERE ${primaryKeyClause(schema, serverTableSchema, value)}`,
      );
      const tx = this[dbTxSymbol];
      await tx.query(stmt.text, stmt.values);
    },
  };
}

function serverName(x: {name: string; serverName?: string | undefined}) {
  return x.serverName ?? x.name;
}

function primaryKeyClause(
  schema: TableSchema,
  serverTableSchema: ServerTableSchema,
  row: Record<string, unknown>,
) {
  const primaryKey = origAndServerNamesFor(schema.primaryKey, schema);
  return sql`${sql.join(
    primaryKey.map(
      ([origName, serverName]) =>
        sql`${sql.ident(serverName)}${maybeCastColumn(serverTableSchema[serverName])} = ${sqlValue(row[origName], serverTableSchema[serverName])}`,
    ),
    ' AND ',
  )}`;
}

function maybeCastColumn(col: ServerColumnSchema) {
  if (col.type === 'uuid' || col.isEnum) {
    return sql`::text`;
  }
  return sql``;
}

function origAndServerNamesFor(
  originalNames: readonly string[],
  schema: TableSchema,
): [origName: string, serverName: string][] {
  return originalNames.map(
    name => [name, serverNameFor(name, schema)] as const,
  );
}

function serverNameFor(originalName: string, schema: TableSchema): string {
  const col = schema.columns[originalName];
  assert(
    col,
    `Column ${originalName} was not found in the Zero schema for the table ${schema.name}`,
  );
  return col.serverName ?? originalName;
}

function sqlValue(value: unknown, serverColumnSchema: ServerColumnSchema) {
  return sqlConvertColumnArg(serverColumnSchema, value, false, true);
}

function sqlInsertValue(
  value: unknown,
  serverColumnSchema: ServerColumnSchema,
) {
  return sqlConvertColumnArg(serverColumnSchema, value, false, false);
}
