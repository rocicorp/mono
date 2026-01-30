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
import {
  type CRUDExecutor,
  type CRUDExecutorOptions,
  type CRUDKind,
  makeCRUDMutate,
  makeTransactionMutate,
  type SchemaCRUD,
  type TableCRUD,
  type TransactionMutate,
  type UpsertOptions,
} from '../../zql/src/mutate/crud.ts';
import type {
  DBTransaction,
  MutateCRUD,
  ServerTransaction,
} from '../../zql/src/mutate/custom.ts';
import {createRunnableBuilder} from '../../zql/src/query/create-builder.ts';
import {QueryDelegateBase} from '../../zql/src/query/query-delegate-base.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import type {
  HumanReadable,
  Query,
  RunOptions,
} from '../../zql/src/query/query.ts';
import type {ConditionalSchemaQuery} from '../../zql/src/query/schema-query.ts';
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
  readonly mutate: TransactionMutate<TSchema>;
  /**
   * @deprecated Use {@linkcode createBuilder} with `tx.run(zql.table.where(...))` instead.
   */
  readonly query: ConditionalSchemaQuery<TSchema>;

  readonly #schema: TSchema;
  readonly #serverSchema: ServerSchema;

  constructor(
    dbTransaction: DBTransaction<TWrappedTransaction>,
    clientID: string,
    mutationID: number,
    mutate: TransactionMutate<TSchema>,
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

/**
 * Factory for creating MutateCRUD instances efficiently.
 *
 * Pre-creates the SQL-generating TableCRUD methods once from the schema,
 * caches the serverSchema after first fetch, and only binds the transaction
 * at transaction time.
 *
 * Use this when you need to create many transactions and want to avoid
 * the overhead of re-creating CRUD methods for each one.
 */
export class CRUDMutatorFactory<S extends Schema> {
  readonly #schema: S;
  readonly #tableCRUDs: Record<string, TableCRUD<TableSchema>>;
  #serverSchema: ServerSchema | undefined;

  constructor(schema: S) {
    this.#schema = schema;
    // Pre-create TableCRUD methods for each table once
    this.#tableCRUDs = {};
    for (const tableSchema of Object.values(schema.tables)) {
      this.#tableCRUDs[tableSchema.name] = makeServerTableCRUD(tableSchema);
    }
  }

  /**
   * Gets the cached serverSchema, or fetches and caches it on first call.
   */
  async #getOrFetchServerSchema(
    dbTransaction: DBTransaction<unknown>,
  ): Promise<ServerSchema> {
    if (!this.#serverSchema) {
      this.#serverSchema = await getServerSchema(dbTransaction, this.#schema);
    }
    return this.#serverSchema;
  }

  /**
   * Creates a CRUDExecutor bound to the given transaction and serverSchema.
   * Uses the pre-created TableCRUD methods from construction time.
   */
  createExecutor(
    dbTransaction: DBTransaction<unknown>,
    serverSchema: ServerSchema,
  ): CRUDExecutor {
    const txHolder: WithHiddenTxAndSchema = {
      [dbTxSymbol]: dbTransaction,
      [serverSchemaSymbol]: serverSchema,
    };
    const boundCRUDs = recordProxy(this.#tableCRUDs, tableCRUD =>
      mapValues(tableCRUD, method => method.bind(txHolder)),
    ) as unknown as SchemaCRUD<S>;

    return (table: string, kind: CRUDKind, args: unknown, options?: CRUDExecutorOptions) => {
      const tableCRUD = boundCRUDs[table as keyof S['tables']];
      if (kind === 'upsert' && options?.onConflict) {
        return tableCRUD.upsert(args as never, {onConflict: options.onConflict});
      }
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      return (tableCRUD as any)[kind](args);
    };
  }

  /**
   * Creates a full ServerTransaction.
   * Fetches/caches serverSchema automatically.
   */
  async createTransaction<TWrappedTransaction>(
    dbTransaction: DBTransaction<TWrappedTransaction>,
    clientID: string,
    mutationID: number,
  ): Promise<TransactionImpl<S, TWrappedTransaction>> {
    const serverSchema = await this.#getOrFetchServerSchema(dbTransaction);
    const executor = this.createExecutor(dbTransaction, serverSchema);
    const mutate = makeTransactionMutate(this.#schema, executor);
    return new TransactionImpl(
      dbTransaction,
      clientID,
      mutationID,
      mutate,
      this.#schema,
      serverSchema,
    );
  }
}

export async function makeServerTransaction<
  TSchema extends Schema,
  TWrappedTransaction,
>(
  dbTransaction: DBTransaction<TWrappedTransaction>,
  clientID: string,
  mutationID: number,
  schema: TSchema,
) {
  const serverSchema = await getServerSchema(dbTransaction, schema);
  // Use the internal executor and shared function directly,
  // bypassing the validation in makeMutateCRUD/makeSchemaCRUD
  const executor = makeServerCRUDExecutor(schema, dbTransaction, serverSchema);
  const mutate = makeTransactionMutate(schema, executor);
  return new TransactionImpl(
    dbTransaction,
    clientID,
    mutationID,
    mutate,
    schema,
    serverSchema,
  );
}

/**
 * @deprecated Use transactions instead.
 *
 * Returns a curried function for backwards compatibility.
 */
export function makeSchemaCRUD<S extends Schema>(
  schema: S,
): (
  dbTransaction: DBTransaction<unknown>,
  serverSchema: ServerSchema,
) => MutateCRUD<S, true> {
  return (dbTransaction: DBTransaction<unknown>, serverSchema: ServerSchema) =>
    makeCRUDMutate(
      schema,
      true,
      makeServerCRUDExecutor(schema, dbTransaction, serverSchema),
    );
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

/**
 * Creates a CRUDExecutor for server-side SQL execution.
 *
 * For users with very large schemas it is expensive to re-create
 * all the CRUD mutators for each transaction. Instead, we create
 * the SQL-generating methods once up-front and then bind them to
 * the transaction as requested.
 */
function makeServerCRUDExecutor<S extends Schema>(
  schema: S,
  dbTransaction: DBTransaction<unknown>,
  serverSchema: ServerSchema,
): CRUDExecutor {
  // Pre-create TableCRUD methods for each table (optimization for large schemas)
  const tableCRUDs: Record<string, TableCRUD<TableSchema>> = {};
  for (const tableSchema of Object.values(schema.tables)) {
    tableCRUDs[tableSchema.name] = makeServerTableCRUD(tableSchema);
  }

  // Bind transaction context to the methods
  const txHolder: WithHiddenTxAndSchema = {
    [dbTxSymbol]: dbTransaction,
    [serverSchemaSymbol]: serverSchema,
  };
  const boundCRUDs = recordProxy(tableCRUDs, tableCRUD =>
    mapValues(tableCRUD, method => method.bind(txHolder)),
  ) as unknown as SchemaCRUD<S>;

  // Return executor that dispatches to bound methods
  return (table: string, kind: CRUDKind, args: unknown, options?: CRUDExecutorOptions) => {
    const tableCRUD = boundCRUDs[table as keyof S['tables']];
    if (kind === 'upsert' && options?.onConflict) {
      return tableCRUD.upsert(args as never, {onConflict: options.onConflict});
    }
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    return (tableCRUD as any)[kind](args);
  };
}

/**
 * Creates SQL-generating TableCRUD methods for a table.
 * Methods use `this` context to access transaction and server schema.
 */
function makeServerTableCRUD(schema: TableSchema): TableCRUD<TableSchema> {
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
    async upsert(this: WithHiddenTxAndSchema, value, options?: UpsertOptions<TableSchema>) {
      value = removeUndefined(value);
      const serverTableSchema = this[serverSchemaSymbol][serverName(schema)];
      const targetedColumns = origAndServerNamesFor(Object.keys(value), schema);
      // use custom conflict columns if provided, otherwise fall back to primary key
      const conflictColumns = origAndServerNamesFor(
        options?.onConflict ?? schema.primaryKey,
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
          conflictColumns.map(([, serverName]) => sql.ident(serverName)),
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
