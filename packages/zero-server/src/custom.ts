import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {TableSchema} from '../../zero-schema/src/table-schema.ts';
import type {
  SchemaCRUD,
  SchemaQuery,
  TableCRUD,
  TransactionBase,
  DBTransaction,
} from '../../zql/src/mutate/custom.ts';
import {
  formatPgInternalConvert,
  sql,
  sqlConvertColumnArg,
} from '../../z2s/src/sql.ts';
import type {
  ServerColumnSchema,
  ServerSchema,
  ServerTableSchema,
} from '../../zero-schema/src/server-schema.ts';
import {getServerSchema} from './schema.ts';
import {assert} from '../../shared/src/asserts.ts';

interface ServerTransaction<S extends Schema, TWrappedTransaction>
  extends TransactionBase<S> {
  readonly location: 'server';
  readonly reason: 'authoritative';
  readonly dbTransaction: DBTransaction<TWrappedTransaction>;
}

export type CustomMutatorDefs<TDBTransaction> = {
  [namespaceOrKey: string]:
    | {
        [key: string]: CustomMutatorImpl<TDBTransaction>;
      }
    | CustomMutatorImpl<TDBTransaction>;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type CustomMutatorImpl<TDBTransaction, TArgs = any> = (
  tx: TDBTransaction,
  args: TArgs,
) => Promise<void>;

export class TransactionImpl<S extends Schema, TWrappedTransaction>
  implements ServerTransaction<S, TWrappedTransaction>
{
  readonly location = 'server';
  readonly reason = 'authoritative';
  readonly dbTransaction: DBTransaction<TWrappedTransaction>;
  readonly clientID: string;
  readonly mutationID: number;
  readonly mutate: SchemaCRUD<S>;
  readonly query: SchemaQuery<S>;

  constructor(
    dbTransaction: DBTransaction<TWrappedTransaction>,
    clientID: string,
    mutationID: number,
    mutate: SchemaCRUD<S>,
    query: SchemaQuery<S>,
  ) {
    this.dbTransaction = dbTransaction;
    this.clientID = clientID;
    this.mutationID = mutationID;
    this.mutate = mutate;
    this.query = query;
  }
}

const dbTxSymbol = Symbol();
const serverSchemaSymbol = Symbol();
type WithHiddenTxAndSchema = {
  [dbTxSymbol]: DBTransaction<unknown>;
  [serverSchemaSymbol]: ServerSchema;
};

export async function makeServerTransaction<
  S extends Schema,
  TWrappedTransaction,
>(
  dbTransaction: DBTransaction<TWrappedTransaction>,
  clientID: string,
  mutationID: number,
  schema: S,
  mutate: (
    dbTransaction: DBTransaction<TWrappedTransaction>,
    serverSchema: ServerSchema,
  ) => SchemaCRUD<S>,
  query: (
    dbTransaction: DBTransaction<TWrappedTransaction>,
    serverSchema: ServerSchema,
  ) => SchemaQuery<S>,
) {
  const serverSchema = await getServerSchema(dbTransaction, schema);
  return new TransactionImpl(
    dbTransaction,
    clientID,
    mutationID,
    mutate(dbTransaction, serverSchema),
    query(dbTransaction, serverSchema),
  );
}

export function makeSchemaCRUD<S extends Schema>(
  schema: S,
): (
  dbTransaction: DBTransaction<unknown>,
  serverSchema: ServerSchema,
) => SchemaCRUD<S> {
  const schemaCRUDs: Record<string, TableCRUD<TableSchema>> = {};
  for (const tableSchema of Object.values(schema.tables)) {
    schemaCRUDs[tableSchema.name] = makeTableCRUD(tableSchema);
  }

  /**
   * For users with very large schemas it is expensive to re-create
   * all the CRUD mutators for each transaction. Instead, we create
   * them all once up-front and then bind them to the transaction
   * as requested.
   */
  class CRUDHandler {
    readonly #dbTransaction: DBTransaction<unknown>;
    readonly #serverSchema: ServerSchema;
    constructor(
      dbTransaction: DBTransaction<unknown>,
      serverSchema: ServerSchema,
    ) {
      this.#dbTransaction = dbTransaction;
      this.#serverSchema = serverSchema;
    }

    get(target: Record<string, TableCRUD<TableSchema>>, prop: string) {
      if (prop in target) {
        return target[prop];
      }

      const txHolder: WithHiddenTxAndSchema = {
        [dbTxSymbol]: this.#dbTransaction,
        [serverSchemaSymbol]: this.#serverSchema,
      };
      target[prop] = Object.fromEntries(
        Object.entries(schemaCRUDs[prop]).map(([name, method]) => [
          name,
          method.bind(txHolder),
        ]),
      ) as TableCRUD<TableSchema>;

      return target[prop];
    }
  }

  return (dbTransaction: DBTransaction<unknown>, serverSchema: ServerSchema) =>
    new Proxy(
      {},
      new CRUDHandler(dbTransaction, serverSchema),
    ) as SchemaCRUD<S>;
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
