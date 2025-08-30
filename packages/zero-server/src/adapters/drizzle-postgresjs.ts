import type {PgTransaction} from 'drizzle-orm/pg-core';
import type {
  PostgresJsDatabase,
  PostgresJsQueryResultHKT,
} from 'drizzle-orm/postgres-js';
import type {ExtractTablesWithRelations} from 'drizzle-orm/relations';
import type postgres from 'postgres';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {
  DBConnection,
  DBTransaction,
} from '../../../zql/src/mutate/custom.ts';
import {ZQLDatabase} from '../zql-database.ts';

/**
 * Helper type for the wrapped transaction used by drizzle-orm/postgres-js.
 *
 * @remarks Use with `ServerTransaction` as `ServerTransaction<Schema, PostgresJsDrizzleTransaction>`.
 */
export type PostgresJsDrizzleTransaction<
  TDbOrSchema extends
    | (PostgresJsDatabase<Record<string, unknown>> & {
        $client: postgres.Sql;
      })
    | Record<string, unknown>,
  TSchema extends Record<
    string,
    unknown
  > = TDbOrSchema extends PostgresJsDatabase<infer TSchema>
    ? TSchema
    : TDbOrSchema,
> = PgTransaction<
  PostgresJsQueryResultHKT,
  TSchema,
  ExtractTablesWithRelations<TSchema>
>;

export class PostgresJsDrizzleConnection<
  TDrizzle extends PostgresJsDatabase<Record<string, unknown>> & {
    $client: postgres.Sql;
  },
  TSchema extends TDrizzle extends PostgresJsDatabase<infer TSchema>
    ? TSchema
    : never,
  TTransaction extends PostgresJsDrizzleTransaction<TDrizzle, TSchema>,
> implements DBConnection<TTransaction>
{
  readonly #drizzle: TDrizzle;

  constructor(drizzle: TDrizzle) {
    this.#drizzle = drizzle;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.#drizzle.$client.unsafe(
      sql,
      params as postgres.ParameterOrJSON<never>[],
    );
  }

  transaction<T>(
    fn: (tx: DBTransaction<TTransaction>) => Promise<T>,
  ): Promise<T> {
    return this.#drizzle.transaction(drizzleTx =>
      fn(
        new PostgresJsDrizzleInternalTransaction<
          TDrizzle,
          TSchema,
          TTransaction
        >(drizzleTx as TTransaction),
      ),
    );
  }
}

class PostgresJsDrizzleInternalTransaction<
  TDrizzle extends PostgresJsDatabase<Record<string, unknown>> & {
    $client: postgres.Sql;
  },
  TSchema extends TDrizzle extends PostgresJsDatabase<infer TSchema>
    ? TSchema
    : never,
  TTransaction extends PgTransaction<
    PostgresJsQueryResultHKT,
    TSchema,
    ExtractTablesWithRelations<TSchema>
  >,
> implements DBTransaction<TTransaction>
{
  readonly wrappedTransaction: TTransaction;

  constructor(drizzleTx: TTransaction) {
    this.wrappedTransaction = drizzleTx;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    const session = this.wrappedTransaction._.session as unknown as {
      client: TDrizzle['$client'];
    };
    return session.client.unsafe(
      sql,
      params as postgres.ParameterOrJSON<never>[],
    );
  }
}

/**
 * Wrap a `drizzle-orm/postgres-js` database for Zero ZQL.
 *
 * Provides ZQL querying plus access to the underlying drizzle transaction.
 * Use {@link PostgresJsDrizzleTransaction} to type your server mutator transaction.
 *
 * @param schema - Zero schema.
 * @param client - Drizzle postgres-js database.
 *
 * @example
 * ```ts
 * import postgres from 'postgres';
 * import {drizzle} from 'drizzle-orm/postgres-js';
 * import type {ServerTransaction} from '@rocicorp/zero';
 *
 * const sql = postgres(process.env.ZERO_UPSTREAM_DB!);
 * const drizzleDb = drizzle(sql, {schema: drizzleSchema});
 *
 * const zql = zeroDrizzlePostgresJS(schema, drizzleDb);
 *
 * // Define the server mutator transaction type using the helper
 * type ServerTx = ServerTransaction<
 *   Schema,
 *   PostgresJsDrizzleTransaction<typeof drizzleDb>
 * >;
 *
 * async function createUser(
 *   tx: ServerTx,
 *   {id, name}: {id: string; name: string},
 * ) {
 *   await tx.dbTransaction.wrappedTransaction
 *     .insert(drizzleSchema.user)
 *     .values({id, name})
 * }
 * ```
 */
export function zeroDrizzlePostgresJS<
  S extends Schema,
  TDrizzle extends PostgresJsDatabase<Record<string, unknown>> & {
    $client: postgres.Sql;
  },
>(schema: S, client: TDrizzle) {
  return new ZQLDatabase(new PostgresJsDrizzleConnection(client), schema);
}
