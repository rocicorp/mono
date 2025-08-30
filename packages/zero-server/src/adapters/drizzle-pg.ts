import type {
  NodePgClient,
  NodePgDatabase,
  NodePgQueryResultHKT,
} from 'drizzle-orm/node-postgres';
import type {PgTransaction} from 'drizzle-orm/pg-core';
import type {ExtractTablesWithRelations} from 'drizzle-orm/relations';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {
  DBConnection,
  DBTransaction,
} from '../../../zql/src/mutate/custom.ts';
import {ZQLDatabase} from '../zql-database.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';

/**
 * Helper type for the wrapped transaction used by drizzle-orm/node-postgres.
 *
 * @remarks Use with `ServerTransaction` as `ServerTransaction<Schema, NodePgDrizzleTransaction>`.
 */
export type NodePgDrizzleTransaction<
  TDbOrSchema extends
    | (NodePgDatabase<Record<string, unknown>> & {$client: NodePgClient})
    | Record<string, unknown>,
  TSchema extends Record<string, unknown> = TDbOrSchema extends NodePgDatabase<
    infer TSchema
  >
    ? TSchema
    : TDbOrSchema,
> = PgTransaction<
  NodePgQueryResultHKT,
  TSchema,
  ExtractTablesWithRelations<TSchema>
>;

export class NodePgDrizzleConnection<
  TDrizzle extends NodePgDatabase<Record<string, unknown>> & {
    $client: NodePgClient;
  },
  TSchema extends TDrizzle extends NodePgDatabase<infer TSchema>
    ? TSchema
    : never,
  TTransaction extends NodePgDrizzleTransaction<TDrizzle, TSchema>,
> implements DBConnection<TTransaction>
{
  readonly #drizzle: TDrizzle;

  constructor(drizzle: TDrizzle) {
    this.#drizzle = drizzle;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.#drizzle.$client.query(sql, params).then(({rows}) => rows);
  }

  transaction<T>(
    fn: (tx: DBTransaction<TTransaction>) => Promise<T>,
  ): Promise<T> {
    return this.#drizzle.transaction(drizzleTx =>
      fn(
        new NodePgDrizzleInternalTransaction<TDrizzle, TSchema, TTransaction>(
          drizzleTx as TTransaction,
        ),
      ),
    );
  }
}

class NodePgDrizzleInternalTransaction<
  TDrizzle extends NodePgDatabase<Record<string, unknown>> & {
    $client: NodePgClient;
  },
  TSchema extends TDrizzle extends NodePgDatabase<infer TSchema>
    ? TSchema
    : never,
  TTransaction extends PgTransaction<
    NodePgQueryResultHKT,
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
    return session.client.query(sql, params).then(({rows}) => rows);
  }
}

/**
 * Wrap a `drizzle-orm/node-postgres` database for Zero ZQL.
 *
 * Provides ZQL querying plus access to the underlying drizzle transaction.
 * Use {@link NodePgDrizzleTransaction} to type your server mutator transaction.
 *
 * @param schema - Zero schema.
 * @param client - Drizzle node-postgres database.
 *
 * @example
 * ```ts
 * import {Pool} from 'pg';
 * import {drizzle} from 'drizzle-orm/node-postgres';
 * import type {ServerTransaction} from '@rocicorp/zero';
 *
 * const pool = new Pool({connectionString: process.env.ZERO_UPSTREAM_DB!});
 * const drizzleDb = drizzle(pool, {schema: drizzleSchema});
 *
 * const zql = zeroDrizzleNodePg(schema, drizzleDb);
 *
 * // Define the server mutator transaction type using the helper
 * type ServerTx = ServerTransaction<
 *   Schema,
 *   NodePgDrizzleTransaction<typeof drizzleDb>
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
export function zeroDrizzleNodePg<
  S extends Schema,
  TDrizzle extends NodePgDatabase<Record<string, unknown>> & {
    $client: NodePgClient;
  },
>(schema: S, client: TDrizzle) {
  return new ZQLDatabase(new NodePgDrizzleConnection(client), schema);
}

/**
 * @deprecated Use {@link zeroDrizzleNodePg} instead.
 */
export function zeroNodePg<
  S extends Schema,
  TDrizzle extends NodePgDatabase<Record<string, unknown>> & {
    $client: NodePgClient;
  },
>(schema: S, client: TDrizzle) {
  return new ZQLDatabase(new NodePgDrizzleConnection(client), schema);
}
