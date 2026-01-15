import postgres from 'postgres';
import type {JSONValue} from '../../../shared/src/json.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Format} from '../../../zero-types/src/format.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {ServerSchema} from '../../../zero-types/src/server-schema.ts';
import type {
  DBConnection,
  DBTransaction,
  Row,
} from '../../../zql/src/mutate/custom.ts';
import type {HumanReadable} from '../../../zql/src/query/query.ts';
import {executePostgresQuery} from '../pg-query-executor.ts';
import {ZQLDatabase} from '../zql-database.ts';

export type {ZQLDatabase};

/**
 * Helper type for the wrapped transaction used by postgres.js.
 *
 * @typeParam T - The row-shape context bound to the postgres.js client.
 * @remarks Use with `ServerTransaction` as `ServerTransaction<Schema, PostgresJsTransaction>`.
 */
export type PostgresJsTransaction<
  T extends Record<string, unknown> = Record<string, unknown>,
> = postgres.TransactionSql<T>;

export class PostgresJSConnection<T extends Record<string, unknown>>
  implements DBConnection<PostgresJsTransaction<T>>
{
  readonly #pg: postgres.Sql<T>;
  constructor(pg: postgres.Sql<T>) {
    this.#pg = pg;
  }

  transaction<TRet>(
    fn: (tx: DBTransaction<PostgresJsTransaction<T>>) => Promise<TRet>,
  ): Promise<TRet> {
    return this.#pg.begin(pgTx =>
      fn(new PostgresJsTransactionInternal(pgTx)),
    ) as Promise<TRet>;
  }
}

export class PostgresJsTransactionInternal<T extends Record<string, unknown>>
  implements DBTransaction<PostgresJsTransaction<T>>
{
  readonly wrappedTransaction: PostgresJsTransaction<T>;
  constructor(pgTx: PostgresJsTransaction<T>) {
    this.wrappedTransaction = pgTx;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.wrappedTransaction.unsafe(sql, params as JSONValue[]);
  }

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
  }
}

/**
 * Wrap a `postgres` client for Zero ZQL.
 *
 * Provides ZQL querying plus access to the underlying postgres.js transaction.
 * Use {@link PostgresJsTransaction} to type your server mutator transaction.
 *
 * @param schema - Zero schema.
 * @param pg - `postgres` client or connection string.
 *
 * @example
 * ```ts
 * import postgres from 'postgres';
 * import {defineMutator, defineMutators} from '@rocicorp/zero';
 * import {zeroPostgresJS} from '@rocicorp/zero/server/adapters/postgresjs';
 * import {z} from 'zod/mini';
 *
 * const sql = postgres(process.env.ZERO_UPSTREAM_DB!);
 * const zql = zeroPostgresJS(schema, sql);
 *
 * export const serverMutators = defineMutators({
 *   user: {
 *     create: defineMutator(
 *       z.object({id: z.string(), name: z.string()}),
 *       async ({tx, args}) => {
 *         if (tx.location !== 'server') {
 *           throw new Error('Server-only mutator');
 *         }
 *         await tx.dbTransaction.wrappedTransaction`
 *           INSERT INTO "user" (id, name, status)
 *           VALUES (${args.id}, ${args.name}, ${'active'})
 *         `;
 *       },
 *     ),
 *   },
 * });
 * ```
 */
export function zeroPostgresJS<
  S extends Schema,
  T extends Record<string, unknown> = Record<string, unknown>,
>(schema: S, pg: postgres.Sql<T> | string) {
  if (typeof pg === 'string') {
    pg = postgres(pg) as postgres.Sql<T>;
  }
  return new ZQLDatabase(new PostgresJSConnection(pg), schema);
}
