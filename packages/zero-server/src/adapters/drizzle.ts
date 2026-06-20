import type {
  PgDatabase,
  PgQueryResultHKT,
  PgTransaction,
} from 'drizzle-orm/pg-core';
import type {ExtractTablesWithRelations} from 'drizzle-orm/relations';
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
import {drizzleUsesModeArg} from './detect-drizzle-version.ts';

export type {ZQLDatabase};

export type DrizzleDatabase<
  TQueryResult extends PgQueryResultHKT = PgQueryResultHKT,
  TSchema extends Record<string, unknown> = Record<string, unknown>,
> = PgDatabase<TQueryResult, TSchema>;

/**
 * Helper type for the wrapped transaction used by drizzle-orm.
 *
 * @remarks Use with `ServerTransaction` as `ServerTransaction<Schema, DrizzleTransaction<typeof drizzleDb>>`.
 */
export type DrizzleTransaction<
  TDbOrSchema extends DrizzleDatabase | Record<string, unknown>,
  TSchema extends Record<string, unknown> = TDbOrSchema extends PgDatabase<
    PgQueryResultHKT,
    infer TInferredSchema
  >
    ? TInferredSchema
    : TDbOrSchema,
> = PgTransaction<
  PgQueryResultHKT,
  TSchema,
  ExtractTablesWithRelations<TSchema>
>;

export class DrizzleConnection<
  TDrizzle extends DrizzleDatabase,
  TTransaction extends DrizzleTransaction<TDrizzle> =
    DrizzleTransaction<TDrizzle>,
> implements DBConnection<TTransaction> {
  readonly #drizzle: TDrizzle;

  constructor(drizzle: TDrizzle) {
    this.#drizzle = drizzle;
  }

  transaction<T>(
    fn: (tx: DBTransaction<TTransaction>) => Promise<T>,
  ): Promise<T> {
    return this.#drizzle.transaction(drizzleTx =>
      fn(
        new DrizzleInternalTransaction(
          drizzleTx,
        ) as DBTransaction<TTransaction>,
      ),
    );
  }
}

class DrizzleInternalTransaction<
  TTransaction extends DrizzleTransaction<DrizzleDatabase>,
> implements DBTransaction<TTransaction> {
  readonly wrappedTransaction: TTransaction;

  constructor(drizzleTx: TTransaction) {
    this.wrappedTransaction = drizzleTx;
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

  async query(sql: string, params: unknown[]): Promise<Iterable<Row>> {
    const {session} = this.wrappedTransaction._;
    const query = {sql, params};
    // `drizzleUsesModeArg` only introspects the function (`toString`/`length`),
    // never invokes it, so the unbound reference is safe here.
    // oxlint-disable-next-line unbound-method
    const prepared = drizzleUsesModeArg(session.prepareQuery)
      ? (session.prepareQuery as unknown as ModePrepareQuery)(
          query,
          'objects',
          undefined,
          undefined,
        )
      : session.prepareQuery(query, undefined, undefined, false);
    const result = await prepared.execute();
    return toIterableRows(result);
  }
}

/**
 * `drizzle-orm`'s `PgSession.prepareQuery` signature changed in the 1.0 RC
 * line:
 * - `<= 1.0.0-beta`: `(query, fields, name, isResponseInArrayMode, customResultMapper?, queryMetadata?, cacheConfig?)`
 * - `>= 1.0.0-rc.1`: `(query, mode: 'arrays' | 'objects' | 'raw', name, mapper?, queryMetadata?, cacheConfig?)`
 *
 * Passing the old positional form to a newer drizzle leaves `mode` undefined,
 * so it returns rows as arrays instead of objects. That silently breaks
 * `getServerSchema`'s `information_schema` introspection (it reads
 * `row.dataType`, which is `undefined` on an array row) and every transaction
 * fails with `Cannot read properties of undefined (reading 'toLowerCase')`.
 *
 * `zero` is built against `drizzle-orm@^0.45`, so the typed call uses the old
 * signature; this detects a newer drizzle at runtime and calls the new one.
 */
type ModePrepareQuery = (
  query: {sql: string; params: unknown[]},
  mode: 'arrays' | 'objects' | 'raw',
  name: undefined,
  mapper: undefined,
) => {execute(): Promise<unknown>};

function isIterable(value: unknown): value is Iterable<unknown> {
  return (
    // oxlint-disable-next-line eqeqeq
    value != null &&
    typeof (value as Iterable<unknown>)[Symbol.iterator] === 'function'
  );
}

export function toIterableRows(result: unknown): Iterable<Row> {
  if (result === null || result === undefined) {
    return [] as Row[];
  }
  if (Array.isArray(result)) {
    return result as Row[];
  }
  if (isIterable(result)) {
    return result as Iterable<Row>;
  }
  if (typeof result === 'object') {
    const rows = (result as {rows?: unknown}).rows;
    if (rows === null || rows === undefined) {
      return [] as Row[];
    }
    if (Array.isArray(rows)) {
      return rows as Row[];
    }
    if (isIterable(rows)) {
      return rows as Iterable<Row>;
    }
  }
  throw new TypeError('Drizzle query result is not iterable');
}

/**
 * Wrap a `drizzle-orm` database for Zero ZQL.
 *
 * Provides ZQL querying plus access to the underlying drizzle transaction.
 * Use {@link DrizzleTransaction} to type your server mutator transaction.
 *
 * @param schema - Zero schema.
 * @param client - Drizzle database.
 *
 * @example
 * ```ts
 * import {Pool} from 'pg';
 * import {drizzle} from 'drizzle-orm/node-postgres';
 * import {defineMutator, defineMutators} from '@rocicorp/zero';
 * import {zeroDrizzle} from '@rocicorp/zero/server/adapters/drizzle';
 * import {z} from 'zod/mini';
 *
 * const pool = new Pool({connectionString: process.env.ZERO_UPSTREAM_DB!});
 * const drizzleDb = drizzle(pool, {schema: drizzleSchema});
 * const zql = zeroDrizzle(schema, drizzleDb);
 *
 * export const serverMutators = defineMutators({
 *   user: {
 *     create: defineMutator(
 *       z.object({id: z.string(), name: z.string()}),
 *       async ({tx, args}) => {
 *         if (tx.location !== 'server') {
 *           throw new Error('Server-only mutator');
 *         }
 *         await tx.dbTransaction.wrappedTransaction
 *           .insert(drizzleSchema.user)
 *           .values({id: args.id, name: args.name, status: 'active'});
 *       },
 *     ),
 *   },
 * });
 * ```
 */
export function zeroDrizzle<
  TSchema extends Schema,
  TDrizzle extends DrizzleDatabase,
>(
  schema: TSchema,
  client: TDrizzle,
): ZQLDatabase<TSchema, DrizzleTransaction<TDrizzle>> {
  return new ZQLDatabase(
    new DrizzleConnection<TDrizzle, DrizzleTransaction<TDrizzle>>(client),
    schema,
  );
}
