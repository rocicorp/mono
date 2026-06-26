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

type DrizzleQuery = {sql: string; params: unknown[]};
type DrizzlePreparedQuery = {execute(): Promise<unknown>};

type DrizzlePrepareQueryV0 = (
  query: DrizzleQuery,
  fields: undefined,
  name: undefined,
  isResponseInArrayMode: false,
) => DrizzlePreparedQuery;

type DrizzlePrepareQueryV1 = (
  query: DrizzleQuery,
  mode: 'objects',
  name: undefined,
  mapper: undefined,
) => DrizzlePreparedQuery;

type DrizzleSession = {
  prepareQuery: {length: number};
};

type DrizzleTransactionLike = {
  _: {session: DrizzleSession};
};

type DrizzleQueryResult<TResult> = PromiseLike<TResult> & {
  execute(): Promise<TResult>;
};

type DrizzleInferSelect<TTable> = TTable extends {$inferSelect: infer TSelect}
  ? TSelect
  : Record<string, unknown>;

type DrizzleTransactionFromSchema<TSchema> = DrizzleTransactionLike & {
  query: {
    [TTable in keyof TSchema]: {
      findFirst(
        args?: unknown,
      ): DrizzleQueryResult<DrizzleInferSelect<TSchema[TTable]> | undefined>;
    };
  };
};

export type DrizzleDatabase<
  TTransaction extends DrizzleTransactionLike = DrizzleTransactionLike,
> = {
  transaction<T>(
    transaction: (tx: TTransaction) => Promise<T>,
    config?: never,
  ): Promise<T>;
};

/**
 * Helper type for the wrapped transaction used by drizzle-orm.
 *
 * @remarks Use with `ServerTransaction` as `ServerTransaction<Schema, DrizzleTransaction<typeof drizzleDb>>`.
 */
export type DrizzleTransaction<TDbOrSchema = Record<string, unknown>> =
  TDbOrSchema extends DrizzleDatabase<infer TTransaction>
    ? TTransaction
    : DrizzleTransactionFromSchema<TDbOrSchema>;

export class DrizzleConnection<
  TDrizzle,
  TTransaction extends DrizzleTransactionLike = DrizzleTransaction<TDrizzle>,
> implements DBConnection<TTransaction> {
  readonly #drizzle: DrizzleDatabase<TTransaction>;

  constructor(drizzle: TDrizzle & DrizzleDatabase<TTransaction>) {
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
  TTransaction extends DrizzleTransactionLike,
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
    const prepared =
      session.prepareQuery.length < 7
        ? (session.prepareQuery as DrizzlePrepareQueryV1)(
            query,
            'objects',
            undefined,
            undefined,
          )
        : (session.prepareQuery as DrizzlePrepareQueryV0)(
            query,
            undefined,
            undefined,
            false,
          );
    const result = await prepared.execute();
    return toIterableRows(result);
  }
}

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
  TDrizzle,
  TTransaction extends DrizzleTransactionLike = DrizzleTransaction<TDrizzle>,
>(
  schema: TSchema,
  client: TDrizzle & DrizzleDatabase<TTransaction>,
): ZQLDatabase<TSchema, TTransaction> {
  return new ZQLDatabase(
    new DrizzleConnection<TDrizzle, TTransaction>(client),
    schema,
  );
}
