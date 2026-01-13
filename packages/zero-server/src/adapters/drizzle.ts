import {sql, type SQL} from 'drizzle-orm';
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
  TTransaction extends
    DrizzleTransaction<TDrizzle> = DrizzleTransaction<TDrizzle>,
> implements DBConnection<TTransaction>
{
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
> implements DBTransaction<TTransaction>
{
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
    const stmt = fromDollarParams(sql, params);
    const result = await this.wrappedTransaction.execute(stmt);
    return toIterableRows(result);
  }
}

/**
 * Turn `$1, $2...` placeholders into a Drizzle SQL object with bound params.
 */
export function fromDollarParams(text: string, params: unknown[]): SQL {
  const re = /\$(\d+)/g;
  const s = sql.empty();
  let last = 0;
  let m: RegExpExecArray | null;

  while ((m = re.exec(text)) !== null) {
    const idx = Number(m[1]) - 1;
    if (idx < 0 || idx >= params.length) {
      throw new Error(`Missing param for $${m[1]}`);
    }
    if (m.index > last) s.append(sql.raw(text.slice(last, m.index)));
    s.append(sql`${params[idx]}`); // parameterized value
    last = m.index + m[0].length;
  }
  if (last < text.length) s.append(sql.raw(text.slice(last)));
  return s;
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
