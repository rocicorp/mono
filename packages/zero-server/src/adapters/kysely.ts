import {CompiledQuery} from 'kysely';
import type {Kysely, Transaction as WrappedKyselyTransaction} from 'kysely';
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

export type KyselyDatabase<TDatabase = unknown> = Kysely<TDatabase>;

/**
 * Helper type for the wrapped transaction used by Kysely.
 *
 * @remarks Use with `ServerTransaction` as `ServerTransaction<Schema, KyselyTransaction<typeof db>>`.
 */
export type KyselyTransaction<TDbOrSchema = KyselyDatabase> =
  TDbOrSchema extends Kysely<infer TInferredDatabase>
    ? WrappedKyselyTransaction<TInferredDatabase>
    : WrappedKyselyTransaction<TDbOrSchema>;

export class KyselyConnection<TDatabase> implements DBConnection<
  WrappedKyselyTransaction<TDatabase>
> {
  readonly #client: Kysely<TDatabase>;

  constructor(client: Kysely<TDatabase>) {
    this.#client = client;
  }

  transaction<T>(
    fn: (tx: DBTransaction<WrappedKyselyTransaction<TDatabase>>) => Promise<T>,
  ): Promise<T> {
    return this.#client
      .transaction()
      .execute(kyselyTx => fn(new KyselyInternalTransaction(kyselyTx)));
  }
}

class KyselyInternalTransaction<TDatabase> implements DBTransaction<
  WrappedKyselyTransaction<TDatabase>
> {
  readonly wrappedTransaction: WrappedKyselyTransaction<TDatabase>;

  constructor(kyselyTx: WrappedKyselyTransaction<TDatabase>) {
    this.wrappedTransaction = kyselyTx;
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

  async query(sql: string, params: unknown[]): Promise<Row[]> {
    const result = await this.wrappedTransaction.executeQuery<Row>(
      CompiledQuery.raw(sql, params),
    );
    return result.rows;
  }
}

/**
 * Wrap a Postgres-backed Kysely client for Zero ZQL.
 *
 * Provides ZQL querying plus access to the underlying Kysely transaction.
 * Use {@link KyselyTransaction} to type your server mutator transaction.
 *
 * @param schema - Zero schema.
 * @param client - Kysely client.
 *
 * @example
 * ```ts
 * import {Pool} from 'pg';
 * import {Kysely, PostgresDialect} from 'kysely';
 * import {defineMutator, defineMutators} from '@rocicorp/zero';
 * import {zeroKysely} from '@rocicorp/zero/server/adapters/kysely';
 * import {z} from 'zod/mini';
 *
 * interface Database {
 *   user: {
 *     id: string;
 *     name: string | null;
 *     status: 'active' | 'inactive';
 *   };
 * }
 *
 * const db = new Kysely<Database>({
 *   dialect: new PostgresDialect({
 *     pool: new Pool({connectionString: process.env.ZERO_UPSTREAM_DB!}),
 *   }),
 * });
 * const zql = zeroKysely(schema, db);
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
 *           .insertInto('user')
 *           .values({id: args.id, name: args.name, status: 'active'})
 *           .execute();
 *       },
 *     ),
 *   },
 * });
 * ```
 */
export function zeroKysely<TSchema extends Schema, TDatabase>(
  schema: TSchema,
  client: Kysely<TDatabase>,
): ZQLDatabase<TSchema, WrappedKyselyTransaction<TDatabase>> {
  return new ZQLDatabase(new KyselyConnection(client), schema);
}
