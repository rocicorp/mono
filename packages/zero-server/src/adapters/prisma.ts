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

export type PrismaTransactionLike = {
  $queryRawUnsafe: (query: string, ...params: unknown[]) => Promise<unknown>;
};

export type PrismaClientLike<
  TTransaction extends PrismaTransactionLike = PrismaTransactionLike,
> = {
  $transaction: <T>(fn: (tx: TTransaction) => Promise<T>) => Promise<T>;
};

/**
 * Helper type for the wrapped transaction used by Prisma.
 *
 * @remarks Use with `ServerTransaction` as `ServerTransaction<Schema, PrismaTransaction<typeof prisma>>`.
 */
export type PrismaTransaction<
  TClient extends PrismaClientLike = PrismaClientLike,
> =
  TClient extends PrismaClientLike<infer TTransaction>
    ? TTransaction
    : PrismaTransactionLike;

export class PrismaConnection<TClient extends PrismaClientLike>
  implements DBConnection<PrismaTransaction<TClient>>
{
  readonly #client: TClient;

  constructor(client: TClient) {
    this.#client = client;
  }

  transaction<T>(
    fn: (tx: DBTransaction<PrismaTransaction<TClient>>) => Promise<T>,
  ): Promise<T> {
    return this.#client.$transaction(prismaTx =>
      fn(
        new PrismaInternalTransaction(prismaTx) as DBTransaction<
          PrismaTransaction<TClient>
        >,
      ),
    );
  }
}

class PrismaInternalTransaction<TTransaction extends PrismaTransactionLike>
  implements DBTransaction<TTransaction>
{
  readonly wrappedTransaction: TTransaction;

  constructor(prismaTx: TTransaction) {
    this.wrappedTransaction = prismaTx;
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
    const result = await this.wrappedTransaction.$queryRawUnsafe(
      sql,
      ...params,
    );
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

function toIterableRows(result: unknown): Iterable<Row> {
  if (result === null || result === undefined) {
    return [] as Row[];
  }
  if (Array.isArray(result)) {
    return result as Row[];
  }
  if (isIterable(result)) {
    return result as Iterable<Row>;
  }
  return [] as Row[];
}

/**
 * Wrap a Prisma client for Zero ZQL.
 *
 * Provides ZQL querying plus access to the underlying Prisma transaction.
 * Use {@link PrismaTransaction} to type your server mutator transaction.
 *
 * @param schema - Zero schema.
 * @param client - Prisma client.
 *
 * @example
 * ```ts
 * import {PrismaPg} from '@prisma/adapter-pg';
 * import {PrismaClient} from '@prisma/client';
 * import {defineMutator, defineMutators} from '@rocicorp/zero';
 * import {zeroPrisma} from '@rocicorp/zero/server/adapters/prisma';
 * import {z} from 'zod/mini';
 *
 * const prisma = new PrismaClient({
 *   adapter: new PrismaPg({connectionString: process.env.ZERO_UPSTREAM_DB!}),
 * });
 * const zql = zeroPrisma(schema, prisma);
 *
 * export const serverMutators = defineMutators({
 *   user: {
 *     create: defineMutator(
 *       z.object({id: z.string(), name: z.string()}),
 *       async ({tx, args}) => {
 *         if (tx.location !== 'server') {
 *           throw new Error('Server-only mutator');
 *         }
 *         await tx.dbTransaction.wrappedTransaction.user.create({
 *           data: {
 *             id: args.id,
 *             name: args.name,
 *             status: 'active',
 *           },
 *         });
 *       },
 *     ),
 *   },
 * });
 * ```
 */
export function zeroPrisma<
  TSchema extends Schema,
  TClient extends PrismaClientLike,
>(
  schema: TSchema,
  client: TClient,
): ZQLDatabase<TSchema, PrismaTransaction<TClient>> {
  return new ZQLDatabase(new PrismaConnection(client), schema);
}
