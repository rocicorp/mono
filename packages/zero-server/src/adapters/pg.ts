import type {Client} from 'pg';
import {Pool, type PoolClient} from 'pg';
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
 * Helper type for the wrapped transaction used by node-postgres.
 *
 * @remarks Use with `ServerTransaction` as `ServerTransaction<Schema, NodePgTransaction>`.
 */
export type NodePgTransaction = Pool | PoolClient | Client;

export class NodePgConnection implements DBConnection<NodePgTransaction> {
  readonly #pool: NodePgTransaction;

  constructor(pool: NodePgTransaction) {
    this.#pool = pool;
  }

  async transaction<TRet>(
    fn: (tx: DBTransaction<NodePgTransaction>) => Promise<TRet>,
  ): Promise<TRet> {
    const client =
      this.#pool instanceof Pool ? await this.#pool.connect() : this.#pool;
    try {
      await client.query('BEGIN');
      const result = await fn(new NodePgTransactionInternal(client));
      await client.query('COMMIT');
      return result;
    } catch (error) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // ignore rollback error; original error will be thrown
      }
      throw error;
    } finally {
      if (this.#pool instanceof Pool && 'release' in client) {
        client.release();
      }
    }
  }
}

export class NodePgTransactionInternal
  implements DBTransaction<NodePgTransaction>
{
  readonly wrappedTransaction: NodePgTransaction;

  constructor(client: NodePgTransaction) {
    this.wrappedTransaction = client;
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
    const res = await this.wrappedTransaction.query(sql, params as unknown[]);
    return res.rows as Row[];
  }
}

/**
 * Wrap a `pg` Pool for Zero ZQL.
 *
 * Provides ZQL querying plus access to the underlying node-postgres client.
 * Use {@link NodePgTransaction} to type your server mutator transaction.
 *
 * @param schema - Zero schema.
 * @param pg - `pg` Pool or connection string.
 *
 * @example
 * ```ts
 * import {Pool} from 'pg';
 * import {defineMutator, defineMutators} from '@rocicorp/zero';
 * import {zeroNodePg} from '@rocicorp/zero/server/adapters/pg';
 * import {z} from 'zod/mini';
 *
 * const pool = new Pool({connectionString: process.env.ZERO_UPSTREAM_DB!});
 * const zql = zeroNodePg(schema, pool);
 *
 * export const serverMutators = defineMutators({
 *   user: {
 *     create: defineMutator(
 *       z.object({id: z.string(), name: z.string()}),
 *       async ({tx, args}) => {
 *         if (tx.location !== 'server') {
 *           throw new Error('Server-only mutator');
 *         }
 *         await tx.dbTransaction.wrappedTransaction.query(
 *           'INSERT INTO "user" (id, name, status) VALUES ($1, $2, $3)',
 *           [args.id, args.name, 'active'],
 *         );
 *       },
 *     ),
 *   },
 * });
 * ```
 */
export function zeroNodePg<S extends Schema>(
  schema: S,
  pg: NodePgTransaction | string,
) {
  if (typeof pg === 'string') {
    pg = new Pool({connectionString: pg});
  }
  return new ZQLDatabase(new NodePgConnection(pg), schema);
}
