import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {
  DBConnection,
  DBTransaction,
  Row,
} from '../../../zql/src/mutate/custom.ts';
import {Pool, type PoolClient} from 'pg';
import {ZQLDatabase} from '../zql-database.ts';

/**
 * Helper type for the wrapped transaction used by node-postgres.
 *
 * @remarks Use with `ServerTransaction` as `ServerTransaction<Schema, NodePgTransaction>`.
 */
export type NodePgTransaction = PoolClient;

export class NodePgConnection implements DBConnection<PoolClient> {
  readonly #pool: Pool;
  constructor(pool: Pool) {
    this.#pool = pool;
  }

  async transaction<TRet>(
    fn: (tx: DBTransaction<PoolClient>) => Promise<TRet>,
  ): Promise<TRet> {
    const client = await this.#pool.connect();
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
      client.release();
    }
  }
}

class NodePgTransactionInternal implements DBTransaction<PoolClient> {
  readonly wrappedTransaction: PoolClient;
  constructor(client: PoolClient) {
    this.wrappedTransaction = client;
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
 *
 * const pool = new Pool({connectionString: process.env.ZERO_UPSTREAM_DB!});
 * const zql = zeroNodePg(schema, pool);
 *
 * // Define the server mutator transaction type using the helper
 * type ServerTx = ServerTransaction<
 *   Schema,
 *   NodePgTransaction
 * >;
 *
 * async function createUser(
 *   tx: ServerTx,
 *   {id, name}: {id: string; name: string},
 * ) {
 *   await tx.dbTransaction.wrappedTransaction
 *     .query('SELECT * FROM "user" WHERE id = $1', [id]);
 * }
 * ```
 */
export function zeroNodePg<S extends Schema>(schema: S, pg: Pool | string) {
  if (typeof pg === 'string') {
    pg = new Pool({connectionString: pg});
  }
  return new ZQLDatabase(new NodePgConnection(pg), schema);
}
