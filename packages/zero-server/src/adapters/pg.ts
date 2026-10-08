import type {LogContext} from '@rocicorp/logger';
import type {Client} from 'pg';
import {DatabaseError, Pool, type PoolClient} from 'pg';
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
import {createLogContext} from '../logging.ts';
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

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return nodePgQuery(this.#pool, sql, params);
  }

  async transaction<TRet>(
    fn: (tx: DBTransaction<NodePgTransaction>) => Promise<TRet>,
  ): Promise<TRet> {
    const client =
      this.#pool instanceof Pool ? await this.#pool.connect() : this.#pool;
    // The pool only listens for 'error' on a client idle in the pool. If the
    // server ends the connection while the client is checked out here and no
    // query is running (an idle_in_transaction_session_timeout while the
    // mutator awaits something else, a restart, a failover), node-postgres
    // emits 'error' on the client, and with no listener the process dies.
    let connectionError: Error | undefined;
    const onError = (e: Error) => {
      connectionError ??= e;
    };
    client.on('error', onError);
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
      // After a connection error, a query only fails with "Client has
      // encountered a connection error and is not queryable". Throw what
      // ended the connection instead, unless the server reported an error of
      // its own to the query.
      throw connectionError && !(error instanceof DatabaseError)
        ? connectionError
        : error;
    } finally {
      client.off('error', onError);
      if (this.#pool instanceof Pool && 'release' in client) {
        client.release(connectionError);
      }
    }
  }
}

export class NodePgTransactionInternal implements DBTransaction<NodePgTransaction> {
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

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return nodePgQuery(this.wrappedTransaction, sql, params);
  }
}

async function nodePgQuery(
  client: NodePgTransaction,
  sql: string,
  params: unknown[],
): Promise<Row[]> {
  const res = await client.query(sql, params);
  return res.rows as Row[];
}

/**
 * Wrap a `pg` Pool for Zero ZQL.
 *
 * Provides ZQL querying plus access to the underlying node-postgres client.
 * Use {@link NodePgTransaction} to type your server mutator transaction.
 *
 * @param schema - Zero schema.
 * @param pg - `pg` Pool or connection string.
 * @param lc - Where to log errors from a pool built from a connection string.
 * Defaults to `console` at level `warn`.
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
  lc: LogContext = createLogContext('warn'),
) {
  if (typeof pg === 'string') {
    const pool = new Pool({connectionString: pg});
    // node-postgres emits 'error' on the pool when the server terminates a
    // client idle in the pool (an idle_session_timeout, a restart, a
    // failover). The pool has already discarded that client and will open
    // another; with no listener the event is an uncaught exception and the
    // process dies with whatever request it was serving. Log it and carry on.
    pool.on('error', e => {
      lc.warn?.(
        'node-postgres pool error; the client was removed from the pool',
        e,
      );
    });
    pg = pool;
  }
  return new ZQLDatabase(new NodePgConnection(pg), schema);
}
