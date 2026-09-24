import type {MaybePromise} from '../../shared/src/types.ts';
import {formatPg, sql} from '../../z2s/src/sql.ts';
import type {CleanupResultsArg} from '../../zero-protocol/src/mutation.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import {decodeQueryResult} from '../../zql/src/ivm/codec.ts';
import type {
  DBConnection,
  DBTransaction,
  Queryable,
} from '../../zql/src/mutate/custom.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import type {
  HumanReadable,
  Query,
  RunOptions,
} from '../../zql/src/query/query.ts';
import {CRUDMutatorFactory, type TransactionImpl} from './custom.ts';
import {executePostgresQuery} from './pg-query-executor.ts';
import type {
  Database,
  TransactionProviderHooks,
  TransactionProviderInput,
} from './process-mutations.ts';

/**
 * Implements a Database for use with PushProcessor that is backed by Postgres.
 *
 * This implementation also implements the same ZQL interfaces for reading and
 * writing data that the Zero client does, so that mutator functions can be
 * shared across client and server.
 */
export class ZQLDatabase<
  TSchema extends Schema,
  TWrappedTransaction,
> implements Database<TransactionImpl<TSchema, TWrappedTransaction>> {
  readonly connection: DBConnection<TWrappedTransaction>;
  readonly #crudFactory: CRUDMutatorFactory<TSchema>;
  readonly #schema: TSchema;

  constructor(connection: DBConnection<TWrappedTransaction>, schema: TSchema) {
    this.connection = connection;
    this.#crudFactory = new CRUDMutatorFactory(schema);
    this.#schema = schema;
  }

  transaction<R>(
    callback: (
      tx: TransactionImpl<TSchema, TWrappedTransaction>,
      transactionHooks: TransactionProviderHooks,
    ) => MaybePromise<R>,
    transactionInput?: TransactionProviderInput,
  ): Promise<R> {
    // Icky hack. This is just here to have user not have to do this.
    // These interfaces need to be factored better.
    const {
      upstreamSchema = '',
      clientGroupID = '',
      clientID = '',
      mutationID = 0,
    } = transactionInput ?? {};
    return this.connection.transaction(async dbTx => {
      const zeroTx = await this.#makeServerTransaction(
        dbTx,
        clientID,
        mutationID,
      );

      return callback(zeroTx, {
        async updateClientMutationID() {
          const formatted = formatPg(
            sql`INSERT INTO ${sql.ident(upstreamSchema)}.clients 
                    as current ("clientGroupID", "clientID", "lastMutationID")
                        VALUES (${clientGroupID}, ${clientID}, ${1})
                    ON CONFLICT ("clientGroupID", "clientID")
                    DO UPDATE SET "lastMutationID" = current."lastMutationID" + 1
                    RETURNING "lastMutationID"`,
          );

          const [{lastMutationID}] = (await dbTx.query(
            formatted.text,
            formatted.values,
          )) as {lastMutationID: bigint}[];

          return {lastMutationID};
        },

        async writeMutationResult(result) {
          const formatted = formatPg(
            sql`INSERT INTO ${sql.ident(upstreamSchema)}.mutations
                    ("clientGroupID", "clientID", "mutationID", "result")
                VALUES (${clientGroupID}, ${result.id.clientID}, ${result.id.id}, ${JSON.stringify(
                  result.result,
                )}::text::json)`,
          );
          await dbTx.query(formatted.text, formatted.values);
        },

        async deleteMutationResults(args: CleanupResultsArg) {
          if ('type' in args && args.type === 'bulk') {
            // Bulk deletion: delete all mutations for multiple clients
            const formatted = formatPg(
              sql`DELETE FROM ${sql.ident(upstreamSchema)}."mutations"
                  WHERE "clientGroupID" = ${args.clientGroupID}
                    AND "clientID" = ANY(${args.clientIDs})`,
            );
            await dbTx.query(formatted.text, formatted.values);
          } else {
            // Single client (explicit 'single' or legacy without type): delete up to mutation ID
            const formatted = formatPg(
              sql`DELETE FROM ${sql.ident(upstreamSchema)}."mutations"
                  WHERE "clientGroupID" = ${args.clientGroupID}
                    AND "clientID" = ${args.clientID}
                    AND "mutationID" <= ${args.upToMutationID}`,
            );
            await dbTx.query(formatted.text, formatted.values);
          }
        },
      });
    });
  }

  #makeServerTransaction(
    dbTx: DBTransaction<TWrappedTransaction>,
    clientID: string,
    mutationID: number,
  ) {
    return this.#crudFactory.createTransaction(dbTx, clientID, mutationID);
  }

  /**
   * Runs a single read query.
   *
   * When the {@linkcode DBConnection} implements `query`, the compiled SQL is
   * issued as a bare statement with no `BEGIN`/`COMMIT` around it (plus a
   * one-time server schema lookup the first time this instance touches the
   * database). Postgres then releases the statement's locks the moment it
   * finishes instead of waiting for a `COMMIT` round-trip, which matters in
   * serverless environments where the process can be frozen or reclaimed
   * between the query resolving and the `COMMIT` being sent, leaving the
   * transaction and its locks open indefinitely.
   *
   * Because no transaction is opened, any per-transaction setup the adapter
   * performs inside `transaction` does not apply to these reads. If the
   * connection does not implement `query`, the read is wrapped in a
   * transaction as before.
   *
   * If you need multiple reads to observe a consistent snapshot, use
   * {@linkcode transaction} and call `tx.run(...)` for each query instead.
   */
  run<TTable extends keyof TSchema['tables'] & string, TReturn>(
    query: Query<TTable, TSchema, TReturn>,
    _options?: RunOptions,
  ): Promise<HumanReadable<TReturn>> {
    const {connection} = this;
    if (connection.query) {
      // TS narrows `connection.query`, not `connection` itself.
      return this.#runOn(connection as ReadTarget<TWrappedTransaction>, query);
    }
    return connection.transaction(dbTx => this.#runOn(dbTx, query));
  }

  async #runOn<TTable extends keyof TSchema['tables'] & string, TReturn>(
    target: ReadTarget<TWrappedTransaction>,
    query: Query<TTable, TSchema, TReturn>,
  ): Promise<HumanReadable<TReturn>> {
    const {ast, format} = asQueryInternals(query);
    const serverSchema = await this.#crudFactory.getOrFetchServerSchema(target);
    const result = await (target.runQuery
      ? target.runQuery<TReturn>(ast, format, this.#schema, serverSchema)
      : executePostgresQuery<TReturn>(
          target,
          ast,
          format,
          this.#schema,
          serverSchema,
        ));
    // `runQuery` returns codec columns in their stored form (it may be a custom
    // adapter); decode here so server reads see the same app-typed values as
    // the client's IVM views.
    return decodeQueryResult(
      result,
      ast,
      format,
      this.#schema,
    ) as HumanReadable<TReturn>;
  }
}

/**
 * Something a read can be executed against: a `DBTransaction`, or a
 * `DBConnection` that implements `query`.
 */
type ReadTarget<TWrappedTransaction> = Queryable & {
  runQuery?: DBTransaction<TWrappedTransaction>['runQuery'] | undefined;
};
