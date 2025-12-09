import type {MaybePromise} from '../../shared/src/types.ts';
import {formatPg, sql} from '../../z2s/src/sql.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {DBConnection, DBTransaction} from '../../zql/src/mutate/custom.ts';
import type {
  HumanReadable,
  Query,
  RunOptions,
} from '../../zql/src/query/query.ts';
import {CRUDMutatorFactory, type TransactionImpl} from './custom.ts';
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
export class ZQLDatabase<TSchema extends Schema, TWrappedTransaction>
  implements Database<TransactionImpl<TSchema, TWrappedTransaction>>
{
  readonly connection: DBConnection<TWrappedTransaction>;
  readonly #crudFactory: CRUDMutatorFactory<TSchema>;

  constructor(connection: DBConnection<TWrappedTransaction>, schema: TSchema) {
    this.connection = connection;
    this.#crudFactory = new CRUDMutatorFactory(schema);
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

  run<TTable extends keyof TSchema['tables'] & string, TReturn>(
    query: Query<TTable, TSchema, TReturn>,
    options?: RunOptions,
  ): Promise<HumanReadable<TReturn>> {
    return this.transaction(tx => tx.run(query, options));
  }
}
