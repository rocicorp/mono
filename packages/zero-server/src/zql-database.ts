import {formatPg, sql} from '../../z2s/src/sql.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {ServerSchema} from '../../zero-schema/src/server-schema.ts';
import type {
  DBConnection,
  DBTransaction,
  SchemaCRUD,
  SchemaQuery,
} from '../../zql/src/mutate/custom.ts';
import type {HumanReadable, Query} from '../../zql/src/query/query.ts';
import {
  makeSchemaCRUD,
  makeServerTransaction,
  TransactionImpl,
} from './custom.ts';
import type {
  Database,
  TransactionProviderHooks,
  TransactionProviderInput,
} from './process-mutations.ts';
import {runQuery} from './query-runner.ts';
import {makeSchemaQuery} from './query.ts';

/**
 * Implements a Database for use with PushProcessor that is backed by Postgres.
 *
 * This implementation also implements the same ZQL interfaces for reading and
 * writing data that the Zero client does, so that mutator functions can be
 * shared across client and server.
 */
export class ZQLDatabase<TSchema extends Schema, WrappedTransaction, TContext>
  implements Database<TransactionImpl<TSchema, WrappedTransaction, TContext>>
{
  readonly connection: DBConnection<WrappedTransaction>;
  readonly #mutate: (
    dbTransaction: DBTransaction<WrappedTransaction>,
    serverSchema: ServerSchema,
  ) => SchemaCRUD<TSchema>;
  readonly #query: (
    dbTransaction: DBTransaction<WrappedTransaction>,
    serverSchema: ServerSchema,
  ) => SchemaQuery<TSchema, TContext>;
  readonly #schema: TSchema;
  readonly #context: TContext;

  constructor(
    connection: DBConnection<WrappedTransaction>,
    schema: TSchema,
    context: TContext,
  ) {
    this.connection = connection;
    this.#mutate = makeSchemaCRUD(schema);
    this.#query = makeSchemaQuery<TSchema, TContext>(schema);
    this.#schema = schema;
    this.#context = context;
  }

  transaction<R>(
    callback: (
      tx: TransactionImpl<TSchema, WrappedTransaction, TContext>,
      transactionHooks: TransactionProviderHooks,
    ) => Promise<R>,
    transactionInput?: TransactionProviderInput,
  ): Promise<R> {
    if (!transactionInput) {
      // Icky hack. This is just here to have user not have to do this.
      // These interfaces need to be factored better.
      transactionInput = {
        upstreamSchema: undefined as unknown as string,
        clientGroupID: undefined as unknown as string,
        clientID: undefined as unknown as string,
        mutationID: undefined as unknown as number,
      };
    }
    return this.connection.transaction(async dbTx => {
      const zeroTx = await makeServerTransaction(
        dbTx,
        transactionInput.clientID,
        transactionInput.mutationID,
        this.#schema,
        this.#mutate,
        this.#query,
        this.#context,
      );

      return callback(zeroTx, {
        async updateClientMutationID() {
          const formatted = formatPg(
            sql`INSERT INTO ${sql.ident(transactionInput.upstreamSchema)}.clients 
                    as current ("clientGroupID", "clientID", "lastMutationID")
                        VALUES (${transactionInput.clientGroupID}, ${transactionInput.clientID}, ${1})
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
            sql`INSERT INTO ${sql.ident(transactionInput.upstreamSchema)}.mutations
                    ("clientGroupID", "clientID", "mutationID", "result")
                VALUES (${transactionInput.clientGroupID}, ${result.id.clientID}, ${result.id.id}, ${JSON.stringify(
                  result.result,
                )}::text::json)`,
          );
          await dbTx.query(formatted.text, formatted.values);
        },
      });
    });
  }

  run<TTable extends keyof TSchema['tables'] & string, TReturn>(
    query: Query<TSchema, TTable, TReturn, TContext>,
  ): Promise<HumanReadable<TReturn>> {
    return this.connection.transaction(tx =>
      runQuery(tx, this.#schema, this.#context, query),
    );
  }
}
