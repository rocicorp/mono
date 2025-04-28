import type {
  DatabaseProvider,
  TransactionProviderHooks,
  TransactionProviderInput,
} from './push-processor.ts';
import type {
  DBConnection,
  DBTransaction,
  SchemaCRUD,
  SchemaQuery,
} from '../../zql/src/mutate/custom.ts';
import {formatPg, sql} from '../../z2s/src/sql.ts';
import type {ServerSchema} from '../../z2s/src/schema.ts';
import {makeSchemaQuery} from './query.ts';
import {makeSchemaCRUD, TransactionImpl} from './custom.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import {makeServerTransaction} from './custom.ts';

export class ZQLDatabaseProvider<S extends Schema, WrappedTransaction>
  implements DatabaseProvider<TransactionImpl<S, WrappedTransaction>>
{
  readonly #connection: DBConnection<WrappedTransaction>;

  readonly #mutate: (
    dbTransaction: DBTransaction<WrappedTransaction>,
    serverSchema: ServerSchema,
  ) => SchemaCRUD<S>;
  readonly #query: (
    dbTransaction: DBTransaction<WrappedTransaction>,
    serverSchema: ServerSchema,
  ) => SchemaQuery<S>;
  readonly #schema: S;

  constructor(connection: DBConnection<WrappedTransaction>, schema: S) {
    this.#connection = connection;
    this.#mutate = makeSchemaCRUD(schema);
    this.#query = makeSchemaQuery(schema);
    this.#schema = schema;
  }

  transaction<R>(
    callback: (
      tx: TransactionImpl<S, WrappedTransaction>,
      transactionHooks: TransactionProviderHooks,
    ) => Promise<R>,
    transactionInput: TransactionProviderInput,
  ): Promise<R> {
    return this.#connection.transaction(async dbTx => {
      const zeroTx = await makeServerTransaction(
        dbTx,
        transactionInput.clientID,
        transactionInput.mutationID,
        this.#schema,
        this.#mutate,
        this.#query,
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
      });
    });
  }
}
