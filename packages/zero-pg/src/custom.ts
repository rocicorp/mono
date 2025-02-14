import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {
  SchemaCRUD,
  SchemaQuery,
  TransactionBase,
} from '../../zql/src/mutate/custom.ts';
import type {ConnectionProvider, DBTransaction} from './db.ts';
import {PushProcessor, type PushHandler} from './web.ts';

export interface Transaction<S extends Schema, TWrappedTransaction>
  extends TransactionBase<S> {
  readonly location: 'server';
  readonly reason: 'authoritative';
  readonly dbTransaction: DBTransaction<TWrappedTransaction>;
  readonly token: string | undefined;
}

export type CustomMutatorDefs<S extends Schema, TDBTransaction> = {
  readonly [Table in keyof S['tables']]?: {
    readonly [key: string]: CustomMutatorImpl<S, TDBTransaction>;
  };
} & {
  [namespace: string]: {
    [key: string]: CustomMutatorImpl<S, TDBTransaction>;
  };
};

export type CustomMutatorImpl<S extends Schema, TDBTransaction> = (
  tx: Transaction<S, TDBTransaction>,
  args: ReadonlyJSONValue,
) => Promise<void>;

type Options<
  S extends Schema,
  TDBTransaction,
  MD extends CustomMutatorDefs<S, TDBTransaction>,
> = {
  schema: S;
  dbConnectionProvider: ConnectionProvider<TDBTransaction>;
  mutators: MD;
  shardID?: string;
};

export function createPushHandler<
  S extends Schema,
  TDBTransaction,
  MD extends CustomMutatorDefs<S, TDBTransaction>,
>({
  schema,
  dbConnectionProvider,
  mutators,
  shardID,
}: Options<S, TDBTransaction, MD>): PushHandler {
  const processor = new PushProcessor(
    shardID ?? '0',
    schema,
    dbConnectionProvider,
    mutators,
  );
  return (headers, body) => processor.process(headers, body);
}

export class TransactionImpl<S extends Schema, TWrappedTransaction>
  implements Transaction<S, TWrappedTransaction>
{
  readonly location = 'server';
  readonly reason = 'authoritative';
  readonly dbTransaction: DBTransaction<TWrappedTransaction>;
  readonly token: string | undefined;
  readonly clientID: string;
  readonly mutationID: number;
  readonly mutate: SchemaCRUD<S>;
  readonly query: SchemaQuery<S>;

  constructor(
    dbTransaction: DBTransaction<TWrappedTransaction>,
    _schema: S,
    token: string | undefined,
    clientID: string,
    mutationID: number,
  ) {
    this.dbTransaction = dbTransaction;
    this.token = token;
    this.clientID = clientID;
    this.mutationID = mutationID;

    // TODO: implement both of these.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    this.mutate = {} as any;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    this.query = {} as any;
    // this.mutate = makeSchemaCRUD(schema, dbTransaction);
    // this.query = makeSchemaQuery(schema, dbTransaction);
  }
}
