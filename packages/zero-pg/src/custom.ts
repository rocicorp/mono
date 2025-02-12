import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {TransactionBase} from '../../zql/src/mutate/custom.ts';
import type {ConnectionProvider, DBTransaction} from './db.ts';
import type {PushHandler} from './web.ts';

export interface Transaction<S extends Schema, TWrappedTransaction>
  extends TransactionBase<S> {
  readonly location: 'server';
  readonly reason: 'authoritative';
  readonly dbTransaction: DBTransaction<TWrappedTransaction>;
  readonly token: string;
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

export function createPushHandler<
  S extends Schema,
  TDbTransaction,
  MD extends CustomMutatorDefs<S, TDbTransaction>,
>(
  _schema: S,
  _dbConnectionProvider: ConnectionProvider<TDbTransaction>,
  _customMutatorDefs: MD,
): PushHandler {
  throw new Error('Not implemented');
}
