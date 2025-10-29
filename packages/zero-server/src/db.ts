import type {Schema} from '../../zero-types/src/schema.ts';
import type {TransactionBase} from '../../zql/src/mutate/custom.ts';

export interface ZeroTransaction<
  TSchema extends Schema,
  TDBTransaction,
  TContext,
> extends TransactionBase<TSchema, TContext> {
  readonly location: 'server';
  readonly reason: 'authoritative';
  readonly dbTransaction: TDBTransaction;
}
