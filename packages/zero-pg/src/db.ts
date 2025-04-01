import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {TransactionBase} from '../../zql/src/mutate/custom.ts';

export interface ZeroTransaction<S extends Schema, TDBTransaction>
  extends TransactionBase<S> {
  readonly location: 'server';
  readonly reason: 'authoritative';
  readonly dbTransaction: TDBTransaction;
}
