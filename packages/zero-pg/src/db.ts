import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {TransactionBase} from '../../zql/src/mutate/custom.ts';

export interface ZeroTransaction<S extends Schema, TDBTransaction>
  extends TransactionBase<S> {
  readonly location: 'server';
  readonly reason: 'authoritative';
  readonly dbTransaction: TDBTransaction;
}

interface Row {
  [column: string]: unknown;
}

/**
 * A function that returns a connection to the database which
 * will be used by custom mutators.
 */
export type ConnectionProvider<TWrappedTransaction> = () => Promise<
  DbConnection<TWrappedTransaction>
>;

export interface DbConnection<TWrappedTransaction> extends Queryable {
  transaction: (
    cb: (tx: DBTransaction<TWrappedTransaction>) => Promise<void>,
  ) => void;
}

export interface DBTransaction<T> extends Queryable {
  readonly wrappedTransaction: T;
}

interface Queryable {
  query: (query: string, args: unknown[]) => Promise<Row>;
}
