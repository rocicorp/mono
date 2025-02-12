import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {TransactionBase} from '../../zql/src/mutate/custom.ts';
import type {MaybePromise} from '../../shared/src/types.ts';

export interface ZeroTransaction<S extends Schema, TDBTransaction>
  extends TransactionBase<S> {
  readonly location: 'server';
  readonly reason: 'authoritative';
  readonly dbTransaction: TDBTransaction;
}

export interface Row {
  [column: string]: unknown;
}

/**
 * A function that returns a connection to the database which
 * will be used by custom mutators.
 */
export type ConnectionProvider<TWrappedTransaction> = () => MaybePromise<
  DBConnection<TWrappedTransaction>
>;

export interface DBConnection<TWrappedTransaction> extends Queryable {
  transaction: <T>(
    cb: (tx: DBTransaction<TWrappedTransaction>) => Promise<T>,
  ) => Promise<T>;
}

export interface DBTransaction<T> extends Queryable {
  readonly wrappedTransaction: T;
}

interface Queryable {
  query: (query: string, args: unknown[]) => Promise<Iterable<Row>>;
}
