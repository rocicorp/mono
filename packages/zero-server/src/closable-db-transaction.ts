import type {DBTransaction, Queryable} from '../../zql/src/mutate/custom.ts';
import type {HumanReadable} from '../../zql/src/query/query.ts';

/**
 * A {@linkcode DBTransaction} that stops working once closed. The mutator's
 * `tx` is built on it, so code that outlives the transaction cannot reach a
 * connection that has been committed, rolled back or returned to the pool.
 */
export class ClosableDBTransaction<T> implements DBTransaction<T> {
  readonly #inner: DBTransaction<T>;
  #closed = false;

  constructor(inner: DBTransaction<T>) {
    this.#inner = inner;
  }

  close(): void {
    this.#closed = true;
  }

  get wrappedTransaction(): T {
    if (this.#closed) {
      throw transactionEndedError();
    }
    return this.#inner.wrappedTransaction;
  }

  query: Queryable['query'] = (query, args) =>
    this.#closed
      ? Promise.reject(transactionEndedError())
      : this.#inner.query(query, args);

  runQuery<TReturn>(
    ...args: Parameters<DBTransaction<T>['runQuery']>
  ): Promise<HumanReadable<TReturn>> {
    return this.#closed
      ? Promise.reject(transactionEndedError())
      : this.#inner.runQuery<TReturn>(...args);
  }
}

function transactionEndedError(): Error {
  return new Error(
    'This transaction has ended and can no longer be used. A tx.retryOn() predicate runs after its transaction is over.',
  );
}
