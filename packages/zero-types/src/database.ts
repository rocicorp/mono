import type {MaybePromise} from '../../shared/src/types.ts';

/**
 * Minimal database contract used by PushProcessor to execute mutators.
 *
 * Implementations open a transaction, invoke the callback with a transaction
 * object and hooks, and commit/roll back based on the callback result. All
 * bookkeeping performed via {@link TransactionProviderHooks} must participate
 * in the same transaction as user writes to preserve lastMutationID ordering
 * guarantees.
 */
export interface BaseDatabase<
  TTransaction,
  TTransactionHooks,
  TTransactionInput,
> {
  /**
   * Runs a callback inside a database transaction and returns its result.
   *
   * @param callback Called with a transaction object that supports the
   *   mutator's reads/writes and the hooks PushProcessor uses for bookkeeping.
   * @param transactionInput Metadata describing the current mutation
   *   being processed (schema, client IDs, mutation ID).
   */
  transaction: <R>(
    callback: (
      tx: TTransaction,
      transactionHooks: TTransactionHooks,
    ) => MaybePromise<R>,
    transactionInput?: TTransactionInput,
  ) => Promise<R>;
}
