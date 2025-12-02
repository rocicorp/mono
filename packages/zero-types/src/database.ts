import type {MaybePromise} from '../../shared/src/types.ts';
import {type MutationResponse} from '../../zero-protocol/src/push.ts';

/**
 * Hooks invoked by the PushProcessor while executing a mutation inside a
 * database transaction.
 *
 * Implementations must run these operations inside the same transaction as the
 * mutator callback so that lastMutationID updates, mutation result persistence,
 * and user writes commit or roll back together.
 */
export interface TransactionProviderHooks {
  /**
   * Increments the client's lastMutationID and returns the updated value.
   */
  updateClientMutationID: () => Promise<{lastMutationID: number | bigint}>;

  /**
   * Persists the result of a mutation so it can be sent to the client via
   * replication.
   */
  writeMutationResult: (result: MutationResponse) => Promise<void>;
}

export interface TransactionProviderInput {
  upstreamSchema: string;
  clientGroupID: string;
  clientID: string;
  mutationID: number;
}

/**
 * Minimal database contract used by PushProcessor to execute mutators.
 *
 * Implementations open a transaction, invoke the callback with a transaction
 * object and hooks, and commit/roll back based on the callback result. All
 * bookkeeping performed via {@link TransactionProviderHooks} must participate
 * in the same transaction as user writes to preserve lastMutationID ordering
 * guarantees.
 */
export interface Database<T> {
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
      tx: T,
      transactionHooks: TransactionProviderHooks,
    ) => MaybePromise<R>,
    transactionInput?: TransactionProviderInput,
  ) => Promise<R>;
}
