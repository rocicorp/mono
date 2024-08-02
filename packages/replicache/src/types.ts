import type {MaybePromise} from 'shared/src/types.js';
import type {Hash} from './hash.js';
import type {ReadonlyJSONValue, WriteTransaction} from './mod.js';
import type {PullResponseV1, PullResponseV1Internal} from './puller.js';
import type {ReadTransactionImpl} from './transactions.js';

export type BeginPullResult = {
  requestID: string;
  syncHead: Hash;
  ok: boolean;
};
export type Poke = {
  baseCookie: ReadonlyJSONValue;
  pullResponse: PullResponseV1;
};

export type PokeInternal = {
  baseCookie: ReadonlyJSONValue;
  pullResponse: PullResponseV1Internal;
};

export type MutatorReturn<T extends ReadonlyJSONValue = ReadonlyJSONValue> =
  MaybePromise<T | void>; /**
 * The type used to describe the mutator definitions passed into [Replicache](classes/Replicache)
 * constructor as part of the {@link ReplicacheOptions}.
 *
 * See {@link ReplicacheOptions} {@link ReplicacheOptions.mutators | mutators} for more
 * info.
 */

export type MutatorDefs = {
  [key: string]: (
    tx: WriteTransaction,
    // Not sure how to not use any here...
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    args?: any,
  ) => MutatorReturn;
};

export type MakeMutator<
  F extends (
    tx: WriteTransaction,
    ...args: [] | [ReadonlyJSONValue]
  ) => MutatorReturn,
> = F extends (tx: WriteTransaction, ...args: infer Args) => infer Ret
  ? (...args: Args) => ToPromise<Ret>
  : never; /**
 * Base options for {@link PullOptions} and {@link PushOptions}
 */

export interface RequestOptions {
  /**
   * When there are pending pull or push requests this is the _minimum_ amount
   * of time to wait until we try another pull/push.
   */
  minDelayMs?: number;

  /**
   * When there are pending pull or push requests this is the _maximum_ amount
   * of time to wait until we try another pull/push.
   */
  maxDelayMs?: number;
}

export type MakeMutators<T extends MutatorDefs> = {
  readonly [P in keyof T]: MakeMutator<T[P]>;
};

export type ToPromise<P> = P extends Promise<unknown> ? P : Promise<P>;

export type QueryInternal = <R>(
  body: (tx: ReadTransactionImpl) => MaybePromise<R>,
) => Promise<R>; /**
 * The reason {@link onUpdateNeeded} was called.
 */

export type UpdateNeededReason =
  | {
      // There is a new client group due to a new tab loading new code with
      // different mutators, indexes, schema version, or format version.
      // This tab cannot sync locally with this new tab until it updates to
      // the new code.
      type: 'NewClientGroup';
    }
  | {
      type: 'VersionNotSupported';
      versionType?: 'push' | 'pull' | 'schema' | undefined;
    };
