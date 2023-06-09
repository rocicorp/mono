import type {
  ReadTransaction as ReplicacheReadTransaction,
  WriteTransaction as ReplicacheWriteTransaction,
  ReadonlyJSONObject,
  MutatorReturn,
} from 'replicache';

/**
 * `AuthData` must include a `userID` which is unique stable identifier
 * for the user.
 * `AuthData` has a size limit of 6 KB.
 * `AuthData` is passed via {@link WriteTransaction.auth} to mutators
 * when they are run on the server, which can use it to supplement
 * mutator args and to authorize the mutation.
 */
export type AuthData = ReadonlyJSONObject & {readonly userID: string};

/**
 * Alias for {@link AuthData}, kept for backward compatibility.
 */
export type UserData = AuthData;

export interface ReadTransaction extends ReplicacheReadTransaction {
  /**
   * When a mutation is run on the server, the `AuthData` for the connection
   * that pushed the mutation.  Always undefined on the client. This can be
   * used to implement fine-grained server-side authorization of mutations.
   */
  readonly auth?: AuthData | undefined;
}

export interface WriteTransaction extends ReplicacheWriteTransaction {
  /**
   * When a mutation is run on the server, the `AuthData` for the connection
   * that pushed the mutation.  Always undefined on the client. This can be
   * used to implement fine-grained server-side authorization of mutations.
   */
  readonly auth?: AuthData | undefined;
}

export type MutatorDefs = {
  [key: string]: (
    tx: WriteTransaction,
    // Not sure how to not use any here...
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    args?: any,
  ) => MutatorReturn;
};
