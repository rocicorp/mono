import type {
  ReadTransaction as ReplicacheReadTransaction,
  WriteTransaction as ReplicacheWriteTransaction,
  ReadonlyJSONObject,
  MutatorReturn,
} from 'replicache';

/**
 * `UserData` must include a `userID` which is unique stable identifier
 * for the user.
 * `UserData` has a size limit of 6 KB.
 * `UserData` is passed via {@link WriteTransaction.userData} to mutators
 * when they are run on the server, which can use it to supplement
 * mutator args and to validate the mutation.
 */
export type UserData = ReadonlyJSONObject & {userID: string};

export interface ReadTransaction extends ReplicacheReadTransaction {
  readonly userData?: UserData | undefined;
}

export interface WriteTransaction extends ReplicacheWriteTransaction {
  readonly userData?: UserData | undefined;
}

export type MutatorDefs = {
  [key: string]: (
    tx: WriteTransaction,
    // Not sure how to not use any here...
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    args?: any,
  ) => MutatorReturn;
};
