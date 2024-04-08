import {ReadonlyJSONValue} from './json.js';

type DiffOperationAdd = {
  readonly op: 'add';
  readonly key: string;
  readonly newValue: ReadonlyJSONValue;
};

type DiffOperationDel = {
  readonly op: 'del';
  readonly key: string;
  readonly oldValue: ReadonlyJSONValue;
};

type DiffOperationChange = {
  readonly op: 'change';
  readonly key: string;
  readonly oldValue: ReadonlyJSONValue;
  readonly newValue: ReadonlyJSONValue;
};

type DiffOperation = DiffOperationAdd | DiffOperationDel | DiffOperationChange;

type WatchCallback = (diff: readonly DiffOperation[]) => void;

type WatchOptions = {
  prefix?: string | undefined;
  initialValuesInFirstDiff?: boolean | undefined;
};

export type ReplicacheLike = {
  experimentalWatch(
    callback: WatchCallback,
    options?: WatchOptions,
  ): () => void;
};
