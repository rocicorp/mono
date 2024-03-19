import type {ReadonlyJSONValue} from 'shared/src/json.js';

/**
 * Store defines a transactional key/value store that Replicache stores all data
 * within.
 *
 * For correct operation of Replicache, implementations of this interface must
 * provide [strict
 * serializable](https://jepsen.io/consistency/models/strict-serializable)
 * transactions.
 *
 * Informally, read and write transactions must behave like a ReadWrite Lock -
 * multiple read transactions are allowed in parallel, or one write.
 * Additionally writes from a transaction must appear all at one, atomically.
 *
 */
export interface Store {
  read(): Promise<Read>;
  write(): Promise<Write>;
  close(): Promise<void>;
  closed: boolean;
}

/**
 * Factory function for creating {@link Store} instances.
 *
 * The name is used to identify the store. If the same name is used for multiple
 * stores, they should share the same data. It is also desirable to have these
 * stores share an {@link RWLock}.
 *
 */
export type CreateStore = (name: string) => Store;

/**
 * Function for deleting {@link Store} instances.
 *
 * The name is used to identify the store. If the same name is used for multiple
 * stores, they should share the same data.
 *
 */
export type DropStore = (name: string) => Promise<void>;

/**
 * Provider for creating and deleting {@link Store} instances.
 *
 */
export type StoreProvider = {create: CreateStore; drop: DropStore};

/**
 * This interface is used so that we can release the lock when the transaction
 * is done.
 *
 * @experimental This interface is experimental and might be removed or changed
 * in the future without following semver versioning. Please be cautious.
 */
interface Release {
  release(): void;
}

/**
 * @experimental This interface is experimental and might be removed or changed
 * in the future without following semver versioning. Please be cautious.
 */
export interface Read extends Release {
  has(key: string): Promise<boolean>;
  // This returns ReadonlyJSONValue instead of FrozenJSONValue because we don't
  // want to FrozenJSONValue to be part of our public API. Our implementations
  // really return FrozenJSONValue but it is not required by the interface.
  get(key: string): Promise<ReadonlyJSONValue | undefined>;
  closed: boolean;
}

/**
 * @experimental This interface is experimental and might be removed or changed
 * in the future without following semver versioning. Please be cautious.
 */
export interface Write extends Read {
  put(key: string, value: ReadonlyJSONValue): Promise<void>;
  del(key: string): Promise<void>;
  commit(): Promise<void>;
}
