import {
  getStorageFailure,
  type StorageFailureError,
} from '../storage-failure.ts';
import {dropMemStore, MemStore} from './mem-store.ts';
import type {Read, Store, StoreProvider, Write} from './store.ts';

/**
 * Wraps a {@link StoreProvider} so that the stores it creates move onto memory
 * together when their storage fails while the instance is opening (see
 * `onStorageFailure`): a store whose `create` throws a
 * {@link StorageFailureError} starts on a {@link MemStore}, and after
 * {@link fallBack} the other stores switch on their next `read()` or
 * `write()`. The memory stores keep the same names, so drops reach them too.
 *
 * Only before {@link opened}. After that the in-memory dag holds data loaded
 * from the real store and loads more on demand, which an empty memory store
 * cannot serve, so a failure is left to the caller.
 */
export class MemFallbackStoreProvider implements StoreProvider {
  readonly #inner: StoreProvider;
  readonly #onFallBack: (failure: StorageFailureError) => void;
  #failure: StorageFailureError | undefined;
  #opened = false;

  constructor(
    inner: StoreProvider,
    onFallBack: (failure: StorageFailureError) => void,
  ) {
    this.#inner = inner;
    this.#onFallBack = onFallBack;
  }

  /** Set once the stores have moved onto memory. */
  get failure(): StorageFailureError | undefined {
    return this.#failure;
  }

  /** The open has finished; storage failures no longer move the stores. */
  opened(): void {
    this.#opened = true;
  }

  /**
   * Moves the stores onto memory if `error` reports a storage failure, the
   * open has not finished, and they are not on memory already. Returns
   * whether it moved them.
   */
  fallBack(error: unknown): boolean {
    if (this.#failure !== undefined || this.#opened) {
      return false;
    }
    const failure = getStorageFailure(error);
    if (failure === undefined) {
      return false;
    }
    this.#failure = failure;
    this.#onFallBack(failure);
    return true;
  }

  create = (name: string): MemFallbackStore => {
    if (this.#failure === undefined) {
      try {
        return new MemFallbackStore(name, this.#inner.create(name), this);
      } catch (e) {
        if (!this.fallBack(e)) {
          throw e;
        }
      }
    }
    return new MemFallbackStore(name, new MemStore(name), this);
  };

  drop = (name: string): Promise<void> =>
    this.#failure === undefined ? this.#inner.drop(name) : dropMemStore(name);
}

export class MemFallbackStore implements Store {
  readonly #name: string;
  readonly #provider: MemFallbackStoreProvider;
  #store: Store;
  #onMemory: boolean;
  #closed = false;

  constructor(name: string, store: Store, provider: MemFallbackStoreProvider) {
    this.#name = name;
    this.#store = store;
    this.#onMemory = store instanceof MemStore;
    this.#provider = provider;
  }

  read(): Promise<Read> {
    return this.backing.read();
  }

  write(): Promise<Write> {
    return this.backing.write();
  }

  /** The store currently backing this one: the real store, or memory. */
  get backing(): Store {
    if (
      !this.#onMemory &&
      !this.#closed &&
      this.#provider.failure !== undefined
    ) {
      // Not awaited: a store whose storage failed may not complete its close.
      this.#store.close().catch(() => undefined);
      this.#store = new MemStore(this.#name);
      this.#onMemory = true;
    }
    return this.#store;
  }

  close(): Promise<void> {
    this.#closed = true;
    return this.#store.close();
  }

  get closed(): boolean {
    return this.#closed || this.#store.closed;
  }

  /** `'mem'` from the moment the stores are on memory. */
  get kind(): string | undefined {
    return this.backing.kind;
  }
}
