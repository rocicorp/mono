import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
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
 * cannot serve, so a failure is left to the caller. It is still reported, the
 * first time a transaction fails to begin or to commit on a storage failure,
 * whichever caller ran it (persist, heartbeat, garbage collection, ...).
 */
export class MemFallbackStoreProvider implements StoreProvider {
  readonly #inner: StoreProvider;
  readonly #onFallBack: (failure: StorageFailureError) => void;
  readonly #onFailureAfterOpen: (failure: StorageFailureError) => void;
  #failure: StorageFailureError | undefined;
  #opened = false;

  constructor(
    inner: StoreProvider,
    onFallBack: (failure: StorageFailureError) => void,
    onFailureAfterOpen: (failure: StorageFailureError) => void,
  ) {
    this.#inner = inner;
    this.#onFallBack = onFallBack;
    this.#onFailureAfterOpen = onFailureAfterOpen;
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

  /**
   * Reports `error` if it is a storage failure after the open. Before it,
   * the open's own error handling decides whether to fall back.
   */
  report(error: unknown): void {
    if (this.#opened && this.#failure === undefined) {
      const failure = getStorageFailure(error);
      if (failure !== undefined) {
        this.#onFailureAfterOpen(failure);
      }
    }
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

  async read(): Promise<Read> {
    return new ReportingRead(
      await this.backing.read().catch(this.#reportAndRethrow),
      this.#reportAndRethrow,
    );
  }

  async write(): Promise<Write> {
    return new ReportingWrite(
      await this.backing.write().catch(this.#reportAndRethrow),
      this.#reportAndRethrow,
    );
  }

  readonly #reportAndRethrow = (e: unknown): never => {
    this.#provider.report(e);
    throw e;
  };

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

/**
 * Reports a transaction that fails to end on a storage failure: its commit,
 * or its release (the SQLite store commits a read transaction and rolls back
 * an uncommitted write there). Not the per-key reads and writes: they are on
 * the hot path, and the SQLite store runs a write's statements in the commit.
 */
class ReportingRead<R extends Read> implements Read {
  protected readonly _tx: R;
  protected readonly _reportAndRethrow: (e: unknown) => never;

  constructor(tx: R, reportAndRethrow: (e: unknown) => never) {
    this._tx = tx;
    this._reportAndRethrow = reportAndRethrow;
  }

  has(key: string): Promise<boolean> {
    return this._tx.has(key);
  }

  get(key: string): Promise<ReadonlyJSONValue | undefined> {
    return this._tx.get(key);
  }

  release(): void {
    try {
      this._tx.release();
    } catch (e) {
      this._reportAndRethrow(e);
    }
  }

  get closed(): boolean {
    return this._tx.closed;
  }
}

class ReportingWrite extends ReportingRead<Write> implements Write {
  put(key: string, value: ReadonlyJSONValue): Promise<void> {
    return this._tx.put(key, value);
  }

  del(key: string): Promise<void> {
    return this._tx.del(key);
  }

  commit(): Promise<void> {
    return this._tx.commit().catch(this._reportAndRethrow);
  }
}
