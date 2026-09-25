import {expect, test, vi} from 'vitest';
import {StorageFailureError} from '../storage-failure.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import {MemFallbackStoreProvider} from './mem-fallback-store.ts';
import {MemStore, dropMemStore, hasMemStore} from './mem-store.ts';
import type {Store, StoreProvider} from './store.ts';

const failure = () =>
  new StorageFailureError('cannot-open', 'unable to open database file');

/** Creates tracked stores that look like a real (non-memory) store. */
function trackingProvider(create?: (name: string) => Store): {
  provider: StoreProvider;
  created: Store[];
  dropped: string[];
} {
  const created: Store[] = [];
  const dropped: string[] = [];
  return {
    created,
    dropped,
    provider: {
      create: name => {
        const store = create?.(name) ?? new RealStore(name);
        created.push(store);
        return store;
      },
      drop: name => {
        dropped.push(name);
        return Promise.resolve();
      },
    },
  };
}

/** A memory-backed store that is not a MemStore, so it can fall back. */
class RealStore implements Store {
  readonly #inner: MemStore;
  constructor(name: string) {
    this.#inner = new MemStore(`real-${name}`);
  }
  read() {
    return this.#inner.read();
  }
  write() {
    return this.#inner.write();
  }
  close() {
    return this.#inner.close();
  }
  get closed() {
    return this.#inner.closed;
  }
  get kind() {
    return 'real';
  }
}

test('a store whose create throws a storage failure starts on memory', async () => {
  const onFallBack = vi.fn();
  const {provider: inner} = trackingProvider(() => {
    throw failure();
  });
  const provider = new MemFallbackStoreProvider(inner, onFallBack, vi.fn());

  const store = provider.create('create-throws');
  expect(store.kind).toBe('mem');
  expect(onFallBack).toHaveBeenCalledTimes(1);
  expect(onFallBack.mock.calls[0][0]).toBeInstanceOf(StorageFailureError);
  await withWrite(store, write => write.put('k', 'v'));
  expect(await withRead(store, read => read.get('k'))).toBe('v');
  // Later stores go straight to memory, without asking the inner provider.
  expect(provider.create('later').kind).toBe('mem');
  expect(onFallBack).toHaveBeenCalledTimes(1);
});

test('other errors from create are rethrown', () => {
  const error = new Error('something else');
  const {provider: inner} = trackingProvider(() => {
    throw error;
  });
  const provider = new MemFallbackStoreProvider(inner, vi.fn(), vi.fn());
  expect(() => provider.create('other-error')).toThrow(error);
  expect(provider.failure).toBeUndefined();
});

test('fallBack moves every store onto memory together, closing the real ones', async () => {
  const onFallBack = vi.fn();
  const {provider: inner, created} = trackingProvider();
  const provider = new MemFallbackStoreProvider(inner, onFallBack, vi.fn());
  const replica = provider.create('replica');
  const registry = provider.create('registry');
  expect(replica.kind).toBe('real');

  expect(provider.fallBack(new Error('wrapped', {cause: failure()}))).toBe(
    true,
  );
  expect(onFallBack).toHaveBeenCalledTimes(1);
  // Reported as memory right away, before either store is used again.
  expect(replica.kind).toBe('mem');
  expect(registry.kind).toBe('mem');
  await withRead(replica, () => undefined);
  await withRead(registry, () => undefined);
  await vi.waitFor(() => expect(created.every(s => s.closed)).toBe(true));

  // They are on memory already: nothing moves and nobody is told twice.
  expect(provider.fallBack(failure())).toBe(false);
  expect(onFallBack).toHaveBeenCalledTimes(1);
});

test('fallBack ignores errors that are not storage failures', () => {
  const provider = new MemFallbackStoreProvider(
    trackingProvider().provider,
    vi.fn(),
    vi.fn(),
  );
  expect(provider.fallBack(new Error('Chunk not found'))).toBe(false);
  expect(provider.failure).toBeUndefined();
});

test('after opened a storage failure no longer moves the stores', async () => {
  const onFallBack = vi.fn();
  const provider = new MemFallbackStoreProvider(
    trackingProvider().provider,
    onFallBack,
    vi.fn(),
  );
  const store = provider.create('opened');
  provider.opened();
  expect(provider.fallBack(failure())).toBe(false);
  await withRead(store, () => undefined);
  expect(store.kind).toBe('real');
  expect(onFallBack).not.toHaveBeenCalled();
});

test('drop goes to the memory stores once they are in use', async () => {
  const {provider: inner, dropped} = trackingProvider();
  const provider = new MemFallbackStoreProvider(inner, vi.fn(), vi.fn());
  await provider.drop('before');
  expect(dropped).toEqual(['before']);

  provider.fallBack(failure());
  const store = provider.create('after');
  await withWrite(store, write => write.put('k', 'v'));
  expect(hasMemStore('after')).toBe(true);
  await provider.drop('after');
  expect(hasMemStore('after')).toBe(false);
  expect(dropped).toEqual(['before']);
  await dropMemStore('real-before');
});

/** A RealStore whose transactions can be made to fail to begin or commit. */
class FailingStore extends RealStore {
  failBegin: Error | undefined;
  failCommit: Error | undefined;
  failRelease: Error | undefined;

  override async read() {
    const read = await super.read();
    this.#failReleaseOf(read);
    return read;
  }

  override async write() {
    if (this.failBegin) {
      throw this.failBegin;
    }
    const write = await super.write();
    const failCommit = this.failCommit;
    if (failCommit) {
      write.commit = () => Promise.reject(failCommit);
    }
    this.#failReleaseOf(write);
    return write;
  }

  #failReleaseOf(tx: {release(): void}) {
    const failRelease = this.failRelease;
    if (failRelease) {
      const release = tx.release.bind(tx);
      tx.release = () => {
        release();
        throw failRelease;
      };
    }
  }
}

test('after opened a transaction that fails to begin, commit or end on a storage failure is reported', async () => {
  const onFailureAfterOpen = vi.fn();
  const inner = new FailingStore('after-open');
  const provider = new MemFallbackStoreProvider(
    {create: () => inner, drop: () => Promise.resolve()},
    vi.fn(),
    onFailureAfterOpen,
  );
  const store = provider.create('after-open');

  // Before the open finishes, the open's own error handling decides.
  inner.failCommit = failure();
  await expect(withWrite(store, write => write.put('k', 'v'))).rejects.toBe(
    inner.failCommit,
  );
  expect(onFailureAfterOpen).not.toHaveBeenCalled();

  provider.opened();
  await expect(withWrite(store, write => write.put('k', 'v'))).rejects.toBe(
    inner.failCommit,
  );
  expect(onFailureAfterOpen).toHaveBeenCalledTimes(1);
  expect(onFailureAfterOpen.mock.calls[0][0]).toBe(inner.failCommit);

  inner.failCommit = undefined;
  inner.failBegin = failure();
  await expect(store.write()).rejects.toBe(inner.failBegin);
  expect(onFailureAfterOpen).toHaveBeenCalledTimes(2);
  expect(store.kind).toBe('real');

  // A read or write that fails as it ends: the SQLite store commits a read
  // transaction, and rolls back an uncommitted write, in release().
  inner.failBegin = undefined;
  inner.failRelease = failure();
  await expect(withRead(store, () => undefined)).rejects.toBe(
    inner.failRelease,
  );
  expect(onFailureAfterOpen).toHaveBeenCalledTimes(3);
  const write = await store.write();
  expect(() => write.release()).toThrow(inner.failRelease);
  expect(onFailureAfterOpen).toHaveBeenCalledTimes(4);
  inner.failRelease = undefined;

  // Other errors are not storage failures.
  inner.failBegin = new Error('something else');
  await expect(store.write()).rejects.toBe(inner.failBegin);
  expect(onFailureAfterOpen).toHaveBeenCalledTimes(4);
});
