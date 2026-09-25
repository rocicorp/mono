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
  const provider = new MemFallbackStoreProvider(inner, onFallBack);

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
  const provider = new MemFallbackStoreProvider(inner, vi.fn());
  expect(() => provider.create('other-error')).toThrow(error);
  expect(provider.failure).toBeUndefined();
});

test('fallBack moves every store onto memory together, closing the real ones', async () => {
  const onFallBack = vi.fn();
  const {provider: inner, created} = trackingProvider();
  const provider = new MemFallbackStoreProvider(inner, onFallBack);
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
  );
  expect(provider.fallBack(new Error('Chunk not found'))).toBe(false);
  expect(provider.failure).toBeUndefined();
});

test('after opened a storage failure no longer moves the stores', async () => {
  const onFallBack = vi.fn();
  const provider = new MemFallbackStoreProvider(
    trackingProvider().provider,
    onFallBack,
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
  const provider = new MemFallbackStoreProvider(inner, vi.fn());
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
