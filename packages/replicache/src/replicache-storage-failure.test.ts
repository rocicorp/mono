import {expect, test, vi} from 'vitest';
import {MemStore, dropMemStore} from './kv/mem-store.ts';
import type {Read, Store, Write} from './kv/store.ts';
import type {StorageFailure} from './storage-failure.ts';
import {
  ReplicacheTest,
  addData,
  disableAllBackgroundProcesses,
  initReplicacheTesting,
  replicacheForTesting,
} from './test-util.ts';

initReplicacheTesting();

/**
 * A MemStore whose write transactions can be made to fail the way a SQLite
 * store's do when the storage underneath it fails. Reads keep working, as
 * they do on a device whose disk is full.
 */
class StorageFailingStore implements Store {
  readonly #inner: MemStore;
  failWith: Error | undefined;
  failReadsWith: Error | undefined;
  writeAttempts = 0;

  constructor(name: string) {
    this.#inner = new MemStore(name);
  }

  read(): Promise<Read> {
    if (this.failReadsWith) {
      return Promise.reject(this.failReadsWith);
    }
    return this.#inner.read();
  }

  write(): Promise<Write> {
    this.writeAttempts++;
    if (this.failWith) {
      return Promise.reject(this.failWith);
    }
    return this.#inner.write();
  }

  close(): Promise<void> {
    return this.#inner.close();
  }

  get closed(): boolean {
    return this.#inner.closed;
  }
}

test('a storage failure during persist is reported once and stops persistence', async () => {
  const stores = new Map<string, StorageFailingStore>();
  const rep = await replicacheForTesting(
    'storage-failure',
    {
      kvStore: {
        create: name => {
          const store = new StorageFailingStore(name);
          stores.set(name, store);
          return store;
        },
        drop: name => dropMemStore(name),
      },
      mutators: {addData},
    },
    disableAllBackgroundProcesses,
  );
  const failures: StorageFailure[] = [];
  rep.onStorageFailure = failure => failures.push(failure);
  rep.onClientStateNotFound = () => {
    throw new Error('a storage failure must not read as a lost client');
  };

  const perdag = stores.get(rep.idbName);
  expect(perdag).toBeDefined();

  await rep.mutate.addData({a: 1});
  await rep.persist();
  expect(failures).toEqual([]);

  // The disk fails under the next persist, the way it does on a phone that
  // has run out of space or whose storage has gone away.
  const diskError = new Error(
    '[op-sqlite] SQLite error code: 10, description: disk I/O error',
  );
  perdag!.failWith = diskError;
  await rep.mutate.addData({b: 2});

  // Reported, not thrown: the app is told once, with the kind and the error.
  await expect(rep.persist()).resolves.toBeUndefined();
  expect(failures).toEqual([{kind: 'io-error', error: diskError}]);

  // And not retried: every later persist and refresh on this instance is a
  // no-op, because a rebuild would open the same failing storage.
  const attemptsAtFailure = perdag!.writeAttempts;
  await rep.persist();
  await rep.impl.refresh();
  await rep.mutate.addData({c: 3});
  await rep.persist();
  expect(perdag!.writeAttempts).toBe(attemptsAtFailure);
  expect(failures).toHaveLength(1);

  // What the in-memory dag already holds keeps answering, even once the
  // store refuses reads too. A read that needs a chunk not yet loaded from
  // the store is NOT covered here: it goes to the store and fails the way any
  // store read does, and the callback docs say so.
  perdag!.failReadsWith = diskError;
  expect(await rep.query(tx => tx.get('c'))).toBe(3);
  await rep.mutate.addData({d: 4});
  expect(await rep.query(tx => tx.get('d'))).toBe(4);
  expect(failures).toHaveLength(1);
});

test('an error that is not a storage failure still propagates from persist', async () => {
  const stores = new Map<string, StorageFailingStore>();
  const rep = await replicacheForTesting(
    'not-a-storage-failure',
    {
      kvStore: {
        create: name => {
          const store = new StorageFailingStore(name);
          stores.set(name, store);
          return store;
        },
        drop: name => dropMemStore(name),
      },
      mutators: {addData},
    },
    disableAllBackgroundProcesses,
  );
  const failures: StorageFailure[] = [];
  rep.onStorageFailure = failure => failures.push(failure);

  await rep.mutate.addData({a: 1});
  stores.get(rep.idbName)!.failWith = new Error('something else entirely');
  await expect(rep.persist()).rejects.toThrow('something else entirely');
  expect(failures).toEqual([]);
});

test('a storage failure during the initial open is reported and moves the instance to memory', async () => {
  const stores = new Map<string, StorageFailingStore>();
  const diskError = new Error(
    '[op-sqlite] SQLite error code: 14, description: unable to open database file',
  );
  const failures: StorageFailure[] = [];
  // Construct directly: replicacheForTesting awaits readiness, which an
  // instance whose open failed never reaches.
  const rep = new ReplicacheTest(
    {
      name: 'storage-failure-at-open',
      pullURL: '',
      pushURL: '',
      kvStore: {
        create: name => {
          const store = new StorageFailingStore(name);
          store.failWith = diskError;
          stores.set(name, store);
          return store;
        },
        drop: name => dropMemStore(name),
      },
      mutators: {addData},
    },
    disableAllBackgroundProcesses,
  );
  rep.onStorageFailure = failure => failures.push(failure);
  rep.onClientStateNotFound = () => {
    throw new Error('a storage failure must not read as a lost client');
  };

  // The open fails on the first write. The app is told once, nothing is
  // rethrown (vitest fails the test on an unhandled rejection), and the
  // instance reopens on memory: readiness arrives, queries, mutations and
  // subscriptions work for the session, and persist and refresh are no-ops.
  await vi.waitFor(() => expect(failures).toHaveLength(1));
  expect(failures[0]).toEqual({kind: 'cannot-open', error: diskError});
  expect(rep.kvStore.kind).toBe('mem');
  const seen: unknown[] = [];
  const unsubscribe = rep.subscribe(tx => tx.get('a'), {
    onData: value => seen.push(value),
  });
  await rep.mutate.addData({a: 1});
  expect(await rep.query(tx => tx.get('a'))).toBe(1);
  await vi.waitFor(() => expect(seen).toContain(1));
  unsubscribe();
  await expect(rep.persist()).resolves.toBeUndefined();
  await expect(rep.impl.refresh()).resolves.toBeUndefined();
  expect(failures).toHaveLength(1);
  // Nothing reached the failing stores after the fallback.
  const attempts = Array.from(stores.values(), s => s.writeAttempts);
  await rep.mutate.addData({b: 2});
  await rep.persist();
  expect(Array.from(stores.values(), s => s.writeAttempts)).toEqual(attempts);
  await rep.close();
});

test('a store that cannot be opened runs the instance in memory and reports once', async () => {
  // The SQLite providers open synchronously in their constructors, so a
  // device whose database file cannot be opened throws out of
  // `kvStoreProvider.create` while `new Replicache(...)` is still running —
  // before any callback can exist.
  const openError = new Error(
    '[op-sqlite] SQLite error code: 14, description: unable to open database file',
  );
  const failures: StorageFailure[] = [];
  const rep = new ReplicacheTest(
    {
      name: 'storage-failure-at-create',
      pullURL: '',
      pushURL: '',
      kvStore: {
        create: () => {
          throw openError;
        },
        drop: () => Promise.resolve(),
      },
      mutators: {addData},
    },
    disableAllBackgroundProcesses,
  );
  rep.onStorageFailure = failure => failures.push(failure);
  rep.onClientStateNotFound = () => {
    throw new Error('a storage failure must not read as a lost client');
  };

  // Construction did not throw, the report arrived after the callback was
  // attached, and the instance works from memory for the session.
  await vi.waitFor(() => expect(failures).toHaveLength(1));
  expect(failures[0]).toEqual({kind: 'cannot-open', error: openError});
  expect(rep.kvStore.kind).toBe('mem');
  await rep.clientGroupID;
  await rep.mutate.addData({a: 1});
  expect(await rep.query(tx => tx.get('a'))).toBe(1);
  await expect(rep.persist()).resolves.toBeUndefined();
  await rep.close();
});
