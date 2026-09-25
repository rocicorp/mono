import {afterEach, expect, test, vi} from 'vitest';
import {assert} from '../../shared/src/asserts.ts';
import {IDBOpenError} from './kv/idb-store.ts';
import {MemStore, dropMemStore, hasMemStore} from './kv/mem-store.ts';
import type {Read, Store, Write} from './kv/store.ts';
import {makeChannelNameV1ForTesting} from './new-client-channel.ts';
import {getClientGroup} from './persist/client-groups.ts';
import {
  dropAllDatabases,
  dropDatabase,
} from './persist/collect-idb-databases.ts';
import {StorageFailureError} from './storage-failure.ts';
import {
  ReplicacheTest,
  addData,
  disableAllBackgroundProcesses,
  initReplicacheTesting,
  replicacheForTesting,
} from './test-util.ts';
import {withRead} from './with-transactions.ts';

initReplicacheTesting();

afterEach(() => {
  vi.restoreAllMocks();
});

/**
 * A MemStore whose write transactions can be made to fail the way the SQLite
 * store's do when the storage underneath it fails. Reads keep working, as
 * they do on a device whose disk is full.
 */
class StorageFailingStore implements Store {
  readonly #inner: MemStore;
  failWith: Error | undefined;
  failReadsWith: Error | undefined;
  writeAttempts = 0;
  readAttempts = 0;

  constructor(name: string) {
    this.#inner = new MemStore(name);
  }

  read(): Promise<Read> {
    this.readAttempts++;
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
  const failures: StorageFailureError[] = [];
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
  const diskError = new StorageFailureError('io-error', 'disk I/O error');
  perdag!.failWith = diskError;
  await rep.mutate.addData({b: 2});

  // Reported, not thrown: the app is told once, with the kind and the error.
  await expect(rep.persist()).resolves.toBeUndefined();
  expect(failures).toHaveLength(1);
  expect(failures[0]).toBe(diskError);

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
  const failures: StorageFailureError[] = [];
  rep.onStorageFailure = failure => failures.push(failure);

  await rep.mutate.addData({a: 1});
  stores.get(rep.idbName)!.failWith = new Error('something else entirely');
  await expect(rep.persist()).rejects.toThrow('something else entirely');
  expect(failures).toEqual([]);
});

test('a storage failure during the initial open is reported and moves the instance to memory', async () => {
  const stores = new Map<string, StorageFailingStore>();
  const diskError = new StorageFailureError(
    'cannot-open',
    'unable to open database file',
  );
  const failures: StorageFailureError[] = [];
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
  // instance reopens on memory: readiness arrives, and queries, mutations,
  // subscriptions, persist and refresh work against the memory stores.
  await vi.waitFor(() => expect(failures).toHaveLength(1));
  expect(failures[0]).toBe(diskError);
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
  // The mutation was persisted, to memory: without that the in-memory dag
  // could never evict what it holds.
  const clientGroupID = await rep.clientGroupID;
  const clientGroup = await withRead(rep.impl.perdag, read =>
    getClientGroup(clientGroupID, read),
  );
  expect(clientGroup?.mutationIDs[rep.clientID]).toBe(1);
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
  const openError = new StorageFailureError(
    'cannot-open',
    'unable to open database file',
  );
  const failingProvider = {
    create: (): Store => {
      throw openError;
    },
    drop: () => Promise.resolve(),
  };
  const failures: StorageFailureError[] = [];
  const rep = new ReplicacheTest(
    {
      name: 'storage-failure-at-create',
      pullURL: '',
      pushURL: '',
      kvStore: failingProvider,
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
  expect(failures[0]).toBe(openError);
  expect(rep.kvStore.kind).toBe('mem');
  await rep.clientGroupID;
  await rep.mutate.addData({a: 1});
  expect(await rep.query(tx => tx.get('a'))).toBe(1);
  await expect(rep.persist()).resolves.toBeUndefined();
  await rep.close();

  // Dropping with the same provider clears the memory store the instance ran
  // on, and still reports that the SQLite store could not be opened.
  expect(hasMemStore(rep.idbName)).toBe(true);
  await expect(
    dropDatabase(rep.idbName, {kvStore: failingProvider}),
  ).rejects.toBe(openError);
  expect(hasMemStore(rep.idbName)).toBe(false);
});

test('an IndexedDB that cannot be opened runs the instance in memory and reports once', async () => {
  // `indexedDB.open` fails, and IDBStore's first read or write rejects with
  // an IDBOpenError.
  const openError = new DOMException(
    'A mutation operation was attempted on a database that did not allow mutations.',
    'InvalidStateError',
  );
  const openRequests: IDBOpenDBRequest[] = [];
  vi.spyOn(indexedDB, 'open').mockImplementation(() => {
    const req = {error: openError} as IDBOpenDBRequest;
    openRequests.push(req);
    queueMicrotask(() => {
      assert(req.onerror, 'Expected onerror to be defined');
      req.onerror(new Event('error'));
    });
    return req;
  });
  const failures: StorageFailureError[] = [];
  const rep = new ReplicacheTest(
    {
      name: 'idb-cannot-open',
      pullURL: '',
      pushURL: '',
      kvStore: 'idb',
      mutators: {addData},
    },
    disableAllBackgroundProcesses,
  );
  rep.onStorageFailure = failure => failures.push(failure);
  rep.onClientStateNotFound = () => {
    throw new Error('a storage failure must not read as a lost client');
  };

  await vi.waitFor(() => expect(failures).toHaveLength(1));
  expect(failures[0].kind).toBe('cannot-open');
  expect(failures[0].cause).toBe(openError);
  expect(rep.kvStore.kind).toBe('mem');
  await rep.clientGroupID;
  await rep.mutate.addData({a: 1});
  expect(await rep.query(tx => tx.get('a'))).toBe(1);
  await expect(rep.persist()).resolves.toBeUndefined();
  expect(failures).toHaveLength(1);
  // No IndexedDB was opened after the switch to memory.
  const opened = openRequests.length;
  await rep.mutate.addData({b: 2});
  await rep.persist();
  expect(openRequests).toHaveLength(opened);

  // The in-memory instance still runs its background processes, including
  // the new-client channel: a client with a newer idbName asks it to update.
  let updateNeeded = 0;
  rep.onUpdateNeeded = () => updateNeeded++;
  const channel = new BroadcastChannel(
    makeChannelNameV1ForTesting('idb-cannot-open'),
  );
  channel.postMessage({clientGroupID: 'other-cg', idbName: 'newer-idb'});
  await vi.waitFor(() => expect(updateNeeded).toBe(1));
  channel.close();
  await rep.close();

  // Dropping clears the memory stores the instance fell back to, and still
  // reports that the IndexedDB could not be opened.
  expect(hasMemStore(rep.idbName)).toBe(true);
  await expect(dropAllDatabases()).rejects.toBeInstanceOf(IDBOpenError);
  expect(hasMemStore(rep.idbName)).toBe(false);
});

test('an onStorageFailure that throws does not stop the open on memory', async () => {
  const diskError = new StorageFailureError(
    'cannot-open',
    'unable to open database file',
  );
  const rep = new ReplicacheTest(
    {
      name: 'storage-failure-callback-throws',
      pullURL: '',
      pushURL: '',
      kvStore: {
        create: name => {
          const store = new StorageFailingStore(name);
          store.failWith = diskError;
          return store;
        },
        drop: name => dropMemStore(name),
      },
      mutators: {addData},
    },
    disableAllBackgroundProcesses,
  );
  let calls = 0;
  rep.onStorageFailure = () => {
    calls++;
    throw new Error('app bug');
  };

  await vi.waitFor(() => expect(calls).toBe(1));
  await rep.mutate.addData({a: 1});
  expect(await rep.query(tx => tx.get('a'))).toBe(1);
  await rep.close();
});

test('mutation recovery does not touch the store once it has failed', async () => {
  const stores = new Map<string, StorageFailingStore>();
  const rep = await replicacheForTesting(
    'storage-failure-recovery',
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
    {
      ...disableAllBackgroundProcesses,
      // The reconnect path calls recoverMutations directly, so leave it on.
      enableMutationRecovery: true,
    },
  );
  const failures: StorageFailureError[] = [];
  rep.onStorageFailure = failure => failures.push(failure);

  const perdag = stores.get(rep.idbName)!;
  perdag.failWith = new StorageFailureError('io-error', 'disk I/O error');
  await rep.mutate.addData({a: 1});
  await rep.persist();
  expect(failures).toHaveLength(1);

  const reads = Array.from(stores.values(), s => s.readAttempts);
  const writes = Array.from(stores.values(), s => s.writeAttempts);
  await expect(rep.recoverMutations()).resolves.toBe(false);
  expect(Array.from(stores.values(), s => s.readAttempts)).toEqual(reads);
  expect(Array.from(stores.values(), s => s.writeAttempts)).toEqual(writes);
});

test('drops clear the memory stores instances fell back to, not those of kvStore mem instances', async () => {
  const memRep = await replicacheForTesting(
    'live-mem',
    {kvStore: 'mem', mutators: {addData}},
    disableAllBackgroundProcesses,
  );
  await memRep.mutate.addData({a: 1});
  await memRep.persist();

  const openError = new StorageFailureError(
    'cannot-open',
    'unable to open database file',
  );
  const fallenBack = new ReplicacheTest(
    {
      name: 'fell-back',
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
  await fallenBack.clientGroupID;
  await fallenBack.close();
  expect(hasMemStore(fallenBack.idbName)).toBe(true);

  await dropAllDatabases();
  expect(hasMemStore(fallenBack.idbName)).toBe(false);
  expect(hasMemStore(memRep.idbName)).toBe(true);
  expect(await memRep.query(tx => tx.get('a'))).toBe(1);
});
