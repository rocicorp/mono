import {resolver} from '@rocicorp/resolver';
import {afterEach, expect, test, vi} from 'vitest';
import {assert} from '../../shared/src/asserts.ts';
import {MemStore, dropMemStore} from './kv/mem-store.ts';
import type {Read, Store, Write} from './kv/store.ts';
import {
  addData,
  disableAllBackgroundProcesses,
  initReplicacheTesting,
  replicacheForTesting,
} from './test-util.ts';

initReplicacheTesting();

afterEach(() => {
  vi.restoreAllMocks();
});

/** A MemStore that counts transactions and can hold a write open. */
class CountingStore implements Store {
  readonly #inner: MemStore;
  writeAttempts = 0;
  readAttempts = 0;
  holdWrites: Promise<void> | undefined;
  onWrite: (() => void) | undefined;

  constructor(name: string) {
    this.#inner = new MemStore(name);
  }

  read(): Promise<Read> {
    this.readAttempts++;
    return this.#inner.read();
  }

  async write(): Promise<Write> {
    this.writeAttempts++;
    this.onWrite?.();
    await this.holdWrites;
    return this.#inner.write();
  }

  close(): Promise<void> {
    return this.#inner.close();
  }

  get closed(): boolean {
    return this.#inner.closed;
  }
}

const makeRep = async (name: string) => {
  const stores = new Map<string, CountingStore>();
  const rep = await replicacheForTesting(
    name,
    {
      kvStore: {
        create: n => {
          const store = new CountingStore(n);
          stores.set(n, store);
          return store;
        },
        drop: n => dropMemStore(n),
      },
      mutators: {addData},
    },
    {...disableAllBackgroundProcesses, enablePullAndPushInOpen: false},
  );
  const perdag = stores.get(rep.idbName);
  assert(perdag, 'the perdag store was created');
  return {rep, perdag};
};

test('stopPersistence makes every later persist and refresh a no-op', async () => {
  const {rep, perdag} = await makeRep('stop-persistence');
  await rep.mutate.addData({a: 1});
  await rep.persist();
  const writes = perdag.writeAttempts;
  const reads = perdag.readAttempts;

  await rep.impl.stopPersistence();

  await rep.mutate.addData({b: 2});
  await expect(rep.persist()).resolves.toBeUndefined();
  await expect(rep.impl.refresh()).resolves.toBeUndefined();
  await expect(rep.impl.runRefresh()).resolves.toBeUndefined();
  expect(perdag.writeAttempts).toBe(writes);
  expect(perdag.readAttempts).toBe(reads);

  // The in-memory dag keeps serving.
  expect(await rep.query(tx => tx.get('b'))).toBe(2);
  // Idempotent.
  await rep.impl.stopPersistence();
});

test('stopPersistence waits for a persist already in flight', async () => {
  const {rep, perdag} = await makeRep('stop-persistence-in-flight');
  await rep.mutate.addData({a: 1});

  // Hold the persist's write open, and learn when it has reached it. Fake
  // timers are on in this file, so the test settles on promises alone.
  const gate = resolver<void>();
  const reached = resolver<void>();
  perdag.holdWrites = gate.promise;
  perdag.onWrite = reached.resolve;
  const inFlight = rep.persist();
  await reached.promise;

  let stopped = false;
  const stopping = rep.impl.stopPersistence().then(() => {
    stopped = true;
  });
  for (let i = 0; i < 10; i++) {
    await Promise.resolve();
  }
  expect(stopped).toBe(false);

  gate.resolve();
  perdag.holdWrites = undefined;
  await inFlight;
  await stopping;
  expect(stopped).toBe(true);
});
