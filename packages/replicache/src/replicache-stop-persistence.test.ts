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

test('stopPersist makes every later persist a no-op, but not refresh', async () => {
  const {rep, perdag} = await makeRep('stop-persist');
  await rep.mutate.addData({a: 1});
  await rep.persist();
  const writes = perdag.writeAttempts;

  await rep.impl.stopPersist();

  await rep.mutate.addData({b: 2});
  await expect(rep.persist()).resolves.toBeUndefined();
  expect(perdag.writeAttempts).toBe(writes);

  // Refresh is stopped separately.
  await rep.impl.refresh();
  expect(perdag.writeAttempts).toBeGreaterThan(writes);

  // The in-memory dag keeps serving.
  expect(await rep.query(tx => tx.get('b'))).toBe(2);
  // Idempotent.
  await rep.impl.stopPersist();
});

test('stopRefresh makes every later refresh a no-op, but not persist', async () => {
  const {rep, perdag} = await makeRep('stop-refresh');
  await rep.mutate.addData({a: 1});
  await rep.persist();
  const writes = perdag.writeAttempts;

  await rep.impl.stopRefresh();

  await expect(rep.impl.refresh()).resolves.toBeUndefined();
  await expect(rep.impl.runRefresh()).resolves.toBeUndefined();
  expect(perdag.writeAttempts).toBe(writes);

  // Persist is stopped separately.
  await rep.mutate.addData({b: 2});
  await rep.persist();
  expect(perdag.writeAttempts).toBeGreaterThan(writes);

  expect(await rep.query(tx => tx.get('b'))).toBe(2);
  // Idempotent.
  await rep.impl.stopRefresh();
});

test('stopPersist and stopRefresh together stop every store access', async () => {
  const {rep, perdag} = await makeRep('stop-both');
  await rep.mutate.addData({a: 1});
  await rep.persist();
  const writes = perdag.writeAttempts;
  const reads = perdag.readAttempts;

  await Promise.all([rep.impl.stopPersist(), rep.impl.stopRefresh()]);

  await rep.mutate.addData({b: 2});
  await expect(rep.persist()).resolves.toBeUndefined();
  await expect(rep.impl.refresh()).resolves.toBeUndefined();
  await expect(rep.impl.runRefresh()).resolves.toBeUndefined();
  expect(perdag.writeAttempts).toBe(writes);
  expect(perdag.readAttempts).toBe(reads);

  // The in-memory dag keeps serving.
  expect(await rep.query(tx => tx.get('b'))).toBe(2);
});

/**
 * Holds the next write to `perdag` open until the returned `release` is
 * called; `reached` resolves once that write has started. Fake timers are on
 * in this file, so the tests settle on promises alone.
 */
function holdNextWrite(perdag: CountingStore) {
  const gate = resolver<void>();
  const reached = resolver<void>();
  perdag.holdWrites = gate.promise;
  perdag.onWrite = reached.resolve;
  return {
    reached: reached.promise,
    release: () => {
      gate.resolve();
      perdag.holdWrites = undefined;
      perdag.onWrite = undefined;
    },
  };
}

async function expectPending(p: Promise<void>) {
  let settled = false;
  void p.then(() => {
    settled = true;
  });
  for (let i = 0; i < 10; i++) {
    await Promise.resolve();
  }
  expect(settled).toBe(false);
}

test('stopPersist waits for a persist already in flight', async () => {
  const {rep, perdag} = await makeRep('stop-persist-in-flight');
  await rep.mutate.addData({a: 1});

  const {reached, release} = holdNextWrite(perdag);
  const inFlight = rep.persist();
  await reached;

  const stopping = rep.impl.stopPersist();
  await expectPending(stopping);

  release();
  await inFlight;
  await stopping;
});

test('stopRefresh waits for a refresh already in flight', async () => {
  const {rep, perdag} = await makeRep('stop-refresh-in-flight');
  await rep.mutate.addData({a: 1});
  await rep.persist();

  const {reached, release} = holdNextWrite(perdag);
  const inFlight = rep.impl.refresh();
  await reached;

  const stopping = rep.impl.stopRefresh();
  await expectPending(stopping);

  release();
  await inFlight;
  await stopping;
});
