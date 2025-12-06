import {resolver} from '@rocicorp/resolver';
import {
  afterEach,
  beforeEach,
  expect,
  vi,
  type Assertion,
  type Mock,
  type MockInstance,
  type VitestUtils,
} from 'vitest';
import type {JSONValue} from '../../shared/src/json.ts';
import {must} from '../../shared/src/must.ts';
import {randomUint64} from '../../shared/src/random-uint64.ts';
import type {Cookie} from './cookies.ts';
import type {Store} from './dag/store.ts';
import {FetchMocker} from './fetch-mocker.ts';
import type {Hash} from './hash.ts';
import {dropIDBStoreWithMemFallback} from './kv/idb-store-with-mem-fallback.ts';
import {MemStore} from './kv/mem-store.ts';
import type {Store as KVStore} from './kv/store.ts';
import type {PatchOperation} from './patch-operation.ts';
import {
  setupForTest as setupIDBDatabasesStoreForTest,
  teardownForTest as teardownIDBDatabasesStoreForTest,
} from './persist/idb-databases-store-db-name.ts';
import type {PullResponseV1} from './puller.ts';
import {ReplicacheImpl, type ReplicacheImplOptions} from './replicache-impl.ts';
import type {ReplicacheOptions} from './replicache-options.ts';
import {
  Replicache,
  restoreMakeImplForTest,
  setMakeImplForTest,
} from './replicache.ts';
import type {DiffComputationConfig} from './sync/diff.ts';
import type {ClientID} from './sync/ids.ts';
import type {WriteTransaction} from './transactions.ts';
import type {BeginPullResult, MutatorDefs} from './types.ts';

export class ReplicacheTest<
  MD extends MutatorDefs = {},
> extends Replicache<MD> {
  readonly #impl: ReplicacheImpl<MD>;
  recoverMutationsFake: Mock<(r: Promise<boolean>) => Promise<boolean>>;

  constructor(
    options: ReplicacheOptions<MD>,
    implOptions?: ReplicacheImplOptions,
  ) {
    let impl: ReplicacheImpl<MD> | undefined = undefined;
    setMakeImplForTest(<M extends MutatorDefs>(ops: ReplicacheOptions<M>) => {
      const repImpl = new ReplicacheImpl<M>(ops, implOptions);
      impl = repImpl as unknown as ReplicacheImpl<MD>;
      return repImpl;
    });
    super(options);
    restoreMakeImplForTest();
    this.#impl = must<ReplicacheImpl<MD>>(impl);
    this.recoverMutationsFake = this.onRecoverMutations = vi.fn(r => r);
  }

  pullIgnorePromise(opts?: Parameters<Replicache['pull']>[0]): void {
    void this.pull(opts).catch(e => e);
  }

  beginPull(): Promise<BeginPullResult> {
    return this.#impl.beginPull();
  }

  maybeEndPull(syncHead: Hash, requestID: string): Promise<void> {
    return this.#impl.maybeEndPull(syncHead, requestID);
  }

  persist() {
    return this.#impl.persist();
  }

  recoverMutations(): Promise<boolean> {
    return this.#impl.recoverMutations() as Promise<boolean>;
  }

  get perdag() {
    return this.#impl.perdag;
  }

  get isClientGroupDisabled(): boolean {
    return this.#impl.isClientGroupDisabled;
  }

  get memdag(): Store {
    return this.#impl.memdag;
  }

  get lastMutationID(): number {
    return this.#impl.lastMutationID;
  }

  get onBeginPull() {
    return this.#impl.onBeginPull;
  }
  set onBeginPull(v) {
    this.#impl.onBeginPull = v;
  }

  get onPushInvoked() {
    return this.#impl.onPushInvoked;
  }
  set onPushInvoked(v) {
    this.#impl.onPushInvoked = v;
  }

  get onRecoverMutations() {
    return this.#impl.onRecoverMutations;
  }
  set onRecoverMutations(v) {
    this.#impl.onRecoverMutations = v;
  }

  get impl() {
    return this.#impl;
  }
}

export const reps: Set<ReplicacheTest> = new Set();
export async function closeAllReps(): Promise<void> {
  for (const rep of reps) {
    if (!rep.closed) {
      await rep.close();
    }
  }
  reps.clear();
}

/**
 * Additional closeables to close as part of teardown.
 * Likely kb.Store(s) or dag.Store(s), which should be closed before
 * deleting the underlying IndexedDB databases.  These are closed before
 * `dbsToDrop` are deleted.
 */
export const closeablesToClose: Set<{close: () => Promise<unknown>}> =
  new Set();

async function closeAllCloseables(): Promise<void> {
  for (const closeable of closeablesToClose) {
    await closeable.close();
  }
  closeablesToClose.clear();
}

export const dbsToDrop: Set<string> = new Set();
export async function deleteAllDatabases(): Promise<void> {
  for (const name of dbsToDrop) {
    await dropIDBStoreWithMemFallback(name);
  }
  dbsToDrop.clear();
}

type ReplicacheTestOptions<MD extends MutatorDefs> = Omit<
  ReplicacheOptions<MD>,
  'name' | 'licenseKey'
> & {
  onClientStateNotFound?: (() => void) | null | undefined;
  licenseKey?: string | undefined;
};

export async function replicacheForTesting<MD extends MutatorDefs = {}>(
  name: string,
  options: ReplicacheTestOptions<MD> = {},
  implOptions: ReplicacheImplOptions = {},
  testOptions: {
    useDefaultURLs?: boolean | undefined; // default true
    useUniqueName?: boolean | undefined; // default true
  } = {},
): Promise<ReplicacheTest<MD>> {
  const defaultURLs = {
    pullURL: 'https://pull.com/?name=' + name,
    pushURL: 'https://push.com/?name=' + name,
  };
  const {useDefaultURLs = true, useUniqueName = true} = testOptions;
  const {
    pullURL,
    pushDelay = 60_000, // Large to prevent interfering
    pushURL,
    onClientStateNotFound = () => {
      throw new Error(
        'Unexpected call to onClientStateNotFound. Did you forget to pass it as an option?',
      );
    },
    ...rest
  }: ReplicacheTestOptions<MD> = useDefaultURLs
    ? {...defaultURLs, ...options}
    : options;

  const rep = new ReplicacheTest<MD>(
    {
      pullURL,
      pushDelay,
      pushURL,
      name: useUniqueName ? `${randomUint64().toString(36)}:${name}` : name,
      ...rest,
    },
    implOptions,
  );
  dbsToDrop.add(rep.idbName);
  reps.add(rep);

  rep.onClientStateNotFound = onClientStateNotFound;

  const {clientID} = rep;
  // Wait for open to be done.
  await rep.clientGroupID;
  if (pullURL) {
    fetchMocker.post(
      pullURL,
      makePullResponseV1(clientID, undefined, [], null),
    );
  }
  if (pushURL) {
    fetchMocker.post(pushURL, 'ok');
  }
  await tickAFewTimes(vi);
  return rep;
}

export let fetchMocker: FetchMocker;

export function initReplicacheTesting(): void {
  beforeEach(() => {
    fetchMocker = new FetchMocker(vi);
    // Set up a default response that looks like a valid pull response
    fetchMocker.post(undefined, {
      cookie: null,
      lastMutationIDChanges: {},
      patch: [],
    });
    vi.useFakeTimers({now: 0});
    setupIDBDatabasesStoreForTest();
  });

  afterEach(async () => {
    restoreMakeImplForTest();
    vi.useRealTimers();
    vi.restoreAllMocks();
    await closeAllReps();
    await closeAllCloseables();
    await deleteAllDatabases();
    await teardownIDBDatabasesStoreForTest();
  });
}

export async function tickAFewTimes(vi: VitestUtils, n = 10, time = 10) {
  for (let i = 0; i < n; i++) {
    await vi.advanceTimersByTimeAsync(time);
  }
}

export async function tickUntil(
  vi: VitestUtils,
  f: () => boolean,
  msPerTest = 10,
) {
  while (!f()) {
    await vi.advanceTimersByTimeAsync(msPerTest);
  }
}

export class MemStoreWithCounters implements KVStore {
  readonly store: KVStore;
  readCount = 0;
  writeCount = 0;
  closeCount = 0;

  constructor(name: string) {
    this.store = new MemStore(name);
  }

  resetCounters() {
    this.readCount = 0;
    this.writeCount = 0;
    this.closeCount = 0;
  }

  read() {
    this.readCount++;
    return this.store.read();
  }

  write() {
    this.writeCount++;
    return this.store.write();
  }

  async close() {
    this.closeCount++;
    await this.store.close();
  }

  get closed(): boolean {
    return this.store.closed;
  }
}

export async function addData(
  tx: WriteTransaction,
  data: {[key: string]: JSONValue},
) {
  for (const [key, value] of Object.entries(data)) {
    await tx.set(key, value);
  }
}

export function expectLogContext(
  consoleLogStub: MockInstance,
  index: number,
  rep: Replicache,
  expectedContext: string,
) {
  expect(consoleLogStub.mock.calls.length).to.greaterThan(index);
  const args = consoleLogStub.mock.calls[index];
  expect(args).to.have.length(2);
  expect(args[0]).to.equal(`name=${rep.name}`);
  expect(args[1]).to.equal(expectedContext);
}

export async function expectPromiseToReject(p: unknown): Promise<Assertion> {
  let e;
  try {
    await p;
  } catch (ex) {
    e = ex;
  }
  return expect(e);
}

export async function expectAsyncFuncToThrow(f: () => unknown, c: unknown) {
  (await expectPromiseToReject(f())).to.be.instanceof(c);
}

/**
 * SubscriptionsManagerOptions that always generates DiffsMaps.
 */
export const testSubscriptionsManagerOptions: DiffComputationConfig = {
  shouldComputeDiffs: () => true,
  shouldComputeDiffsForIndex: () => true,
};

export function makePullResponseV1(
  clientID: ClientID,
  lastMutationID: number | undefined,
  patch: PatchOperation[] = [],
  cookie: Cookie = '',
): PullResponseV1 {
  return {
    cookie,
    lastMutationIDChanges:
      lastMutationID === undefined ? {} : {[clientID]: lastMutationID},
    patch,
  };
}

export function expectConsoleLogContextStub(
  name: string,
  args: unknown[],
  expectedMessage: string,
  additionalContexts: (string | RegExp)[] = [],
) {
  expect(args).to.have.length(2 + additionalContexts.length);
  expect(args[0]).to.equal(`name=${name}`);
  let i = 1;
  for (const context of additionalContexts) {
    if (typeof context === 'string') {
      expect(args[i++]).to.equal(context);
    } else {
      expect(args[i++]).to.match(context);
    }
  }
  expect(args[i]).to.equal(expectedMessage);
}

export const requestIDLogContextRegex = /^requestID=[a-z,0-9,-]*$/;

export function waitForSync(rep: {
  onSync?: ((syncing: boolean) => void) | null | undefined;
}) {
  const {promise, resolve} = resolver();
  rep.onSync = syncing => {
    if (!syncing) {
      resolve();
    }
  };
  return promise;
}

export const disableAllBackgroundProcesses = {
  enableMutationRecovery: false,
  enableScheduledRefresh: false,
  enableScheduledPersist: false,
};
