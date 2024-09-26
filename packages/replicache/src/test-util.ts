import {TEST_LICENSE_KEY} from '@rocicorp/licensing/out/client';
import {resolver} from '@rocicorp/resolver';
import {expect} from 'chai';
import type {JSONValue} from 'shared/src/json.js';
import {must} from 'shared/src/must.js';
import {randomUint64} from 'shared/src/random-uint64.js';
import * as sinon from 'sinon';
import {type SinonFakeTimers, useFakeTimers} from 'sinon';
import type {Cookie} from './cookies.js';
import type {Store} from './dag/store.js';
import type {Hash} from './hash.js';
import {dropIDBStoreWithMemFallback} from './kv/idb-store-with-mem-fallback.js';
import {MemStore} from './kv/mem-store.js';
import type {Store as KVStore} from './kv/store.js';
import type {PatchOperation} from './patch-operation.js';
import {
  setupForTest as setupIDBDatabasesStoreForTest,
  teardownForTest as teardownIDBDatabasesStoreForTest,
} from './persist/idb-databases-store-db-name.js';
import type {PullResponseV1} from './puller.js';
import {ReplicacheImpl, type ReplicacheImplOptions} from './replicache-impl.js';
import type {ReplicacheOptions} from './replicache-options.js';
import {
  Replicache,
  restoreMakeImplForTest,
  setMakeImplForTest,
} from './replicache.js';
import type {DiffComputationConfig} from './sync/diff.js';
import type {ClientID} from './sync/ids.js';
import type {WriteTransaction} from './transactions.js';
import type {BeginPullResult, MutatorDefs} from './types.js';

// fetch-mock has invalid d.ts file so we removed that on npm install.
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-expect-error
import fetchMock from 'fetch-mock/esm/client';

export class ReplicacheTest<
  // eslint-disable-next-line @typescript-eslint/ban-types
  MD extends MutatorDefs = {},
> extends Replicache<MD> {
  readonly #impl: ReplicacheImpl<MD>;
  recoverMutationsFake: sinon.SinonSpy<[r: Promise<boolean>], Promise<boolean>>;

  constructor(
    options: ReplicacheOptions<MD>,
    implOptions?: ReplicacheImplOptions | undefined,
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
    this.recoverMutationsFake = this.onRecoverMutations = sinon.fake(r => r);
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
    return this.#impl.recoverMutations();
  }

  licenseActive(): Promise<boolean> {
    return this.#impl.licenseActivePromise;
  }

  licenseValid(): Promise<boolean> {
    return this.#impl.licenseCheckPromise;
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

export async function replicacheForTesting<
  // eslint-disable-next-line @typescript-eslint/ban-types
  MD extends MutatorDefs = {},
>(
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
    licenseKey,
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
      licenseKey: licenseKey ?? TEST_LICENSE_KEY,
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
  fetchMock.post(pullURL, makePullResponseV1(clientID, undefined, [], null));
  fetchMock.post(pushURL, 'ok');
  await tickAFewTimes();
  return rep;
}

export let clock: SinonFakeTimers;

export function initReplicacheTesting(): void {
  fetchMock.config.overwriteRoutes = true;

  setup(() => {
    clock = useFakeTimers(0);
    setupIDBDatabasesStoreForTest();
  });

  teardown(async () => {
    restoreMakeImplForTest();
    clock.restore();
    fetchMock.restore();
    sinon.restore();
    await closeAllReps();
    await closeAllCloseables();
    await deleteAllDatabases();
    await teardownIDBDatabasesStoreForTest();
  });
}

export async function tickAFewTimes(n = 10, time = 10) {
  for (let i = 0; i < n; i++) {
    await clock.tickAsync(time);
  }
}

export async function tickUntil(f: () => boolean, msPerTest = 10) {
  while (!f()) {
    await clock.tickAsync(msPerTest);
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
  consoleLogStub: sinon.SinonStub,
  index: number,
  rep: Replicache,
  expectedContext: string,
) {
  expect(consoleLogStub.callCount).to.greaterThan(index);
  const {args} = consoleLogStub.getCall(index);
  expect(args).to.have.length(2);
  expect(args[0]).to.equal(`name=${rep.name}`);
  expect(args[1]).to.equal(expectedContext);
}

export async function expectPromiseToReject(
  p: unknown,
): Promise<Chai.Assertion> {
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
  call: sinon.SinonSpyCall,
  expectedMessage: string,
  additionalContexts: (string | RegExp)[] = [],
) {
  const {args} = call;
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
  enableLicensing: false,
  enableMutationRecovery: false,
  enableScheduledRefresh: false,
  enableScheduledPersist: false,
};
