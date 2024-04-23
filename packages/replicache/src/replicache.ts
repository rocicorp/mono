import type {LogContext} from '@rocicorp/logger';
import {must} from 'shared/src/must.js';
import type {Store} from './dag/store.js';
import {FormatVersion} from './format-version.js';
import type {Hash} from './hash.js';
import {
  dropIDBStoreWithMemFallback,
  newIDBStoreWithMemFallback,
} from './kv/idb-store-with-mem-fallback.js';
import {MemStore, dropMemStore} from './kv/mem-store.js';
import type {StoreProvider} from './kv/store.js';
import type {PendingMutation} from './pending-mutations.js';
import type {Puller} from './puller.js';
import type {Pusher} from './pusher.js';
import {ReplicacheImpl} from './replicache-impl.js';
import type {ReplicacheOptions} from './replicache-options.js';
import type {
  SubscribeOptions,
  WatchCallbackForOptions,
  WatchNoIndexCallback,
  WatchOptions,
} from './subscriptions.js';
import type {ReadTransaction} from './transactions.js';
import type {
  BeginPullResult,
  MakeMutators,
  MaybePromise,
  MutatorDefs,
  Poke,
  RequestOptions,
  UpdateNeededReason,
} from './types.js';

export interface TestingReplicacheWithTesting extends Replicache {
  memdag: Store;
}

type TestingInstance = {
  beginPull: () => Promise<BeginPullResult>;
  isClientGroupDisabled: () => boolean;
  licenseActivePromise: Promise<boolean>;
  licenseCheckPromise: Promise<boolean>;
  maybeEndPull: (syncHead: Hash, requestID: string) => Promise<void>;
  memdag: Store;
  onBeginPull: () => void;
  onPushInvoked: () => void;
  onRecoverMutations: <T>(r: T) => T;
  perdag: Store;
  recoverMutations: () => Promise<boolean>;
  lastMutationID: () => number;
};

const exposedToTestingMap = new WeakMap<ReplicacheImpl, TestingInstance>();

export function getTestInstance(
  rep: Replicache | ReplicacheImpl,
): TestingInstance {
  if (rep instanceof Replicache) {
    rep = getImpl(rep);
  }
  return must(exposedToTestingMap.get(rep));
}

export function exposeToTesting(
  rep: ReplicacheImpl,
  testingInstance: TestingInstance,
): void {
  exposedToTestingMap.set(rep, testingInstance);
}

const repToImpl = new WeakMap<Replicache, ReplicacheImpl>();

export function getImpl(rep: Replicache): ReplicacheImpl {
  return must(repToImpl.get(rep));
}

export const httpStatusUnauthorized = 401;

export const LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT = 100 * 2 ** 20; // 100 MB

export const RECOVER_MUTATIONS_INTERVAL_MS = 5 * 60 * 1000; // 5 mins
export const LICENSE_ACTIVE_INTERVAL_MS = 24 * 60 * 60 * 1000; // 24 hours
export const TEST_LICENSE_KEY_TTL_MS = 5 * 60 * 1000;

/**
 * Returns the name of the IDB database that will be used for a particular Replicache instance.
 * @param name The name of the Replicache instance (i.e., the `name` field of `ReplicacheOptions`).
 * @param schemaVersion The schema version of the database (i.e., the `schemaVersion` field of `ReplicacheOptions`).
 * @returns
 */
export function makeIDBName(name: string, schemaVersion?: string): string {
  return makeIDBNameInternal(name, schemaVersion, FormatVersion.Latest);
}

function makeIDBNameInternal(
  name: string,
  schemaVersion: string | undefined,
  formatVersion: number,
): string {
  const n = `rep:${name}:${formatVersion}`;
  return schemaVersion ? `${n}:${schemaVersion}` : n;
}

export {makeIDBNameInternal as makeIDBNameForTesting};

/**
 * The maximum number of time to call out to getAuth before giving up
 * and throwing an error.
 */
export const MAX_REAUTH_TRIES = 8;

export const PERSIST_IDLE_TIMEOUT_MS = 1000;
export const REFRESH_IDLE_TIMEOUT_MS = 1000;

export const PERSIST_THROTTLE_MS = 500;
export const REFRESH_THROTTLE_MS = 500;

export const noop = () => {
  // noop
};

export const updateNeededReasonNewClientGroup: UpdateNeededReason = {
  type: 'NewClientGroup',
} as const;

// eslint-disable-next-line @typescript-eslint/ban-types
export class Replicache<MD extends MutatorDefs = {}> {
  readonly #rep: ReplicacheImpl<MD>;

  /** The URL to use when doing a pull request. */
  get pullURL(): string {
    return this.#rep.pullURL;
  }
  set pullURL(value: string) {
    this.#rep.pullURL = value;
  }

  /** The URL to use when doing a push request. */
  get pushURL(): string {
    return this.#rep.pushURL;
  }
  set pushURL(value: string) {
    this.#rep.pushURL = value;
  }

  /** The authorization token used when doing a push request. */
  get auth(): string {
    return this.#rep.auth;
  }
  set auth(value: string) {
    this.#rep.auth = value;
  }

  /** The name of the Replicache database. Populated by {@link ReplicacheOptions#name}. */
  readonly name: string;

  /**
   * This is the name Replicache uses for the IndexedDB database where data is
   * stored.
   */
  get idbName(): string {
    return makeIDBName(this.name, this.schemaVersion);
  }

  /** The schema version of the data understood by this application. */
  readonly schemaVersion: string;

  /**
   * The mutators that was registered in the constructor.
   */
  readonly mutate: MakeMutators<MD>;

  /**
   * The duration between each periodic {@link pull}. Setting this to `null`
   * disables periodic pull completely. Pull will still happen if you call
   * {@link pull} manually.
   */
  get pullInterval(): number | null {
    return this.#rep.pullInterval;
  }
  set pullInterval(value: number | null) {
    this.#rep.pullInterval = value;
  }

  /**
   * The delay between when a change is made to Replicache and when Replicache
   * attempts to push that change.
   */
  get pushDelay(): number {
    return this.#rep.pushDelay;
  }
  set pushDelay(value: number) {
    this.#rep.pushDelay = value;
  }

  /**
   * The function to use to pull data from the server.
   */
  get puller(): Puller {
    return this.#rep.puller;
  }
  set puller(value: Puller) {
    this.#rep.puller = value;
  }

  /**
   * The function to use to push data to the server.
   */
  get pusher(): Pusher {
    return this.#rep.pusher;
  }
  set pusher(value: Pusher) {
    this.#rep.pusher = value;
  }

  /**
   * The options used to control the {@link pull} and push request behavior. This
   * object is live so changes to it will affect the next pull or push call.
   */
  get requestOptions(): Required<RequestOptions> {
    return this.#rep.requestOptions;
  }

  /**
   * `onSync(true)` is called when Replicache transitions from no push or pull
   * happening to at least one happening. `onSync(false)` is called in the
   * opposite case: when Replicache transitions from at least one push or pull
   * happening to none happening.
   *
   * This can be used in a React like app by doing something like the following:
   *
   * ```js
   * const [syncing, setSyncing] = useState(false);
   * useEffect(() => {
   *   rep.onSync = setSyncing;
   * }, [rep]);
   * ```
   */
  get onSync(): ((syncing: boolean) => void) | null {
    return this.#rep.onSync;
  }
  set onSync(value: ((syncing: boolean) => void) | null) {
    this.#rep.onSync = value;
  }

  /**
   * `onClientStateNotFound` is called when the persistent client has been
   * garbage collected. This can happen if the client has no pending mutations
   * and has not been used for a while.
   *
   * The default behavior is to reload the page (using `location.reload()`). Set
   * this to `null` or provide your own function to prevent the page from
   * reloading automatically.
   */
  get onClientStateNotFound(): (() => void) | null {
    return this.#rep.onClientStateNotFound;
  }
  set onClientStateNotFound(value: (() => void) | null) {
    this.#rep.onClientStateNotFound = value;
  }

  /**
   * `onUpdateNeeded` is called when a code update is needed.
   *
   * A code update can be needed because:
   * - the server no longer supports the {@link pushVersion},
   *   {@link pullVersion} or {@link schemaVersion} of the current code.
   * - a new Replicache client has created a new client group, because its code
   *   has different mutators, indexes, schema version and/or format version
   *   from this Replicache client. This is likely due to the new client having
   *   newer code. A code update is needed to be able to locally sync with this
   *   new Replicache client (i.e. to sync while offline, the clients can can
   *   still sync with each other via the server).
   *
   * The default behavior is to reload the page (using `location.reload()`). Set
   * this to `null` or provide your own function to prevent the page from
   * reloading automatically. You may want to provide your own function to
   * display a toast to inform the end user there is a new version of your app
   * available and prompting them to refresh.
   */
  get onUpdateNeeded(): ((reason: UpdateNeededReason) => void) | null {
    return this.#rep.onUpdateNeeded;
  }
  set onUpdateNeeded(value: ((reason: UpdateNeededReason) => void) | null) {
    this.#rep.onUpdateNeeded = value;
  }

  /**
   * This gets called when we get an HTTP unauthorized (401) response from the
   * push or pull endpoint. Set this to a function that will ask your user to
   * reauthenticate.
   */
  get getAuth():
    | (() => MaybePromise<string | null | undefined>)
    | null
    | undefined {
    return this.#rep.getAuth;
  }
  set getAuth(
    value: (() => MaybePromise<string | null | undefined>) | null | undefined,
  ) {
    this.#rep.getAuth = value;
  }

  constructor(options: ReplicacheOptions<MD>) {
    this.#rep = new ReplicacheImpl<MD>(options);
    this.name = this.#rep.name;
    this.schemaVersion = this.#rep.schemaVersion;
    this.mutate = this.#rep.mutate;
    repToImpl.set(this, this.#rep);
  }

  /**
   * The browser profile ID for this browser profile. Every instance of Replicache
   * browser-profile-wide shares the same profile ID.
   */
  get profileID(): Promise<string> {
    return this.#rep.profileID;
  }

  /**
   * The client ID for this instance of Replicache. Each instance of Replicache
   * gets a unique client ID.
   */
  get clientID(): string {
    return this.#rep.clientID;
  }

  /**
   * The client group ID for this instance of Replicache. Instances of
   * Replicache will have the same client group ID if and only if they have
   * the same name, mutators, indexes, schema version, format version, and
   * browser profile.
   */
  get clientGroupID(): Promise<string> {
    return this.#rep.clientGroupID;
  }

  /**
   * `onOnlineChange` is called when the {@link online} property changes. See
   * {@link online} for more details.
   */
  get onOnlineChange(): ((online: boolean) => void) | null {
    return this.#rep.onOnlineChange;
  }
  set onOnlineChange(value: ((online: boolean) => void) | null) {
    this.#rep.onOnlineChange = value;
  }

  /**
   * A rough heuristic for whether the client is currently online. Note that
   * there is no way to know for certain whether a client is online - the next
   * request can always fail. This property returns true if the last sync attempt succeeded,
   * and false otherwise.
   */
  get online(): boolean {
    return this.#rep.online;
  }

  /**
   * Whether the Replicache database has been closed. Once Replicache has been
   * closed it no longer syncs and you can no longer read or write data out of
   * it. After it has been closed it is pretty much useless and should not be
   * used any more.
   */
  get closed(): boolean {
    return this.#rep.closed;
  }

  /**
   * Closes this Replicache instance.
   *
   * When closed all subscriptions end and no more read or writes are allowed.
   */
  close(): Promise<void> {
    return this.#rep.close();
  }

  /**
   * Push pushes pending changes to the {@link pushURLXXX}.
   *
   * You do not usually need to manually call push. If {@link pushDelay} is
   * non-zero (which it is by default) pushes happen automatically shortly after
   * mutations.
   *
   * If the server endpoint fails push will be continuously retried with an
   * exponential backoff.
   *
   * @param [now=false] If true, push will happen immediately and ignore
   *   {@link pushDelay}, {@link RequestOptions.minDelayMs} as well as the
   *   exponential backoff in case of errors.
   * @returns A promise that resolves when the next push completes. In case of
   * errors the first error will reject the returned promise. Subsequent errors
   * will not be reflected in the promise.
   */
  push({now = false} = {}): Promise<void> {
    return this.#rep.push({now});
  }

  /**
   * Pull pulls changes from the {@link pullURL}. If there are any changes local
   * changes will get replayed on top of the new server state.
   *
   * If the server endpoint fails pull will be continuously retried with an
   * exponential backoff.
   *
   * @param [now=false] If true, pull will happen immediately and ignore
   *   {@link RequestOptions.minDelayMs} as well as the exponential backoff in
   *   case of errors.
   * @returns A promise that resolves when the next pull completes. In case of
   * errors the first error will reject the returned promise. Subsequent errors
   * will not be reflected in the promise.
   */
  pull({now = false} = {}): Promise<void> {
    return this.#rep.pull({now});
  }

  /**
   * Applies an update from the server to Replicache.
   * Throws an error if cookie does not match. In that case the server thinks
   * this client has a different cookie than it does; the caller should disconnect
   * from the server and re-register, which transmits the cookie the client actually
   * has.
   *
   * @experimental This method is under development and its semantics will change.
   */
  poke(poke: Poke): Promise<void> {
    return this.#rep.poke(poke);
  }

  /**
   * Subscribe to the result of a {@link query}. The `body` function is
   * evaluated once and its results are returned via `onData`.
   *
   * Thereafter, each time the the result of `body` changes, `onData` is fired
   * again with the new result.
   *
   * `subscribe()` goes to significant effort to avoid extraneous work
   * re-evaluating subscriptions:
   *
   * 1. subscribe tracks the keys that `body` accesses each time it runs. `body`
   *    is only re-evaluated when those keys change.
   * 2. subscribe only re-fires `onData` in the case that a result changes by
   *    way of the `isEqual` option which defaults to doing a deep JSON value
   *    equality check.
   *
   * Because of (1), `body` must be a pure function of the data in Replicache.
   * `body` must not access anything other than the `tx` parameter passed to it.
   *
   * Although subscribe is as efficient as it can be, it is somewhat constrained
   * by the goal of returning an arbitrary computation of the cache. For even
   * better performance (but worse dx), see {@link experimentalWatch}.
   *
   * If an error occurs in the `body` the `onError` function is called if
   * present. Otherwise, the error is logged at log level 'error'.
   *
   * To cancel the subscription, call the returned function.
   *
   * @param body The function to evaluate to get the value to pass into
   *    `onData`.
   * @param options Options is either a function or an object. If it is a
   *    function it is equivalent to passing it as the `onData` property of an
   *    object.
   */
  subscribe<R>(
    body: (tx: ReadTransaction) => Promise<R>,
    options: SubscribeOptions<R> | ((result: R) => void),
  ): () => void {
    return this.#rep.subscribe(body, options);
  }

  /**
   * Watches Replicache for changes.
   *
   * The `callback` gets called whenever the underlying data changes and the
   * `key` changes matches the `prefix` of {@link ExperimentalWatchIndexOptions} or
   * {@link ExperimentalWatchNoIndexOptions} if present. If a change
   * occurs to the data but the change does not impact the key space the
   * callback is not called. In other words, the callback is never called with
   * an empty diff.
   *
   * This gets called after commit (a mutation or a rebase).
   *
   * @experimental This method is under development and its semantics will
   * change.
   */
  experimentalWatch(callback: WatchNoIndexCallback): () => void;
  experimentalWatch<Options extends WatchOptions>(
    callback: WatchCallbackForOptions<Options>,
    options?: Options,
  ): () => void;
  experimentalWatch<Options extends WatchOptions>(
    callback: WatchCallbackForOptions<Options>,
    options?: Options,
  ): () => void {
    return this.#rep.experimentalWatch(callback, options);
  }

  /**
   * Query is used for read transactions. It is recommended to use transactions
   * to ensure you get a consistent view across multiple calls to `get`, `has`
   * and `scan`.
   */
  query<R>(body: (tx: ReadTransaction) => Promise<R> | R): Promise<R> {
    return this.#rep.query(body);
  }

  /**
   * List of pending mutations. The order of this is from oldest to newest.
   *
   * Gives a list of local mutations that have `mutationID` >
   * `syncHead.mutationID` that exists on the main client group.
   *
   * @experimental This method is experimental and may change in the future.
   */
  experimentalPendingMutations(): Promise<readonly PendingMutation[]> {
    return this.#rep.experimentalPendingMutations();
  }
}

// This map is used to keep track of closing instances of Replicache. When an
// instance is opening we wait for any currently closing instances.
export const closingInstances: Map<string, Promise<unknown>> = new Map();

export function reload(): void {
  if (typeof location !== 'undefined') {
    location.reload();
  }
}

/**
 * Wrapper error class that should be reported as error (logger.error)
 */
export class ReportError extends Error {}

export async function throwIfError(p: Promise<undefined | {error: unknown}>) {
  const res = await p;
  if (res) {
    throw res.error;
  }
}

function createMemStore(name: string): MemStore {
  return new MemStore(name);
}

export function getKVStoreProvider(
  lc: LogContext,
  kvStore: 'mem' | 'idb' | StoreProvider | undefined,
): StoreProvider {
  switch (kvStore) {
    case 'idb':
    case undefined:
      return {
        create: (name: string) => newIDBStoreWithMemFallback(lc, name),
        drop: dropIDBStoreWithMemFallback,
      };
    case 'mem':
      return {
        create: createMemStore,
        drop: (name: string) => dropMemStore(name),
      };
    default:
      return kvStore;
  }
}
