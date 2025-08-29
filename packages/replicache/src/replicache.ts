import type {LogContext} from '@rocicorp/logger';
import type {MaybePromise} from '../../shared/src/types.ts';
import * as FormatVersion from './format-version-enum.ts';
import {
  dropIDBStoreWithMemFallback,
  newIDBStoreWithMemFallback,
} from './kv/idb-store-with-mem-fallback.ts';
import {MemStore, dropMemStore} from './kv/mem-store.ts';
import type {StoreProvider} from './kv/store.ts';
import type {PendingMutation} from './pending-mutations.ts';
import type {Puller} from './puller.ts';
import type {Pusher} from './pusher.ts';
import {ReplicacheImpl} from './replicache-impl.ts';
import type {ReplicacheOptions} from './replicache-options.ts';
import type {
  SubscribeOptions,
  WatchCallbackForOptions,
  WatchNoIndexCallback,
  WatchOptions,
} from './subscriptions.ts';
import type {ReadTransaction} from './transactions.ts';
import type {
  MakeMutators,
  MutatorDefs,
  Poke,
  RequestOptions,
  UpdateNeededReason,
} from './types.ts';

type MakeImpl = <MD extends MutatorDefs>(
  options: ReplicacheOptions<MD>,
) => ReplicacheImpl<MD>;

const defaultMakeImpl: MakeImpl = <MD extends MutatorDefs>(
  options: ReplicacheOptions<MD>,
) => new ReplicacheImpl<MD>(options);
let makeImpl: MakeImpl = defaultMakeImpl;

export function setMakeImplForTest(testMakeImpl: MakeImpl) {
  makeImpl = testMakeImpl;
}

export function restoreMakeImplForTest() {
  makeImpl = defaultMakeImpl;
}

export const httpStatusUnauthorized = 401;

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

// eslint-disable-next-line @typescript-eslint/ban-types
export class Replicache<MD extends MutatorDefs = {}> {
  readonly #impl: ReplicacheImpl<MD>;

  constructor(options: ReplicacheOptions<MD>) {
    this.#impl = makeImpl(options);
  }

  /** The URL to use when doing a pull request. */
  get pullURL(): string {
    return this.#impl.pullURL;
  }
  set pullURL(value: string) {
    this.#impl.pullURL = value;
  }

  /** The URL to use when doing a push request. */
  get pushURL(): string {
    return this.#impl.pushURL;
  }
  set pushURL(value: string) {
    this.#impl.pushURL = value;
  }

  /** The authorization token used when doing a push request. */
  get auth(): string {
    return this.#impl.auth;
  }
  set auth(value: string) {
    this.#impl.auth = value;
  }

  /** The name of the Replicache database. Populated by {@link ReplicacheOptions#name}. */
  get name(): string {
    return this.#impl.name;
  }

  /**
   * This is the name Replicache uses for the IndexedDB database where data is
   * stored.
   */
  get idbName(): string {
    return makeIDBName(this.name, this.schemaVersion);
  }

  /** The schema version of the data understood by this application. */
  get schemaVersion(): string {
    return this.#impl.schemaVersion;
  }

  /**
   * The mutators that was registered in the constructor.
   */
  get mutate(): MakeMutators<MD> {
    return this.#impl.mutate;
  }

  /**
   * The duration between each periodic {@link pull}. Setting this to `null`
   * disables periodic pull completely. Pull will still happen if you call
   * {@link pull} manually.
   */
  get pullInterval(): number | null {
    return this.#impl.pullInterval;
  }
  set pullInterval(value: number | null) {
    this.#impl.pullInterval = value;
  }

  /**
   * The delay between when a change is made to Replicache and when Replicache
   * attempts to push that change.
   */
  get pushDelay(): number {
    return this.#impl.pushDelay;
  }
  set pushDelay(value: number) {
    this.#impl.pushDelay = value;
  }

  /**
   * The function to use to pull data from the server.
   */
  get puller(): Puller {
    return this.#impl.puller;
  }
  set puller(value: Puller) {
    this.#impl.puller = value;
  }

  /**
   * The function to use to push data to the server.
   */
  get pusher(): Pusher {
    return this.#impl.pusher;
  }
  set pusher(value: Pusher) {
    this.#impl.pusher = value;
  }

  /**
   * The options used to control the {@link pull} and push request behavior. This
   * object is live so changes to it will affect the next pull or push call.
   */
  get requestOptions(): Required<RequestOptions> {
    return this.#impl.requestOptions;
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
    return this.#impl.onSync;
  }
  set onSync(value: ((syncing: boolean) => void) | null) {
    this.#impl.onSync = value;
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
    return this.#impl.onClientStateNotFound;
  }
  set onClientStateNotFound(value: (() => void) | null) {
    this.#impl.onClientStateNotFound = value;
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
    return this.#impl.onUpdateNeeded;
  }
  set onUpdateNeeded(value: ((reason: UpdateNeededReason) => void) | null) {
    this.#impl.onUpdateNeeded = value;
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
    return this.#impl.getAuth;
  }
  set getAuth(
    value: (() => MaybePromise<string | null | undefined>) | null | undefined,
  ) {
    this.#impl.getAuth = value;
  }

  /**
   * The browser profile ID for this browser profile. Every instance of Replicache
   * browser-profile-wide shares the same profile ID.
   */
  get profileID(): Promise<string> {
    return this.#impl.profileID;
  }

  /**
   * The client ID for this instance of Replicache. Each instance of Replicache
   * gets a unique client ID.
   */
  get clientID(): string {
    return this.#impl.clientID;
  }

  /**
   * The client group ID for this instance of Replicache. Instances of
   * Replicache will have the same client group ID if and only if they have
   * the same name, mutators, indexes, schema version, format version, and
   * browser profile.
   */
  get clientGroupID(): Promise<string> {
    return this.#impl.clientGroupID;
  }

  /**
   * `onOnlineChange` is called when the {@link online} property changes. See
   * {@link online} for more details.
   */
  get onOnlineChange(): ((online: boolean) => void) | null {
    return this.#impl.onOnlineChange;
  }
  set onOnlineChange(value: ((online: boolean) => void) | null) {
    this.#impl.onOnlineChange = value;
  }

  /**
   * A rough heuristic for whether the client is currently online. Note that
   * there is no way to know for certain whether a client is online - the next
   * request can always fail. This property returns true if the last sync attempt succeeded,
   * and false otherwise.
   */
  get online(): boolean {
    return this.#impl.online;
  }

  /**
   * Whether the Replicache database has been closed. Once Replicache has been
   * closed it no longer syncs and you can no longer read or write data out of
   * it. After it has been closed it is pretty much useless and should not be
   * used any more.
   */
  get closed(): boolean {
    return this.#impl.closed;
  }

  /**
   * Closes this Replicache instance.
   *
   * When closed all subscriptions end and no more read or writes are allowed.
   */
  close(): Promise<void> {
    return this.#impl.close();
  }

  /**
   * Push pushes pending changes to the {@link pushURL}.
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
    return this.#impl.push({now});
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
    return this.#impl.pull({now});
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
    return this.#impl.poke(poke);
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
    return this.#impl.subscribe(body, options);
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
    return this.#impl.experimentalWatch(callback, options);
  }

  /**
   * Query is used for read transactions. It is recommended to use transactions
   * to ensure you get a consistent view across multiple calls to `get`, `has`
   * and `scan`.
   */
  query<R>(body: (tx: ReadTransaction) => Promise<R> | R): Promise<R> {
    return this.#impl.query(body);
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
    return this.#impl.experimentalPendingMutations();
  }
}

/**
 * Wrapper error class that should be reported as error (logger.error)
 */
export class ReportError extends Error {}

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
