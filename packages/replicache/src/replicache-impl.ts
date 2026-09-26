import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import {consoleLogSink} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {AbortError} from '../../shared/src/abort-error.ts';
import {assert} from '../../shared/src/asserts.ts';
import {getBrowserGlobal} from '../../shared/src/browser-env.ts';
import {getDocumentVisibilityWatcher} from '../../shared/src/document-visible.ts';
import type {JSONValue, ReadonlyJSONValue} from '../../shared/src/json.ts';
import {promiseFalse, promiseVoid} from '../../shared/src/resolved-promises.ts';
import {TESTING} from '../../shared/src/testing.ts';
import type {MaybePromise} from '../../shared/src/types.ts';
import {PullDelegate, PushDelegate} from './connection-loop-delegates.ts';
import {ConnectionLoop, MAX_DELAY_MS, MIN_DELAY_MS} from './connection-loop.ts';
import {assertCookie, type Cookie} from './cookies.ts';
import {InvalidRefCountError} from './dag/invalid-ref-count-error.ts';
import {LazyStore} from './dag/lazy-store.ts';
import {StoreImpl} from './dag/store-impl.ts';
import {ChunkNotFoundError, mustGetHeadHash, type Store} from './dag/store.ts';
import {
  baseSnapshotFromHash,
  DEFAULT_HEAD_NAME,
  isLocalMetaDD31,
  type LocalMeta,
} from './db/commit.ts';
import {readFromDefaultHead} from './db/read.ts';
import {rebaseMutationAndCommit} from './db/rebase.ts';
import {newWriteLocal} from './db/write.ts';
import {
  isClientStateNotFoundResponse,
  isVersionNotSupportedResponse,
  type VersionNotSupportedResponse,
} from './error-responses.ts';
import * as FormatVersion from './format-version-enum.ts';
import {deepFreeze} from './frozen-json.ts';
import {getDefaultPuller, isDefaultPuller} from './get-default-puller.ts';
import {getDefaultPusher, isDefaultPusher} from './get-default-pusher.ts';
import {getKVStoreProvider} from './get-kv-store-provider.ts';
import {assertHash, emptyHash, type Hash, newRandomHash} from './hash.ts';
import type {HTTPRequestInfo} from './http-request-info.ts';
import {httpStatusUnauthorized} from './http-status-unauthorized.ts';
import type {IndexDefinitions} from './index-defs.ts';
import {
  type MemFallbackStore,
  MemFallbackStoreProvider,
} from './kv/mem-fallback-store.ts';
import type {Store as KVStore} from './kv/store.ts';
import {createLogContext} from './log-options.ts';
import {makeIDBName} from './make-idb-name.ts';
import {MutationRecovery} from './mutation-recovery.ts';
import {initNewClientChannel} from './new-client-channel.ts';
import {
  initOnPersistChannel,
  type OnPersist,
  type PersistInfo,
} from './on-persist-channel.ts';
import {
  type PendingMutation,
  pendingMutationsForAPI,
} from './pending-mutations.ts';
import {
  CLIENT_MAX_INACTIVE_TIME,
  GC_INTERVAL,
  initClientGC,
} from './persist/client-gc.ts';
import {initClientGroupGC} from './persist/client-group-gc.ts';
import {disableClientGroup} from './persist/client-groups.ts';
import {
  ClientStateNotFoundError,
  initClientV6,
  type OnClientsDeleted,
  hasClientState as persistHasClientState,
} from './persist/clients.ts';
import {
  COLLECT_IDB_INTERVAL,
  dropDatabaseInternal,
  initCollectIDBDatabases,
  INITIAL_COLLECT_IDB_DELAY,
} from './persist/collect-idb-databases.ts';
import {HEARTBEAT_INTERVAL, startHeartbeats} from './persist/heartbeat.ts';
import {
  IDBDatabasesStore,
  type IndexedDBDatabase,
} from './persist/idb-databases-store.ts';
import {makeClientID} from './persist/make-client-id.ts';
import {persistDD31} from './persist/persist.ts';
import {refresh} from './persist/refresh.ts';
import {ProcessScheduler} from './process-scheduler.ts';
import type {Puller} from './puller.ts';
import {type Pusher, PushError} from './pusher.ts';
import type {
  MutationTrackingData,
  ReplicacheOptions,
  ZeroOption,
} from './replicache-options.ts';
import {ReportError} from './report-error.ts';
import {setIntervalWithSignal} from './set-interval-with-signal.ts';
import {
  getStorageFailure,
  type StorageFailureError,
} from './storage-failure.ts';
import {
  type SubscribeOptions,
  SubscriptionImpl,
  type SubscriptionsManager,
  SubscriptionsManagerImpl,
  type WatchCallback,
  type WatchCallbackForOptions,
  type WatchNoIndexCallback,
  type WatchOptions,
  WatchSubscription,
} from './subscriptions.ts';
import type {DiffsMap} from './sync/diff.ts';
import * as HandlePullResponseResultEnum from './sync/handle-pull-response-result-type-enum.ts';
import type {ClientGroupID, ClientID} from './sync/ids.ts';
import {PullError} from './sync/pull-error.ts';
import {beginPullV1, handlePullResponseV1, maybeEndPull} from './sync/pull.ts';
import {push, PUSH_VERSION_DD31} from './sync/push.ts';
import {newRequestID} from './sync/request-id.ts';
import {SYNC_HEAD_NAME} from './sync/sync-head-name.ts';
import {throwIfClosed} from './transaction-closed-error.ts';
import type {ReadTransaction, WriteTransaction} from './transactions.ts';
import {ReadTransactionImpl, WriteTransactionImpl} from './transactions.ts';
import type {
  BeginPullResult,
  MakeMutator,
  MakeMutators,
  MutatorDefs,
  MutatorReturn,
  PokeInternal,
  QueryInternal,
  RequestOptions,
  UpdateNeededReason,
} from './types.ts';
import {version} from './version.ts';
import {
  withRead,
  withWrite,
  withWriteNoImplicitCommit,
} from './with-transactions.ts';

/**
 * The maximum number of time to call out to getAuth before giving up
 * and throwing an error.
 */
const MAX_REAUTH_TRIES = 8;

const PERSIST_IDLE_TIMEOUT_MS = 1000;
const REFRESH_IDLE_TIMEOUT_MS = 1000;

const PERSIST_THROTTLE_MS = 500;
const REFRESH_THROTTLE_MS = 500;

/**
 * Opening the database blocks everything Replicache does, including Zero's
 * first connect attempt, so a slow open is worth a warning rather than a
 * debug line.
 */
const SLOW_DB_OPEN_THRESHOLD_MS = 50;

const LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT = 100 * 2 ** 20; // 100 MB

const RECOVER_MUTATIONS_INTERVAL_MS = 5 * 60 * 1000; // 5 mins

const noop = () => {
  // noop
};

const updateNeededReasonNewClientGroup: UpdateNeededReason = {
  type: 'NewClientGroup',
} as const;

/** @deprecated Not used any more */
export interface MakeSubscriptionsManager {
  (queryInternal: QueryInternal, lc: LogContext): SubscriptionsManager;
}

export interface ReplicacheImplOptions {
  /**
   * Defaults to true.
   */
  enableMutationRecovery?: boolean | undefined;

  /**
   * Defaults to true.
   */
  enableScheduledPersist?: boolean | undefined;

  /**
   * Defaults to true.
   */
  enableScheduledRefresh?: boolean | undefined;

  /**
   * Defaults to `() => true`.
   */
  enableRefresh?: (() => boolean) | undefined;

  /**
   * Defaults to true.
   */
  enablePullAndPushInOpen?: boolean | undefined;

  /**
   * @deprecated Not used anymore.
   */
  makeSubscriptionsManager?: unknown;

  /**
   * Default is `true`.  If `false` if an exact match client group
   * is not found, a new client group is always made instead of forking
   * from an existing client group.
   */
  enableClientGroupForking?: boolean | undefined;

  /**
   * Callback for when Replicache has deleted clients.
   */
  onClientsDeleted?: OnClientsDeleted | undefined;

  /**
   * Internal option used by Zero.
   * Replicache will call this to and, if zero is enabled, will
   * invoke various hooks to allow Zero the keep IVM in sync with Replicache's b-trees.
   */
  zero?: ZeroOption | undefined;
}

export class ReplicacheImpl<MD extends MutatorDefs = {}> {
  /** The URL to use when doing a pull request. */
  pullURL: string;

  /** The URL to use when doing a push request. */
  pushURL: string;

  /** The authorization token used when doing a push request. */
  #auth: string;

  /** The name of the Replicache database. Populated by {@link ReplicacheOptions#name}. */
  readonly name: string;

  readonly #subscriptions: SubscriptionsManager;
  readonly #mutationRecovery: MutationRecovery | undefined;

  /**
   * Client groups gets disabled when the server does not know about it.
   * A disabled client group prevents the client from pushing and pulling.
   */
  isClientGroupDisabled = false;

  readonly #kvStoreProvider: MemFallbackStoreProvider;
  readonly #perKVStore: MemFallbackStore;

  lastMutationID: number = 0;

  /**
   * This is the name Replicache uses for the IndexedDB database where data is
   * stored.
   */
  get idbName(): string {
    return makeIDBName(this.name, this.schemaVersion);
  }

  /**
   * The KV store backing this instance. It is a memory store once a storage
   * failure while opening moved the instance onto memory (see
   * `onStorageFailure`).
   */
  get kvStore(): KVStore {
    return this.#perKVStore.backing;
  }

  set auth(auth: string) {
    if (this.#zero) {
      this.#zero.auth = auth;
    }

    this.#auth = auth;
  }

  get auth() {
    return this.#auth;
  }

  /** The schema version of the data understood by this application. */
  readonly schemaVersion: string;

  get #idbDatabase(): IndexedDBDatabase {
    return {
      name: this.idbName,
      replicacheName: this.name,
      replicacheFormatVersion: FormatVersion.Latest,
      schemaVersion: this.schemaVersion,
    };
  }
  #closed = false;
  #online = true;
  readonly #clientID = makeClientID();
  readonly #ready: Promise<void>;
  /**
   * Resolves if open fails, for whatever reason. `#ready` never resolves in
   * that case, so `close()` waits on whichever of the two settles first
   * instead of hanging forever.
   */
  readonly #openFailed = resolver<void>();
  readonly #profileIDPromise: Promise<string>;
  readonly #clientGroupIDPromise: Promise<string>;
  readonly #mutatorRegistry: MutatorDefs = {};

  /**
   * The mutators that was registered in the constructor.
   */
  readonly mutate: MakeMutators<MD>;

  // Number of pushes/pulls at the moment.
  #pushCounter = 0;
  #pullCounter = 0;

  #pullConnectionLoop: ConnectionLoop;
  #pushConnectionLoop: ConnectionLoop;

  /**
   * The duration between each periodic {@link pull}. Setting this to `null`
   * disables periodic pull completely. Pull will still happen if you call
   * {@link pull} manually.
   */
  pullInterval: number | null;

  /**
   * The delay between when a change is made to Replicache and when Replicache
   * attempts to push that change.
   */
  pushDelay: number;

  readonly #requestOptions: Required<RequestOptions>;

  /**
   * The function to use to pull data from the server.
   */
  puller: Puller;

  /**
   * The function to use to push data to the server.
   */
  pusher: Pusher;

  readonly memdag: LazyStore;
  readonly perdag: Store;
  readonly #idbDatabases: IDBDatabasesStore;
  readonly #lc: LogContext;
  readonly #zero: ZeroOption | undefined;

  readonly #closeAbortController = new AbortController();
  /**
   * Aborted on close AND on a storage failure after the open. The store-using
   * background processes (heartbeat, client and client-group GC, database
   * collection, mutation recovery, the new-client channel's store read) run
   * on this signal, so the first detected failure stops them instead of
   * letting each retry the fault at its interval and log `Error running.`
   * every time. A failure during the open moves the stores onto memory
   * instead, where they keep running.
   */
  readonly #storeProcessesAbortController = new AbortController();

  readonly #persistLock = new Lock();
  readonly #enableScheduledPersist: boolean;
  readonly #enableScheduledRefresh: boolean;
  readonly #enableRefresh: () => boolean;
  readonly #enablePullAndPushInOpen: boolean;
  #persistScheduler = new ProcessScheduler(
    () => this.persist(),
    PERSIST_IDLE_TIMEOUT_MS,
    PERSIST_THROTTLE_MS,
    this.#closeAbortController.signal,
  );
  readonly #onPersist: OnPersist;
  #refreshScheduler = new ProcessScheduler(
    () => this.refresh(),
    REFRESH_IDLE_TIMEOUT_MS,
    REFRESH_THROTTLE_MS,
    this.#closeAbortController.signal,
  );

  /**
   * The options used to control the {@link pull} and push request behavior. This
   * object is live so changes to it will affect the next pull or push call.
   */
  get requestOptions(): Required<RequestOptions> {
    return this.#requestOptions;
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
  onSync: ((syncing: boolean) => void) | null = null;

  /**
   * `onClientStateNotFound` is called when the persistent client state can no
   * longer be used. This happens when:
   * - the persistent client has been garbage collected. This can happen if the
   *   client has no pending mutations and has not been used for a while.
   * - the persistent store was found to be corrupt. Replicache then tries to
   *   drop the database, including any pending mutations, so that a fresh one
   *   is created on reload.
   *
   * The default behavior is to reload the page (using `location.reload()`). Set
   * this to `null` or provide your own function to prevent the page from
   * reloading automatically.
   */
  onClientStateNotFound: (() => void) | null = reload;

  /**
   * `onStorageFailure` is called once, the first time the local kv store
   * reports that its storage has failed rather than its data, by throwing a
   * {@link StorageFailureError}: the device is out of space (`full`), the
   * database cannot be opened (`cannot-open`), or a read or write to it failed
   * (`io-error`). The SQLite stores and the IndexedDB store report these; a
   * custom `StoreProvider` can throw a `StorageFailureError` itself.
   *
   * A failure while the instance is opening moves it onto memory for the
   * session, where it runs as with `kvStore: 'mem'`: it persists and
   * refreshes against the memory stores, and nothing reaches the disk.
   *
   * A failure after that stops persisting and refreshing: a rebuild would
   * open the same failing storage, and a wipe would destroy an intact replica
   * for a fault that is not in the data. Queries and mutations keep running
   * against what the in-memory dag holds, and mutations keep pushing to the
   * server, but nothing is persisted locally until the app creates a new
   * instance. A read that needs a chunk the in-memory dag has not loaded from
   * the store yet, or has since evicted from its cache, still goes to the
   * store and fails the way any store read does; that failure reaches its
   * caller, and is reported here as well, whichever caller it was (a query,
   * a mutation, a poke, a background process). An invalid ref count reported while
   * the store is failing with `cannot-open` or `io-error` is NOT treated as
   * corruption (the database is not dropped and {@link onClientStateNotFound}
   * is not called), because a store that cannot complete a read or write is
   * what produces such readings. A `full` disk keeps the drop: a torn write
   * there is real corruption, and deleting the database is also what frees
   * the space.
   *
   * After the open, the failure is detected when a transaction on the store
   * fails to begin or to commit, whichever caller ran it (`persist()`, the
   * heartbeat, garbage collection, ...), when `persist()` or `refresh()`
   * fails, when the in-memory dag fails to load a chunk from the store for
   * any caller, and when a background process fails on a store read inside
   * its transaction. The background maintenance processes (heartbeat, client and
   * client-group GC, database collection, mutation recovery) stop on it
   * instead of retrying against the store at their interval.
   *
   * There is no default behavior other than logging. An app that shows a
   * "free up space" or "restart" screen can do so from here.
   */
  onStorageFailure: ((failure: StorageFailureError) => void) | null = null;

  /**
   * Set the first time the kv store reports a storage failure. Read by every
   * path that writes the perdag so none of them retries against storage
   * that has failed.
   */
  #storageFailure: StorageFailureError | undefined;

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
   *   new Replicache client (i.e. to sync while offline, the clients can still
   *   sync with each other via the server).
   *
   * The default behavior is to reload the page (using `location.reload()`). Set
   * this to `null` or provide your own function to prevent the page from
   * reloading automatically. You may want to provide your own function to
   * display a toast to inform the end user there is a new version of your app
   * available and prompting them to refresh.
   */
  onUpdateNeeded: ((reason: UpdateNeededReason) => void) | null = reload;

  /**
   * This gets called when we get an HTTP unauthorized (401) response from the
   * push or pull endpoint. Set this to a function that will ask your user to
   * reauthenticate.
   */
  getAuth: (() => MaybePromise<string | null | undefined>) | null | undefined =
    null;

  // These three are used for testing
  onPushInvoked = () => undefined;
  onBeginPull = () => undefined;
  onRecoverMutations = (r: Promise<boolean>) => r;

  constructor(
    options: ReplicacheOptions<MD>,
    implOptions: ReplicacheImplOptions = {},
  ) {
    validateOptions(options);
    const {
      name,
      logLevel = 'info',
      logSinks = [consoleLogSink],
      pullURL = '',
      auth,
      pushDelay = 10,
      pushURL = '',
      schemaVersion = '',
      pullInterval = 60000,
      mutators = {} as MD,
      requestOptions = {},
      puller,
      pusher,
      indexes = {},
      clientMaxAgeMs = CLIENT_MAX_INACTIVE_TIME,
    } = options;
    const {
      enableMutationRecovery = true,
      enableScheduledPersist = true,
      enableScheduledRefresh = true,
      enableRefresh = () => true,
      enablePullAndPushInOpen = true,
      enableClientGroupForking = true,
      onClientsDeleted = () => promiseVoid,
    } = implOptions;
    this.#zero = implOptions.zero;
    this.#auth = auth ?? '';
    this.pullURL = pullURL;
    this.pushURL = pushURL;
    this.name = name;
    this.schemaVersion = schemaVersion;
    this.pullInterval = pullInterval;
    this.pushDelay = pushDelay;
    this.puller = puller ?? getDefaultPuller(this);
    this.pusher = pusher ?? getDefaultPusher(this);

    this.#enableScheduledPersist = enableScheduledPersist;
    this.#enableScheduledRefresh = enableScheduledRefresh;
    this.#enableRefresh = enableRefresh;
    this.#enablePullAndPushInOpen = enablePullAndPushInOpen;

    this.#lc = createLogContext(logLevel, logSinks, {name});
    this.#lc.debug?.('Constructing Replicache', {
      name,
      'replicache version': version,
    });

    // Close stops the store-using background processes too. Wired here, before
    // any close can happen, because an abort that already fired is not replayed
    // to a listener added later (an engine closed while its open is in flight).
    this.#closeAbortController.signal.addEventListener(
      'abort',
      () => this.#storeProcessesAbortController.abort(),
      {once: true},
    );
    this.#subscriptions = new SubscriptionsManagerImpl(
      this.#queryInternal,
      this.#lc,
      this.#closeAbortController.signal,
    );

    const kvStoreProvider = new MemFallbackStoreProvider(
      getKVStoreProvider(this.#lc, options.kvStore),
      failure => this.#onMemFallBack(failure),
      failure => this.#handleStorageFailure(failure),
    );
    this.#kvStoreProvider = kvStoreProvider;

    const perKVStore = kvStoreProvider.create(this.idbName);
    this.#perKVStore = perKVStore;

    this.#idbDatabases = new IDBDatabasesStore(kvStoreProvider.create);
    this.perdag = new StoreImpl(
      perKVStore,
      newRandomHash,
      assertHash,
      this.#onInvalidRefCount,
    );
    this.memdag = new LazyStore(
      this.perdag,
      LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT,
      newRandomHash,
      assertHash,
      undefined,
      this.#reportStorageFailure,
    );

    // Use a promise-resolve pair so that we have a promise to use even before
    // we call the Open RPC.
    const readyResolver = resolver<void>();
    this.#ready = readyResolver.promise;

    const {minDelayMs = MIN_DELAY_MS, maxDelayMs = MAX_DELAY_MS} =
      requestOptions;
    this.#requestOptions = {maxDelayMs, minDelayMs};

    const visibilityWatcher = getDocumentVisibilityWatcher(
      getBrowserGlobal('document'),
      0,
      this.#closeAbortController.signal,
    );

    this.#pullConnectionLoop = new ConnectionLoop(
      this.#lc.withContext('PULL'),
      new PullDelegate(this, () => this.#invokePull()),
      visibilityWatcher,
    );

    this.#pushConnectionLoop = new ConnectionLoop(
      this.#lc.withContext('PUSH'),
      new PushDelegate(this, () => this.#invokePush()),
    );

    this.mutate = this.#registerMutators(mutators);

    const profileIDResolver = resolver<string>();
    this.#profileIDPromise = profileIDResolver.promise;
    const clientGroupIDResolver = resolver<string>();
    this.#clientGroupIDPromise = clientGroupIDResolver.promise;

    if (!process.env.DISABLE_MUTATION_RECOVERY) {
      this.#mutationRecovery = new MutationRecovery({
        delegate: this,
        lc: this.#lc,
        enableMutationRecovery,
        wrapInOnlineCheck: this.#wrapInOnlineCheck.bind(this),
        wrapInReauthRetries: this.#wrapInReauthRetries.bind(this),
        isPullDisabled: this.#isPullDisabled.bind(this),
        isPushDisabled: this.#isPushDisabled.bind(this),
        clientGroupIDPromise: this.#clientGroupIDPromise,
      });
    }

    this.#onPersist = initOnPersistChannel(
      this.name,
      this.#closeAbortController.signal,
      persistInfo => {
        void this.#handlePersist(persistInfo);
      },
    );

    const open = () =>
      this.#open(
        indexes,
        enableClientGroupForking,
        enableMutationRecovery,
        clientMaxAgeMs,
        profileIDResolver.resolve,
        clientGroupIDResolver.resolve,
        readyResolver.resolve,
        onClientsDeleted,
      );
    void open().catch(e => {
      if (e instanceof InvalidRefCountError) {
        // #onInvalidRefCount (called from the write's release()) already
        // started recovery: drop the database and fire onClientStateNotFound.
        // Nothing else can be done with this instance: `#ready` stays pending
        // so reads and writes never run against the corrupt store, and
        // `#openFailed` lets `close()` dispose the instance anyway.
        this.#openFailed.resolve();
        this.#lc.debug?.('Open failed because the persistent store is corrupt');
        return;
      }
      if (!this.#closed && this.#kvStoreProvider.fallBack(e)) {
        // The storage failed while opening, and the stores are on memory now.
        // Nothing has been written to the in-memory dag that the memory
        // stores cannot serve, so open again on the same stores.
        return open().catch(e => {
          this.#openFailed.resolve();
          if (this.#closed) {
            this.#lc.debug?.('Open on memory ended by close', e);
          } else {
            this.#lc.error?.('Open failed again on memory', e);
          }
        });
      }
      // Whatever the reason, this open is over: `#ready` stays pending so
      // reads and writes never run against a store that failed to open, but
      // `close()` must still be able to dispose the instance.
      this.#openFailed.resolve();
      throw e;
    });
  }

  async #open(
    indexes: IndexDefinitions,
    enableClientGroupForking: boolean,
    enableMutationRecovery: boolean,
    clientMaxAgeMs: number,
    profileIDResolver: (profileID: string) => void,
    resolveClientGroupID: (clientGroupID: ClientGroupID) => void,
    resolveReady: () => void,
    onClientsDeleted: OnClientsDeleted,
  ): Promise<void> {
    const {clientID} = this;
    // Everything up to the head write is what it takes to have a usable
    // database, including waiting out a previous instance of the same name.
    // The timer stops before the replica is loaded into Zero's IVM sources,
    // which is separate work and usually the larger of the two: a number that
    // spanned both would report a slow hydration as a slow disk.
    const openStart = performance.now();
    // If we are currently closing a Replicache instance with the same name,
    // wait for it to finish closing.
    await closingInstances.get(this.name);
    await this.#idbDatabases.getProfileID().then(profileIDResolver);
    await this.#idbDatabases.putDatabase(this.#idbDatabase);
    const [client, headHash, , isNewClientGroup] = await initClientV6(
      clientID,
      this.#lc,
      this.perdag,
      Object.keys(this.#mutatorRegistry),
      indexes,
      FormatVersion.Latest,
      enableClientGroupForking,
    );

    resolveClientGroupID(client.clientGroupID);
    await withWrite(this.memdag, write =>
      write.setHead(DEFAULT_HEAD_NAME, headHash),
    );

    // Now we have a profileID, a clientID, a clientGroupID and DB!
    const dbOpenMs = performance.now() - openStart;
    if (dbOpenMs > SLOW_DB_OPEN_THRESHOLD_MS) {
      this.#lc.warn?.(`Opening the database took ${Math.round(dbOpenMs)}ms`, {
        name: this.idbName,
      });
    } else {
      this.#lc.debug?.('Opened the database in', Math.round(dbOpenMs), 'ms');
    }

    await this.#zero?.init(headHash, this.memdag);
    this.#kvStoreProvider.opened();
    resolveReady();

    if (this.#enablePullAndPushInOpen) {
      this.pull().catch(noop);
      this.push().catch(noop);
    }

    const {signal} = this.#storeProcessesAbortController;

    startHeartbeats(
      clientID,
      this.perdag,
      () => {
        this.#clientStateNotFoundOnClient(clientID);
      },
      HEARTBEAT_INTERVAL,
      this.#lc,
      signal,
      this.#reportStorageFailure,
    );
    initClientGC(
      clientID,
      this.perdag,
      clientMaxAgeMs,
      GC_INTERVAL,
      onClientsDeleted,
      this.#lc,
      signal,
      this.#reportStorageFailure,
    );
    initCollectIDBDatabases(
      this.#idbDatabases,
      this.#kvStoreProvider,
      COLLECT_IDB_INTERVAL,
      INITIAL_COLLECT_IDB_DELAY,
      2 * clientMaxAgeMs,
      enableMutationRecovery,
      onClientsDeleted,
      this.#lc,
      signal,
      // Collection writes deleted clients into every surviving database using
      // its own dag store. When that is our database, a corrupt ref count
      // found there must reach the same recovery as writes through `perdag`.
      // A corrupt foreign database is left for its own instance to discover
      // and recover from on its next write.
      (name, createKVStore) =>
        new StoreImpl(
          createKVStore(name),
          newRandomHash,
          assertHash,
          name === this.idbName ? this.#onInvalidRefCount : undefined,
        ),
      this.#reportStorageFailure,
    );
    initClientGroupGC(
      this.perdag,
      enableMutationRecovery,
      this.#lc,
      signal,
      this.#reportStorageFailure,
    );
    initNewClientChannel(
      this.name,
      this.idbName,
      signal,
      client.clientGroupID,
      isNewClientGroup,
      () => {
        this.#fireOnUpdateNeeded(updateNeededReasonNewClientGroup);
      },
      this.perdag,
    );

    setIntervalWithSignal(
      () => this.recoverMutations(),
      RECOVER_MUTATIONS_INTERVAL_MS,
      signal,
    );
    void this.recoverMutations();

    getBrowserGlobal('document')?.addEventListener(
      'visibilitychange',
      this.#onVisibilityChange,
    );
  }

  #onVisibilityChange = async () => {
    // A failing store cannot answer whether the client state exists; asking
    // it would report a lost client for a fault that is not in the data.
    if (this.#closed || this.#storageFailure !== undefined) {
      return;
    }

    // In case of running in a worker, we don't have a document.
    if (getBrowserGlobal('document')?.visibilityState !== 'visible') {
      return;
    }

    await this.#checkForClientStateNotFoundAndCallHandler();
  };

  async #checkForClientStateNotFoundAndCallHandler(): Promise<boolean> {
    const {clientID} = this;
    const hasClientState = await withRead(this.perdag, read =>
      persistHasClientState(clientID, read),
    );
    if (!hasClientState) {
      this.#clientStateNotFoundOnClient(clientID);
    }
    return !hasClientState;
  }

  /**
   * The browser profile ID for this browser profile. Every instance of Replicache
   * browser-profile-wide shares the same profile ID.
   */
  get profileID(): Promise<string> {
    return this.#profileIDPromise;
  }

  /**
   * The client ID for this instance of Replicache. Each instance of Replicache
   * gets a unique client ID.
   */
  get clientID(): string {
    return this.#clientID;
  }

  /**
   * The client group ID for this instance of Replicache. Instances of
   * Replicache will have the same client group ID if and only if they have
   * the same name, mutators, indexes, schema version, format version, and
   * browser profile.
   */
  get clientGroupID(): Promise<string> {
    return this.#clientGroupIDPromise;
  }

  /**
   * `onOnlineChange` is called when the {@link online} property changes. See
   * {@link online} for more details.
   */
  onOnlineChange: ((online: boolean) => void) | null = null;

  /**
   * A rough heuristic for whether the client is currently online. Note that
   * there is no way to know for certain whether a client is online - the next
   * request can always fail. This property returns true if the last sync attempt succeeded,
   * and false otherwise.
   */
  get online(): boolean {
    return this.#online;
  }

  /**
   * Whether the Replicache database has been closed. Once Replicache has been
   * closed it no longer syncs and you can no longer read or write data out of
   * it. After it has been closed it is pretty much useless and should not be
   * used any more.
   */
  get closed(): boolean {
    return this.#closed;
  }

  /**
   * Closes this Replicache instance.
   *
   * When closed all subscriptions end and no more read or writes are allowed.
   */
  async close(): Promise<void> {
    this.#closed = true;
    const {promise, resolve} = resolver();
    closingInstances.set(this.name, promise);

    this.#closeAbortController.abort();

    getBrowserGlobal('document')?.removeEventListener(
      'visibilitychange',
      this.#onVisibilityChange,
    );

    await Promise.race([this.#ready, this.#openFailed.promise]);
    const closingPromises = [
      this.memdag.close(),
      this.perdag.close(),
      this.#idbDatabases.close(),
    ];

    this.#pullConnectionLoop.close();
    this.#pushConnectionLoop.close();

    this.#subscriptions.clear();

    await Promise.all(closingPromises);
    closingInstances.delete(this.name);
    resolve();
  }

  async maybeEndPull(syncHead: Hash, requestID: string): Promise<void> {
    for (;;) {
      if (this.#closed) {
        return;
      }

      await this.#ready;
      const {clientID} = this;
      const lc = this.#lc
        .withContext('maybeEndPull')
        .withContext('requestID', requestID);
      const {replayMutations, diffs, oldMainHead, mainHead} =
        await maybeEndPull<LocalMeta>(
          this.memdag,
          lc,
          syncHead,
          clientID,
          this.#subscriptions,
          FormatVersion.Latest,
        );

      if (!replayMutations || replayMutations.length === 0) {
        // All done.
        this.#zero?.advance(oldMainHead, mainHead, diffs.get('') ?? []);
        await this.#subscriptions.fire(diffs);
        void this.#schedulePersist();
        return;
      }

      // Replay.
      let zeroData = await this.#zero?.getTxData?.(syncHead);
      for (const mutation of replayMutations) {
        // TODO(greg): I'm not sure why this was in Replicache#_mutate...
        // Ensure that we run initial pending subscribe functions before starting a
        // write transaction.
        if (this.#subscriptions.hasPendingSubscriptionRuns) {
          await Promise.resolve();
        }
        const {meta} = mutation;
        const {result, zeroData: next} = await withWriteNoImplicitCommit(
          this.memdag,
          dagWrite =>
            rebaseMutationAndCommit(
              mutation,
              dagWrite,
              syncHead,
              SYNC_HEAD_NAME,
              this.#mutatorRegistry,
              lc,
              isLocalMetaDD31(meta) ? meta.clientID : clientID,
              FormatVersion.Latest,
              zeroData,
            ),
        );
        syncHead = result;
        // The fork the mutation ran against when it succeeded, or the branch we
        // came in with when it threw.
        zeroData = next;
      }
    }
  }

  #invokePull(): Promise<boolean> {
    if (this.#isPullDisabled()) {
      return Promise.resolve(true);
    }

    return this.#wrapInOnlineCheck(async () => {
      try {
        this.#changeSyncCounters(0, 1);
        const {syncHead, requestID, ok} = await this.beginPull();
        if (!ok) {
          return false;
        }
        if (syncHead !== emptyHash) {
          await this.maybeEndPull(syncHead, requestID);
        }
      } catch (e) {
        throw await this.#convertToClientStateNotFoundError(e);
      } finally {
        this.#changeSyncCounters(0, -1);
      }
      return true;
    }, 'Pull');
  }

  #isPullDisabled() {
    return (
      this.isClientGroupDisabled ||
      (this.pullURL === '' && isDefaultPuller(this.puller))
    );
  }

  async #wrapInOnlineCheck(
    f: () => Promise<boolean>,
    name: string,
  ): Promise<boolean> {
    let online = true;

    try {
      return await f();
    } catch (e) {
      // The error paths of beginPull and maybeEndPull need to be reworked.
      //
      // We want to distinguish between:
      // a) network requests failed -- we're offline basically
      // b) sync was aborted because one's already in progress
      // c) oh noes - something unexpected happened
      //
      // Right now, all of these come out as errors. We distinguish (b) with a
      // hacky string search. (a) and (c) are not distinguishable currently
      // because repc doesn't provide sufficient information, so we treat all
      // errors that aren't (b) as (a).
      if (e instanceof PushError || e instanceof PullError) {
        online = false;
        this.#lc.debug?.(`${name} threw:\n`, e, '\nwith cause:\n', e.causedBy);
      } else if (e instanceof ReportError) {
        this.#lc.error?.(e);
      } else {
        this.#lc.info?.(`${name} threw:\n`, e);
      }
      return false;
    } finally {
      if (this.#online !== online) {
        this.#online = online;
        this.onOnlineChange?.(online);
        if (online) {
          void this.recoverMutations();
        }
      }
    }
  }

  async #wrapInReauthRetries<R>(
    f: (
      requestID: string,
      requestLc: LogContext,
    ) => Promise<{
      httpRequestInfo: HTTPRequestInfo | undefined;
      result: R;
    }>,
    verb: string,
    lc: LogContext,
    preAuth: () => MaybePromise<void> = noop,
    postAuth: () => MaybePromise<void> = noop,
  ): Promise<{
    result: R;
    authFailure: boolean;
  }> {
    const {clientID} = this;
    let reauthAttempts = 0;
    let lastResult;
    lc = lc.withContext(verb);
    do {
      const requestID = newRequestID(clientID);
      const requestLc = lc.withContext('requestID', requestID);
      const {httpRequestInfo, result} = await f(requestID, requestLc);
      lastResult = result;
      if (!httpRequestInfo) {
        return {
          result,
          authFailure: false,
        };
      }
      const {errorMessage, httpStatusCode} = httpRequestInfo;

      if (errorMessage || httpStatusCode !== 200) {
        // TODO(arv): Maybe we should not log the server URL when the error comes
        // from a Pusher/Puller?
        requestLc.error?.(
          `Got a non 200 response doing ${verb}: ${httpStatusCode}` +
            (errorMessage ? `: ${errorMessage}` : ''),
        );
      }
      if (httpStatusCode !== httpStatusUnauthorized) {
        return {
          result,
          authFailure: false,
        };
      }
      if (!this.getAuth) {
        return {
          result,
          authFailure: true,
        };
      }
      let auth;
      try {
        await preAuth();
        auth = await this.getAuth();
      } finally {
        await postAuth();
      }
      if (auth === null || auth === undefined) {
        return {
          result,
          authFailure: true,
        };
      }
      this.auth = auth;
      reauthAttempts++;
    } while (reauthAttempts < MAX_REAUTH_TRIES);
    lc.info?.('Tried to reauthenticate too many times');
    return {
      result: lastResult,
      authFailure: true,
    };
  }

  #isPushDisabled() {
    return (
      this.isClientGroupDisabled ||
      (this.pushURL === '' && isDefaultPusher(this.pusher))
    );
  }

  async #invokePush(): Promise<boolean> {
    if (TESTING) {
      this.onPushInvoked();
    }
    if (this.#isPushDisabled()) {
      return true;
    }

    await this.#ready;
    const profileID = await this.#profileIDPromise;
    const {clientID} = this;
    const clientGroupID = await this.#clientGroupIDPromise;
    return this.#wrapInOnlineCheck(async () => {
      const {result: pusherResult} = await this.#wrapInReauthRetries(
        async (requestID: string, requestLc: LogContext) => {
          try {
            this.#changeSyncCounters(1, 0);
            const pusherResult = await push(
              requestID,
              this.memdag,
              requestLc,
              profileID,
              clientGroupID,
              clientID,
              this.pusher,
              this.schemaVersion,
              PUSH_VERSION_DD31,
            );
            return {
              result: pusherResult,
              httpRequestInfo: pusherResult?.httpRequestInfo,
            };
          } finally {
            this.#changeSyncCounters(-1, 0);
          }
        },
        'push',
        this.#lc,
      );

      if (pusherResult === undefined) {
        // No pending mutations.
        return true;
      }

      const {response, httpRequestInfo} = pusherResult;

      if (isVersionNotSupportedResponse(response)) {
        this.#handleVersionNotSupportedResponse(response);
      } else if (isClientStateNotFoundResponse(response)) {
        await this.#clientStateNotFoundOnServer();
      }

      // No pushResponse means we didn't do a push because there were no
      // pending mutations.
      return httpRequestInfo.httpStatusCode === 200;
    }, 'Push');
  }

  #handleVersionNotSupportedResponse(response: VersionNotSupportedResponse) {
    const reason: UpdateNeededReason = {
      type: response.error,
    };
    if (response.versionType) {
      reason.versionType = response.versionType;
    }
    this.#fireOnUpdateNeeded(reason);
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
    return throwIfError(this.#pushConnectionLoop.send(now));
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
    return throwIfError(this.#pullConnectionLoop.send(now));
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
  async poke(poke: PokeInternal): Promise<void> {
    await this.#ready;
    // TODO(MP) Previously we created a request ID here and included it with the
    // PullRequest to the server so we could tie events across client and server
    // together. Since the direction is now reversed, creating and adding a request ID
    // here is kind of silly. We should consider creating the request ID
    // on the *server* and passing it down in the poke for inclusion here in the log
    // context
    const {clientID} = this;
    const requestID = newRequestID(clientID);
    const lc = this.#lc
      .withContext('handlePullResponse')
      .withContext('requestID', requestID);

    const {pullResponse} = poke;

    if (isVersionNotSupportedResponse(pullResponse)) {
      this.#handleVersionNotSupportedResponse(pullResponse);
      return;
    }

    if (isClientStateNotFoundResponse(pullResponse)) {
      await this.#clientStateNotFoundOnServer();
      return;
    }

    const result = await handlePullResponseV1(
      lc,
      this.memdag,
      deepFreeze(poke.baseCookie),
      pullResponse,
      clientID,
      FormatVersion.Latest,
    );

    switch (result.type) {
      case HandlePullResponseResultEnum.Applied:
        await this.maybeEndPull(result.syncHead, requestID);
        break;
      case HandlePullResponseResultEnum.CookieMismatch:
        throw new Error(
          'unexpected base cookie for poke: ' + JSON.stringify(poke),
        );
      case HandlePullResponseResultEnum.NoOp:
        break;
    }
  }

  async beginPull(): Promise<BeginPullResult> {
    if (TESTING) {
      this.onBeginPull();
    }
    await this.#ready;
    const profileID = await this.profileID;
    const {clientID} = this;
    const clientGroupID = await this.#clientGroupIDPromise;
    const {
      result: {beginPullResponse, requestID},
    } = await this.#wrapInReauthRetries(
      async (requestID: string, requestLc: LogContext) => {
        const beginPullResponse = await beginPullV1(
          profileID,
          clientID,
          clientGroupID,
          this.schemaVersion,
          this.puller,
          requestID,
          this.memdag,
          FormatVersion.Latest,
          requestLc,
        );
        return {
          result: {beginPullResponse, requestID},
          httpRequestInfo: beginPullResponse.httpRequestInfo,
        };
      },
      'pull',
      this.#lc,
      () => this.#changeSyncCounters(0, -1),
      () => this.#changeSyncCounters(0, 1),
    );

    const {pullResponse} = beginPullResponse;
    if (isVersionNotSupportedResponse(pullResponse)) {
      this.#handleVersionNotSupportedResponse(pullResponse);
    } else if (isClientStateNotFoundResponse(beginPullResponse.pullResponse)) {
      await this.#clientStateNotFoundOnServer();
    }

    const {syncHead, httpRequestInfo} = beginPullResponse;
    return {requestID, syncHead, ok: httpRequestInfo.httpStatusCode === 200};
  }

  persist(): Promise<void> {
    // Prevent multiple persist calls from running at the same time.
    return this.#persistLock.withLock(async () => {
      const {clientID} = this;
      await this.#ready;
      // After a storage failure nothing is persisted.
      if (this.#closed || this.#storageFailure) {
        return;
      }
      try {
        await persistDD31(
          this.#lc,
          clientID,
          this.memdag,
          this.perdag,
          this.#mutatorRegistry,
          () => this.#closed,
          FormatVersion.Latest,
          this.#zero?.getTxData,
        );
      } catch (e) {
        if (e instanceof ClientStateNotFoundError) {
          this.#clientStateNotFoundOnClient(clientID);
        } else if (e instanceof InvalidRefCountError) {
          await this.#handleInvalidRefCount(e);
          // Nothing was persisted and the database is gone. Do not tell this
          // or other instances to refresh from it.
          return;
        } else if (this.#closed) {
          this.#lc.debug?.('Exception persisting during close', e);
        } else {
          const failure = getStorageFailure(e);
          if (failure === undefined) {
            throw e;
          }
          // Nothing was persisted, and nothing will be until the app replaces
          // this instance. Do not tell other instances to refresh.
          this.#handleStorageFailure(failure);
          return;
        }
      }

      const clientGroupID = await this.#clientGroupIDPromise;
      assert(clientGroupID, 'Expected clientGroupID to be defined');
      this.#onPersist({clientID, clientGroupID});
    });
  }

  async refresh(): Promise<void> {
    await this.#ready;
    const {clientID} = this;
    if (this.#closed || this.#storageFailure || !this.#enableRefresh()) {
      return;
    }
    let refreshResult: Awaited<ReturnType<typeof refresh>>;
    try {
      refreshResult = await refresh(
        this.#lc,
        this.memdag,
        this.perdag,
        clientID,
        this.#mutatorRegistry,
        this.#subscriptions,
        () => this.closed,
        FormatVersion.Latest,
        this.#zero,
      );
    } catch (e) {
      if (e instanceof ClientStateNotFoundError) {
        this.#clientStateNotFoundOnClient(clientID);
      } else if (e instanceof InvalidRefCountError) {
        await this.#handleInvalidRefCount(e);
      } else if (this.#closed) {
        this.#lc.debug?.('Exception refreshing during close', e);
      } else {
        const failure = getStorageFailure(e);
        if (failure === undefined) {
          throw e;
        }
        this.#handleStorageFailure(failure);
        return;
      }
    }
    if (refreshResult !== undefined) {
      await this.#subscriptions.fire(refreshResult.diffs);
    }
  }

  #fireOnClientStateNotFound() {
    this.onClientStateNotFound?.();
  }

  #clientStateNotFoundOnClient(clientID: ClientID) {
    this.#lc.error?.(`Client state not found on client, clientID: ${clientID}`);
    this.#fireOnClientStateNotFound();
  }

  /**
   * Set once recovery from a corrupt database has started. Every write path
   * into the perdag (open, persist, refresh, heartbeat, GC, ...) can trip on
   * the same corrupt key, so recovery runs once and later reports await it.
   */
  #corruptDatabaseRecovery: Promise<void> | undefined;

  /**
   * Handed to the in-memory dag and the background processes, which have no
   * other way to reach this instance when a store read inside them fails.
   * Skipped while closing: a read that fails because the store was closed
   * under it is not a storage failure of the instance.
   */
  readonly #reportStorageFailure = (failure: StorageFailureError): void => {
    if (!this.#closed) {
      this.#handleStorageFailure(failure);
    }
  };

  /**
   * A storage failure after the open. Records the first one, stops the
   * store-using background processes, logs it once and tells the app. Later
   * failures of the same instance are the same fault and are not reported
   * again; the scheduled persist and refresh stop asking.
   */
  #handleStorageFailure(failure: StorageFailureError): void {
    if (this.#storageFailure !== undefined) {
      return;
    }
    this.#storageFailure = failure;
    this.#storeProcessesAbortController.abort();
    // warn, not error: a full or failing disk is the device's condition, and
    // it is handled here (retries stop, the app is told once through
    // onStorageFailure), so there is nothing for a developer to fix.
    this.#lc.warn?.(
      `Local store storage failed (${failure.kind}) for ${this.idbName}; persistence is stopped for this instance`,
      failure,
    );
    this.#fireOnStorageFailure(failure);
  }

  /**
   * A storage failure during the open: the stores have moved onto memory, so
   * the instance runs on them for the session, as with `kvStore: 'mem'`.
   * Reported on a
   * later microtask: the SQLite stores open in `create`, while the
   * constructor is still running and before the app can attach the callback.
   */
  #onMemFallBack(failure: StorageFailureError): void {
    this.#lc.warn?.(
      `Local store storage failed (${failure.kind}) opening ${this.idbName}; running this instance in memory`,
      failure,
    );
    queueMicrotask(() => this.#fireOnStorageFailure(failure));
  }

  /**
   * The app's callback must not be able to stop the recovery around it: in
   * the open path, a throw here would skip the open on memory and leave the
   * instance never ready.
   */
  #fireOnStorageFailure(failure: StorageFailureError): void {
    try {
      this.onStorageFailure?.(failure);
    } catch (e) {
      this.#lc.error?.('onStorageFailure threw', e);
    }
  }

  /**
   * Installed on the dag store that writes to our database. The store calls
   * it after the failing transaction has been released, so dropping the
   * database from here does not race with the rollback.
   */
  readonly #onInvalidRefCount = (e: InvalidRefCountError): void => {
    void this.#handleInvalidRefCount(e);
  };

  /**
   * The persistent dag store contains an invalid ref count. This means the
   * store is corrupt (due to some unknown bug) and every subsequent write that
   * touches that chunk would fail the same way. There is no way to repair it,
   * so we treat it like the client state was lost and fire
   * `onClientStateNotFound`.
   *
   * Disabling the client group is not enough: the corrupt ref count stays in
   * the underlying kv store and any later write that touches that chunk (for
   * example rewriting the `clients` chunk when a new client starts) would hit
   * it again. Instead we drop the whole database so that the reload starts
   * from a fresh store. Pending local mutations in this database are lost.
   *
   * This is expected to be exceedingly rare. Other instances sharing the
   * database are not told: they simply hit the same corrupt key (or, once
   * this instance drops the database, a generic storage error) on their own
   * next write and recover the same way then. That's an acceptable delay for
   * something we don't expect to ever happen.
   */
  #handleInvalidRefCount(e: InvalidRefCountError): Promise<void> {
    if (
      this.#storageFailure !== undefined &&
      this.#storageFailure.kind !== 'full'
    ) {
      // A store that cannot complete its reads and writes produces invalid
      // ref counts without the data being corrupt. Dropping the database here
      // would destroy an intact replica and rebuild onto the same failing
      // storage, which is the loop `onStorageFailure` exists to stop.
      //
      // A FULL disk is the exception, and keeps the drop: a torn write on a
      // full disk is a real corruption, and deleting the database is also the
      // one thing that frees the space the device needs.
      this.#lc.warn?.(
        `Invalid ref count while the local store is failing (${this.#storageFailure.kind}); not dropping database ${this.idbName}`,
        e,
      );
      return promiseVoid;
    }
    if (this.#corruptDatabaseRecovery === undefined) {
      this.#lc.error?.(
        `Client state is corrupt on client, clientID: ${this.clientID}. Dropping database ${this.idbName}`,
        e,
      );
      this.#corruptDatabaseRecovery =
        this.#dropDatabaseAndFireOnClientStateNotFound();
    }
    return this.#corruptDatabaseRecovery;
  }

  /**
   * Drops our database, then fires `onClientStateNotFound` so the app can
   * recover, whether or not the drop itself succeeded.
   *
   * Uses its own handle on the databases registry rather than `#idbDatabases`
   * so it works even when `close()` is already closing this instance.
   */
  async #dropDatabaseAndFireOnClientStateNotFound(): Promise<void> {
    const {idbName, clientID} = this;
    let idbDatabases: IDBDatabasesStore | undefined;
    try {
      idbDatabases = new IDBDatabasesStore(this.#kvStoreProvider.create);
      await dropDatabaseInternal(
        idbName,
        idbDatabases,
        this.#kvStoreProvider.drop,
      );
    } catch (dropError) {
      this.#lc.error?.(
        `Failed to drop database ${idbName}, clientID: ${clientID}`,
        dropError,
      );
    }
    try {
      await idbDatabases?.close();
    } catch (closeError) {
      // Closing the registry handle is best effort. The callback below must
      // fire regardless, and the drop outcome was already logged.
      this.#lc.debug?.(
        `Failed to close the databases registry after dropping ${idbName}, clientID: ${clientID}`,
        closeError,
      );
    }
    this.#fireOnClientStateNotFound();
  }

  async #clientStateNotFoundOnServer() {
    const clientGroupID = await this.#clientGroupIDPromise;
    this.#lc.error?.(
      `Client state not found on server, clientGroupID: ${clientGroupID}`,
    );
    await this.disableClientGroup();
    this.#fireOnClientStateNotFound();
  }

  async disableClientGroup(): Promise<void> {
    const clientGroupID = await this.#clientGroupIDPromise;
    assert(clientGroupID, 'Expected clientGroupID to be defined');
    this.isClientGroupDisabled = true;
    await withWrite(this.perdag, dagWrite =>
      disableClientGroup(clientGroupID, dagWrite),
    );
  }

  #fireOnUpdateNeeded(reason: UpdateNeededReason) {
    this.#lc.debug?.(`Update needed, reason: ${reason}`);
    this.onUpdateNeeded?.(reason);
  }

  async #schedulePersist(): Promise<void> {
    if (!this.#enableScheduledPersist || this.#storageFailure) {
      return;
    }
    await this.#schedule('persist', this.#persistScheduler);
  }

  async #handlePersist(persistInfo: PersistInfo): Promise<void> {
    this.#lc.debug?.('Handling persist', persistInfo);
    const clientGroupID = await this.#clientGroupIDPromise;
    if (persistInfo.clientGroupID === clientGroupID) {
      void this.#scheduleRefresh();
    }
  }

  async #scheduleRefresh(): Promise<void> {
    if (!this.#enableScheduledRefresh || this.#storageFailure) {
      return;
    }
    await this.#schedule('refresh from storage', this.#refreshScheduler);
  }

  async #schedule(name: string, scheduler: ProcessScheduler): Promise<void> {
    try {
      await scheduler.schedule();
    } catch (e) {
      if (e instanceof AbortError) {
        this.#lc.debug?.(`Scheduled ${name} did not complete before close.`);
      } else {
        this.#lc.error?.(`Error during ${name}`, e);
      }
    }
  }

  /**
   * Runs a refresh as soon as possible through the refresh scheduler.
   */
  runRefresh(): Promise<void> {
    return this.#refreshScheduler.run();
  }

  #changeSyncCounters(pushDelta: 0, pullDelta: 1 | -1): void;
  #changeSyncCounters(pushDelta: 1 | -1, pullDelta: 0): void;
  #changeSyncCounters(pushDelta: number, pullDelta: number): void {
    this.#pushCounter += pushDelta;
    this.#pullCounter += pullDelta;
    const delta = pushDelta + pullDelta;
    const counter = this.#pushCounter + this.#pullCounter;
    if ((delta === 1 && counter === 1) || counter === 0) {
      const syncing = counter > 0;
      // Run in a new microtask.
      void Promise.resolve().then(() => this.onSync?.(syncing));
    }
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
    if (typeof options === 'function') {
      options = {onData: options};
    }

    const {onData, onError, onDone, isEqual} = options;
    return this.#subscriptions.add(
      new SubscriptionImpl(body, onData, onError, onDone, isEqual),
    );
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
    return this.#subscriptions.add(
      new WatchSubscription(callback as WatchCallback, options),
    );
  }

  /**
   * Query is used for read transactions. It is recommended to use transactions
   * to ensure you get a consistent view across multiple calls to `get`, `has`
   * and `scan`.
   */
  query<R>(body: (tx: ReadTransaction) => Promise<R> | R): Promise<R> {
    return this.#queryInternal(body);
  }

  get cookie(): Promise<Cookie> {
    return this.#ready.then(() =>
      withRead(this.memdag, async dagRead => {
        const mainHeadHash = await dagRead.getHead(DEFAULT_HEAD_NAME);
        if (!mainHeadHash) {
          throw new Error('Internal no main head found');
        }
        const baseSnapshot = await baseSnapshotFromHash(mainHeadHash, dagRead);
        const baseSnapshotMeta = baseSnapshot.meta;
        const cookie = baseSnapshotMeta.cookieJSON;
        assertCookie(cookie);
        return cookie;
      }),
    );
  }

  #queryInternal: QueryInternal = async body => {
    await this.#ready;
    const {clientID} = this;
    return withRead(this.memdag, async dagRead => {
      try {
        const dbRead = await readFromDefaultHead(dagRead, FormatVersion.Latest);
        const tx = new ReadTransactionImpl(clientID, dbRead, this.#lc);
        return await body(tx);
      } catch (ex) {
        throw await this.#convertToClientStateNotFoundError(ex);
      }
    });
  };

  #register<Return extends ReadonlyJSONValue | void, Args extends JSONValue>(
    name: string,
    mutatorImpl: (tx: WriteTransaction, args?: Args) => MaybePromise<Return>,
  ): (
    args?: Args,
  ) => Promise<Return> | {client: Promise<Return>; server: Promise<unknown>} {
    this.#mutatorRegistry[name] = mutatorImpl as (
      tx: WriteTransaction,
      args: JSONValue | undefined,
    ) => Promise<void | JSONValue>;

    return (
      args?: Args,
    ):
      | Promise<Return>
      | {client: Promise<Return>; server: Promise<unknown>} => {
      // DO NOT track CRUD mutations as they do not receive responses from
      // the server.
      const trackingData =
        name === '_zero_crud' ? undefined : this.#zero?.trackMutation();

      const result = this.#mutate(
        trackingData,
        name,
        mutatorImpl,
        args,
        performance.now(),
      );

      if (trackingData) {
        return {
          client: result,
          server: trackingData.serverPromise,
        };
      }

      return result;
    };
  }

  #registerMutators<
    M extends {
      [key: string]: (
        tx: WriteTransaction,
        args?: ReadonlyJSONValue,
      ) => MutatorReturn;
    },
  >(regs: M): MakeMutators<M> {
    type Mut = MakeMutators<M>;
    const rv: Partial<Mut> = Object.create(null);
    for (const k in regs) {
      rv[k] = this.#register(k, regs[k]) as MakeMutator<M[typeof k]>;
    }
    return rv as Mut;
  }

  async #mutate<
    R extends ReadonlyJSONValue | void,
    A extends ReadonlyJSONValue,
  >(
    trackingData: MutationTrackingData | undefined,
    name: string,
    mutatorImpl: (tx: WriteTransaction, args?: A) => MaybePromise<R>,
    args: A | undefined,
    timestamp: number,
  ): Promise<R> {
    const frozenArgs = deepFreeze(args ?? null);

    // Ensure that we run initial pending subscribe functions before starting a
    // write transaction.
    if (this.#subscriptions.hasPendingSubscriptionRuns) {
      await Promise.resolve();
    }

    await this.#ready;
    const {clientID} = this;
    return withWriteNoImplicitCommit(this.memdag, async dagWrite => {
      try {
        let result: R;
        let newHead: Hash;
        let diffs: DiffsMap;
        let headHash: Hash;
        try {
          headHash = await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite);
          const originalHash = null;

          const dbWrite = await newWriteLocal(
            headHash,
            name,
            frozenArgs,
            originalHash,
            dagWrite,
            timestamp,
            clientID,
            FormatVersion.Latest,
          );

          const mutationID = await dbWrite.getMutationID();
          const tx = new WriteTransactionImpl(
            clientID,
            mutationID,
            'initial',
            await this.#zero?.getTxData(headHash, {
              openLazyRead: dagWrite,
            }),
            dbWrite,
            this.#lc,
          );

          if (trackingData) {
            this.#zero?.mutationIDAssigned(
              trackingData.ephemeralID,
              mutationID,
            );
          }

          result = await mutatorImpl(tx, args);

          throwIfClosed(dbWrite);
          const lastMutationID = await dbWrite.getMutationID();
          [newHead, diffs] = await dbWrite.commitWithDiffs(
            DEFAULT_HEAD_NAME,
            this.#subscriptions,
          );

          // Update this after the commit in case the commit fails.
          this.lastMutationID = lastMutationID;
        } catch (e) {
          // If we threw before we could persist the mutation
          // then we need to reject the mutation.
          if (trackingData) {
            this.#zero?.rejectMutation(trackingData.ephemeralID, e);
          }
          throw e;
        }

        this.#zero?.advance(headHash, newHead, diffs.get('') ?? []);

        // Send is not supposed to reject
        this.#pushConnectionLoop.send(false).catch(() => void 0);
        await this.#subscriptions.fire(diffs);
        void this.#schedulePersist();
        return result;
      } catch (ex) {
        throw await this.#convertToClientStateNotFoundError(ex);
      }
    });
  }

  /**
   * In the case we get a ChunkNotFoundError we check if the client got garbage
   * collected and if so change the error to a ClientStateNotFoundError instead
   */
  async #convertToClientStateNotFoundError(ex: unknown): Promise<unknown> {
    if (
      ex instanceof ChunkNotFoundError &&
      (await this.#checkForClientStateNotFoundAndCallHandler())
    ) {
      return new ClientStateNotFoundError(this.clientID);
    }

    return ex;
  }

  recoverMutations(): Promise<boolean> | void {
    // Also reached on every offline->online change, which does not go
    // through the store-process signal.
    if (this.#storeProcessesAbortController.signal.aborted) {
      return promiseFalse;
    }
    if (!process.env.DISABLE_MUTATION_RECOVERY) {
      // oxlint-disable-next-line no-non-null-assertion
      const result = this.#mutationRecovery!.recoverMutations(
        this.#ready,
        this.perdag,
        this.#idbDatabase,
        this.#idbDatabases,
        this.#kvStoreProvider.create,
      );
      if (TESTING) {
        void this.onRecoverMutations(result);
      }
      return result;
    }
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
    return withRead(this.memdag, pendingMutationsForAPI);
  }
}

// This map is used to keep track of closing instances of Replicache. When an
// instance is opening we wait for any currently closing instances.
const closingInstances: Map<string, Promise<unknown>> = new Map();

async function throwIfError(p: Promise<undefined | {error: unknown}>) {
  const res = await p;
  if (res) {
    throw res.error;
  }
}

function reload(): void {
  if (typeof location !== 'undefined') {
    location.reload();
  }
}

function validateOptions<MD extends MutatorDefs>(
  options: ReplicacheOptions<MD>,
): void {
  const {name, clientMaxAgeMs} = options;
  if (typeof name !== 'string' || !name) {
    throw new TypeError('name is required and must be non-empty');
  }

  if (clientMaxAgeMs !== undefined) {
    const min = Math.max(GC_INTERVAL, HEARTBEAT_INTERVAL);
    if (typeof clientMaxAgeMs !== 'number' || clientMaxAgeMs <= min) {
      throw new TypeError(
        `clientAgeMaxMs must be a number larger than ${min}ms`,
      );
    }
  }
}
