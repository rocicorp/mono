import {consoleLogSink, LogContext, TeeLogSink} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {ReadonlyJSONValue, deepFreeze} from './json.js';
import type {JSONValue} from './json.js';
import {Pusher, PushError} from './pusher.js';
import {
  assertPullResponseDD31,
  assertPullResponseSDD,
  isClientStateNotFoundResponse,
  Puller,
  PullError,
  PullResponse,
  PullResponseDD31,
} from './puller.js';
import {
  CreateIndexDefinition,
  IndexTransactionImpl,
  ReadTransactionImpl,
  WriteTransactionImpl,
} from './transactions.js';
import type {ReadTransaction, WriteTransaction} from './transactions.js';
import {ConnectionLoop, MAX_DELAY_MS, MIN_DELAY_MS} from './connection-loop.js';
import {defaultPuller} from './puller.js';
import {defaultPusher} from './pusher.js';
import type {
  ReplicacheInternalOptions,
  ReplicacheOptions,
} from './replicache-options.js';
import {PullDelegate, PushDelegate} from './connection-loop-delegates.js';
import {
  WatchNoIndexCallback,
  SubscribeOptions,
  SubscriptionsManager,
  WatchOptions,
  WatchCallbackForOptions,
  WatchCallback,
} from './subscriptions.js';
import {IDBStore} from './kv/mod.js';
import * as dag from './dag/mod.js';
import * as db from './db/mod.js';
import * as sync from './sync/mod.js';
import {assertHash, emptyHash, Hash} from './hash.js';
import * as persist from './persist/mod.js';
import type {HTTPRequestInfo} from './http-request-info.js';
import {assert} from './asserts.js';
import {
  getLicenseStatus,
  licenseActive,
  PROD_LICENSE_SERVER_URL,
  LicenseStatus,
  TEST_LICENSE_KEY,
  SimpleFetch,
} from '@rocicorp/licensing/src/client';
import {mustSimpleFetch} from './simple-fetch.js';
import {initBgIntervalProcess} from './bg-interval.js';
import {setIntervalWithSignal} from './set-interval-with-signal.js';
import {MutationRecovery} from './mutation-recovery.js';
import type {IndexDefinitions} from './index-defs.js';
import {assertClientDD31} from './persist/clients.js';
import {throwIfClosed} from './transaction-closed-error.js';
import {version} from './version.js';
import {PUSH_VERSION_DD31, PUSH_VERSION_SDD} from './sync/push.js';
import {
  initOnPersistChannel,
  OnPersist,
  PersistInfo,
} from './on-persist-channel.js';
import {ProcessScheduler} from './process-scheduler.js';
import {AbortError} from './abort-error.js';

export type BeginPullResult = {
  requestID: string;
  syncHead: Hash;
  ok: boolean;
};

export type Poke = {
  baseCookie: ReadonlyJSONValue;
  pullResponse: PullResponse;
};

export type PokeSDD = Poke;

export type PokeDD31 = {
  baseCookie: ReadonlyJSONValue;
  pullResponse: PullResponseDD31;
};

export function assertPokeDD31(
  poke: PokeSDD | PokeDD31,
): asserts poke is PokeDD31 {
  assertPullResponseDD31(poke.pullResponse);
}

export function assertPokeSDD(
  poke: PokeSDD | PokeDD31,
): asserts poke is PokeSDD {
  assertPullResponseSDD(poke.pullResponse);
}

export const httpStatusUnauthorized = 401;

export const REPLICACHE_FORMAT_VERSION_SDD = 4;
export const REPLICACHE_FORMAT_VERSION_DD31 = 5;

export const REPLICACHE_FORMAT_VERSION = DD31
  ? REPLICACHE_FORMAT_VERSION_DD31
  : REPLICACHE_FORMAT_VERSION_SDD;

const LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT = 100 * 2 ** 20; // 100 MB

const RECOVER_MUTATIONS_INTERVAL_MS = 5 * 60 * 1000; // 5 mins
const LICENSE_ACTIVE_INTERVAL_MS = 24 * 60 * 60 * 1000; // 24 hours
const TEST_LICENSE_KEY_TTL_MS = 5 * 60 * 1000;

export type MaybePromise<T> = T | Promise<T>;

type ToPromise<P> = P extends Promise<unknown> ? P : Promise<P>;

/**
 * Returns the name of the IDB database that will be used for a particular Replicache instance.
 * @param name The name of the Replicache instance (i.e., the `name` field of `ReplicacheOptions`).
 * @param schemaVersion The schema version of the database (i.e., the `schemaVersion` field of `ReplicacheOptions`).
 * @returns
 */
export function makeIDBName(name: string, schemaVersion?: string): string {
  return makeIDBNameInternal(name, schemaVersion, REPLICACHE_FORMAT_VERSION);
}

function makeIDBNameInternal(
  name: string,
  schemaVersion: string | undefined,
  replicacheFormatVersion: number,
): string {
  const n = `rep:${name}:${replicacheFormatVersion}`;
  return schemaVersion ? `${n}:${schemaVersion}` : n;
}

export {makeIDBNameInternal as makeIDBNameForTesting};

/**
 * The maximum number of time to call out to getAuth before giving up
 * and throwing an error.
 */
const MAX_REAUTH_TRIES = 8;

const PERSIST_IDLE_TIMEOUT_MS = 1000;
const REFRESH_IDLE_TIMEOUT_MS = 1000;

const PERSIST_THROTTLE_MS = 500;
const REFRESH_THROTTLE_MS = 500;

const noop = () => {
  // noop
};

export type MutatorReturn<T extends ReadonlyJSONValue = ReadonlyJSONValue> =
  MaybePromise<T | void>;
/**
 * The type used to describe the mutator definitions passed into [Replicache](classes/Replicache)
 * constructor as part of the {@link ReplicacheOptions}.
 *
 * See {@link ReplicacheOptions} {@link ReplicacheOptions.mutators | mutators} for more
 * info.
 */
export type MutatorDefs = {
  [key: string]: (
    tx: WriteTransaction,
    // Not sure how to not use any here...
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    args?: any,
  ) => MutatorReturn;
};

type MakeMutator<
  F extends (
    tx: WriteTransaction,
    ...args: [] | [ReadonlyJSONValue]
  ) => MutatorReturn,
> = F extends (tx: WriteTransaction, ...args: infer Args) => infer Ret
  ? (...args: Args) => ToPromise<Ret>
  : never;

type MakeMutators<T extends MutatorDefs> = {
  readonly [P in keyof T]: MakeMutator<T[P]>;
};

/**
 * Base options for {@link PullOptions} and {@link PushOptions}
 */
export interface RequestOptions {
  /**
   * When there are pending pull or push requests this is the _minimum_ amount
   * of time to wait until we try another pull/push.
   */
  minDelayMs?: number;

  /**
   * When there are pending pull or push requests this is the _maximum_ amount
   * of time to wait until we try another pull/push.
   */
  maxDelayMs?: number;
}

/**
 * The reason {@link onClientStateNotFound} was called.
 */
export type ClientStateNotFoundReason =
  | {type: 'NotFoundOnServer'}
  | {type: 'NotFoundOnClient'};

const reasonServer = {
  type: 'NotFoundOnServer',
} as const;

const reasonClient = {
  type: 'NotFoundOnClient',
} as const;

export type QueryInternal = <R>(
  body: (tx: ReadTransactionImpl) => MaybePromise<R>,
) => Promise<R>;

export type PendingMutation = {
  readonly name: string;
  readonly id: number;
  readonly args: ReadonlyJSONValue;
};

// eslint-disable-next-line @typescript-eslint/ban-types
export class Replicache<MD extends MutatorDefs = {}> {
  /** The URL to use when doing a pull request. */
  pullURL: string;

  /** The URL to use when doing a push request. */
  pushURL: string;

  /** The authorization token used when doing a push request. */
  auth: string;

  /** The name of the Replicache database. Populated by {@link ReplicacheOptions#name}. */
  readonly name: string;

  private readonly _subscriptions: SubscriptionsManager;
  private readonly _mutationRecovery: MutationRecovery;

  /**
   * This is the name Replicache uses for the IndexedDB database where data is
   * stored.
   */
  get idbName(): string {
    return makeIDBName(this.name, this.schemaVersion);
  }

  /** The schema version of the data understood by this application. */
  readonly schemaVersion: string;

  private get _idbDatabase(): persist.IndexedDBDatabase {
    return {
      name: this.idbName,
      replicacheName: this.name,
      replicacheFormatVersion: REPLICACHE_FORMAT_VERSION,
      schemaVersion: this.schemaVersion,
    };
  }
  private _closed = false;
  private _online = true;
  private readonly _ready: Promise<void>;
  private readonly _profileIDPromise: Promise<string>;
  private readonly _clientIDPromise: Promise<string>;
  private readonly _clientGroupIDPromise: Promise<string | undefined>;
  protected readonly _licenseCheckPromise: Promise<boolean>;

  /* The license is active if we have sent at least one license active ping
   * (and we will continue to). We do not send license active pings when
   * for the TEST_LICENSE_KEY.
   */
  protected _licenseActivePromise: Promise<boolean>;
  private _testLicenseKeyTimeout: ReturnType<typeof setTimeout> | null = null;
  private _root: Promise<Hash | undefined> = Promise.resolve(undefined);
  private readonly _mutatorRegistry: MutatorDefs = {};

  /**
   * The mutators that was registered in the constructor.
   */
  readonly mutate: MakeMutators<MD>;

  // Number of pushes/pulls at the moment.
  private _pushCounter = 0;
  private _pullCounter = 0;

  private _pullConnectionLoop: ConnectionLoop;
  private _pushConnectionLoop: ConnectionLoop;

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

  private readonly _requestOptions: Required<RequestOptions>;

  /**
   * The function to use to pull data from the server.
   */
  puller: Puller;

  /**
   * The function to use to push data to the server.
   */
  pusher: Pusher;

  private readonly _licenseKey: string | undefined;

  private readonly _memdag: dag.LazyStore;
  private readonly _perdag: dag.Store;
  private readonly _idbDatabases: persist.IDBDatabasesStore =
    new persist.IDBDatabasesStore();
  private readonly _lc: LogContext;

  private readonly _closeAbortController = new AbortController();

  private _persistIsRunning = false;
  private readonly _enableScheduledPersist: boolean;
  private readonly _enableRefresh: boolean;
  private _persistScheduler = new ProcessScheduler(
    () => this._persist(),
    PERSIST_IDLE_TIMEOUT_MS,
    PERSIST_THROTTLE_MS,
    this._closeAbortController.signal,
  );
  private readonly _onPersist: OnPersist;
  private _refreshScheduler = new ProcessScheduler(
    () => this._refresh(),
    REFRESH_IDLE_TIMEOUT_MS,
    REFRESH_THROTTLE_MS,
    this._closeAbortController.signal,
  );

  private readonly _enableLicensing: boolean;

  /**
   * The options used to control the {@link pull} and push request behavior. This
   * object is live so changes to it will affect the next pull or push call.
   */
  get requestOptions(): Required<RequestOptions> {
    return this._requestOptions;
  }

  /**
   * `onSync` is called when a sync begins, and again when the sync ends. The parameter `syncing`
   * is set to `true` when `onSync` is called at the beginning of a sync, and `false` when it
   * is called at the end of a sync.
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
   * `onClientStateNotFound` is called when the persistent client has been
   * garbage collected. This can happen if the client has not been used for over
   * a week.
   *
   * It can also happen if the server no longer knows about this client.
   *
   * The default behavior is to reload the page (using `location.reload()`). Set
   * this to `null` or provide your own function to prevent the page from
   * reloading automatically.
   */
  onClientStateNotFound: ((reason: ClientStateNotFoundReason) => void) | null =
    reload;

  /**
   * This gets called when we get an HTTP unauthorized (401) response from the
   * push or pull endpoint. Set this to a function that will ask your user to
   * reauthenticate.
   */
  getAuth: (() => MaybePromise<string | null | undefined>) | null | undefined =
    null;

  constructor(options: ReplicacheOptions<MD>) {
    const {
      name,
      logLevel = 'info',
      logSinks = [consoleLogSink],
      pullURL = '',
      auth,
      pushDelay = 10,
      pushURL = '',
      schemaVersion = '',
      pullInterval = 60_000,
      mutators = {} as MD,
      requestOptions = {},
      puller = defaultPuller,
      pusher = defaultPusher,
      licenseKey,
      experimentalKVStore,
      indexes = {},
    } = options;
    this.auth = auth ?? '';
    this.pullURL = pullURL;
    this.pushURL = pushURL;
    if (name === undefined || name === '') {
      throw new Error('name is required and must be non-empty');
    }
    this.name = name;
    this.schemaVersion = schemaVersion;
    this.pullInterval = pullInterval;
    this.pushDelay = pushDelay;
    this.puller = puller;
    this.pusher = pusher;

    const internalOptions = options as ReplicacheInternalOptions;
    const {
      enableLicensing = true,
      enableMutationRecovery = true,
      enableScheduledPersist = true,
      enableRefresh = true,
    } = internalOptions;
    this._enableLicensing = enableLicensing;
    this._enableScheduledPersist = enableScheduledPersist;
    this._enableRefresh = enableRefresh;

    if (internalOptions.exposeInternalAPI) {
      internalOptions.exposeInternalAPI({
        persist: () => this._persist(),
      });
    }

    const logSink =
      logSinks.length === 1 ? logSinks[0] : new TeeLogSink(logSinks);
    this._lc = new LogContext(logLevel, logSink).addContext('name', name);
    this._lc.debug?.('Constructing Replicache', {
      name,
      'replicache version': version,
    });

    this._subscriptions = new SubscriptionsManager(
      this._queryInternal,
      this._lc,
    );
    const perKvStore = experimentalKVStore || new IDBStore(this.idbName);
    this._perdag = new dag.StoreImpl(
      perKvStore,
      dag.uuidChunkHasher,
      assertHash,
    );
    this._memdag = new dag.LazyStore(
      this._perdag,
      LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT,
      dag.uuidChunkHasher,
      assertHash,
    );

    // Use a promise-resolve pair so that we have a promise to use even before
    // we call the Open RPC.
    const readyResolver = resolver<void>();
    this._ready = readyResolver.promise;

    this._licenseKey = licenseKey;
    const licenseCheckResolver = resolver<boolean>();
    this._licenseCheckPromise = licenseCheckResolver.promise;
    const licenseActiveResolver = resolver<boolean>();
    this._licenseActivePromise = licenseActiveResolver.promise;

    const {minDelayMs = MIN_DELAY_MS, maxDelayMs = MAX_DELAY_MS} =
      requestOptions;
    this._requestOptions = {maxDelayMs, minDelayMs};

    this._pullConnectionLoop = new ConnectionLoop(
      new PullDelegate(
        this,
        () => this._invokePull(),
        this._lc.addContext('PULL'),
      ),
    );

    this._pushConnectionLoop = new ConnectionLoop(
      new PushDelegate(
        this,
        () => this._invokePush(),
        this._lc.addContext('PUSH'),
      ),
    );

    this.mutate = this._registerMutators(mutators);

    const profileIDResolver = resolver<string>();
    this._profileIDPromise = profileIDResolver.promise;
    const clientGroupIDResolver = resolver<string | undefined>();
    this._clientGroupIDPromise = clientGroupIDResolver.promise;
    const clientIDResolver = resolver<string>();
    this._clientIDPromise = clientIDResolver.promise;

    this._mutationRecovery = new MutationRecovery({
      delegate: this,
      lc: this._lc,
      enableMutationRecovery,
      wrapInOnlineCheck: this._wrapInOnlineCheck.bind(this),
      wrapInReauthRetries: this._wrapInReauthRetries.bind(this),
      isPullDisabled: this._isPullDisabled.bind(this),
      isPushDisabled: this._isPushDisabled.bind(this),
      clientGroupIDPromise: this._clientGroupIDPromise,
    });

    this._onPersist = DD31
      ? initOnPersistChannel(
          this.name,
          this._closeAbortController.signal,
          persistInfo => {
            void this._handlePersist(persistInfo);
          },
        )
      : (_: PersistInfo) => undefined;

    void this._open(
      indexes,
      profileIDResolver.resolve,
      clientGroupIDResolver.resolve,
      clientIDResolver.resolve,
      readyResolver.resolve,
      licenseCheckResolver.resolve,
      licenseActiveResolver.resolve,
    );
  }

  private async _open(
    indexes: IndexDefinitions,
    profileIDResolver: (profileID: string) => void,
    resolveClientGroupID: (
      clientGroupID: sync.ClientGroupID | undefined,
    ) => void,
    resolveClientID: (clientID: sync.ClientID) => void,
    resolveReady: () => void,
    resolveLicenseCheck: (valid: boolean) => void,
    resolveLicenseActive: (active: boolean) => void,
  ): Promise<void> {
    // If we are currently closing a Replicache instance with the same name,
    // wait for it to finish closing.
    await closingInstances.get(this.name);
    await this._idbDatabases.getProfileID().then(profileIDResolver);
    await this._idbDatabases.putDatabase(this._idbDatabase);
    const [clientID, client, clients] = await persist.initClient(
      this._lc,
      this._perdag,
      Object.keys(this._mutatorRegistry),
      indexes,
    );
    if (DD31) {
      assertClientDD31(client);
      resolveClientGroupID(client.clientGroupID);
    } else {
      resolveClientGroupID(undefined);
    }
    resolveClientID(clientID);
    await this._memdag.withWrite(async write => {
      await write.setHead(db.DEFAULT_HEAD_NAME, client.headHash);
      await write.commit();
    });

    if (!DD31) {
      // DD31 syncs the indexes as part of initClient
      await this._syncIndexes(indexes);
    }

    // Now we have a profileID, a clientID, a clientGroupID and DB!
    resolveReady();

    this._root = this._getRoot();
    await this._root;

    await this._licenseCheck(resolveLicenseCheck);

    this.pull();
    this._push();

    const {signal} = this._closeAbortController;

    persist.startHeartbeats(
      clientID,
      this._perdag,
      () => {
        this._fireOnClientStateNotFound(clientID, reasonClient);
      },
      this._lc,
      signal,
    );
    persist.initClientGC(clientID, this._perdag, this._lc, signal);
    persist.initCollectIDBDatabases(this._idbDatabases, this._lc, signal);
    if (DD31) {
      persist.initClientGroupGC(this._perdag, this._lc, signal);
    }

    setIntervalWithSignal(
      () => this._recoverMutations(),
      RECOVER_MUTATIONS_INTERVAL_MS,
      signal,
    );
    void this._recoverMutations(clients);

    getDocument()?.addEventListener(
      'visibilitychange',
      this._onVisibilityChange,
    );

    await this._startLicenseActive(resolveLicenseActive, this._lc, signal);
  }
  private async _syncIndexes(indexes: IndexDefinitions): Promise<void> {
    assert(!DD31);
    // Do not wait for _ready here since this needs to be done before we
    // consider the database ready.
    await this._indexOp(tx => tx.syncIndexes(indexes));
  }

  private _onVisibilityChange = async () => {
    if (this._closed) {
      return;
    }

    // In case of running in a worker, we don't have a document.
    if (getDocument()?.visibilityState !== 'visible') {
      return;
    }

    await this._checkForClientStateNotFoundAndCallHandler();
  };

  private async _checkForClientStateNotFoundAndCallHandler(): Promise<boolean> {
    const clientID = await this._clientIDPromise;
    const hasClientState = await this._perdag.withRead(read =>
      persist.hasClientState(clientID, read),
    );
    if (!hasClientState) {
      this._fireOnClientStateNotFound(clientID, reasonClient);
    }
    return !hasClientState;
  }

  private async _licenseCheck(
    resolveLicenseCheck: (valid: boolean) => void,
  ): Promise<void> {
    if (!this._enableLicensing) {
      resolveLicenseCheck(true);
      return;
    }
    if (!this._licenseKey) {
      await this._licenseInvalid(
        this._lc,
        `license key ReplicacheOptions.licenseKey is not set`,
        true /* disable replicache */,
        resolveLicenseCheck,
      );
      return;
    }
    this._lc.debug?.(`Replicache license key: ${this._licenseKey}`);
    if (this._licenseKey === TEST_LICENSE_KEY) {
      this._lc.info?.(
        `Skipping license check for TEST_LICENSE_KEY. ` +
          `You may ONLY use this key for automated (e.g., unit/CI) testing. ` +
          // TODO(phritz) maybe use a more specific URL
          `See https://replicache.dev for more information.`,
      );
      resolveLicenseCheck(true);

      this._testLicenseKeyTimeout = setTimeout(async () => {
        await this._licenseInvalid(
          this._lc,
          'Test key expired',
          true,
          resolveLicenseCheck,
        );
      }, TEST_LICENSE_KEY_TTL_MS);

      return;
    }
    try {
      const resp = await getLicenseStatus(
        // TODO(arv): Fix @rocicorp/license
        mustSimpleFetch as SimpleFetch,
        PROD_LICENSE_SERVER_URL,
        this._licenseKey,
        this._lc,
      );
      if (resp.pleaseUpdate) {
        this._lc.error?.(
          `You are using an old version of Replicache that uses deprecated licensing features. ` +
            `Please update Replicache else it may stop working.`,
        );
      }
      if (resp.status === LicenseStatus.Valid) {
        this._lc.debug?.(`License is valid.`);
      } else {
        await this._licenseInvalid(
          this._lc,
          `status: ${resp.status}`,
          resp.disable,
          resolveLicenseCheck,
        );
        return;
      }
    } catch (err) {
      this._lc.error?.(`Error checking license: ${err}`);
      // Note: on error we fall through to assuming the license is valid.
    }
    resolveLicenseCheck(true);
  }

  private async _licenseInvalid(
    lc: LogContext,
    reason: string,
    disable: boolean,
    resolveLicenseCheck: (valid: boolean) => void,
  ): Promise<void> {
    lc.error?.(
      `** REPLICACHE LICENSE NOT VALID ** Replicache license key '${this._licenseKey}' is not valid (${reason}). ` +
        `Please run 'npx replicache get-license' to get a license key or contact hello@replicache.dev for help.`,
    );
    if (disable) {
      await this.close();
      lc.error?.(`** REPLICACHE DISABLED **`);
    }
    resolveLicenseCheck(false);
    return;
  }

  private async _startLicenseActive(
    resolveLicenseActive: (valid: boolean) => void,
    lc: LogContext,
    signal: AbortSignal,
  ): Promise<void> {
    if (
      !this._enableLicensing ||
      !this._licenseKey ||
      this._licenseKey === TEST_LICENSE_KEY
    ) {
      resolveLicenseActive(false);
      return;
    }

    const markActive = async () => {
      try {
        await licenseActive(
          // TODO(arv): Update @rocicorp/licensing
          mustSimpleFetch as SimpleFetch,
          PROD_LICENSE_SERVER_URL,
          this._licenseKey as string,
          await this.profileID,
          lc,
        );
      } catch (err) {
        this._lc.info?.(`Error sending license active ping: ${err}`);
      }
    };
    await markActive();
    resolveLicenseActive(true);

    initBgIntervalProcess(
      'LicenseActive',
      markActive,
      () => LICENSE_ACTIVE_INTERVAL_MS,
      lc,
      signal,
    );
  }

  /**
   * The browser profile ID for this browser profile. Every instance of Replicache
   * browser-profile-wide shares the same profile ID.
   */
  get profileID(): Promise<string> {
    return this._profileIDPromise;
  }

  /**
   * The client ID for this instance of Replicache. Each instance of Replicache
   * gets a unique client ID.
   */
  get clientID(): Promise<string> {
    return this._clientIDPromise;
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
    return this._online;
  }

  /**
   * Whether the Replicache database has been closed. Once Replicache has been
   * closed it no longer syncs and you can no longer read or write data out of
   * it. After it has been closed it is pretty much useless and should not be
   * used any more.
   */
  get closed(): boolean {
    return this._closed;
  }

  /**
   * Closes this Replicache instance.
   *
   * When closed all subscriptions end and no more read or writes are allowed.
   */
  async close(): Promise<void> {
    this._closed = true;
    const {promise, resolve} = resolver();
    closingInstances.set(this.name, promise);

    this._closeAbortController.abort();

    getDocument()?.removeEventListener(
      'visibilitychange',
      this._onVisibilityChange,
    );

    await this._ready;
    const closingPromises = [
      this._memdag.close(),
      this._perdag.close(),
      this._idbDatabases.close(),
    ];

    this._pullConnectionLoop.close();
    this._pushConnectionLoop.close();

    this._subscriptions.clear();

    if (this._testLicenseKeyTimeout) {
      clearTimeout(this._testLicenseKeyTimeout);
    }

    await Promise.all(closingPromises);
    closingInstances.delete(this.name);
    resolve();
  }

  private async _getRoot(): Promise<Hash | undefined> {
    if (this._closed) {
      return undefined;
    }
    await this._ready;
    return await db.getRoot(this._memdag, db.DEFAULT_HEAD_NAME);
  }

  private async _checkChange(
    root: Hash | undefined,
    diffs: sync.DiffsMap,
  ): Promise<void> {
    const currentRoot = await this._root; // instantaneous except maybe first time
    if (root !== undefined && root !== currentRoot) {
      this._root = Promise.resolve(root);
      await this._subscriptions.fire(diffs);
    }
  }

  /**
   * Creates a persistent secondary index in Replicache which can be used with scan.
   *
   * If the named index already exists with the same definition this returns success
   * immediately. If the named index already exists, but with a different definition
   * an error is thrown.
   * @deprecated Use {@link ReplicacheOptions.indexes} instead.
   */
  async createIndex(def: CreateIndexDefinition): Promise<void> {
    await this._ready;
    await this._indexOp(tx => tx.createIndex(def));
  }

  /**
   * Drops an index previously created with {@link createIndex}.
   * @deprecated Use {@link ReplicacheOptions.indexes} instead.
   */
  async dropIndex(name: string): Promise<void> {
    await this._ready;
    await this._indexOp(tx => tx.dropIndex(name));
  }

  private async _indexOp(
    f: (tx: IndexTransactionImpl) => Promise<void>,
  ): Promise<void> {
    // TODO(arv): Remove generateDiffs when the deprecated callers have been removed.
    const clientID = await this._clientIDPromise;
    await this._memdag.withWrite(async dagWrite => {
      const dbWrite = await db.newWriteIndexChange(
        db.whenceHead(db.DEFAULT_HEAD_NAME),
        dagWrite,
        clientID,
      );
      const tx = new IndexTransactionImpl(clientID, dbWrite, this._lc);
      await f(tx);
      throwIfClosed(dbWrite);
      const [ref, diffs] = await dbWrite.commitWithDiffs(
        db.DEFAULT_HEAD_NAME,
        this._subscriptions,
      );
      // Changing an index should not affect the primary map.
      assert(!diffs.has(''));
      await this._checkChange(ref, diffs);
    });
  }

  protected async _maybeEndPull(
    syncHead: Hash,
    requestID: string,
  ): Promise<void> {
    for (;;) {
      if (this._closed) {
        return;
      }

      await this._ready;
      const clientID = await this._clientIDPromise;
      const lc = this._lc
        .addContext('maybeEndPull')
        .addContext('request_id', requestID);
      const {replayMutations, diffs} = await sync.maybeEndPull<db.LocalMeta>(
        this._memdag,
        lc,
        syncHead,
        clientID,
        this._subscriptions,
      );

      if (!replayMutations || replayMutations.length === 0) {
        // All done.
        await this._checkChange(syncHead, diffs);
        void this._schedulePersist();
        return;
      }

      // Replay.
      for (const mutation of replayMutations) {
        // TODO(greg): I'm not sure why this was in Replicache#_mutate...
        // Ensure that we run initial pending subscribe functions before starting a
        // write transaction.
        if (this._subscriptions.hasPendingSubscriptionRuns) {
          await Promise.resolve();
        }
        syncHead = await this._memdag.withWrite(dagWrite =>
          db.rebaseMutationAndCommit(
            mutation,
            dagWrite,
            syncHead,
            sync.SYNC_HEAD_NAME,
            this._mutatorRegistry,
            lc,
            clientID,
          ),
        );
      }
    }
  }

  private _invokePull(): Promise<boolean> {
    if (this._isPullDisabled()) {
      return Promise.resolve(true);
    }

    return this._wrapInOnlineCheck(async () => {
      try {
        this._changeSyncCounters(0, 1);
        const {syncHead, requestID, ok} = await this._beginPull();
        if (!ok) {
          return false;
        }
        if (syncHead !== emptyHash) {
          await this._maybeEndPull(syncHead, requestID);
        }
      } catch (e) {
        throw await this._convertToClientStateNotFoundError(e);
      } finally {
        this._changeSyncCounters(0, -1);
      }
      return true;
    }, 'Pull');
  }

  private _isPullDisabled() {
    return this.pullURL === '' && this.puller === defaultPuller;
  }

  private async _wrapInOnlineCheck(
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
        this._lc.info?.(`${name} threw:\n`, e, '\nwith cause:\n', e.causedBy);
      } else {
        this._lc.info?.(`${name} threw:\n`, e);
      }
      return false;
    } finally {
      if (this._online !== online) {
        this._online = online;
        this.onOnlineChange?.(online);
        if (online) {
          void this._recoverMutations();
        }
      }
    }
  }

  private async _wrapInReauthRetries<R>(
    f: (
      requestID: string,
      requestLc: LogContext,
    ) => Promise<{
      httpRequestInfo: HTTPRequestInfo | undefined;
      result: R;
    }>,
    verb: string,
    serverURL: string,
    lc: LogContext,
    preAuth: () => MaybePromise<void> = noop,
    postAuth: () => MaybePromise<void> = noop,
  ): Promise<{
    result: R;
    authFailure: boolean;
  }> {
    const clientID = await this.clientID;
    let reauthAttempts = 0;
    let lastResult;
    lc = lc.addContext(verb);
    do {
      const requestID = sync.newRequestID(clientID);
      const requestLc = lc.addContext('request_id', requestID);
      const {httpRequestInfo, result} = await f(requestID, requestLc);
      lastResult = result;
      if (!httpRequestInfo) {
        return {
          result,
          authFailure: false,
        };
      }
      const {errorMessage, httpStatusCode} = httpRequestInfo;

      if (errorMessage || httpStatusCode >= 400) {
        // TODO(arv): Maybe we should not log the server URL when the error comes
        // from a Pusher/Puller?
        requestLc.error?.(
          `Got error response from server (${serverURL}) doing ${verb}: ${httpStatusCode}` +
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

  private _isPushDisabled() {
    return this.pushURL === '' && this.pusher === defaultPusher;
  }

  protected async _invokePush(): Promise<boolean> {
    if (this._isPushDisabled()) {
      return true;
    }

    await this._ready;
    const profileID = await this._profileIDPromise;
    const clientID = await this._clientIDPromise;
    const clientGroupID = await this._clientGroupIDPromise;
    return this._wrapInOnlineCheck(async () => {
      const {result: pushResponse} = await this._wrapInReauthRetries(
        async (requestID: string, requestLc: LogContext) => {
          try {
            this._changeSyncCounters(1, 0);
            const pushResponse = await sync.push(
              requestID,
              this._memdag,
              requestLc,
              profileID,
              clientGroupID,
              clientID,
              this.pusher,
              this.pushURL,
              this.auth,
              this.schemaVersion,
              DD31 ? PUSH_VERSION_DD31 : PUSH_VERSION_SDD,
            );
            return {result: pushResponse, httpRequestInfo: pushResponse};
          } finally {
            this._changeSyncCounters(-1, 0);
          }
        },
        'push',
        this.pushURL,
        this._lc,
      );
      // No pushResponse means we didn't do a push because there were no
      // pending mutations.
      return pushResponse === undefined || pushResponse.httpStatusCode === 200;
    }, 'Push');
  }

  /**
   * Push pushes pending changes to the {@link pushURL}.
   *
   * You do not usually need to manually call push. If {@link pushDelay} is non-zero
   * (which it is by default) pushes happen automatically shortly after
   * mutations.
   */
  private _push(): void {
    this._pushConnectionLoop.send();
  }

  /**
   * Pull pulls changes from the {@link pullURL}. If there are any changes
   * local changes will get replayed on top of the new server state.
   */
  pull(): void {
    this._pullConnectionLoop.send();
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
  async poke(poke: Poke): Promise<void> {
    // TODO(DD31): We keep the type of poke for now and cast to allow main to not
    // introduce API changes until DD31 is on by default.
    // poke is really PokeSDD | PokeDD31
    const internalPoke = poke as PokeDD31 | PokeSDD;

    await this._ready;
    // TODO(MP) Previously we created a request ID here and included it with the
    // PullRequest to the server so we could tie events across client and server
    // together. Since the direction is now reversed, creating and adding a request ID
    // here is kind of silly. We should consider creating the request ID
    // on the *server* and passing it down in the poke for inclusion here in the log
    // context.
    const clientID = await this._clientIDPromise;
    const requestID = sync.newRequestID(clientID);
    const lc = this._lc
      .addContext('handlePullResponse')
      .addContext('request_id', requestID);

    if (isClientStateNotFoundResponse(internalPoke.pullResponse)) {
      this._fireOnClientStateNotFound(clientID, reasonServer);
      return;
    }

    const syncHead = await sync.handlePullResponse(
      lc,
      this._memdag,
      deepFreeze(internalPoke.baseCookie),
      internalPoke.pullResponse,
      clientID,
    );

    if (syncHead === null) {
      throw new Error(
        'unexpected base cookie for poke: ' + JSON.stringify(internalPoke),
      );
    }

    await this._maybeEndPull(syncHead, requestID);
  }

  protected async _beginPull(): Promise<BeginPullResult> {
    await this._ready;
    const profileID = await this.profileID;
    const clientID = await this._clientIDPromise;
    const clientGroupID = await this._clientGroupIDPromise;
    const {
      result: {beginPullResponse, requestID},
    } = await this._wrapInReauthRetries(
      async (requestID: string, requestLc: LogContext) => {
        const req = {
          pullAuth: this.auth,
          pullURL: this.pullURL,
          schemaVersion: this.schemaVersion,
        };
        const beginPullResponse = await sync.beginPull(
          profileID,
          clientID,
          clientGroupID,
          req,
          this.puller,
          requestID,
          this._memdag,
          requestLc,
        );
        return {
          result: {beginPullResponse, requestID},
          httpRequestInfo: beginPullResponse.httpRequestInfo,
        };
      },
      'pull',
      this.pullURL,
      this._lc,
      () => this._changeSyncCounters(0, -1),
      () => this._changeSyncCounters(0, 1),
    );

    if (isClientStateNotFoundResponse(beginPullResponse.pullResponse)) {
      const clientID = await this._clientIDPromise;
      this._fireOnClientStateNotFound(clientID, reasonServer);
    }

    const {syncHead, httpRequestInfo} = beginPullResponse;
    return {requestID, syncHead, ok: httpRequestInfo.httpStatusCode === 200};
  }

  private async _persist(): Promise<void> {
    assert(!this._persistIsRunning);
    this._persistIsRunning = true;
    try {
      const clientID = await this.clientID;
      await this._ready;
      if (this._closed) {
        return;
      }
      try {
        await persist.persist(
          this._lc,
          clientID,
          this._memdag,
          this._perdag,
          this._mutatorRegistry,
          () => this.closed,
        );
      } catch (e) {
        if (e instanceof persist.ClientStateNotFoundError) {
          this._fireOnClientStateNotFound(clientID, reasonClient);
        } else if (this._closed) {
          this._lc.debug?.('Exception persisting during close', e);
        } else {
          throw e;
        }
      }
    } finally {
      this._persistIsRunning = false;
    }
    if (DD31) {
      const clientID = await this.clientID;
      const clientGroupID = await this._clientGroupIDPromise;
      assert(clientGroupID);
      this._onPersist({clientID, clientGroupID});
    }
  }

  private async _refresh(): Promise<void> {
    if (DD31 && this._enableRefresh) {
      await this._ready;
      const clientID = await this.clientID;
      if (this._closed) {
        return;
      }
      let result;
      try {
        result = await persist.refresh(
          this._lc,
          this._memdag,
          this._perdag,
          clientID,
          this._mutatorRegistry,
          this._subscriptions,
          () => this.closed,
        );
      } catch (e) {
        if (e instanceof persist.ClientStateNotFoundError) {
          this._fireOnClientStateNotFound(clientID, reasonClient);
        } else if (this._closed) {
          this._lc.debug?.('Exception refreshing during close', e);
        } else {
          throw e;
        }
      }
      if (result !== undefined) {
        await this._checkChange(result[0], result[1]);
      }
    }
  }

  private _fireOnClientStateNotFound(
    clientID: sync.ClientID,
    reason: ClientStateNotFoundReason,
  ) {
    this._lc.error?.(`Client state not found, clientID: ${clientID}`);
    this.onClientStateNotFound?.(reason);
  }

  private async _schedulePersist(): Promise<void> {
    if (!this._enableScheduledPersist) {
      return;
    }
    await this._schedule('persist', this._persistScheduler);
  }

  private async _handlePersist(persistInfo: PersistInfo): Promise<void> {
    this._lc.debug?.('Handling persist', persistInfo);
    const clientGroupID = await this._clientGroupIDPromise;
    if (persistInfo.clientGroupID === clientGroupID) {
      void this._scheduleRefresh();
    }
  }

  private async _scheduleRefresh(): Promise<void> {
    await this._schedule('refresh from storage', this._refreshScheduler);
  }

  private async _schedule(
    name: string,
    scheduler: ProcessScheduler,
  ): Promise<void> {
    try {
      await scheduler.schedule();
    } catch (e) {
      if (e instanceof AbortError) {
        this._lc.debug?.(`Scheduled ${name} did not complete before close.`);
      } else {
        this._lc.error?.(`Error during ${name}`, e);
      }
    }
  }

  private _changeSyncCounters(pushDelta: 0, pullDelta: 1 | -1): void;
  private _changeSyncCounters(pushDelta: 1 | -1, pullDelta: 0): void;
  private _changeSyncCounters(pushDelta: number, pullDelta: number): void {
    this._pushCounter += pushDelta;
    this._pullCounter += pullDelta;
    const delta = pushDelta + pullDelta;
    const counter = this._pushCounter + this._pullCounter;
    if ((delta === 1 && counter === 1) || counter === 0) {
      const syncing = counter > 0;
      // Run in a new microtask.
      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      Promise.resolve().then(() => this.onSync?.(syncing));
    }
  }

  /**
   * Subscribe to the result of a {@link query}. The `body` function is
   * evaluated once and its results are returned via `onData`.
   *
   * Thereafter, each time the the result of `body` changes, `onData`
   * is fired again with the new result.
   *
   * `subscribe()` goes to significant effort to avoid extraneous work
   * re-evaluating subscriptions:
   *
   * 1. subscribe tracks the keys that `body` accesses each time it
   *    runs. `body` is only re-evaluated when those keys change.
   * 2. subscribe only re-fires `onData` in the case that a result
   *    changes by way of `deepEquals`.
   *
   * Because of (1), `body` must be a pure function of the data in
   * Replicache. `body` must not access anything other than the `tx`
   * parameter passed to it.
   *
   * Although subscribe is as efficient as it can be, it is somewhat
   * constrained by the goal of returning an arbitrary computation of the
   * cache. For even better performance (but worse dx), see
   * {@link experimentalWatch}.
   *
   * If an error occurs in the `body` the `onError` function is called if
   * present. Otherwise, the error is thrown.
   *
   * To cancel the subscription, call the returned function.
   */
  subscribe<R extends ReadonlyJSONValue | undefined>(
    body: (tx: ReadTransaction) => Promise<R>,
    options: SubscribeOptions<R>,
  ): () => void {
    return this._subscriptions.addSubscription(body, options);
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
    return this._subscriptions.addWatch(callback as WatchCallback, options);
  }

  /**
   * Query is used for read transactions. It is recommended to use transactions
   * to ensure you get a consistent view across multiple calls to `get`, `has`
   * and `scan`.
   */
  query<R>(body: (tx: ReadTransaction) => Promise<R> | R): Promise<R> {
    return this._queryInternal(body);
  }

  private _queryInternal: QueryInternal = async body => {
    await this._ready;
    const clientID = await this._clientIDPromise;
    return this._memdag.withRead(async dagRead => {
      const dbRead = await db.readFromDefaultHead(dagRead);
      const tx = new ReadTransactionImpl(clientID, dbRead, this._lc);
      try {
        return await body(tx);
      } catch (ex) {
        throw await this._convertToClientStateNotFoundError(ex);
      }
    });
  };

  private _register<
    Return extends ReadonlyJSONValue | void,
    Args extends JSONValue,
  >(
    name: string,
    mutatorImpl: (tx: WriteTransaction, args?: Args) => MaybePromise<Return>,
  ): (args?: Args) => Promise<Return> {
    this._mutatorRegistry[name] = mutatorImpl as (
      tx: WriteTransaction,
      args: JSONValue | undefined,
    ) => Promise<void | JSONValue>;

    return async (args?: Args): Promise<Return> =>
      (await this._mutate(name, mutatorImpl, args, performance.now())).result;
  }

  private _registerMutators<
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
      rv[k] = this._register(k, regs[k]) as MakeMutator<M[typeof k]>;
    }
    return rv as Mut;
  }

  private async _mutate<
    R extends ReadonlyJSONValue | void,
    A extends ReadonlyJSONValue,
  >(
    name: string,
    mutatorImpl: (tx: WriteTransaction, args?: A) => MaybePromise<R>,
    args: A | undefined,
    timestamp: number,
  ): Promise<{result: R; ref: Hash}> {
    const frozenArgs = deepFreeze(args ?? null);

    // Ensure that we run initial pending subscribe functions before starting a
    // write transaction.
    if (this._subscriptions.hasPendingSubscriptionRuns) {
      await Promise.resolve();
    }

    await this._ready;
    const clientID = await this._clientIDPromise;
    return await this._memdag.withWrite(async dagWrite => {
      const whence: db.Whence = db.whenceHead(db.DEFAULT_HEAD_NAME);
      const originalHash = null;

      const dbWrite = await db.newWriteLocal(
        whence,
        name,
        frozenArgs,
        originalHash,
        dagWrite,
        timestamp,
        clientID,
        DD31,
      );

      const tx = new WriteTransactionImpl(clientID, dbWrite, this._lc);
      try {
        const result: R = await mutatorImpl(tx, args);
        throwIfClosed(dbWrite);
        const [ref, diffs] = await dbWrite.commitWithDiffs(
          db.DEFAULT_HEAD_NAME,
          this._subscriptions,
        );
        this._pushConnectionLoop.send();
        await this._checkChange(ref, diffs);
        void this._schedulePersist();
        return {result, ref};
      } catch (ex) {
        throw await this._convertToClientStateNotFoundError(ex);
      }
    });
  }

  /**
   * In the case we get a ChunkNotFoundError we check if the client got garbage
   * collected and if so change the error to a ClientStateNotFoundError instead
   */
  private async _convertToClientStateNotFoundError(
    ex: unknown,
  ): Promise<unknown> {
    if (
      ex instanceof dag.ChunkNotFoundError &&
      (await this._checkForClientStateNotFoundAndCallHandler())
    ) {
      return new persist.ClientStateNotFoundError(await this._clientIDPromise);
    }

    return ex;
  }

  protected _recoverMutations(
    preReadClientMap?: persist.ClientMap,
  ): Promise<boolean> {
    return this._mutationRecovery.recoverMutations(
      preReadClientMap,
      this._ready,
      this._perdag,
      this._idbDatabase,
      this._idbDatabases,
    );
  }

  /**
   * List of pending mutations.
   *
   * Gives a list of local mutations that have
   * mutationID > syncHead.mutationID that exists on the main client group.
   *
   * @experimental This method is experimental and may change in the future.
   */
  experimentalPendingMutations(): Promise<readonly PendingMutation[]> {
    return this._memdag.withRead(async dagRead => {
      const mainHeadHash = await dagRead.getHead(db.DEFAULT_HEAD_NAME);
      if (mainHeadHash === undefined) {
        throw new Error('Missing main head');
      }
      // TODO(arv, DD31): Should we rename localMutations?
      const pending = await db.localMutations(mainHeadHash, dagRead);
      const clientID = await this._clientIDPromise;
      return Promise.all(
        pending.map(async p => {
          return {
            id: await p.getMutationID(clientID, dagRead),
            name: p.meta.mutatorName,
            args: p.meta.mutatorArgsJSON,
          };
        }),
      );
    });
  }
}

// This map is used to keep track of closing instances of Replicache. When an
// instance is opening we wait for any currently closing instances.
const closingInstances: Map<string, Promise<unknown>> = new Map();

/**
 * Returns the document object. This is wrapped in a function because Replicache
 * runs in environments that do not have a document (such as Web Workers, Deno
 * etc)
 */
function getDocument(): Document | undefined {
  return typeof document !== 'undefined' ? document : undefined;
}

function reload(): void {
  if (typeof location !== 'undefined') {
    location.reload();
  }
}
