import {consoleLogSink, LogContext, TeeLogSink} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {Lock} from '@rocicorp/lock';
import type {ReadonlyJSONValue} from './json';
import type {JSONValue} from './json';
import {Pusher, PushError} from './pusher';
import {
  defaultPullerDD31,
  isClientStateNotFoundResponse,
  Puller,
  PullError,
  PullResponse,
} from './puller';
import {
  IndexTransactionImpl,
  ReadTransactionImpl,
  WriteTransactionImpl,
} from './transactions';
import type {
  CreateIndexDefinition,
  ReadTransaction,
  WriteTransaction,
} from './transactions';
import {ConnectionLoop, MAX_DELAY_MS, MIN_DELAY_MS} from './connection-loop';
import {defaultPuller} from './puller';
import {defaultPusher} from './pusher';
import type {
  ReplicacheInternalOptions,
  ReplicacheOptions,
} from './replicache-options';
import {PullDelegate, PushDelegate} from './connection-loop-delegates';
import {
  WatchNoIndexCallback,
  SubscribeOptions,
  SubscriptionsManager,
  WatchOptions,
  WatchCallbackForOptions,
  WatchCallback,
} from './subscriptions';
import {IDBStore} from './kv/mod';
import * as dag from './dag/mod';
import * as db from './db/mod';
import * as sync from './sync/mod';
import {
  assertHash,
  assertNotTempHash,
  emptyHash,
  Hash,
  hashOf,
  newTempHash,
} from './hash';
import * as persist from './persist/mod';
import {requestIdle} from './request-idle';
import type {HTTPRequestInfo} from './http-request-info';
import {assert} from './asserts';
import {
  getLicenseStatus,
  licenseActive,
  PROD_LICENSE_SERVER_URL,
  LicenseStatus,
  TEST_LICENSE_KEY,
} from '@rocicorp/licensing/src/client';
import {mustSimpleFetch} from './simple-fetch';
import {initBgIntervalProcess} from './persist/bg-interval';
import {setIntervalWithSignal} from './set-interval-with-signal';
import {MutationRecovery} from './mutation-recovery';
import {
  fromInternalValue,
  FromInternalValueReason,
  toInternalValue,
  ToInternalValueReason,
} from './internal-value.js';
import {rebaseMutation} from './sync/rebase';

export type BeginPullResult = {
  requestID: string;
  syncHead: Hash;
  ok: boolean;
};

export type Poke = {
  baseCookie: ReadonlyJSONValue;
  pullResponse: PullResponse;
};

export const httpStatusUnauthorized = 401;

export const REPLICACHE_FORMAT_VERSION = 4;
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
  const n = `rep:${name}:${REPLICACHE_FORMAT_VERSION}`;
  return schemaVersion ? `${n}:${schemaVersion}` : n;
}

/**
 * The maximum number of time to call out to getAuth before giving up
 * and throwing an error.
 */
const MAX_REAUTH_TRIES = 8;

const PERSIST_TIMEOUT = 1000;

const noop = () => {
  // noop
};

export type MutatorReturn = MaybePromise<JSONValue | void>;
/**
 * The type used to describe the mutator definitions passed into [Replicache](classes/Replicache)
 * constructor as part of the [[ReplicacheOptions]].
 *
 * See [[ReplicacheOptions]] [[ReplicacheOptions.mutators|mutators]] for more
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
  F extends (tx: WriteTransaction, ...args: [] | [JSONValue]) => MutatorReturn,
> = F extends (tx: WriteTransaction, ...args: infer Args) => infer Ret
  ? (...args: Args) => ToPromise<Ret>
  : never;

type MakeMutators<T extends MutatorDefs> = {
  readonly [P in keyof T]: MakeMutator<T[P]>;
};

/**
 * Base options for [[PullOptions]] and [[PushOptions]]
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
 * The reason [[onClientStateNotFound]] was called.
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

  /** The name of the Replicache database. */
  readonly name: string;

  private readonly _subscriptions: SubscriptionsManager;
  private readonly _mutationRecovery: MutationRecovery<MD>;

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
   * The duration between each periodic [[pull]]. Setting this to `null`
   * disables periodic pull completely. Pull will still happen if you call
   * [[pull]] manually.
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

  private readonly _memdag: dag.Store;
  private readonly _perdag: dag.Store;
  private readonly _idbDatabases: persist.IDBDatabasesStore =
    new persist.IDBDatabasesStore();
  private readonly _lc: LogContext;

  private readonly _closeAbortController = new AbortController();

  // We must not do persists in parallel. Also, we must not do persists while we
  // are in the middle of a pull because persist rewrites chunks and changes
  // hashes.
  //
  // TODO(arv): This lock makes parallel pulls impossible. The ConnectionLoop
  // supports that but we do not use it. Consider removing that feature from the
  // ConnectionLoop.
  private readonly _persistPullLock = new Lock();
  private _persistIsScheduled = false;
  private _lastPersistFinishedAt = 0;

  private readonly _enableLicensing: boolean;

  /**
   * The options used to control the [[pull]] and push request behavior. This
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
    const {enableLicensing = true, enableMutationRecovery = true} =
      internalOptions;
    this._enableLicensing = enableLicensing;

    if (internalOptions.exposeInternalAPI) {
      internalOptions.exposeInternalAPI({
        persist: () => this._persist(),
      });
    }

    const logSink =
      logSinks.length === 1 ? logSinks[0] : new TeeLogSink(logSinks);
    this._lc = new LogContext(logLevel, logSink).addContext('name', name);

    this._subscriptions = new SubscriptionsManager(
      this._queryInternal,
      this._lc,
    );

    const perKvStore = experimentalKVStore || new IDBStore(this.idbName);
    this._perdag = new dag.StoreImpl(perKvStore, hashOf, assertNotTempHash);
    this._memdag = new dag.LazyStore(
      this._perdag,
      LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT,
      this._memdagHashFunction(),
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
    const clientIDResolver = resolver<string>();
    this._clientIDPromise = clientIDResolver.promise;

    this._mutationRecovery = new MutationRecovery(this, {
      lc: this._lc,
      enableMutationRecovery,
      wrapInOnlineCheck: this._wrapInOnlineCheck.bind(this),
      wrapInReauthRetries: this._wrapInReauthRetries.bind(this),
      isPullDisabled: this._isPullDisabled.bind(this),
      isPushDisabled: this._isPushDisabled.bind(this),
    });

    void this._open(
      profileIDResolver.resolve,
      clientIDResolver.resolve,
      readyResolver.resolve,
      licenseCheckResolver.resolve,
      licenseActiveResolver.resolve,
    );
  }

  protected _memdagHashFunction(): <V extends ReadonlyJSONValue>(
    data: V,
  ) => Hash {
    return newTempHash;
  }

  private async _open(
    profileIDResolver: (profileID: string) => void,
    resolveClientID: (clientID: string) => void,
    resolveReady: () => void,
    resolveLicenseCheck: (valid: boolean) => void,
    resolveLicenseActive: (active: boolean) => void,
  ): Promise<void> {
    // If we are currently closing a Replicache instance with the same name,
    // wait for it to finish closing.
    await closingInstances.get(this.name);
    await this._idbDatabases.getProfileID().then(profileIDResolver);
    await this._idbDatabases.putDatabase(this._idbDatabase);
    const [clientID, client, clients] = DD31
      ? await persist.initClientDD31(this._perdag)
      : await persist.initClient(this._perdag);
    resolveClientID(clientID);
    await this._memdag.withWrite(async write => {
      await write.setHead(db.DEFAULT_HEAD_NAME, client.headHash);
      await write.commit();
    });

    // Now we have a profileID, a clientID, and DB!
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

    setIntervalWithSignal(
      () => this._recoverMutations(),
      RECOVER_MUTATIONS_INTERVAL_MS,
      signal,
    );
    void this._recoverMutations(clients);

    setIntervalWithSignal(
      () => {
        if (Date.now() - this._lastPersistFinishedAt > 2000)
          this._schedulePersist();
      },
      2000,
      signal,
    );

    getDocument()?.addEventListener(
      'visibilitychange',
      this._onVisibilityChange,
    );

    await this._startLicenseActive(resolveLicenseActive, this._lc, signal);
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
        mustSimpleFetch,
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
          mustSimpleFetch,
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
      LICENSE_ACTIVE_INTERVAL_MS,
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
   * `onOnlineChange` is called when the [[online]] property changes. See
   * [[online]] for more details.
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
   */
  async createIndex(def: CreateIndexDefinition): Promise<void> {
    await this._indexOp(tx => tx.createIndex(def));
  }

  /**
   * Drops an index previously created with [[createIndex]].
   */
  async dropIndex(name: string): Promise<void> {
    await this._indexOp(tx => tx.dropIndex(name));
  }

  private async _indexOp(
    f: (tx: IndexTransactionImpl) => Promise<void>,
  ): Promise<void> {
    await this._ready;
    const clientID = await this._clientIDPromise;
    await this._memdag.withWrite(async dagWrite => {
      const dbWrite = await db.newWriteIndexChange(
        db.whenceHead(db.DEFAULT_HEAD_NAME),
        dagWrite,
        clientID,
      );
      const tx = new IndexTransactionImpl(clientID, dbWrite, this._lc);
      await f(tx);
      const [ref, diffs] = await tx.commit(true);
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
      const {replayMutations, diffs} = DD31
        ? await sync.maybeEndPullDD31(this._memdag, lc, syncHead, clientID)
        : await sync.maybeEndPull(this._memdag, lc, syncHead, clientID);

      if (!replayMutations || replayMutations.length === 0) {
        // All done.
        await this._checkChange(syncHead, diffs);
        this._schedulePersist();
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
          rebaseMutation(
            mutation,
            dagWrite,
            syncHead,
            this._mutatorRegistry,
            lc,
            clientID,
          ),
        );
      }
    }
  }

  private async _invokePull(): Promise<boolean> {
    if (this._isPullDisabled()) {
      return true;
    }

    // We must not do a pull and a persist in parallel. Persist changes the head
    // hashes which leads to pull failing.
    return this._persistPullLock.withLock(() =>
      this._wrapInOnlineCheck(async () => {
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
      }, 'Pull'),
    );
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
              clientID,
              this.pusher,
              this.pushURL,
              this.auth,
              this.schemaVersion,
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
   * Push pushes pending changes to the [[pushURL]].
   *
   * You do not usually need to manually call push. If [[pushDelay]] is non-zero
   * (which it is by default) pushes happen automatically shortly after
   * mutations.
   */
  private _push(): void {
    this._pushConnectionLoop.send();
  }

  /**
   * Pull pulls changes from the [[pullURL]]. If there are any changes
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

    await this._persistPullLock.withLock(async () => {
      if (isClientStateNotFoundResponse(poke.pullResponse)) {
        this._fireOnClientStateNotFound(clientID, reasonServer);
        return;
      }

      const syncHead = await sync.handlePullResponse(
        lc,
        this._memdag,
        toInternalValue(
          poke.baseCookie,
          ToInternalValueReason.CookieFromResponse,
        ),
        poke.pullResponse,
        clientID,
      );
      if (syncHead === null) {
        throw new Error(
          'unexpected base cookie for poke: ' + JSON.stringify(poke),
        );
      }

      await this._maybeEndPull(syncHead, requestID);
    });
  }

  protected async _beginPull(): Promise<BeginPullResult> {
    await this._ready;
    const profileID = await this.profileID;
    const clientID = await this._clientIDPromise;
    const {
      result: {beginPullResponse, requestID},
    } = await this._wrapInReauthRetries(
      async (requestID: string, requestLc: LogContext) => {
        const req = {
          pullAuth: this.auth,
          pullURL: this.pullURL,
          schemaVersion: this.schemaVersion,
        };
        const beginPullResponse = DD31
          ? await sync.beginPullDD31(
              profileID,
              clientID,
              req,
              defaultPullerDD31,
              requestID,
              this._memdag,
              requestLc,
            )
          : await sync.beginPull(
              profileID,
              clientID,
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
    if (this._closed) {
      return;
    }
    await this._ready;
    const clientID = await this.clientID;
    try {
      await this._persistPullLock.withLock(async () => {
        await persist.persist(
          clientID,
          this._memdag,
          this._perdag,
          this._mutatorRegistry,
          this._lc,
          (c1, c2) => (c1 as number) - (c2 as number),
          () => this.closed,
        );
        const result = await persist.refresh(
          clientID,
          this._memdag,
          this._perdag,
          this._mutatorRegistry,
          this._lc,
        );
        if (result) {
          const {newHead, diffs} = result;
          await this._checkChange(newHead, diffs);
        }
        this._lastPersistFinishedAt = Date.now();
      });
    } catch (e) {
      if (e instanceof persist.ClientStateNotFoundError) {
        this._fireOnClientStateNotFound(clientID, reasonClient);
      } else if (this._closed) {
        this._lc.debug?.('Exception persisting during close', e);
      } else {
        throw e;
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

  private _schedulePersist(): void {
    if (this._persistIsScheduled) {
      return;
    }
    this._persistIsScheduled = true;
    void (async () => {
      await requestIdle(PERSIST_TIMEOUT);
      await this._persist();
      this._persistIsScheduled = false;
    })();
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
   * Subscribe to changes to the underlying data. Every time the underlying data
   * changes `body` is called and if the result of `body` changes compared to
   * last time `onData` is called. The function is also called once the first
   * time the subscription is added.
   *
   * This returns a function that can be used to cancel the subscription.
   *
   * If an error occurs in the `body` the `onError` function is called if
   * present. Otherwise, the error is thrown.
   */
  subscribe<R extends ReadonlyJSONValue | undefined, E>(
    body: (tx: ReadTransaction) => Promise<R>,
    options: SubscribeOptions<R, E>,
  ): () => void {
    return this._subscriptions.addSubscription(body, options);
  }

  /**
   * Watches Replicache for changes.
   *
   * The `callback` gets called whenever the underlying data changes and the
   * `key` changes matches the
   * [[ExperimentalWatchNoIndexOptions|ExperimentalWatchOptions.prefix]]
   * if present. If a change occurs to the data but the change does not impact
   * the key space the callback is not called. In other words, the callback is
   * never called with an empty diff.
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
  async query<R>(body: (tx: ReadTransaction) => Promise<R> | R): Promise<R> {
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

  private _register<Return extends JSONValue | void, Args extends JSONValue>(
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
      [key: string]: (tx: WriteTransaction, args?: JSONValue) => MutatorReturn;
    },
  >(regs: M): MakeMutators<M> {
    type Mut = MakeMutators<M>;
    const rv: Partial<Mut> = Object.create(null);
    for (const k in regs) {
      rv[k] = this._register(k, regs[k]) as MakeMutator<M[typeof k]>;
    }
    return rv as Mut;
  }

  private async _mutate<R extends JSONValue | void, A extends JSONValue>(
    name: string,
    mutatorImpl: (tx: WriteTransaction, args?: A) => MaybePromise<R>,
    args: A | undefined,
    timestamp: number,
  ): Promise<{result: R; ref: Hash}> {
    const internalArgs = toInternalValue(
      (args ?? null) as ReadonlyJSONValue,
      ToInternalValueReason.WriteTransactionMutateArgs,
    );

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
        internalArgs,
        originalHash,
        dagWrite,
        timestamp,
        clientID,
      );

      const tx = new WriteTransactionImpl(clientID, dbWrite, this._lc);
      try {
        const result: R = await mutatorImpl(tx, args);

        const [ref, diffs] = await tx.commit(true);
        this._pushConnectionLoop.send();
        await this._checkChange(ref, diffs);
        this._schedulePersist();
        return {result, ref};
      } catch (ex) {
        throw await this._convertToClientStateNotFoundError(ex);
      }
    });
  }

  /**
   * In the case we get a ChunkNotFoundError we check if the client got garbage
   * collected and if so change the error to a ClientNotFoundError instead
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

  protected async _recoverMutations(
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
   * mutationID > syncHead.mutationID that exists on the main branch.
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
            args: fromInternalValue(
              p.meta.mutatorArgsJSON,
              FromInternalValueReason.PendingMutationGet,
            ),
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
