import {LogContext, type LogLevel} from '@rocicorp/logger';
import {type Resolver, resolver} from '@rocicorp/resolver';
import {
  ReplicacheImpl,
  type ReplicacheImplOptions,
} from '../../../replicache/src/impl.js';
import {
  type ClientGroupID,
  type ClientID,
  type ExperimentalNoIndexDiff,
  type MutatorDefs,
  type PullRequestV0,
  type PullRequestV1,
  type Puller,
  type PullerResultV1,
  type PushRequestV0,
  type PushRequestV1,
  type Pusher,
  type PusherResult,
  type ReplicacheOptions,
  type UpdateNeededReason as ReplicacheUpdateNeededReason,
  dropDatabase,
} from '../../../replicache/src/mod.js';
import {assert, unreachable} from '../../../shared/src/asserts.js';
import {
  getBrowserGlobal,
  mustGetBrowserGlobal,
} from '../../../shared/src/browser-env.js';
import {getDocumentVisibilityWatcher} from '../../../shared/src/document-visible.js';
import {must} from '../../../shared/src/must.js';
import {navigator} from '../../../shared/src/navigator.js';
import {sleep, sleepWithAbort} from '../../../shared/src/sleep.js';
import type {MaybePromise} from '../../../shared/src/types.js';
import * as valita from '../../../shared/src/valita.js';
import type {ChangeDesiredQueriesMessage} from '../../../zero-protocol/src/change-desired-queries.js';
import {
  type CRUDMutation,
  type CRUDMutationArg,
  CRUD_MUTATION_NAME,
  type ConnectedMessage,
  type CustomMutation,
  type Downstream,
  ErrorKind,
  type ErrorMessage,
  MutationType,
  type NullableVersion,
  type PingMessage,
  type PokeEndMessage,
  type PokePartMessage,
  type PokeStartMessage,
  type PushMessage,
  downstreamSchema,
  nullableVersionSchema,
} from '../../../zero-protocol/src/mod.js';
import type {
  PullRequestMessage,
  PullResponseBody,
  PullResponseMessage,
} from '../../../zero-protocol/src/pull.js';
import {newQuery} from '../../../zql/src/zql/query/query-impl.js';
import type {Query} from '../../../zql/src/zql/query/query.js';
import type {TableSchema} from '../../../zql/src/zql/query/schema.js';
import {nanoid} from '../util/nanoid.js';
import {send} from '../util/socket.js';
import {ZeroContext} from './context.js';
import {
  type MakeCRUDMutate,
  type WithCRUD,
  makeCRUDMutate,
  makeCRUDMutator,
} from './crud.js';
import {shouldEnableAnalytics} from './enable-analytics.js';
import {type HTTPString, type WSString, toWSString} from './http-string.js';
import {ENTITIES_KEY_PREFIX} from './keys.js';
import {type LogOptions, createLogOptions} from './log-options.js';
import {
  DID_NOT_CONNECT_VALUE,
  type DisconnectReason,
  MetricManager,
  REPORT_INTERVAL_MS,
  type Series,
  getLastConnectErrorValue,
} from './metrics.js';
import {type NormalizedSchema, normalizeSchema} from './normalized-schema.js';
import type {ZeroOptions} from './options.js';
import {QueryManager} from './query-manager.js';
import {reloadWithReason, reportReloadReason} from './reload-error-handler.js';
import {ServerError, isAuthError, isServerError} from './server-error.js';
import {getServer} from './server-option.js';
import {version} from './version.js';
import {PokeHandler} from './zero-poke-handler.js';

// TODO: We should enforce the columns matches primaryKey
export type Schema = {
  readonly version: number;
  readonly tables: {readonly [table: string]: TableSchema};
};

export type NoRelations = Record<string, never>;

export type MakeEntityQueriesFromSchema<S extends Schema> = {
  readonly [K in keyof S['tables']]: Query<S['tables'][K]>;
};

declare const TESTING: boolean;

export type TestingContext = {
  puller: Puller;
  pusher: Pusher;
  setReload: (r: () => void) => void;
  logOptions: LogOptions;
  connectStart: () => number | undefined;
  socketResolver: () => Resolver<WebSocket>;
  connectionState: () => ConnectionState;
};

export const onSetConnectionStateSymbol = Symbol();
export const exposedToTestingSymbol = Symbol();
export const createLogOptionsSymbol = Symbol();

interface TestZero {
  [exposedToTestingSymbol]?: TestingContext;
  [onSetConnectionStateSymbol]?: (state: ConnectionState) => void;
  [createLogOptionsSymbol]?: (options: {
    consoleLogLevel: LogLevel;
    server: string | null;
  }) => LogOptions;
}

function asTestZero<S extends Schema>(z: Zero<S>): TestZero {
  return z as TestZero;
}

export const enum ConnectionState {
  Disconnected,
  Connecting,
  Connected,
}

export const RUN_LOOP_INTERVAL_MS = 5_000;

/**
 * How frequently we should ping the server to keep the connection alive.
 */
export const PING_INTERVAL_MS = 5_000;

/**
 * The amount of time we wait for a pong before we consider the ping timed out.
 */
export const PING_TIMEOUT_MS = 5_000;

/**
 * The amount of time we wait for a pull response before we consider a pull
 * request timed out.
 */
export const PULL_TIMEOUT_MS = 5_000;

export const DEFAULT_DISCONNECT_HIDDEN_DELAY_MS = 5_000;

/**
 * The amount of time we wait for a connection to be established before we
 * consider it timed out.
 */
export const CONNECT_TIMEOUT_MS = 10_000;

const CHECK_CONNECTIVITY_ON_ERROR_FREQUENCY = 6;

const NULL_LAST_MUTATION_ID_SENT = {clientID: '', id: -1} as const;

// When the protocol changes (pull, push, poke,...) we need to bump this.
const REFLECT_VERSION = 1;

/**
 * The reason {@link onUpdateNeeded} was called.
 */
export type UpdateNeededReason =
  // There is a new client group due to a new tab loading new code with
  // different mutators, indexes, schema version, or format version.
  // This tab cannot sync locally with this new tab until it updates to
  // the new code.
  | {type: 'NewClientGroup'}
  // This is used when Zero tries to connect with a version that the server
  // does not support
  | {type: 'VersionNotSupported'}
  // This is used when Zero tries to connect with a schema version that the
  // server does not support
  | {type: 'SchemaVersionNotSupported'};

function convertOnUpdateNeededReason(
  reason: ReplicacheUpdateNeededReason,
): UpdateNeededReason {
  return {type: reason.type};
}

function updateNeededReloadReason(
  reason: UpdateNeededReason,
  serverErrMsg?: string | undefined,
) {
  const {type} = reason;
  let reasonMsg = '';
  switch (type) {
    case 'NewClientGroup':
      reasonMsg =
        "This client could not sync with a newer client. This is probably due to another tab loading a newer incompatible version of the app's code.";
      break;
    case 'VersionNotSupported':
      reasonMsg =
        "The server no longer supports this client's protocol version.";
      break;
    case 'SchemaVersionNotSupported':
      reasonMsg = "The server no longer supports this client's schema version.";
      break;
    default:
      unreachable(type);
  }
  if (serverErrMsg) {
    reasonMsg += ' ' + serverErrMsg;
  }
  return reasonMsg;
}

function serverAheadReloadReason(kind: string) {
  return `Server reported that client is ahead of server (${kind}). This probably happened because the server is in development mode and restarted. Currently when this happens, the dev server loses its state and on reconnect sees the client as ahead. If you see this in other cases, it may be a bug in Zero.`;
}

function onClientStateNotFoundServerReason(serverErrMsg: string) {
  return `Server could not find state needed to synchronize this client. ${serverErrMsg}`;
}
const ON_CLIENT_STATE_NOT_FOUND_REASON_CLIENT =
  'The local persistent state needed to synchronize this client has been garbage collected.';

const enum PingResult {
  TimedOut = 0,
  Success = 1,
}

// Keep in sync with packages/replicache/src/replicache-options.ts
export interface ReplicacheInternalAPI {
  lastMutationID(): number;
}

const internalReplicacheImplMap = new WeakMap<object, ReplicacheImpl>();

export function getInternalReplicacheImplForTesting<
  MD extends MutatorDefs,
  S extends Schema,
>(z: Zero<S>): ReplicacheImpl<MD> {
  return must(internalReplicacheImplMap.get(z)) as ReplicacheImpl<MD>;
}

export class Zero<const S extends Schema> {
  readonly version = version;

  readonly #rep: ReplicacheImpl<WithCRUD<MutatorDefs>>;
  readonly #server: HTTPString | null;
  readonly userID: string;

  readonly #lc: LogContext;
  readonly #logOptions: LogOptions;
  readonly #enableAnalytics: boolean;

  readonly #pokeHandler: PokeHandler;
  readonly #queryManager: QueryManager;

  #lastMutationIDSent: {clientID: string; id: number} =
    NULL_LAST_MUTATION_ID_SENT;

  #onPong: () => void = () => undefined;

  #online = false;

  /**
   * `onOnlineChange` is called when the Zero instance's online status
   * changes.
   */
  onOnlineChange: ((online: boolean) => void) | null | undefined = null;

  #onUpdateNeeded:
    | ((reason: UpdateNeededReason, serverErrorMsg?: string) => void)
    | null = null;
  #onClientStateNotFound: ((reason?: string) => void) | null = null;
  readonly #jurisdiction: 'eu' | undefined;
  // Last cookie used to initiate a connection
  #connectCookie: NullableVersion = null;
  // Total number of sockets successfully connected by this client
  #connectedCount = 0;
  // Number of messages received over currently connected socket.  Reset
  // on disconnect.
  #messageCount = 0;
  #connectedAt = 0;
  // Reset on successful connection.
  #connectErrorCount = 0;

  #abortPingTimeout = () => {
    // intentionally empty
  };

  readonly #zeroContext: ZeroContext;

  /**
   * `onUpdateNeeded` is called when a code update is needed.
   *
   * A code update can be needed because:
   * - the server no longer supports the protocol version of the current code,
   * - a new Zero client has created a new client group, because its code
   *   has different mutators, indexes, schema version and/or format version
   *   from this Zero client. This is likely due to the new client having
   *   newer code. A code update is needed to be able to locally sync with this
   *   new Zero client (i.e. to sync while offline, the clients can can
   *   still sync with each other via the server).
   *
   * The default behavior is to reload the page (using `location.reload()`). Set
   * this to `null` or provide your own function to prevent the page from
   * reloading automatically. You may want to provide your own function to
   * display a toast to inform the end user there is a new version of your app
   * available and prompting them to refresh.
   */
  get onUpdateNeeded(): ((reason: UpdateNeededReason) => void) | null {
    return this.#onUpdateNeeded;
  }
  set onUpdateNeeded(callback: ((reason: UpdateNeededReason) => void) | null) {
    this.#onUpdateNeeded = callback;
    this.#rep.onUpdateNeeded =
      callback &&
      (reason => {
        callback(convertOnUpdateNeededReason(reason));
      });
  }

  /**
   * `onClientStateNotFound` is called when this client will no longer be able
   * to sync due to missing synchronization state.  This can be because:
   * - the local persistent synchronization state has been garbage collected.
   *   This can happen if the client has no pending mutations and has not been
   *   used for a while.
   * - the zero-cache fails to find the synchronization state of this client.
   *
   * The default behavior is to reload the page (using `location.reload()`). Set
   * this to `null` or provide your own function to prevent the page from
   * reloading automatically.
   */
  get onClientStateNotFound(): (() => void) | null {
    return this.#onClientStateNotFound;
  }
  set onClientStateNotFound(value: (() => void) | null) {
    this.#onClientStateNotFound = value;
    this.#rep.onClientStateNotFound = value;
  }

  #connectResolver = resolver<void>();
  #pendingPullsByRequestID: Map<string, Resolver<PullResponseBody>> = new Map();
  #lastMutationIDReceived = 0;

  #socket: WebSocket | undefined = undefined;
  #socketResolver = resolver<WebSocket>();

  #connectionStateChangeResolver = resolver<ConnectionState>();

  /**
   * This resolver is only used for rejections. It is awaited in the connected
   * state (including when waiting for a pong). It is rejected when we get an
   * invalid message or an 'error' message.
   */
  #rejectMessageError: Resolver<never> | undefined = undefined;

  #closeAbortController = new AbortController();

  readonly #visibilityWatcher;

  // We use an accessor pair to allow the subclass to override the setter.
  #connectionState: ConnectionState = ConnectionState.Disconnected;

  #setConnectionState(state: ConnectionState) {
    if (state === this.#connectionState) {
      return;
    }

    this.#connectionState = state;
    this.#connectionStateChangeResolver.resolve(state);
    this.#connectionStateChangeResolver = resolver();

    if (TESTING) {
      asTestZero(this)[onSetConnectionStateSymbol]?.(state);
    }
  }

  #connectStart: number | undefined = undefined;
  // Set on connect attempt if currently undefined.
  // Reset to undefined when
  // 1. client stops trying to connect because it is hidden
  // 2. client encounters a connect error and canary request indicates
  //    the client is offline
  // 2. client successfully connects
  #totalToConnectStart: number | undefined = undefined;

  readonly #options: ZeroOptions<S>;

  readonly query: MakeEntityQueriesFromSchema<S>;

  // TODO: Metrics needs to be rethought entirely as we're not going to
  // send metrics to customer server.
  #metrics: MetricManager;

  // Store as field to allow test subclass to override. Web API doesn't allow
  // overwriting location fields for security reasons.
  #reload = () => getBrowserGlobal('location')?.reload();

  /**
   * Constructs a new Zero client.
   */
  constructor(options: ZeroOptions<S>) {
    const {
      userID,
      onOnlineChange,
      jurisdiction,
      hiddenTabDisconnectDelay = DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
      kvStore = 'idb',
      schema,
    } = options;
    if (!userID) {
      throw new Error('ZeroOptions.userID must not be empty.');
    }
    const server = getServer(options.server);
    this.#enableAnalytics = shouldEnableAnalytics(
      server,
      options.enableAnalytics,
    );

    if (jurisdiction !== undefined && jurisdiction !== 'eu') {
      throw new Error('ZeroOptions.jurisdiction must be "eu" if present.');
    }
    if (hiddenTabDisconnectDelay < 0) {
      throw new Error(
        'ZeroOptions.hiddenTabDisconnectDelay must not be negative.',
      );
    }

    this.onOnlineChange = onOnlineChange;
    this.#options = options;

    this.#logOptions = this.#createLogOptions({
      consoleLogLevel: options.logLevel ?? 'error',
      server: null, //server, // Reenable remote logging
      enableAnalytics: this.#enableAnalytics,
    });
    const logOptions = this.#logOptions;

    // TODO: Normalize schemas once and for all!
    const normalizedSchema = normalizeSchema(schema);

    const replicacheMutators = {
      ['_zero_crud']: makeCRUDMutator(normalizedSchema),
    };

    const replicacheOptions: ReplicacheOptions<WithCRUD<MutatorDefs>> = {
      schemaVersion: normalizedSchema.version.toString(),
      logLevel: logOptions.logLevel,
      logSinks: [logOptions.logSink],
      mutators: replicacheMutators,
      name: `zero-${userID}`,
      pusher: (req, reqID) => this.#pusher(req, reqID),
      puller: (req, reqID) => this.#puller(req, reqID),
      pushDelay: 0,
      requestOptions: {
        maxDelayMs: 0,
        minDelayMs: 0,
      },
      licenseKey: 'zero-client-static-key',
      kvStore,
    };
    const replicacheImplOptions: ReplicacheImplOptions = {
      enableClientGroupForking: false,
    };

    const rep = new ReplicacheImpl(replicacheOptions, replicacheImplOptions);
    this.#rep = rep;

    if (TESTING) {
      internalReplicacheImplMap.set(this, rep);
    }

    rep.getAuth = this.#getAuthToken;
    this.#server = server;
    this.userID = userID;
    this.#jurisdiction = jurisdiction;
    this.#lc = new LogContext(
      logOptions.logLevel,
      {clientID: rep.clientID},
      logOptions.logSink,
    );
    this.onUpdateNeeded = (
      reason: UpdateNeededReason,
      serverErrorMsg?: string | undefined,
    ) => {
      reloadWithReason(
        this.#lc,
        this.#reload,
        updateNeededReloadReason(reason, serverErrorMsg),
      );
    };
    this.onClientStateNotFound = (reason?: string) => {
      reloadWithReason(
        this.#lc,
        this.#reload,
        reason ?? ON_CLIENT_STATE_NOT_FOUND_REASON_CLIENT,
      );
    };

    this.mutate = makeCRUDMutate<S>(normalizedSchema, rep.mutate);

    this.#queryManager = new QueryManager(
      rep.clientID,
      msg => this.#sendChangeDesiredQueries(msg),
      rep.experimentalWatch.bind(rep),
    );

    this.#zeroContext = new ZeroContext(
      normalizedSchema.tables,
      (ast, gotCallback) => this.#queryManager.add(ast, gotCallback),
    );

    rep.experimentalWatch(
      diff => this.#zeroContext.processChanges(diff as ExperimentalNoIndexDiff),
      {
        prefix: ENTITIES_KEY_PREFIX,
        initialValuesInFirstDiff: true,
      },
    );

    this.query = this.#registerQueries(normalizedSchema);

    reportReloadReason(this.#lc);

    this.#metrics = new MetricManager({
      reportIntervalMs: REPORT_INTERVAL_MS,
      host: getBrowserGlobal('location')?.host ?? '',
      source: 'client',
      reporter: this.#enableAnalytics
        ? allSeries => this.#reportMetrics(allSeries)
        : () => Promise.resolve(),
      lc: this.#lc,
    });
    this.#metrics.tags.push(`version:${this.version}`);

    this.#pokeHandler = new PokeHandler(
      poke => this.#rep.poke(poke),
      () => this.#onPokeError(),
      rep.clientID,
      normalizedSchema,
      this.#lc,
    );

    this.#visibilityWatcher = getDocumentVisibilityWatcher(
      getBrowserGlobal('document'),
      hiddenTabDisconnectDelay,
      this.#closeAbortController.signal,
    );

    void this.#runLoop();

    if (TESTING) {
      asTestZero(this)[exposedToTestingSymbol] = {
        puller: this.#puller,
        pusher: this.#pusher,
        setReload: (r: () => void) => {
          this.#reload = r;
        },
        logOptions: this.#logOptions,
        connectStart: () => this.#connectStart,
        socketResolver: () => this.#socketResolver,
        connectionState: () => this.#connectionState,
      };
    }
  }

  #sendChangeDesiredQueries(msg: ChangeDesiredQueriesMessage): void {
    if (this.#socket && this.#connectionState === ConnectionState.Connected) {
      send(this.#socket, msg);
    }
  }

  #createLogOptions(options: {
    consoleLogLevel: LogLevel;
    server: string | null;
    enableAnalytics: boolean;
  }): LogOptions {
    if (TESTING) {
      const testZero = asTestZero(this);
      if (testZero[createLogOptionsSymbol]) {
        return testZero[createLogOptionsSymbol](options);
      }
    }
    return createLogOptions(options);
  }

  /**
   * The name of the IndexedDB database in which the data of this
   * instance of Zero is stored.
   */
  get idbName(): string {
    return this.#rep.idbName;
  }

  /**
   * The schema version of the data understood by this application.
   * See [[ZeroOptions.schemaVersion]].
   */
  get schemaVersion(): string {
    return this.#rep.schemaVersion;
  }

  /**
   * The client ID for this instance of Zero. Each instance
   * gets a unique client ID.
   */
  get clientID(): ClientID {
    return this.#rep.clientID;
  }

  get clientGroupID(): Promise<ClientGroupID> {
    return this.#rep.clientGroupID;
  }

  /**
   * Provides facilities to write data to Zero.
   *
   * `mutate` is a function as well as a "namespace" object for doing CRUD style
   * mutations. When used as a function it is used to batch multiple mutations.
   *
   * ```ts
   * await zero.mutate.issue.create({id: '1', title: 'First issue'});
   * await zero.mutate.comment.create({id: '1', text: 'First comment', issueID: '1'});
   *
   * // or as a function:
   * await zero.mutate(m => {
   *   await m.issue.create({id: '1', title: 'First issue'});
   *   await m.comment.create({id: '1', text: 'First comment', issueID: '1'});
   * });
   * ```
   *
   * The benefit of using the function form is that it allows you to batch
   * multiple mutations together. This can be more efficient than making
   * individual calls to `create`, `update`, `set`, and `delete`.
   *
   * The function form of `mutate` is not allowed to be called inside another
   * `mutate` function. Doing so will throw an error.
   */
  readonly mutate: MakeCRUDMutate<S>;

  /**
   * Whether this Zero instance has been closed. Once a Zero instance has
   * been closed it no longer syncs and you can no longer read or write data out
   * of it. After it has been closed it is pretty much useless and should not be
   * used any more.
   */
  get closed(): boolean {
    return this.#rep.closed;
  }

  /**
   * Closes this Zero instance.
   *
   * When closed all subscriptions end and no more read or writes are allowed.
   */
  close(): Promise<void> {
    const lc = this.#lc.withContext('close');

    if (this.#connectionState !== ConnectionState.Disconnected) {
      this.#disconnect(lc, {
        client: 'ClientClosed',
      });
    }
    lc.debug?.('Aborting closeAbortController due to close()');
    this.#closeAbortController.abort();
    this.#metrics.stop();
    return this.#rep.close();
  }

  #onMessage = (e: MessageEvent<string>) => {
    const lc = this.#lc;
    lc.debug?.('received message', e.data);
    if (this.closed) {
      lc.debug?.('ignoring message because already closed');
      return;
    }

    const rejectInvalidMessage = (e?: unknown) =>
      this.#rejectMessageError?.reject(
        new Error(
          `Invalid message received from server: ${
            e instanceof Error ? e.message + '. ' : ''
          }${data}`,
        ),
      );

    let downMessage: Downstream;
    const {data} = e;
    try {
      downMessage = valita.parse(JSON.parse(data), downstreamSchema);
    } catch (e) {
      rejectInvalidMessage(e);
      return;
    }
    this.#messageCount++;
    const msgType = downMessage[0];
    switch (msgType) {
      case 'connected':
        return this.#handleConnectedMessage(lc, downMessage);

      case 'error':
        return this.#handleErrorMessage(lc, downMessage);

      case 'pong':
        return this.#onPong();

      case 'pokeStart':
        return this.#handlePokeStart(lc, downMessage);

      case 'pokePart':
        return this.#handlePokePart(lc, downMessage);

      case 'pokeEnd':
        return this.#handlePokeEnd(lc, downMessage);

      case 'pull':
        return this.#handlePullResponse(lc, downMessage);
      case 'warm':
        // we ignore warming messages
        break;
      default:
        msgType satisfies never;
        rejectInvalidMessage();
    }
  };

  #onOpen = (e: Event) => {
    const l = addWebSocketIDFromSocketToLogContext(
      e.target as WebSocket,
      this.#lc,
    );
    if (this.#connectStart === undefined) {
      l.error?.(
        'Got open event but connect start time is undefined. This should not happen.',
      );
    } else {
      const now = Date.now();
      const timeToOpenMs = now - this.#connectStart;
      l.info?.('Got socket open event', {
        navigatorOnline: navigator?.onLine,
        timeToOpenMs,
      });
    }
  };

  #onClose = (e: CloseEvent) => {
    const l = addWebSocketIDFromSocketToLogContext(
      e.target as WebSocket,
      this.#lc,
    );
    const {code, reason, wasClean} = e;
    l.info?.('Got socket close event', {code, reason, wasClean});

    const closeKind = wasClean ? 'CleanClose' : 'AbruptClose';
    this.#connectResolver.reject(new CloseError(closeKind));
    this.#disconnect(l, {client: closeKind});
  };

  // An error on the connection is fatal for the connection.
  async #handleErrorMessage(
    lc: LogContext,
    downMessage: ErrorMessage,
  ): Promise<void> {
    const [, kind, message] = downMessage;

    // Rate limit errors are not fatal to the connection.
    // We really don't want to disconnect and reconnect a rate limited user as
    // it'll use more resources on the server
    if (kind === ErrorKind.MutationRateLimited) {
      this.#lastMutationIDSent = NULL_LAST_MUTATION_ID_SENT;
      lc.error?.('Mutation rate limited', {message});
      return;
    }

    lc.info?.(`${kind}: ${message}}`);
    const error = new ServerError(kind, message);

    this.#rejectMessageError?.reject(error);
    lc.debug?.('Rejecting connect resolver due to error', error);
    this.#connectResolver.reject(error);
    this.#disconnect(lc, {server: kind});

    if (kind === ErrorKind.VersionNotSupported) {
      this.#onUpdateNeeded?.({type: kind}, message);
    } else if (kind === ErrorKind.SchemaVersionNotSupported) {
      await this.#rep.disableClientGroup();
      this.#onUpdateNeeded?.({type: 'SchemaVersionNotSupported'}, message);
    } else if (kind === ErrorKind.ClientNotFound) {
      await this.#rep.disableClientGroup();
      this.#onClientStateNotFound?.(onClientStateNotFoundServerReason(message));
    } else if (
      kind === ErrorKind.InvalidConnectionRequestLastMutationID ||
      kind === ErrorKind.InvalidConnectionRequestBaseCookie
    ) {
      await dropDatabase(this.#rep.idbName);
      reloadWithReason(lc, this.#reload, serverAheadReloadReason(kind));
    }
  }

  async #handleConnectedMessage(
    lc: LogContext,
    connectedMessage: ConnectedMessage,
  ) {
    const now = Date.now();
    const [, connectBody] = connectedMessage;
    lc = addWebSocketIDToLogContext(connectBody.wsid, lc);

    if (this.#connectedCount === 0) {
      this.#checkConnectivity('firstConnect');
    } else if (this.#connectErrorCount > 0) {
      this.#checkConnectivity('connectAfterError');
    }
    this.#connectedCount++;
    this.#connectedAt = now;
    this.#metrics.lastConnectError.clear();
    const proceedingConnectErrorCount = this.#connectErrorCount;
    this.#connectErrorCount = 0;

    let timeToConnectMs = undefined;
    let connectMsgLatencyMs = undefined;
    if (this.#connectStart === undefined) {
      lc.error?.(
        'Got connected message but connect start time is undefined. This should not happen.',
      );
    } else {
      timeToConnectMs = now - this.#connectStart;
      this.#metrics.timeToConnectMs.set(timeToConnectMs);
      connectMsgLatencyMs =
        connectBody.timestamp !== undefined
          ? now - connectBody.timestamp
          : undefined;
      this.#connectStart = undefined;
    }
    let totalTimeToConnectMs = undefined;
    if (this.#totalToConnectStart === undefined) {
      lc.error?.(
        'Got connected message but total to connect start time is undefined. This should not happen.',
      );
    } else {
      totalTimeToConnectMs = now - this.#totalToConnectStart;
      this.#totalToConnectStart = undefined;
    }

    this.#metrics.setConnected(timeToConnectMs ?? 0, totalTimeToConnectMs ?? 0);

    lc.info?.('Connected', {
      navigatorOnline: navigator?.onLine,
      timeToConnectMs,
      totalTimeToConnectMs,
      connectMsgLatencyMs,
      connectedCount: this.#connectedCount,
      proceedingConnectErrorCount,
    });
    this.#lastMutationIDSent = NULL_LAST_MUTATION_ID_SENT;

    lc.debug?.('Resolving connect resolver');
    const queriesPatch = await this.#rep.query(tx =>
      this.#queryManager.getQueriesPatch(tx),
    );
    assert(this.#socket);
    send(this.#socket, [
      'initConnection',
      {
        desiredQueriesPatch: queriesPatch,
      },
    ]);
    this.#setConnectionState(ConnectionState.Connected);
    this.#connectResolver.resolve();
  }

  /**
   * Starts a new connection. This will create the WebSocket that does the HTTP
   * request to the server.
   *
   * {@link #connect} will throw an assertion error if the
   * {@link #connectionState} is not {@link ConnectionState.Disconnected}.
   * Callers MUST check the connection state before calling this method and log
   * an error as needed.
   *
   * The function will resolve once the socket is connected. If you need to know
   * when a connection has been established, as in we have received the
   * {@link ConnectedMessage}, you should await the {@link #connectResolver}
   * promise. The {@link #connectResolver} promise rejects if an error message
   * is received before the connected message is received or if the connection
   * attempt times out.
   */
  async #connect(l: LogContext): Promise<void> {
    assert(this.#server);

    // All the callers check this state already.
    assert(this.#connectionState === ConnectionState.Disconnected);

    const wsid = nanoid();
    l = addWebSocketIDToLogContext(wsid, l);
    l.info?.('Connecting...', {navigatorOnline: navigator?.onLine});

    this.#setConnectionState(ConnectionState.Connecting);

    // connect() called but connect start time is defined. This should not
    // happen.
    assert(this.#connectStart === undefined);

    const now = Date.now();
    this.#connectStart = now;
    if (this.#totalToConnectStart === undefined) {
      this.#totalToConnectStart = now;
    }

    if (this.closed) {
      return;
    }
    this.#connectCookie = valita.parse(
      await this.#rep.cookie,
      nullableVersionSchema,
    );
    if (this.closed) {
      return;
    }
    // Reject connect after a timeout.
    const timeoutID = setTimeout(() => {
      l.debug?.('Rejecting connect resolver due to timeout');
      this.#connectResolver.reject(new TimedOutError('Connect'));
      this.#disconnect(l, {
        client: 'ConnectTimeout',
      });
    }, CONNECT_TIMEOUT_MS);
    const abortHandler = () => {
      clearTimeout(timeoutID);
    };
    this.#closeAbortController.signal.addEventListener('abort', abortHandler);

    const ws = createSocket(
      toWSString(this.#server),
      this.#connectCookie,
      this.clientID,
      await this.clientGroupID,
      this.#options.schema.version,
      this.userID,
      this.#rep.auth,
      this.#jurisdiction,
      this.#lastMutationIDReceived,
      wsid,
      this.#options.logLevel === 'debug',
      l,
    );

    if (this.closed) {
      return;
    }

    ws.addEventListener('message', this.#onMessage);
    ws.addEventListener('open', this.#onOpen);
    ws.addEventListener('close', this.#onClose);
    this.#socket = ws;
    this.#socketResolver.resolve(ws);

    try {
      l.debug?.('Waiting for connection to be acknowledged');
      await this.#connectResolver.promise;
    } finally {
      clearTimeout(timeoutID);
      this.#closeAbortController.signal.removeEventListener(
        'abort',
        abortHandler,
      );
    }
  }

  #disconnect(l: LogContext, reason: DisconnectReason): void {
    if (this.#connectionState === ConnectionState.Connecting) {
      this.#connectErrorCount++;
    }
    l.info?.('disconnecting', {
      navigatorOnline: navigator?.onLine,
      reason,
      connectStart: this.#connectStart,
      totalToConnectStart: this.#totalToConnectStart,
      connectedAt: this.#connectedAt,
      connectionDuration: this.#connectedAt
        ? Date.now() - this.#connectedAt
        : 0,
      messageCount: this.#messageCount,
      connectionState: this.#connectionState,
      connectErrorCount: this.#connectErrorCount,
    });

    switch (this.#connectionState) {
      case ConnectionState.Connected: {
        if (this.#connectStart !== undefined) {
          l.error?.(
            'disconnect() called while connected but connect start time is defined. This should not happen.',
          );
          // this._connectStart reset below.
        }

        break;
      }
      case ConnectionState.Connecting: {
        this.#metrics.lastConnectError.set(getLastConnectErrorValue(reason));
        this.#metrics.timeToConnectMs.set(DID_NOT_CONNECT_VALUE);
        this.#metrics.setConnectError(reason);
        if (
          this.#connectErrorCount % CHECK_CONNECTIVITY_ON_ERROR_FREQUENCY ===
          1
        ) {
          this.#checkConnectivity(
            `connectErrorCount=${this.#connectErrorCount}`,
          );
        }
        // this._connectStart reset below.
        if (this.#connectStart === undefined) {
          l.error?.(
            'disconnect() called while connecting but connect start time is undefined. This should not happen.',
          );
        }

        break;
      }
      case ConnectionState.Disconnected:
        l.error?.('disconnect() called while disconnected');
        break;
    }

    this.#socketResolver = resolver();
    l.debug?.('Creating new connect resolver');
    this.#connectResolver = resolver();
    this.#setConnectionState(ConnectionState.Disconnected);
    this.#messageCount = 0;
    this.#connectStart = undefined; // don't reset this._totalToConnectStart
    this.#connectedAt = 0;
    this.#socket?.removeEventListener('message', this.#onMessage);
    this.#socket?.removeEventListener('open', this.#onOpen);
    this.#socket?.removeEventListener('close', this.#onClose);
    this.#socket?.close();
    this.#socket = undefined;
    this.#lastMutationIDSent = NULL_LAST_MUTATION_ID_SENT;
    this.#pokeHandler.handleDisconnect();
  }

  async #handlePokeStart(_lc: LogContext, pokeMessage: PokeStartMessage) {
    this.#abortPingTimeout();
    await this.#pokeHandler.handlePokeStart(pokeMessage[1]);
  }

  async #handlePokePart(_lc: LogContext, pokeMessage: PokePartMessage) {
    this.#abortPingTimeout();
    const lastMutationIDChangeForSelf = await this.#pokeHandler.handlePokePart(
      pokeMessage[1],
    );
    if (lastMutationIDChangeForSelf !== undefined) {
      this.#lastMutationIDReceived = lastMutationIDChangeForSelf;
    }
  }

  async #handlePokeEnd(_lc: LogContext, pokeMessage: PokeEndMessage) {
    this.#abortPingTimeout();
    await this.#pokeHandler.handlePokeEnd(pokeMessage[1]);
  }

  #onPokeError() {
    const lc = this.#lc;
    lc.info?.(
      'poke error, disconnecting?',
      this.#connectionState !== ConnectionState.Disconnected,
    );

    // It is theoretically possible that we get disconnected during the
    // async poke above. Only disconnect if we are not already
    // disconnected.
    if (this.#connectionState !== ConnectionState.Disconnected) {
      this.#disconnect(lc, {
        client: 'UnexpectedBaseCookie',
      });
    }
  }

  #handlePullResponse(
    lc: LogContext,
    pullResponseMessage: PullResponseMessage,
  ) {
    this.#abortPingTimeout();
    const body = pullResponseMessage[1];
    lc = lc.withContext('requestID', body.requestID);
    lc.debug?.('Handling pull response', body);
    const resolver = this.#pendingPullsByRequestID.get(body.requestID);
    if (!resolver) {
      // This can happen because resolvers are deleted
      // from this._pendingPullsByRequestID when pulls timeout.
      lc.debug?.('No resolver found');
      return;
    }
    resolver.resolve(pullResponseMessage[1]);
  }

  async #pusher(
    req: PushRequestV0 | PushRequestV1,
    requestID: string,
  ): Promise<PusherResult> {
    // The deprecation of pushVersion 0 predates zero-client
    assert(req.pushVersion === 1);
    // If we are connecting we wait until we are connected.
    await this.#connectResolver.promise;
    const lc = this.#lc.withContext('requestID', requestID);
    lc.debug?.(`pushing ${req.mutations.length} mutations`);
    const socket = this.#socket;
    assert(socket);

    const isMutationRecoveryPush =
      req.clientGroupID !== (await this.clientGroupID);
    const start = isMutationRecoveryPush
      ? 0
      : req.mutations.findIndex(
          m =>
            m.clientID === this.#lastMutationIDSent.clientID &&
            m.id === this.#lastMutationIDSent.id,
        ) + 1;
    lc.debug?.(
      isMutationRecoveryPush ? 'pushing for recovery' : 'pushing',
      req.mutations.length - start,
      'mutations of',
      req.mutations.length,
      'mutations.',
    );
    const now = Date.now();
    for (let i = start; i < req.mutations.length; i++) {
      const m = req.mutations[i];
      const timestamp = now - Math.round(performance.now() - m.timestamp);
      const zeroM =
        m.name === CRUD_MUTATION_NAME
          ? ({
              type: MutationType.CRUD,
              timestamp,
              id: m.id,
              clientID: m.clientID,
              name: m.name,
              args: [m.args as CRUDMutationArg],
            } satisfies CRUDMutation)
          : ({
              type: MutationType.Custom,
              timestamp,
              id: m.id,
              clientID: m.clientID,
              name: m.name,
              args: [m.args],
            } satisfies CustomMutation);
      const msg: PushMessage = [
        'push',
        {
          timestamp: now,
          clientGroupID: req.clientGroupID,
          mutations: [zeroM],
          pushVersion: req.pushVersion,
          // Zero schema versions are always numbers.
          schemaVersion: parseInt(req.schemaVersion),
          requestID,
        },
      ];
      send(socket, msg);
      if (!isMutationRecoveryPush) {
        this.#lastMutationIDSent = {clientID: m.clientID, id: m.id};
      }
    }
    return {
      httpRequestInfo: {
        errorMessage: '',
        httpStatusCode: 200,
      },
    };
  }

  #getAuthToken = (): MaybePromise<string> | undefined => {
    const {auth} = this.#options;
    return typeof auth === 'function' ? auth() : auth;
  };

  async #updateAuthToken(lc: LogContext): Promise<void> {
    const auth = await this.#getAuthToken();
    if (auth) {
      lc.debug?.('Got auth token');
      this.#rep.auth = auth;
    }
  }

  async #runLoop() {
    this.#lc.info?.(`Starting Zero version: ${this.version}`);

    if (this.#server === null) {
      this.#lc.info?.('No socket origin provided, not starting connect loop.');
      return;
    }

    let runLoopCounter = 0;
    const bareLogContext = this.#lc;
    const getLogContext = () => {
      let lc = bareLogContext;
      if (this.#socket) {
        lc = addWebSocketIDFromSocketToLogContext(this.#socket, lc);
      }
      return lc.withContext('runLoopCounter', runLoopCounter);
    };

    await this.#updateAuthToken(bareLogContext);

    let needsReauth = false;
    let gotError = false;

    while (!this.closed) {
      runLoopCounter++;
      let lc = getLogContext();

      try {
        switch (this.#connectionState) {
          case ConnectionState.Disconnected: {
            if (this.#visibilityWatcher.visibilityState === 'hidden') {
              this.#metrics.setDisconnectedWaitingForVisible();
              // reset this._totalToConnectStart since this client
              // is no longer trying to connect due to being hidden.
              this.#totalToConnectStart = undefined;
            }
            // If hidden, we wait for the tab to become visible before trying again.
            await this.#visibilityWatcher.waitForVisible();

            // If we got an auth error we try to get a new auth token before reconnecting.
            if (needsReauth) {
              await this.#updateAuthToken(lc);
            }

            await this.#connect(lc);
            if (this.closed) {
              break;
            }

            // Now we have a new socket, update lc with the new wsid.
            assert(this.#socket);
            lc = getLogContext();

            lc.debug?.('Connected successfully');
            gotError = false;
            needsReauth = false;
            this.#setOnline(true);
            break;
          }

          case ConnectionState.Connecting:
            // Can't get here because Disconnected waits for Connected or
            // rejection.
            lc.error?.('unreachable');
            gotError = true;
            break;

          case ConnectionState.Connected: {
            // When connected we wait for whatever happens first out of:
            // - After PING_INTERVAL_MS we send a ping
            // - We get disconnected
            // - We get a message
            // - We get an error (rejectMessageError rejects)
            // - The tab becomes hidden (with a delay)

            const controller = new AbortController();
            this.#abortPingTimeout = () => controller.abort();
            const [pingTimeoutPromise, pingTimeoutAborted] = sleepWithAbort(
              PING_INTERVAL_MS,
              controller.signal,
            );

            this.#rejectMessageError = resolver();

            const enum RaceCases {
              Ping = 0,
              Hidden = 2,
            }

            const raceResult = await promiseRace([
              pingTimeoutPromise,
              pingTimeoutAborted,
              this.#visibilityWatcher.waitForHidden(),
              this.#connectionStateChangeResolver.promise,
              this.#rejectMessageError.promise,
            ]);

            if (this.closed) {
              this.#rejectMessageError = undefined;
              break;
            }

            switch (raceResult) {
              case RaceCases.Ping: {
                const pingResult = await this.#ping(
                  lc,
                  this.#rejectMessageError.promise,
                );
                if (pingResult === PingResult.TimedOut) {
                  gotError = true;
                }
                break;
              }
              case RaceCases.Hidden:
                this.#disconnect(lc, {
                  client: 'Hidden',
                });
                this.#setOnline(false);
                break;
            }

            this.#rejectMessageError = undefined;
          }
        }
      } catch (ex) {
        if (this.#connectionState !== ConnectionState.Connected) {
          lc.error?.('Failed to connect', ex, {
            lmid: this.#lastMutationIDReceived,
            baseCookie: this.#connectCookie,
          });
        }

        lc.debug?.(
          'Got an exception in the run loop',
          'state:',
          this.#connectionState,
          'exception:',
          ex,
        );

        if (isAuthError(ex)) {
          if (!needsReauth) {
            needsReauth = true;
            // First auth error, try right away without waiting.
            continue;
          }
          needsReauth = true;
        }

        if (
          isServerError(ex) ||
          ex instanceof TimedOutError ||
          ex instanceof CloseError
        ) {
          gotError = true;
        }
      }

      // Only authentication errors are retried immediately the first time they
      // occur. All other errors wait a few seconds before retrying the first
      // time. We specifically do not use a backoff for consecutive errors
      // because it's a bad experience to wait many seconds for reconnection.

      if (gotError) {
        this.#setOnline(false);
        let cfGetCheckSucceeded = false;
        const cfGetCheckURL = new URL(this.#server);
        cfGetCheckURL.pathname = '/api/canary/v0/get';
        cfGetCheckURL.searchParams.set('id', nanoid());
        const cfGetCheckController = new AbortController();
        fetch(cfGetCheckURL, {signal: cfGetCheckController.signal})
          .then(_ => {
            cfGetCheckSucceeded = true;
          })
          .catch(_ => {
            cfGetCheckSucceeded = false;
          });

        lc.debug?.(
          'Sleeping',
          RUN_LOOP_INTERVAL_MS,
          'ms before reconnecting due to error, state:',
          this.#connectionState,
        );
        await sleep(RUN_LOOP_INTERVAL_MS);
        cfGetCheckController.abort();
        if (!cfGetCheckSucceeded) {
          lc.info?.(
            'Canary request failed, resetting total time to connect start time.',
          );
          this.#totalToConnectStart = undefined;
        }
      }
    }
  }

  async #puller(
    req: PullRequestV0 | PullRequestV1,
    requestID: string,
  ): Promise<PullerResultV1> {
    // The deprecation of pushVersion 0 predates zero-client
    assert(req.pullVersion === 1);
    const lc = this.#lc.withContext('requestID', requestID);
    lc.debug?.('Pull', req);
    // Pull request for this instance's client group.  A no-op response is
    // returned as pulls for this client group are handled via poke over the
    // socket.
    if (req.clientGroupID === (await this.clientGroupID)) {
      return {
        httpRequestInfo: {
          errorMessage: '',
          httpStatusCode: 200,
        },
      };
    }

    // If we are connecting we wait until we are connected.
    await this.#connectResolver.promise;
    const socket = this.#socket;
    assert(socket);
    // Mutation recovery pull.
    lc.debug?.('Pull is for mutation recovery');
    const cookie = valita.parse(req.cookie, nullableVersionSchema);
    const pullRequestMessage: PullRequestMessage = [
      'pull',
      {
        clientGroupID: req.clientGroupID,
        cookie,
        requestID,
      },
    ];
    send(socket, pullRequestMessage);
    const pullResponseResolver: Resolver<PullResponseBody> = resolver();
    this.#pendingPullsByRequestID.set(requestID, pullResponseResolver);
    try {
      const enum RaceCases {
        Timeout = 0,
        Response = 1,
      }
      const raceResult = await promiseRace([
        sleep(PULL_TIMEOUT_MS),
        pullResponseResolver.promise,
      ]);
      switch (raceResult) {
        case RaceCases.Timeout:
          lc.debug?.('Mutation recovery pull timed out');
          throw new Error('Pull timed out');
        case RaceCases.Response: {
          lc.debug?.('Returning mutation recovery pull response');
          const response = await pullResponseResolver.promise;
          return {
            response: {
              cookie: response.cookie,
              lastMutationIDChanges: response.lastMutationIDChanges,
              patch: [],
            },
            httpRequestInfo: {
              errorMessage: '',
              httpStatusCode: 200,
            },
          };
        }
        default:
          assert(false, 'unreachable');
      }
    } finally {
      pullResponseResolver.reject('timed out');
      this.#pendingPullsByRequestID.delete(requestID);
    }
  }

  #setOnline(online: boolean): void {
    if (this.#online === online) {
      return;
    }

    this.#online = online;
    this.onOnlineChange?.(online);
  }

  /**
   * A rough heuristic for whether the client is currently online and
   * authenticated.
   */
  get online(): boolean {
    return this.#online;
  }

  /**
   * Starts a a ping and waits for a pong.
   *
   * If it takes too long to get a pong we disconnect and this returns
   * {@code PingResult.TimedOut}.
   */
  async #ping(
    l: LogContext,
    messageErrorRejectionPromise: Promise<never>,
  ): Promise<PingResult> {
    l.debug?.('pinging');
    const {promise, resolve} = resolver();
    this.#onPong = resolve;
    const pingMessage: PingMessage = ['ping', {}];
    const t0 = performance.now();
    assert(this.#socket);
    send(this.#socket, pingMessage);

    const connected =
      (await promiseRace([
        promise,
        sleep(PING_TIMEOUT_MS),
        messageErrorRejectionPromise,
      ])) === 0;

    const delta = performance.now() - t0;
    if (!connected) {
      l.info?.('ping failed in', delta, 'ms - disconnecting');
      this.#disconnect(l, {
        client: 'PingTimeout',
      });
      return PingResult.TimedOut;
    }

    l.debug?.('ping succeeded in', delta, 'ms');
    return PingResult.Success;
  }

  // Sends a set of metrics to the server. Throws unless the server
  // returns 200.
  // TODO: Reenable metrics reporting
  async #reportMetrics(_allSeries: Series[]) {
    // if (this.#server === null) {
    //   this.#lc.info?.('Skipping metrics report, socketOrigin is null');
    //   return;
    // }
    // const body = JSON.stringify({series: allSeries});
    // const url = new URL('/api/metrics/v0/report', this.#server);
    // url.searchParams.set('clientID', this.clientID);
    // url.searchParams.set('clientGroupID', await this.clientGroupID);
    // url.searchParams.set('userID', this.userID);
    // url.searchParams.set('requestID', nanoid());
    // const res = await fetch(url.toString(), {
    //   method: 'POST',
    //   body,
    //   keepalive: true,
    // });
    // if (!res.ok) {
    //   const maybeBody = await res.text();
    //   throw new Error(
    //     `unexpected response: ${res.status} ${res.statusText} body: ${maybeBody}`,
    //   );
    // }
  }

  #checkConnectivity(reason: string) {
    void this.#checkConnectivityAsync(reason);
  }

  #checkConnectivityAsync(_reason: string) {
    // skipping connectivity checks for now - the server doesn't respond to
    // them so it just creates noise.
    // assert(this.#server);
    // if (this.closed) {
    //   return;
    // }
    // try {
    //   await checkConnectivity(
    //     reason,
    //     this.#server,
    //     this.#lc,
    //     this.#closeAbortController.signal,
    //     this.#enableAnalytics,
    //   );
    // } catch (e) {
    //   this.#lc.info?.('Error checking connectivity for', reason, e);
    // }
  }

  #registerQueries(schema: NormalizedSchema): MakeEntityQueriesFromSchema<S> {
    const rv = {} as Record<string, Query<TableSchema>>;
    const context = this.#zeroContext;
    // Not using parse yet
    for (const [name, table] of Object.entries(schema.tables)) {
      rv[name] = newQuery(context, table);
    }

    return rv as MakeEntityQueriesFromSchema<S>;
  }
}

export function createSocket(
  socketOrigin: WSString,
  baseCookie: NullableVersion,
  clientID: string,
  clientGroupID: string,
  schemaVersion: number,
  userID: string,
  auth: string | undefined,
  jurisdiction: 'eu' | undefined,
  lmid: number,
  wsid: string,
  debugPerf: boolean,
  lc: LogContext,
): WebSocket {
  const url = new URL(socketOrigin);
  // Keep this in sync with the server.
  url.pathname = `/api/sync/v${REFLECT_VERSION}/connect`;
  const {searchParams} = url;
  searchParams.set('clientID', clientID);
  searchParams.set('clientGroupID', clientGroupID);
  searchParams.set('schemaVersion', schemaVersion.toString());
  searchParams.set('userID', userID);
  if (jurisdiction !== undefined) {
    searchParams.set('jurisdiction', jurisdiction);
  }
  searchParams.set('baseCookie', baseCookie === null ? '' : String(baseCookie));
  searchParams.set('ts', String(performance.now()));
  searchParams.set('lmid', String(lmid));
  searchParams.set('wsid', wsid);
  if (debugPerf) {
    searchParams.set('debugPerf', true.toString());
  }

  lc.info?.('Connecting to', url.toString());

  // Pass auth to the server via the `Sec-WebSocket-Protocol` header by passing
  // it as a `protocol` to the `WebSocket` constructor.  The empty string is an
  // invalid `protocol`, and will result in an exception, so pass undefined
  // instead.  encodeURIComponent to ensure it only contains chars allowed
  // for a `protocol`.
  const WS = mustGetBrowserGlobal('WebSocket');
  return new WS(
    // toString() required for RN URL polyfill.
    url.toString(),
    auth === '' || auth === undefined ? undefined : encodeURIComponent(auth),
  );
}

/**
 * Adds the wsid query parameter to the log context. If the URL does not
 * have a wsid we use a randomID instead.
 */
function addWebSocketIDFromSocketToLogContext(
  {url}: {url: string},
  lc: LogContext,
): LogContext {
  const wsid = new URL(url).searchParams.get('wsid') ?? nanoid();
  return addWebSocketIDToLogContext(wsid, lc);
}

function addWebSocketIDToLogContext(wsid: string, lc: LogContext): LogContext {
  return lc.withContext('wsid', wsid);
}

/**
 * Like Promise.race but returns the index of the first promise that resolved.
 */
function promiseRace(ps: Promise<unknown>[]): Promise<number> {
  return Promise.race(ps.map((p, i) => p.then(() => i)));
}

class TimedOutError extends Error {
  constructor(m: string) {
    super(`${m} timed out`);
  }
}

class CloseError extends Error {}
