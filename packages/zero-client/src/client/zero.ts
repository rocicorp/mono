import {LogContext, LogLevel} from '@rocicorp/logger';
import {Resolver, resolver} from '@rocicorp/resolver';
import type {Entity} from '@rocicorp/zql/src/entity.js';
import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import type {Context as ZQLContext} from '@rocicorp/zql/src/zql/context/context.js';
import {makeReplicacheContext} from '@rocicorp/zql/src/zql/context/replicache-context.js';
import type {FromSet} from '@rocicorp/zql/src/zql/query/entity-query.js';
import {EntityQuery} from '@rocicorp/zql/src/zql/query/entity-query.js';
import {
  ConnectedMessage,
  Downstream,
  NullableVersion,
  PingMessage,
  PokeMessage,
  PullRequestMessage,
  PullResponseBody,
  PullResponseMessage,
  PushMessage,
  downstreamSchema,
  nullableVersionSchema,
  type ErrorMessage,
} from 'reflect-protocol';
import {ROOM_ID_REGEX, isValidRoomID} from 'reflect-shared/src/room-id.js';
import type {MutatorDefs, ReadTransaction} from 'reflect-shared/src/types.js';
import {
  ClientGroupID,
  ClientID,
  ExperimentalWatchCallbackForOptions,
  ExperimentalWatchNoIndexCallback,
  ExperimentalWatchOptions,
  MaybePromise,
  PullRequestV0,
  PullRequestV1,
  Puller,
  PullerResultV0,
  PullerResultV1,
  PushRequestV0,
  PushRequestV1,
  Pusher,
  PusherResult,
  Replicache,
  ReplicacheOptions,
  UpdateNeededReason as ReplicacheUpdateNeededReason,
  SubscribeOptions,
  dropDatabase,
} from 'replicache';
import {assert} from 'shared/src/asserts.js';
import {getDocumentVisibilityWatcher} from 'shared/src/document-visible.js';
import {getDocument} from 'shared/src/get-document.js';
import {sleep, sleepWithAbort} from 'shared/src/sleep.js';
import * as valita from 'shared/src/valita.js';
import {nanoid} from '../util/nanoid.js';
import {send} from '../util/socket.js';
import {checkConnectivity} from './connect-checks.js';
import {shouldEnableAnalytics} from './enable-analytics.js';
import {toWSString, type HTTPString, type WSString} from './http-string.js';
import {LogOptions, createLogOptions} from './log-options.js';
import {
  DID_NOT_CONNECT_VALUE,
  DisconnectReason,
  MetricManager,
  REPORT_INTERVAL_MS,
  Series,
  getLastConnectErrorValue,
} from './metrics.js';
import type {QueryParseDefs, ZeroOptions} from './options.js';
import {PokeHandler} from './poke-handler.js';
import {reloadWithReason, reportReloadReason} from './reload-error-handler.js';
import {ServerError, isAuthError, isServerError} from './server-error.js';
import {getServer} from './server-option.js';
import {version} from './version.js';

export type QueryDefs = {
  readonly [name: string]: Entity;
};

type MakeEntityQueriesFromQueryDefs<QD extends QueryDefs> = {
  readonly [K in keyof QD]: EntityQuery<{[P in K]: QD[K]}, []>;
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
  [exposedToTestingSymbol]: TestingContext;
  [onSetConnectionStateSymbol]: (state: ConnectionState) => void;
  [createLogOptionsSymbol]: (options: {
    consoleLogLevel: LogLevel;
    server: string | null;
  }) => LogOptions;
}

function forTesting<MD extends MutatorDefs, QD extends QueryDefs>(
  r: Zero<MD, QD>,
): TestZero {
  return r as unknown as TestZero;
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
  | {type: 'VersionNotSupported'};

export function serverAheadReloadReason(kind: string) {
  return `Server reported that client is ahead of server (${kind}). This probably happened because the server is in development mode and restarted. Currently when this happens, the dev server loses its state and on reconnect sees the client as ahead. If you see this in other cases, it may be a bug in Zero.`;
}

function convertOnUpdateNeededReason(
  reason: ReplicacheUpdateNeededReason,
): UpdateNeededReason {
  return {type: reason.type};
}

const enum PingResult {
  TimedOut = 0,
  Success = 1,
}

// Keep in sync with packages/replicache/src/replicache-options.ts
export interface ReplicacheInternalAPI {
  lastMutationID(): number;
}

export class Zero<MD extends MutatorDefs, QD extends QueryDefs> {
  readonly version = version;

  readonly #rep: Replicache<MD>;
  readonly #server: HTTPString | null;
  readonly userID: string;
  readonly roomID: string;

  readonly #lc: LogContext;
  readonly #logOptions: LogOptions;
  readonly #enableAnalytics: boolean;

  readonly #pokeHandler: PokeHandler;

  #lastMutationIDSent: {clientID: string; id: number} =
    NULL_LAST_MUTATION_ID_SENT;

  #onPong: () => void = () => undefined;

  #online = false;

  /**
   * `onOnlineChange` is called when the Zero instance's online status
   * changes.
   */
  onOnlineChange: ((online: boolean) => void) | null | undefined = null;

  #onUpdateNeeded: ((reason: UpdateNeededReason) => void) | null;
  readonly #jurisdiction: 'eu' | undefined;
  #baseCookie: number | null = null;
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

  readonly #zqlContext: ZQLContext;

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

  #connectResolver = resolver<void>();
  #baseCookieResolver: Resolver<NullableVersion> | null = null;
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
      forTesting(this)[onSetConnectionStateSymbol](state);
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

  readonly #options: ZeroOptions<MD, QD>;

  readonly query: MakeEntityQueriesFromQueryDefs<QD>;

  #metrics: MetricManager;

  // Store as field to allow test subclass to override. Web API doesn't allow
  // overwriting location fields for security reasons.
  #reload = () => location.reload();

  /**
   * Constructs a new Zero client.
   */
  constructor(options: ZeroOptions<MD, QD>) {
    const {
      userID,
      roomID,
      onOnlineChange,
      jurisdiction,
      hiddenTabDisconnectDelay = DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
      kvStore = 'mem',
      queries = {} as QueryParseDefs<QD>,
    } = options;
    if (!userID) {
      throw new Error('ZeroOptions.userID must not be empty.');
    }
    if (!isValidRoomID(roomID)) {
      throw new Error(
        `ZeroOptions.roomID must match ${ROOM_ID_REGEX.toString()}.`,
      );
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
      server,
      enableAnalytics: this.#enableAnalytics,
    });
    const logOptions = this.#logOptions;

    const replicacheOptions: ReplicacheOptions<MD> = {
      schemaVersion: options.schemaVersion,
      logLevel: logOptions.logLevel,
      logSinks: [logOptions.logSink],
      mutators: options.mutators,
      name: `zero-${userID}-${roomID}`,
      pusher: (req, reqID) => this.#pusher(req, reqID),
      puller: (req, reqID) => this.#puller(req, reqID),
      // TODO: Do we need these?
      pushDelay: 0,
      requestOptions: {
        maxDelayMs: 0,
        minDelayMs: 0,
      },
      licenseKey: 'zero-client-static-key',
      kvStore,
    };
    const replicacheInternalOptions = {
      enableLicensing: false,
    };

    this.#rep = new Replicache({
      ...replicacheOptions,
      ...replicacheInternalOptions,
    });
    this.#rep.getAuth = this.#getAuthToken;
    this.#onUpdateNeeded = this.#rep.onUpdateNeeded; // defaults to reload.
    this.#server = server;
    this.roomID = roomID;
    this.userID = userID;
    this.#jurisdiction = jurisdiction;
    this.#lc = new LogContext(
      logOptions.logLevel,
      {roomID, clientID: this.#rep.clientID},
      logOptions.logSink,
    );

    this.#zqlContext = makeReplicacheContext(this.#rep, {
      subscriptionAdded: ast => this.#zqlSubscriptionAdded(ast),
      subscriptionRemoved: ast => this.#zqlSubscriptionRemoved(ast),
    });

    this.query = this.#registerQueries(queries);

    reportReloadReason(this.#lc);

    this.#metrics = new MetricManager({
      reportIntervalMs: REPORT_INTERVAL_MS,
      host: location.host,
      source: 'client',
      reporter: this.#enableAnalytics
        ? allSeries => this.#reportMetrics(allSeries)
        : () => Promise.resolve(),
      lc: this.#lc,
    });
    this.#metrics.tags.push(`version:${this.version}`);

    this.#pokeHandler = new PokeHandler(
      pokeDD31 => this.#rep.poke(pokeDD31),
      () => this.#onOutOfOrderPoke(),
      this.#rep.clientID,
      this.#lc,
    );

    this.#visibilityWatcher = getDocumentVisibilityWatcher(
      getDocument(),
      hiddenTabDisconnectDelay,
      this.#closeAbortController.signal,
    );

    void this.#runLoop();

    if (TESTING) {
      forTesting(this)[exposedToTestingSymbol] = {
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

  #createLogOptions(options: {
    consoleLogLevel: LogLevel;
    server: string | null;
    enableAnalytics: boolean;
  }): LogOptions {
    if (TESTING) {
      return forTesting(this)[createLogOptionsSymbol](options);
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
   * The registered mutators (see [[ZeroOptions.mutators]]).
   */
  get mutate() {
    return this.#rep.mutate;
  }

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

  /**
   * Subscribe to changes to Zero data. Every time the underlying data
   * changes `body` is called and if the result of `body` changes compared to
   * last time `onData` is called. The function is also called once the first
   * time the subscription is added.
   *
   * This returns a function that can be used to cancel the subscription.
   *
   * If an error occurs in the `body` the `onError` function is called if
   * present. Otherwise, the error is thrown.
   */
  subscribe<R>(
    body: (tx: ReadTransaction) => Promise<R>,
    options: SubscribeOptions<R> | ((result: R) => void),
  ): () => void {
    return this.#rep.subscribe(body, options);
  }

  /**
   * Watches Zero for changes.
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
  experimentalWatch(callback: ExperimentalWatchNoIndexCallback): () => void;
  experimentalWatch<Options extends ExperimentalWatchOptions>(
    callback: ExperimentalWatchCallbackForOptions<Options>,
    options?: Options,
  ): () => void;
  experimentalWatch<Options extends ExperimentalWatchOptions>(
    callback: ExperimentalWatchCallbackForOptions<Options>,
    options?: Options,
  ): () => void {
    return this.#rep.experimentalWatch(callback, options);
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
    switch (downMessage[0]) {
      case 'connected':
        return this.#handleConnectedMessage(lc, downMessage);

      case 'error':
        return this.#handleErrorMessage(lc, downMessage);

      case 'pong':
        return this.#onPong();

      case 'poke':
        return this.#handlePoke(lc, downMessage);

      case 'pull':
        return this.#handlePullResponse(lc, downMessage);

      default:
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
        navigatorOnline: navigator.onLine,
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

    if (kind === 'VersionNotSupported') {
      this.onUpdateNeeded?.({type: kind});
    }

    if (
      kind === 'InvalidConnectionRequestLastMutationID' ||
      kind === 'InvalidConnectionRequestBaseCookie'
    ) {
      await dropDatabase(this.#rep.idbName);
      reloadWithReason(lc, this.#reload, serverAheadReloadReason(kind));
    }

    const error = new ServerError(kind, message);

    lc.info?.(`${kind}: ${message}}`);

    this.#rejectMessageError?.reject(error);
    lc.debug?.('Rejecting connect resolver due to error', error);
    this.#connectResolver.reject(error);
    this.#disconnect(lc, {server: kind});
  }

  #handleConnectedMessage(lc: LogContext, connectedMessage: ConnectedMessage) {
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
      navigatorOnline: navigator.onLine,
      timeToConnectMs,
      totalTimeToConnectMs,
      connectMsgLatencyMs,
      connectedCount: this.#connectedCount,
      proceedingConnectErrorCount,
    });
    this.#lastMutationIDSent = NULL_LAST_MUTATION_ID_SENT;

    lc.debug?.('Resolving connect resolver');
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
    l.info?.('Connecting...', {navigatorOnline: navigator.onLine});

    this.#setConnectionState(ConnectionState.Connecting);

    // connect() called but connect start time is defined. This should not
    // happen.
    assert(this.#connectStart === undefined);

    const now = Date.now();
    this.#connectStart = now;
    if (this.#totalToConnectStart === undefined) {
      this.#totalToConnectStart = now;
    }

    const baseCookie = await this.#getBaseCookie();
    if (this.closed) {
      return;
    }
    this.#baseCookie = baseCookie;

    // Reject connect after a timeout.
    const timeoutID = setTimeout(() => {
      l.debug?.('Rejecting connect resolver due to timeout');
      this.#connectResolver.reject(new TimedOutError('Connect'));
      this.#disconnect(l, {
        client: 'ConnectTimeout',
      });
    }, CONNECT_TIMEOUT_MS);
    this.#closeAbortController.signal.addEventListener('abort', () => {
      clearTimeout(timeoutID);
    });

    const ws = createSocket(
      toWSString(this.#server),
      baseCookie,
      this.clientID,
      await this.clientGroupID,
      this.roomID,
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
    }
  }

  #disconnect(l: LogContext, reason: DisconnectReason): void {
    if (this.#connectionState === ConnectionState.Connecting) {
      this.#connectErrorCount++;
    }
    l.info?.('disconnecting', {
      navigatorOnline: navigator.onLine,
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

  async #handlePoke(_lc: LogContext, pokeMessage: PokeMessage) {
    this.#abortPingTimeout();
    const pokeBody = pokeMessage[1];
    const lastMutationIDChangeForSelf =
      await this.#pokeHandler.handlePoke(pokeBody);
    if (lastMutationIDChangeForSelf !== undefined) {
      this.#lastMutationIDReceived = lastMutationIDChangeForSelf;
    }
  }

  #onOutOfOrderPoke() {
    const lc = this.#lc;
    lc.info?.('out of order poke, disconnecting');

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
    // If we are connecting we wait until we are connected.
    await this.#connectResolver.promise;
    const lc = this.#lc.withContext('requestID', requestID);
    lc.debug?.(`pushing ${req.mutations.length} mutations`);

    // If pushVersion is 0 this is a mutation recovery push for a pre dd31
    // client.  Zero didn't support mutation recovery pre dd31, so don't
    // try to recover these, just return no-op response.
    if (req.pushVersion === 0) {
      return {
        httpRequestInfo: {
          errorMessage: '',
          httpStatusCode: 200,
        },
      };
    }
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
      const msg: PushMessage = [
        'push',
        {
          timestamp: now,
          clientGroupID: req.clientGroupID,
          mutations: [
            {
              timestamp: now - Math.round(performance.now() - m.timestamp),
              id: m.id,
              clientID: m.clientID,
              name: m.name,
              args: m.args,
            },
          ],
          pushVersion: req.pushVersion,
          schemaVersion: req.schemaVersion,
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
            baseCookie: this.#baseCookie,
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
  ): Promise<PullerResultV0 | PullerResultV1> {
    const lc = this.#lc.withContext('requestID', requestID);
    lc.debug?.('Pull', req);
    // If pullVersion === 0 this is a mutation recovery pull for a pre dd31
    // client.  Zero didn't support mutation recovery pre dd31, so don't
    // try to recover these, just return no-op response.
    if (req.pullVersion === 0) {
      return {
        httpRequestInfo: {
          errorMessage: '',
          httpStatusCode: 200,
        },
      };
    }
    // Pull request for this instance's client group.  The base cookie is
    // intercepted here (in a complete hack), and a no-op response is returned
    // as pulls for this client group are handled via poke over the socket.
    if (req.clientGroupID === (await this.clientGroupID)) {
      const cookie = valita.parse(req.cookie, nullableVersionSchema);
      const resolver = this.#baseCookieResolver;
      this.#baseCookieResolver = null;
      resolver?.resolve(cookie);
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
  async #reportMetrics(allSeries: Series[]) {
    if (this.#server === null) {
      this.#lc.info?.('Skipping metrics report, socketOrigin is null');
      return;
    }
    const body = JSON.stringify({series: allSeries});
    const url = new URL('/api/metrics/v0/report', this.#server);
    url.searchParams.set('clientID', this.clientID);
    url.searchParams.set('clientGroupID', await this.clientGroupID);
    url.searchParams.set('roomID', this.roomID);
    url.searchParams.set('userID', this.userID);
    url.searchParams.set('requestID', nanoid());
    const res = await fetch(url.toString(), {
      method: 'POST',
      body,
      keepalive: true,
    });
    if (!res.ok) {
      const maybeBody = await res.text();
      throw new Error(
        `unexpected response: ${res.status} ${res.statusText} body: ${maybeBody}`,
      );
    }
  }

  #checkConnectivity(reason: string) {
    void this.#checkConnectivityAsync(reason);
  }

  async #checkConnectivityAsync(reason: string) {
    assert(this.#server);
    if (this.closed) {
      return;
    }
    try {
      await checkConnectivity(
        reason,
        this.#server,
        this.#lc,
        this.#closeAbortController.signal,
        this.#enableAnalytics,
      );
    } catch (e) {
      this.#lc.info?.('Error checking connectivity for', reason, e);
    }
  }

  // Total hack to get base cookie, see #puller for how the promise is resolved.
  #getBaseCookie(): Promise<NullableVersion> {
    this.#baseCookieResolver ??= resolver();
    void this.#rep.pull();
    return this.#baseCookieResolver.promise;
  }

  #registerQueries(
    queryDefs: QueryParseDefs<QD>,
  ): MakeEntityQueriesFromQueryDefs<QD> {
    const rv = {} as Record<string, EntityQuery<FromSet, []>>;
    const context = this.#zqlContext;
    // Not using parse yet
    for (const name of Object.keys(queryDefs)) {
      rv[name] = new EntityQuery(context, name);
    }

    return rv as MakeEntityQueriesFromQueryDefs<QD>;
  }

  #zqlSubscriptionRemoved(ast: AST) {
    console.log('TODO: removeZQLSubscription', JSON.stringify(ast));
  }

  #zqlSubscriptionAdded(ast: AST) {
    console.log('TODO: addZQLSubscription', JSON.stringify(ast));
  }
}

export function createSocket(
  socketOrigin: WSString,
  baseCookie: NullableVersion,
  clientID: string,
  clientGroupID: string,
  roomID: string,
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
  searchParams.set('roomID', roomID);
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
  return new WebSocket(
    url,
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
