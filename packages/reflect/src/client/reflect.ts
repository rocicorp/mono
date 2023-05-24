import {consoleLogSink, LogContext, TeeLogSink} from '@rocicorp/logger';
import {Resolver, resolver} from '@rocicorp/resolver';
import {
  ConnectedMessage,
  Downstream,
  downstreamSchema,
  NullableVersion,
  nullableVersionSchema,
  PingMessage,
  PokeMessage,
  PullRequestMessage,
  PullResponseBody,
  PullResponseMessage,
  PushMessage,
  ErrorKind as ServerErrorKind,
  type ErrorMessage,
} from 'reflect-protocol';
import {
  dropDatabase,
  ExperimentalWatchCallbackForOptions,
  ExperimentalWatchNoIndexCallback,
  ExperimentalWatchOptions,
  MaybePromise,
  MutatorDefs,
  PullerResultV0,
  PullerResultV1,
  PullRequestV0,
  PullRequestV1,
  PusherResult,
  PushRequestV0,
  PushRequestV1,
  ReadonlyJSONValue,
  ReadTransaction,
  Replicache,
  ReplicacheOptions,
  UpdateNeededReason as ReplicacheUpdateNeededReason,
} from 'replicache';
import {assert} from 'shared/asserts.js';
import {sleep, sleepWithAbort} from 'shared/sleep.js';
import * as valita from 'shared/valita.js';
import {nanoid} from '../util/nanoid.js';
import {send} from '../util/socket.js';
import {getDocumentVisibilityWatcher} from './document-visible.js';
import {
  DID_NOT_CONNECT_VALUE,
  MetricManager,
  REPORT_INTERVAL_MS,
  Series,
} from './metrics.js';
import type {ReflectOptions} from './options.js';
import {PokeHandler} from './poke-handler.js';
import {reloadWithReason, reportReloadReason} from './reload-error-handler.js';
import {isAuthError, isServerError, ServerError} from './server-error.js';
import {version} from './version.js';

export const enum ConnectionState {
  Disconnected,
  Connecting,
  Connected,
}

export const RUN_LOOP_INTERVAL_MS = 5_000;

type ClientDisconnectReason =
  | 'AbruptClose'
  | 'CleanClose'
  | 'ReflectClosed'
  | 'ConnectTimeout'
  | 'UnexpectedBaseCookie'
  | 'PingTimeout'
  | 'Hidden';

export type DisconnectReason =
  | {
      server: ServerErrorKind;
    }
  | {
      client: ClientDisconnectReason;
    };

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

/**
 * The amount of time to wait before we consider a tab hidden.
 */
export const HIDDEN_INTERVAL_MS = 5_000;

/**
 * The amount of time we wait for a connection to be established before we
 * consider it timed out.
 */
export const CONNECT_TIMEOUT_MS = 10_000;

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
  // This is used When reflect tries to connect with a version that the server
  // does not support
  | {type: 'VersionNotSupported'};

export function serverAheadReloadReason(kind: string) {
  return `Server reported that client is ahead of server (${kind}). This probably happened because the server is in development mode and restarted. Currently when this happens, the dev server loses its state and on reconnect sees the client as ahead. If you see this in other cases, it may be a bug in Reflect.`;
}

function convertOnUpdateNeededReason(
  reason: ReplicacheUpdateNeededReason,
): UpdateNeededReason {
  return {type: reason.type};
}

export class Reflect<MD extends MutatorDefs> {
  readonly version = version;

  private readonly _rep: Replicache<MD>;
  private readonly _socketOrigin: string;
  readonly userID: string;
  readonly roomID: string;

  // This is a promise because it is waiting for the clientID from the
  // Replicache instance.
  private readonly _l: Promise<LogContext>;

  private readonly _pokeHandler: PokeHandler;

  private _lastMutationIDSent: {clientID: string; id: number} =
    NULL_LAST_MUTATION_ID_SENT;

  private _onPong: () => void = () => undefined;

  #online = false;

  /**
   * `onOnlineChange` is called when the Reflect instance's online status
   * changes.
   */
  onOnlineChange: ((online: boolean) => void) | null | undefined = null;

  private _onUpdateNeeded: ((reason: UpdateNeededReason) => void) | null;
  private readonly _jurisdiction: 'eu' | undefined;
  private _baseCookie: number | null = null;
  private _messageCount = 0;
  private _connectedAt = 0;

  #abortPingTimeout = () => {
    // intentionally empty
  };

  /**
   * `onUpdateNeeded` is called when a code update is needed.
   *
   * A code update can be needed because:
   * - the server no longer supports the protocol version of the current code,
   * - a new Reflect client has created a new client group, because its code
   *   has different mutators, indexes, schema version and/or format version
   *   from this Reflect client. This is likely due to the new client having
   *   newer code. A code update is needed to be able to locally sync with this
   *   new Reflect client (i.e. to sync while offline, the clients can can
   *   still sync with each other via the server).
   *
   * The default behavior is to reload the page (using `location.reload()`). Set
   * this to `null` or provide your own function to prevent the page from
   * reloading automatically. You may want to provide your own function to
   * display a toast to inform the end user there is a new version of your app
   * available and prompting them to refresh.
   */
  get onUpdateNeeded(): ((reason: UpdateNeededReason) => void) | null {
    return this._onUpdateNeeded;
  }
  set onUpdateNeeded(callback: ((reason: UpdateNeededReason) => void) | null) {
    this._onUpdateNeeded = callback;
    this._rep.onUpdateNeeded =
      callback &&
      (reason => {
        callback(convertOnUpdateNeededReason(reason));
      });
  }

  private _connectResolver = resolver<void>();
  private _baseCookieResolver: Resolver<NullableVersion> | null = null;
  private _pendingPullsByRequestID: Map<string, Resolver<PullResponseBody>> =
    new Map();
  private _lastMutationIDReceived = 0;

  private _socket: WebSocket | undefined = undefined;
  protected _socketResolver = resolver<WebSocket>();

  #connectionStateChangeResolver = resolver<ConnectionState>();

  /**
   * This resolver is only used for rejections. It is awaited in the connected
   * state (including when waiting for a pong). It is rejected when we get an
   * invalid message or an 'error' message.
   */
  #rejectMessageError: Resolver<never> | undefined = undefined;

  #closeAbortController = new AbortController();

  readonly #visibilityWatcher = getDocumentVisibilityWatcher(
    getDocument(),
    HIDDEN_INTERVAL_MS,
    this.#closeAbortController.signal,
  );

  // We use an accessor pair to allow the subclass to override the setter.
  #connectionState: ConnectionState = ConnectionState.Disconnected;
  protected get _connectionState(): ConnectionState {
    return this.#connectionState;
  }
  protected set _connectionState(state: ConnectionState) {
    if (state === this.#connectionState) {
      return;
    }

    this.#connectionState = state;
    this.#connectionStateChangeResolver.resolve(state);
    this.#connectionStateChangeResolver = resolver();
  }

  // See comment on _metrics.timeToConnectMs for how _connectingStart is used.
  protected _connectingStart: number | undefined = undefined;

  readonly #options: ReflectOptions<MD>;

  private _metrics: MetricManager;

  // Store as field to allow test subclass to override. Web API doesn't allow
  // overwriting location fields for security reasons.
  private _reload = () => location.reload();

  /**
   * Constructs a new Reflect client.
   */
  constructor(options: ReflectOptions<MD>) {
    const {userID, roomID, socketOrigin, onOnlineChange, jurisdiction} =
      options;
    if (!userID) {
      throw new Error('ReflectOptions.userID must not be empty.');
    }

    if (
      !socketOrigin.startsWith('ws://') &&
      !socketOrigin.startsWith('wss://')
    ) {
      throw new Error(
        "ReflectOptions.socketOrigin must use the 'ws' or 'wss' scheme.",
      );
    }
    if (jurisdiction !== undefined && jurisdiction !== 'eu') {
      throw new Error('ReflectOptions.jurisdiction must be "eu" if present.');
    }

    this.onOnlineChange = onOnlineChange;
    this.#options = options;

    const replicacheOptions: ReplicacheOptions<MD> = {
      schemaVersion: options.schemaVersion,
      logLevel: options.logLevel,
      logSinks: options.logSinks,
      mutators: options.mutators,
      name: `reflect-${userID}-${roomID}`,
      pusher: (req, reqID) => this._pusher(req, reqID),
      puller: (req, reqID) => this._puller(req, reqID),
      // TODO: Do we need these?
      pushDelay: 0,
      requestOptions: {
        maxDelayMs: 0,
        minDelayMs: 0,
      },
      licenseKey: 'reflect-client-static-key',
      experimentalCreateKVStore: options.createKVStore,
    };
    const replicacheInternalOptions = {
      enableLicensing: false,
    };

    this._rep = new Replicache({
      ...replicacheOptions,
      ...replicacheInternalOptions,
    });
    this._rep.getAuth = this.#getAuthToken;
    this._onUpdateNeeded = this._rep.onUpdateNeeded; // defaults to reload.
    this._socketOrigin = socketOrigin;
    this.roomID = roomID;
    this.userID = userID;
    this._jurisdiction = jurisdiction;
    this._l = getLogContext(options, this._rep);

    void this._l.then(lc => reportReloadReason(lc, localStorage));

    this._metrics = new MetricManager({
      reportIntervalMs: REPORT_INTERVAL_MS,
      host: location.host,
      source: 'client',
      reporter: allSeries => this._reportMetrics(allSeries),
      lc: this._l,
    });
    this._metrics.tags.push(`version:${this.version}`);

    this._pokeHandler = new PokeHandler(
      pokeDD31 => this._rep.poke(pokeDD31),
      () => this._onOutOfOrderPoke(),
      this.clientID,
      this._l,
    );

    void this._runLoop();
  }

  /**
   * The name of the IndexedDB database in which the data of this
   * instance of Reflect is stored.
   */
  get idbName(): string {
    return this._rep.idbName;
  }

  /**
   * The schema version of the data understood by this application.
   * See [[ReflectOptions.schemaVersion]].
   */
  get schemaVersion(): string {
    return this._rep.schemaVersion;
  }

  /**
   * The client ID for this instance of Reflect. Each instance
   * gets a unique client ID.
   */
  get clientID(): Promise<string> {
    return this._rep.clientID;
  }

  get clientGroupID(): Promise<string> {
    return this._rep.clientGroupID;
  }

  /**
   * The registered mutators (see [[ReflectOptions.mutators]]).
   */
  get mutate() {
    return this._rep.mutate;
  }

  /**
   * Whether this Reflect instance has been closed. Once a Reflect instance has
   * been closed it no longer syncs and you can no longer read or write data out
   * of it. After it has been closed it is pretty much useless and should not be
   * used any more.
   */
  get closed(): boolean {
    return this._rep.closed;
  }

  /**
   * Closes this Reflect instance.
   *
   * When closed all subscriptions end and no more read or writes are allowed.
   */
  async close(): Promise<void> {
    if (this._connectionState !== ConnectionState.Disconnected) {
      const lc = await this._l;
      await this._disconnect(lc, {
        client: 'ReflectClosed',
      });
    }
    this.#closeAbortController.abort();
    this._metrics.stop();
    return this._rep.close();
  }

  /**
   * Subscribe to changes to Reflect data. Every time the underlying data
   * changes `body` is called and if the result of `body` changes compared to
   * last time `onData` is called. The function is also called once the first
   * time the subscription is added.
   *
   * This returns a function that can be used to cancel the subscription.
   *
   * If an error occurs in the `body` the `onError` function is called if
   * present. Otherwise, the error is thrown.
   */
  subscribe<R extends ReadonlyJSONValue | undefined>(
    body: (tx: ReadTransaction) => Promise<R>,
    {
      onData,
      onError,
      onDone,
    }: {
      onData: (result: R) => void;
      onError?: (error: unknown) => void;
      onDone?: () => void;
    },
  ): () => void {
    return this._rep.subscribe(body, {
      onData,
      onError,
      onDone,
    });
  }

  /**
   * Transactionally read Reflect data.
   */
  query<R>(body: (tx: ReadTransaction) => Promise<R> | R): Promise<R> {
    return this._rep.query(body);
  }

  /**
   * Watches Reflect for changes.
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
    return this._rep.experimentalWatch(callback, options);
  }

  private _onMessage = async (e: MessageEvent<string>) => {
    const l = await this._l;
    l.debug?.('received message', e.data);
    if (this.closed) {
      l.debug?.('ignoring message because already closed');
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
    this._messageCount++;
    switch (downMessage[0]) {
      case 'connected':
        this._handleConnectedMessage(l, downMessage);
        return;

      case 'error':
        await this._handleErrorMessage(l, downMessage);
        return;

      case 'pong':
        this._onPong();
        return;

      case 'poke':
        await this._handlePoke(l, downMessage);
        return;

      case 'pull':
        this._handlePullResponse(l, downMessage);
        return;

      default:
        rejectInvalidMessage();
    }
  };

  private _onClose = async (e: CloseEvent) => {
    const l = addWebSocketIDFromSocketToLogContext(
      e.target as WebSocket,
      await this._l,
    );
    const {code, reason, wasClean} = e;
    l.info?.('Got socket close event', {code, reason, wasClean});

    const closeKind = wasClean ? 'CleanClose' : 'AbruptClose';
    this._connectResolver.reject(new CloseError(closeKind));
    await this._disconnect(l, {client: closeKind});
  };

  // An error on the connection is fatal for the connection.
  private async _handleErrorMessage(
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
      await dropDatabase(this._rep.idbName);
      reloadWithReason(
        this._reload,
        localStorage,
        serverAheadReloadReason(kind),
      );
    }

    const error = new ServerError(kind, message);

    lc.info?.(`${kind}: ${message}}`);

    this.#rejectMessageError?.reject(error);
    lc.debug?.('Rejecting connect resolver due to error', error);
    this._connectResolver.reject(error);
    await this._disconnect(lc, {server: kind});
  }

  private _handleConnectedMessage(
    lc: LogContext,
    connectedMessage: ConnectedMessage,
  ) {
    lc = addWebSocketIDToLogContext(connectedMessage[1].wsid, lc);

    this._connectionState = ConnectionState.Connected;
    this._connectedAt = Date.now();
    this._metrics.lastConnectError.clear();

    if (this._connectingStart === undefined) {
      lc.error?.(
        'Got connected message but connect start time is undefined. This should not happen.',
      );
    } else {
      const timeToConnectMs = Date.now() - this._connectingStart;
      this._metrics.timeToConnectMs.set(timeToConnectMs);
      lc.info?.('Connected', {
        navigatorOnline: navigator.onLine,
        timeToConnectMs,
      });
      this._connectingStart = undefined;
    }

    this._lastMutationIDSent = NULL_LAST_MUTATION_ID_SENT;
    lc.debug?.('Resolving connect resolver');
    this._connectResolver.resolve();
  }

  /**
   * Starts a new connection. This will create the WebSocket that does the HTTP
   * request to the server.
   *
   * {@link _connect} will throw an assertion error if the
   * {@link _connectionState} is not {@link ConnectionState.Disconnected}.
   * Callers MUST check the connection state before calling this method and log
   * an error as needed.
   *
   * The function will resolve once the socket is connected. If you need to know
   * when a connection has been established, as in we have received the
   * {@link ConnectedMessage}, you should await the {@link _connectResolver}
   * promise. The {@link _connectResolver} promise rejects if an error message
   * is received before the connected message is received or if the connection
   * attempt times out.
   */
  private async _connect(l: LogContext): Promise<void> {
    // All the callers check this state already.
    assert(this._connectionState === ConnectionState.Disconnected);

    const wsid = nanoid();
    l = addWebSocketIDToLogContext(wsid, l);
    l.info?.('Connecting...', {navigatorOnline: navigator.onLine});

    this._connectionState = ConnectionState.Connecting;

    // connect() called but connect start time is defined. This should not
    // happen.
    assert(this._connectingStart === undefined);

    this._connectingStart = Date.now();

    const baseCookie = await this._getBaseCookie();
    this._baseCookie = baseCookie;

    // Reject connect after a timeout.
    const id = setTimeout(async () => {
      l.debug?.('Rejecting connect resolver due to timeout');
      this._connectResolver.reject(new TimedOutError('Connect'));
      await this._disconnect(l, {
        client: 'ConnectTimeout',
      });
    }, CONNECT_TIMEOUT_MS);
    const clear = () => clearTimeout(id);
    this._connectResolver.promise.then(clear, clear);

    const ws = createSocket(
      this._socketOrigin,
      baseCookie,
      await this.clientID,
      await this.clientGroupID,
      this.roomID,
      this.userID,
      this._rep.auth,
      this._jurisdiction,
      this._lastMutationIDReceived,
      wsid,
      this.#options.logLevel === 'debug',
      l,
    );

    ws.addEventListener('message', this._onMessage);
    ws.addEventListener('close', this._onClose);
    this._socket = ws;
    this._socketResolver.resolve(ws);
  }

  private async _disconnect(
    l: LogContext,
    reason: DisconnectReason,
  ): Promise<void> {
    l.info?.('disconnecting', {
      navigatorOnline: navigator.onLine,
      reason,
      connectedAt: this._connectedAt,
      connectionDuration: this._connectedAt
        ? Date.now() - this._connectedAt
        : 0,
      messageCount: this._messageCount,
    });

    switch (this._connectionState) {
      case ConnectionState.Connected: {
        if (this._connectingStart !== undefined) {
          l.error?.(
            'disconnect() called while connected but connect start time is defined. This should not happen.',
          );
          // this._connectingStart reset below.
        }

        break;
      }
      case ConnectionState.Connecting: {
        this._metrics.lastConnectError.set(getLastConnectMetricState(reason));
        if (this._connectingStart === undefined) {
          l.error?.(
            'disconnect() called while connecting but connect start time is undefined. This should not happen.',
          );
        } else {
          this._metrics.timeToConnectMs.set(DID_NOT_CONNECT_VALUE);
          // this._connectingStart reset below.
        }

        break;
      }
      case ConnectionState.Disconnected:
        l.error?.('disconnect() called while disconnected');
        break;
    }

    this._socketResolver = resolver();
    l.debug?.('Creating new connect resolver');
    this._connectResolver = resolver();
    this._connectionState = ConnectionState.Disconnected;
    this._messageCount = 0;
    this._connectingStart = undefined;
    this._connectedAt = 0;
    this._socket?.removeEventListener('message', this._onMessage);
    this._socket?.removeEventListener('close', this._onClose);
    this._socket?.close();
    this._socket = undefined;
    this._lastMutationIDSent = NULL_LAST_MUTATION_ID_SENT;
    await this._pokeHandler.handleDisconnect();
  }

  private async _handlePoke(_lc: LogContext, pokeMessage: PokeMessage) {
    this.#abortPingTimeout();
    const pokeBody = pokeMessage[1];
    const lastMutationIDChangeForSelf = await this._pokeHandler.handlePoke(
      pokeBody,
    );
    if (lastMutationIDChangeForSelf !== undefined) {
      this._lastMutationIDReceived = lastMutationIDChangeForSelf;
    }
  }

  private async _onOutOfOrderPoke() {
    const lc = await this._l;
    lc.info?.('out of order poke, disconnecting');

    // It is theoretically possible that we get disconnected during the
    // async poke above. Only disconnect if we are not already
    // disconnected.
    if (this._connectionState !== ConnectionState.Disconnected) {
      await this._disconnect(lc, {
        client: 'UnexpectedBaseCookie',
      });
    }
  }

  private _handlePullResponse(
    lc: LogContext,
    pullResponseMessage: PullResponseMessage,
  ) {
    this.#abortPingTimeout();
    const body = pullResponseMessage[1];
    lc = lc.withContext('requestID', body.requestID);
    lc.debug?.('Handling pull response', body);
    const resolver = this._pendingPullsByRequestID.get(body.requestID);
    if (!resolver) {
      // This can happen because resolvers are deleted
      // from this._pendingPullsByRequestID when pulls timeout.
      lc.debug?.('No resolver found');
      return;
    }
    resolver.resolve(pullResponseMessage[1]);
  }

  private async _pusher(
    req: PushRequestV0 | PushRequestV1,
    requestID: string,
  ): Promise<PusherResult> {
    // If we are connecting we wait until we are connected.
    await this._connectResolver.promise;
    const l = (await this._l).withContext('requestID', requestID);
    l.debug?.(`pushing ${req.mutations.length} mutations`);

    // If pushVersion is 0 this is a mutation recovery push for a pre dd31
    // client.  Reflect didn't support mutation recovery pre dd31, so don't
    // try to recover these, just return no-op response.
    if (req.pushVersion === 0) {
      return {
        httpRequestInfo: {
          errorMessage: '',
          httpStatusCode: 200,
        },
      };
    }
    const socket = this._socket;
    assert(socket);

    const isMutationRecoveryPush =
      req.clientGroupID !== (await this.clientGroupID);
    const start = isMutationRecoveryPush
      ? 0
      : req.mutations.findIndex(
          m =>
            m.clientID === this._lastMutationIDSent.clientID &&
            m.id === this._lastMutationIDSent.id,
        ) + 1;
    l.debug?.(
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
        this._lastMutationIDSent = {clientID: m.clientID, id: m.id};
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
      this._rep.auth = auth;
    }
  }

  private async _runLoop() {
    (await this._l).info?.(`Starting Reflect version: ${this.version}`);

    let runLoopCounter = 0;
    const bareLogContext = await this._l;
    const getLogContext = () => {
      let lc = bareLogContext;
      if (this._socket) {
        lc = addWebSocketIDFromSocketToLogContext(this._socket, lc);
      }
      return lc.withContext('runLoopCounter', runLoopCounter);
    };

    await this.#updateAuthToken(bareLogContext);

    let needsReauth = false;
    let errorCount = 0;

    while (!this.closed) {
      runLoopCounter++;
      let lc = getLogContext();

      try {
        switch (this._connectionState) {
          case ConnectionState.Disconnected: {
            // If hidden, we wait for the tab to become visible before trying again.
            await this.#visibilityWatcher.waitForVisible();

            // If we got an auth error we try to get a new auth token before reconnecting.
            if (needsReauth) {
              await this.#updateAuthToken(lc);
            }

            await this._connect(lc);

            // Now we have a new socket, update lc with the new wsid.
            assert(this._socket);
            lc = getLogContext();

            lc.debug?.('Waiting for connection to be acknowledged');
            await this._connectResolver.promise;
            lc.debug?.('Connected successfully');
            errorCount = 0;
            needsReauth = false;
            this.#setOnline(true);
            break;
          }

          case ConnectionState.Connecting:
            // Can't get here because Disconnected waits for Connected or
            // rejection.
            lc.error?.('unreachable');
            errorCount++;
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
              case RaceCases.Ping:
                await this._ping(lc, this.#rejectMessageError.promise);
                break;
              case RaceCases.Hidden:
                await this._disconnect(lc, {
                  client: 'Hidden',
                });
                this.#setOnline(false);
                break;
            }

            this.#rejectMessageError = undefined;
          }
        }
      } catch (ex) {
        if (this._connectionState !== ConnectionState.Connected) {
          lc.error?.('Failed to connect', ex, {
            lmid: this._lastMutationIDReceived,
            baseCookie: this._baseCookie,
          });
        }

        lc.debug?.(
          'Got an exception in the run loop',
          'state:',
          this._connectionState,
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
          errorCount++;
        }
      }

      // Only authentication errors are retried immediately the first time they
      // occur. All other errors wait a few seconds before retrying the first
      // time. We specifically do not use a backoff for consecutive errors
      // because it's a bad experience to wait many seconds for reconnection.

      if (errorCount > 0) {
        this.#setOnline(false);

        lc.debug?.(
          'Sleeping',
          RUN_LOOP_INTERVAL_MS,
          'ms before reconnecting due to error count',
          errorCount,
          'state:',
          this._connectionState,
        );
        await sleep(RUN_LOOP_INTERVAL_MS);
      }
    }
  }

  private async _puller(
    req: PullRequestV0 | PullRequestV1,
    requestID: string,
  ): Promise<PullerResultV0 | PullerResultV1> {
    const l = (await this._l).withContext('requestID', requestID);
    l.debug?.('Pull', req);
    // If pullVersion === 0 this is a mutation recovery pull for a pre dd31
    // client.  Reflect didn't support mutation recovery pre dd31, so don't
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
      const resolver = this._baseCookieResolver;
      this._baseCookieResolver = null;
      resolver?.resolve(cookie);
      return {
        httpRequestInfo: {
          errorMessage: '',
          httpStatusCode: 200,
        },
      };
    }

    // If we are connecting we wait until we are connected.
    await this._connectResolver.promise;
    const socket = this._socket;
    assert(socket);

    // Mutation recovery pull.
    l.debug?.('Pull is for mutation recovery');
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
    this._pendingPullsByRequestID.set(requestID, pullResponseResolver);
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
          l.debug?.('Mutation recovery pull timed out');
          throw new Error('Pull timed out');
        case RaceCases.Response: {
          l.debug?.('Returning mutation recovery pull response');
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
      this._pendingPullsByRequestID.delete(requestID);
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
   * Throws an error if the ping times out.
   */
  private async _ping(
    l: LogContext,
    messageErrorRejectionPromise: Promise<never>,
  ): Promise<void> {
    l.debug?.('pinging');
    const {promise, resolve} = resolver();
    this._onPong = resolve;
    const pingMessage: PingMessage = ['ping', {}];
    const t0 = performance.now();
    assert(this._socket);
    send(this._socket, pingMessage);

    const connected =
      (await promiseRace([
        promise,
        sleep(PING_TIMEOUT_MS),
        messageErrorRejectionPromise,
      ])) === 0;
    if (this._connectionState !== ConnectionState.Connected) {
      return;
    }

    const delta = performance.now() - t0;
    if (connected) {
      l.debug?.('ping succeeded in', delta, 'ms');
    } else {
      l.info?.('ping failed in', delta, 'ms - disconnecting');
      await this._disconnect(l, {
        client: 'PingTimeout',
      });
      throw new TimedOutError('Ping');
    }
  }

  // Sends a set of metrics to the server. Throws unless the server
  // returns 200.
  private async _reportMetrics(allSeries: Series[]) {
    const body = JSON.stringify({series: allSeries});
    const url = new URL('/api/metrics/v0/report', this._socketOrigin);
    url.protocol = url.protocol === 'wss:' ? 'https:' : 'http:';
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

  // Total hack to get base cookie, see _puller for how the promise is resolved.
  private _getBaseCookie(): Promise<NullableVersion> {
    this._baseCookieResolver ??= resolver();
    this._rep.pull();
    return this._baseCookieResolver.promise;
  }
}

export function createSocket(
  socketOrigin: string,
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

async function getLogContext<MD extends MutatorDefs>(
  options: ReflectOptions<MD>,
  rep: Replicache<MD>,
) {
  const {logSinks = [consoleLogSink]} = options;
  const logSink =
    logSinks.length === 1 ? logSinks[0] : new TeeLogSink(logSinks);
  return new LogContext(
    options.logLevel,
    {roomID: options.roomID, clientID: await rep.clientID},
    logSink,
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
 * Returns the document object. This is wrapped in a function because Reflect
 * runs in environments that do not have a document (such as Web Workers, Deno
 * etc)
 */
function getDocument(): Document | undefined {
  return typeof document !== 'undefined' ? document : undefined;
}

/**
 * Like Promise.race but returns the index of the first promise that resolved.
 */
function promiseRace(ps: Promise<unknown>[]): Promise<number> {
  return Promise.race(ps.map((p, i) => p.then(() => i)));
}

function getLastConnectMetricState(reason: DisconnectReason): string {
  if ('server' in reason) {
    return `server_${camelToSnake(reason.server)}`;
  }
  return `client_${camelToSnake(reason.client)}`;
}

// camelToSnake is used to convert a protocol ErrorKind into a suitable
// metric name, eg AuthInvalidated => auth_invalidated. It converts
// both PascalCase and camelCase to snake_case.
function camelToSnake(s: string): string {
  return s
    .split(/\.?(?=[A-Z])/)
    .join('_')
    .toLowerCase();
}

class TimedOutError extends Error {
  constructor(m: string) {
    super(`${m} timed out`);
  }
}

class CloseError extends Error {}
