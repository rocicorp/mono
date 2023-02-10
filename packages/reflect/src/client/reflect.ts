import {consoleLogSink, LogContext, TeeLogSink} from '@rocicorp/logger';
import {Resolver, resolver} from '@rocicorp/resolver';
import {Lock} from '@rocicorp/lock';
import {nanoid} from 'nanoid';
import {
  MutatorDefs,
  Poke,
  PullerResult,
  ReadonlyJSONValue,
  ReadTransaction,
  Replicache,
  ReplicacheOptions,
  ExperimentalWatchNoIndexCallback,
  ExperimentalWatchOptions,
  ExperimentalWatchCallbackForOptions,
  MaybePromise,
} from 'replicache';
import type {Downstream} from '../protocol/down.js';
import type {PingMessage} from '../protocol/ping.js';
import type {PokeBody} from '../protocol/poke.js';
import type {PushBody, PushMessage} from '../protocol/push.js';
import {NullableVersion, nullableVersionSchema} from '../types/version.js';
import {assert} from '../util/asserts.js';
import {sleep} from '../util/sleep.js';
import type {ReflectOptions} from './options.js';
import {
  Gauge,
  State,
  DID_NOT_CONNECT_VALUE,
  NopMetrics,
  Metric,
  camelToSnake,
} from './metrics.js';
import {send} from '../util/socket.js';
import type {ConnectedMessage} from '../protocol/connected.js';
import {ErrorKind, type ErrorMessage} from '../protocol/error.js';
import {MessageError, isAuthError} from './connection-error.js';

export const enum ConnectionState {
  Disconnected,
  Connecting,
  Connected,
}

export const RUN_LOOP_INTERVAL_MS = 5_000;
export const MAX_RUN_LOOP_INTERVAL_MS = 60_000;

export const enum CloseKind {
  AbruptClose = 'AbruptClose',
  CleanClose = 'CleanClose',
  ReflectClosed = 'ReflectClosed',
  Unknown = 'Unknown',
}

export type DisconnectReason = ErrorKind | CloseKind;

/**
 * How frequently we should ping the server to keep the connection alive.
 */
export const PING_INTERVAL_MS = 5_000;

/**
 * The amount of time we wait for a pong before we consider the ping timed out.
 */
export const PING_TIMEOUT_MS = 2_000;

/**
 * The amount of time we wait for a connection to be established before we
 * consider it timed out.
 */
export const CONNECT_TIMEOUT_MS = 10_000;

export class Reflect<MD extends MutatorDefs> {
  private readonly _rep: Replicache<MD>;
  private readonly _socketOrigin: string;
  readonly userID: string;
  readonly roomID: string;

  // This is a promise because it is waiting for the clientID from the
  // Replicache instance.
  private readonly _l: Promise<LogContext>;

  private readonly _metrics: {
    timeToConnectMs: Gauge;
    lastConnectError: State;
  };

  // Protects _handlePoke. We need pokes to be serialized, otherwise we
  // can cause out of order poke errors.
  private readonly _pokeLock = new Lock();

  private _lastMutationIDSent = -1;
  private _onPong: () => void = () => undefined;

  #online = false;

  /**
   * `onOnlineChange` is called when the Reflect instance's online status
   * changes.
   */
  onOnlineChange: ((online: boolean) => void) | null | undefined = null;

  private _connectResolver = resolver<void>();
  private _lastMutationIDReceived = 0;

  private _socket: WebSocket | undefined = undefined;
  protected _socketResolver = resolver<WebSocket>();

  #connectionStateChangeResolver = resolver<ConnectionState>();

  #nextMessageResolver: Resolver<Downstream> | undefined = undefined;

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

  /**
   * Constructs a new Reflect client.
   */
  constructor(options: ReflectOptions<MD>) {
    if (options.userID === '') {
      throw new Error('ReflectOptions.userID must not be empty.');
    }
    const {socketOrigin} = options;
    if (socketOrigin) {
      if (
        !socketOrigin.startsWith('ws://') &&
        !socketOrigin.startsWith('wss://')
      ) {
        throw new Error(
          "ReflectOptions.socketOrigin must use the 'ws' or 'wss' scheme.",
        );
      }
    }

    this.onOnlineChange = options.onOnlineChange;
    this.#options = options;

    const metrics = options.metrics ?? new NopMetrics();
    this._metrics = {
      // timeToConnectMs measures the time from the call to connect() to receiving
      // the 'connected' ws message. We record the DID_NOT_CONNECT_VALUE if the previous
      // connection attempt failed for any reason.
      //
      // We set the gauge using _connectingStart as follows:
      // - _connectingStart is undefined if we are disconnected or connected; it is
      //   defined only in the Connecting state, as a number representing the timestamp
      //   at which we started connecting.
      // - _connectingStart is set to the current time when connect() is called.
      // - When we receive the 'connected' message we record the time to connect and
      //   set _connectingStart to undefined.
      // - If disconnect() is called with a defined _connectingStart then we record
      //   DID_NOT_CONNECT_VALUE and set _connectingStart to undefined.
      //
      // TODO It's clear after playing with the connection code we should encapsulate
      // the ConnectionState along with its state transitions and possibly behavior.
      // In that world the metric gauge(s) and bookkeeping like _connectingStart would
      // be encapsulated with the ConnectionState. This will probably happen as part
      // of https://github.com/rocicorp/reflect-server/issues/255.
      timeToConnectMs: metrics.gauge(Metric.TimeToConnectMs),

      // lastConnectError records the last error that occurred when connecting,
      // if any. It is cleared when connecting successfully or when reported, so this
      // state only gets reported if there was a failure during the reporting period and
      // we are still not connected.
      lastConnectError: metrics.state(
        Metric.LastConnectError,
        true, // clearOnFlush
      ),
    };
    this._metrics.timeToConnectMs.set(DID_NOT_CONNECT_VALUE);

    const replicacheOptions: ReplicacheOptions<MD> = {
      schemaVersion: options.schemaVersion,
      logLevel: options.logLevel,
      logSinks: options.logSinks,
      mutators: options.mutators,
      name: `reflect-${options.userID}-${options.roomID}`,
      pusher: (req: Request) => this._pusher(req),
      // TODO: Do we need these?
      pushDelay: 0,
      requestOptions: {
        maxDelayMs: 0,
        minDelayMs: 0,
      },
      licenseKey: 'reflect-client-static-key',
    };
    const replicacheInternalOptions = {
      enableLicensing: false,
      enableMutationRecovery: false,
    };

    this._rep = new Replicache({
      ...replicacheOptions,
      ...replicacheInternalOptions,
    });
    this._rep.getAuth = this.#getAuthToken;

    this._socketOrigin = options.socketOrigin;
    this.roomID = options.roomID;
    this.userID = options.userID;
    this._l = getLogContext(options, this._rep);

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
      this._disconnect(lc, CloseKind.ReflectClosed);
    }
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

    const rejectInvalidMessage = () =>
      this.#nextMessageResolver?.reject(
        new MessageError(ErrorKind.InvalidMessage, `Invalid message: ${data}`),
      );

    let downMessage: Downstream;
    const {data} = e;
    try {
      // TODO: validate this, at least in debug mode:
      // https://github.com/rocicorp/reflect-server/issues/225
      downMessage = JSON.parse(data) as Downstream; //downstreamSchema.parse(data);
    } catch (e) {
      rejectInvalidMessage();
      return;
    }

    switch (downMessage[0]) {
      case 'connected':
        this._handleConnectedMessage(l, downMessage);
        return;

      case 'error':
        this._handleErrorMessage(l, downMessage);
        return;

      case 'pong':
        this._onPong();
        return;

      case 'poke':
        void this._handlePoke(l, downMessage[1]);
        this.#nextMessageResolver?.resolve(downMessage);
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

    const closeKind = wasClean ? CloseKind.CleanClose : CloseKind.AbruptClose;
    this._connectResolver.reject(new CloseError(closeKind));
    this._disconnect(l, closeKind);
  };

  // An error on the connection is fatal for the connection.
  private _handleErrorMessage(lc: LogContext, downMessage: ErrorMessage): void {
    const [, kind, message] = downMessage;

    const error = new MessageError(kind, message);

    lc.info?.(`${kind}: ${message}}`);

    this.#nextMessageResolver?.reject(error);
    this._connectResolver.reject(error);
    this._disconnect(lc, kind);
  }

  private _handleConnectedMessage(
    lc: LogContext,
    connectedMessage: ConnectedMessage,
  ) {
    lc = addWebSocketIDToLogContext(connectedMessage[1].wsid, lc);
    lc.info?.('Connected', {navigatorOnline: navigator.onLine});

    this._connectionState = ConnectionState.Connected;
    this._metrics.lastConnectError.clear();
    if (this._connectingStart === undefined) {
      lc.error?.(
        'Got connected message but connect start time is undefined. This should not happen.',
      );
    } else {
      this._metrics.timeToConnectMs.set(Date.now() - this._connectingStart);
      this._connectingStart = undefined;
    }

    this._lastMutationIDSent = -1;
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

    // Create a new resolver for the next connection attempt. The previous
    // promise should have been resolved/rejected because we do not allow
    // overlapping connect calls.
    this._connectResolver = resolver();

    const baseCookie = await getBaseCookie(this._rep);

    // Reject connect after a timeout.
    const id = setTimeout(() => {
      this._connectResolver.reject(
        new MessageError(ErrorKind.ConnectTimeout, 'Timed out connecting'),
      );
      this._disconnect(l, ErrorKind.ConnectTimeout);
    }, CONNECT_TIMEOUT_MS);
    const clear = () => clearTimeout(id);
    this._connectResolver.promise.then(clear, clear);
    // We clear the timeout in _disconnect and _handleConnectedMessage.

    const ws = createSocket(
      this._socketOrigin,
      baseCookie,
      await this._rep.clientID,
      this.roomID,
      this._rep.auth,
      this._lastMutationIDReceived,
      wsid,
    );

    ws.addEventListener('message', this._onMessage);
    ws.addEventListener('close', this._onClose);
    this._socket = ws;
    this._socketResolver.resolve(ws);
  }

  private _disconnect(l: LogContext, reason: DisconnectReason): void {
    l.info?.('disconnecting', {navigatorOnline: navigator.onLine, reason});

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
        this._metrics.lastConnectError.set(camelToSnake(reason));
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

    this._connectionState = ConnectionState.Disconnected;

    this._connectingStart = undefined;
    this._socket?.removeEventListener('message', this._onMessage);
    this._socket?.removeEventListener('close', this._onClose);
    this._socket?.close();
    this._socket = undefined;
    this._lastMutationIDSent = -1;
  }

  private async _handlePoke(lc: LogContext, pokeBody: PokeBody) {
    await this._pokeLock.withLock(async () => {
      lc = lc.addContext('requestID', pokeBody.requestID);
      lc.debug?.('Applying poke', pokeBody);

      const {lastMutationID, baseCookie, patch, cookie} = pokeBody;
      this._lastMutationIDReceived = lastMutationID;
      const p: Poke = {
        baseCookie,
        pullResponse: {
          lastMutationID,
          patch,
          cookie,
        },
      };

      try {
        await this._rep.poke(p);
      } catch (e) {
        // TODO(arv): Structured error for poke!
        if (String(e).indexOf('unexpected base cookie for poke') > -1) {
          lc.info?.('out of order poke, disconnecting');
          // This technically happens *after* connection establishment, but
          // we record it as a connect error here because it is the kind of
          // thing that we want to hear about (and is sorta connect failure
          // -ish).
          this._metrics.lastConnectError.set(
            camelToSnake(ErrorKind.UnexpectedBaseCookie),
          );

          // It is theoretically possible that we get disconnected during the
          // async poke above. Only disconnect if we are not already
          // disconnected.
          if (this._connectionState !== ConnectionState.Disconnected) {
            this._disconnect(lc, ErrorKind.UnexpectedBaseCookie);
          }
          return;
        }
        throw e;
      }
    });
  }

  private async _pusher(req: Request) {
    // If we are connecting we wait until we are connected.
    await this._connectResolver.promise;

    const socket = this._socket;
    assert(socket);

    // TODO(arv): With DD31 the Pusher type gets the requestID as an argument.
    const requestID = req.headers.get('X-Replicache-RequestID');
    assert(requestID);

    const pushBody = (await req.json()) as PushBody;

    for (const m of pushBody.mutations) {
      if (m.id > this._lastMutationIDSent) {
        this._lastMutationIDSent = m.id;

        const pushMessage: PushMessage = [
          'push',
          {
            ...pushBody,
            mutations: [m],
            timestamp: performance.now(),
            requestID,
          },
        ];
        send(socket, pushMessage);
      }
    }

    return {
      errorMessage: '',
      httpStatusCode: 200,
    };
  }

  #getAuthToken = (): MaybePromise<string> => {
    const {auth} = this.#options;
    return typeof auth === 'function' ? auth() : auth;
  };

  async #updateAuthToken(lc: LogContext): Promise<void> {
    const auth = await this.#getAuthToken();
    lc.debug?.('Got auth token');
    this._rep.auth = auth;
  }

  private async _runLoop() {
    let runLoopCounter = 0;
    const bareLogContext = await this._l;
    const getLogContext = () => {
      let lc = bareLogContext;
      if (this._socket) {
        lc = addWebSocketIDFromSocketToLogContext(this._socket, lc);
      }
      return lc.addContext('runLoopCounter', runLoopCounter);
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
            const pingTimeoutPromise = sleep(PING_INTERVAL_MS);

            this.#nextMessageResolver = resolver();

            let pingTimerFired = false;
            await Promise.race([
              pingTimeoutPromise.then(() => {
                pingTimerFired = true;
              }),
              this.#connectionStateChangeResolver.promise,
              this.#nextMessageResolver.promise,
            ]);

            this.#nextMessageResolver = undefined;

            if (this.closed) {
              break;
            }

            if (pingTimerFired) {
              await this._ping(lc);
            }
          }
        }
      } catch (ex) {
        if (this._connectionState !== ConnectionState.Connected) {
          lc.error?.('Failed to connect', ex);
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
            // First auth error, try right away without backoff.
            continue;
          }
          needsReauth = true;
        }

        errorCount++;
      }

      // Only authentication errors are retried immediately the first time they
      // occur. All other errors wait a few seconds before retrying the first
      // time. Consecutive errors use a backoff.

      if (errorCount > 0) {
        this.#setOnline(false);

        const duration = Math.min(
          MAX_RUN_LOOP_INTERVAL_MS,
          2 ** (errorCount - 1) * RUN_LOOP_INTERVAL_MS,
        );
        lc.debug?.(
          'Sleeping',
          duration,
          'ms before reconnecting due to error count',
          errorCount,
          'state:',
          this._connectionState,
        );
        await sleep(duration);
      }
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
   * Returns false if the ping timed out.
   */
  private async _ping(l: LogContext): Promise<void> {
    l.debug?.('pinging');
    const {promise, resolve} = resolver();
    this._onPong = resolve;
    const pingMessage: PingMessage = ['ping', {}];
    const t0 = performance.now();
    assert(this._socket);
    send(this._socket, pingMessage);

    const connected = await Promise.race([
      promise.then(() => true),
      sleep(PING_TIMEOUT_MS).then(() => false),
    ]);
    if (this._connectionState !== ConnectionState.Connected) {
      return;
    }

    const delta = performance.now() - t0;
    if (connected) {
      l.debug?.('ping succeeded in', delta, 'ms');
    } else {
      l.info?.('ping failed in', delta, 'ms - disconnecting');
      this._disconnect(l, ErrorKind.PingTimeout);
      throw new MessageError(ErrorKind.PingTimeout, 'Ping timed out');
    }
  }
}

// Total hack to get base cookie
function getBaseCookie(rep: Replicache) {
  const {promise, resolve} = resolver<NullableVersion>();
  rep.puller = async (req): Promise<PullerResult> => {
    const val = await req.json();
    const parsed = nullableVersionSchema.parse(val.cookie);
    resolve(parsed);
    return {
      httpRequestInfo: {
        errorMessage: '',
        httpStatusCode: 200,
      },
    };
  };
  rep.pull();
  return promise;
}

export function createSocket(
  socketOrigin: string,
  baseCookie: NullableVersion,
  clientID: string,
  roomID: string,
  auth: string,
  lmid: number,
  wsid: string,
): WebSocket {
  const url = new URL(socketOrigin);
  url.pathname = '/connect';
  const {searchParams} = url;
  searchParams.set('clientID', clientID);
  searchParams.set('roomID', roomID);
  searchParams.set('baseCookie', baseCookie === null ? '' : String(baseCookie));
  searchParams.set('ts', String(performance.now()));
  searchParams.set('lmid', String(lmid));
  searchParams.set('wsid', wsid);
  // Pass auth to the server via the `Sec-WebSocket-Protocol` header by passing
  // it as a `protocol` to the `WebSocket` constructor.  The empty string is an
  // invalid `protocol`, and will result in an exception, so pass undefined
  // instead.  encodeURIComponent to ensure it only contains chars allowed
  // for a `protocol`.
  return new WebSocket(url, auth === '' ? undefined : encodeURIComponent(auth));
}

async function getLogContext<MD extends MutatorDefs>(
  options: ReflectOptions<MD>,
  rep: Replicache<MD>,
) {
  const {logSinks = [consoleLogSink]} = options;
  const logSink =
    logSinks.length === 1 ? logSinks[0] : new TeeLogSink(logSinks);
  return new LogContext(options.logLevel, logSink)
    .addContext('roomID', options.roomID)
    .addContext('clientID', await rep.clientID);
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
  return lc.addContext('wsid', wsid);
}
class CloseError extends Error {
  readonly name = 'CloseError';
  readonly kind: CloseKind;
  constructor(closeKind: CloseKind) {
    super(`socket closed (${closeKind})`);
    this.kind = closeKind;
  }
}
