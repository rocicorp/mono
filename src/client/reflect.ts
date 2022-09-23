import {consoleLogSink, LogContext, TeeLogSink} from '@rocicorp/logger';
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
} from 'replicache';
import type {Downstream} from '../protocol/down.js';
import type {PingMessage} from '../protocol/ping.js';
import type {PokeBody} from '../protocol/poke.js';
import type {PushBody, PushMessage} from '../protocol/push.js';
import {NullableVersion, nullableVersionSchema} from '../types/version.js';
import {assert} from '../util/asserts.js';
import {GapTracker} from '../util/gap-tracker.js';
import {Lock} from '../util/lock.js';
import {resolver} from '../util/resolver.js';
import {sleep} from '../util/sleep.js';
import type {ReflectOptions} from './options.js';

export const enum ConnectionState {
  Disconnected,
  Connecting,
  Connected,
}

const POKE_BATCH_DEBOUNCE_MS = 20;
const JIFFY_MS = 20;

export class Reflect<MD extends MutatorDefs> {
  private readonly _rep: Replicache<MD>;
  private readonly _socketOrigin: string;
  readonly userID: string;
  readonly roomID: string;
  private readonly _l: LogContext;

  // Protects _handlePoke. We need pokes to be serialized, otherwise we
  // can cause out of order poke errors.
  private readonly _pokeLock = new Lock();

  // A batch of pokes is a series of pokes received within POKE_RECV_DEBOUNCE_MS
  private readonly _pokeBatch: PokeBody[] = [];
  private _batchStartFrame = 0; // server frame
  private _processPokeBatchStartTime = 0; // client time
  private _processingPokeBatch = false;

  private readonly _pushTracker: GapTracker;
  private readonly _updateTracker: GapTracker;
  private readonly _timestampTracker: GapTracker;

  private _lastMutationIDSent = -1;
  private _onPong: () => void = () => undefined;

  /**
   * `onOnlineChange` is called when the Reflect instance's online status
   * changes.
   */
  onOnlineChange: ((online: boolean) => void) | null = null;
  onSocketError: ((msg: string) => void) | null = null;

  private _connectResolver = resolver<WebSocket>();
  private _lastMutationIDReceived = 0;

  protected _WSClass = WebSocket;
  protected _socket: WebSocket | undefined = undefined;
  protected _state: ConnectionState = ConnectionState.Disconnected;

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

    if (options.onOnlineChange) {
      this.onOnlineChange = options.onOnlineChange;
    }

    const replicacheOptions: ReplicacheOptions<MD> = {
      auth: options.auth,
      schemaVersion: options.schemaVersion,
      logLevel: options.logLevel,
      logSinks: options.logSinks,
      mutators: options.mutators,
      name: `reflect-${options.userID}-${options.roomID}`,
      pusher: (req: Request) => this._pusher(req),
      // TODO: Do we need these?
      // TODO: figure out backoff?
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
    this._rep.getAuth = options.getAuth;
    this._socketOrigin = options.socketOrigin;
    this.roomID = options.roomID;
    this.userID = options.userID;

    const {logSinks = [consoleLogSink]} = options;
    const logSink =
      logSinks.length === 1 ? logSinks[0] : new TeeLogSink(logSinks);
    this._l = new LogContext(options.logLevel, logSink).addContext(
      'roomID',
      options.roomID,
    );

    this._pushTracker = new GapTracker('push', this._l);
    this._updateTracker = new GapTracker('update', this._l);
    this._timestampTracker = new GapTracker('timestamp', this._l);
    void this._watchdog();
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
   * The authorization token used when opening a WebSocket connection to
   * the Reflect server.
   */
  get auth(): string {
    return this._rep.auth;
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
    this._disconnect();
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
  subscribe<R extends ReadonlyJSONValue | undefined, E>(
    body: (tx: ReadTransaction) => Promise<R>,
    {
      onData,
      onError,
      onDone,
    }: {
      onData: (result: R) => void;
      onError?: (error: E) => void;
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
  async query<R>(body: (tx: ReadTransaction) => Promise<R> | R): Promise<R> {
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

  private _onMessage = (e: MessageEvent<string>) => {
    const l = this._l;
    l.addContext('req', nanoid());
    l.debug?.('received message', e.data);
    if (this.closed) {
      l.debug?.('ignoring message because already closed');
      return;
    }

    const data = JSON.parse(e.data);
    const downMessage = data as Downstream; //downstreamSchema.parse(data);

    if (downMessage[0] === 'connected') {
      l.info?.('Connected');

      this._state = ConnectionState.Connected;
      this._lastMutationIDSent = -1;
      assert(this._socket);
      this._connectResolver.resolve(this._socket);
      this.onOnlineChange?.(true);
      return;
    }

    if (downMessage[0] === 'error') {
      this.onSocketError?.(downMessage[1]);
      throw new Error(downMessage[1]);
    }

    if (downMessage[0] === 'pong') {
      this._onPong();
      return;
    }

    if (downMessage[0] !== 'poke') {
      throw new Error(`Unexpected message: ${downMessage}`);
    }

    const pokeBody = downMessage[1];

    this._pokeBatch.push(pokeBody);
    void this._processPokeBatch();
  };

  private _onClose = (e: CloseEvent) => {
    const l = this._l;
    l.info?.('got socket close event', e);
    this._disconnect();
  };

  private async _connect(l: LogContext) {
    if (this._state === ConnectionState.Connecting) {
      l.debug?.('Skipping duplicate connect request');
      return;
    }
    l.info?.('Connecting...');

    this._state = ConnectionState.Connecting;

    const baseCookie = await getBaseCookie(this._rep);
    // TODO if connection fails with 401 use this._rep.getAuth to
    // try to refresh this._rep.auth and then retry connection
    const ws = createSocket(
      this._socketOrigin,
      baseCookie,
      await this._rep.clientID,
      this.roomID,
      this._rep.auth,
      this._lastMutationIDReceived,
      this._WSClass,
    );

    ws.addEventListener('message', this._onMessage);
    ws.addEventListener('close', this._onClose);
    this._socket = ws;
  }

  private _disconnect() {
    this._l.info?.('disconnecting');
    if (this._state === ConnectionState.Connected) {
      // Only create a new resolver if the one we have was previously resolved,
      // which happens when the socket became connected.
      this._connectResolver = resolver();
      this.onOnlineChange?.(false);
    }
    this._state = ConnectionState.Disconnected;
    this._socket?.removeEventListener('message', this._onMessage);
    this._socket?.removeEventListener('close', this._onClose);
    this._socket?.close();
    this._socket = undefined;
    this._lastMutationIDSent = -1;
  }

  private async _handlePoke(l: LogContext, pokeBody: PokeBody) {
    await this._pokeLock.withLock(async () => {
      l.debug?.('Applying poke', pokeBody);

      this._updateTracker.push(performance.now());
      this._timestampTracker.push(pokeBody.timestamp);

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
        if (String(e).indexOf('unexpected base cookie for poke') > -1) {
          this._l.info?.('out of order poke, disconnecting');
          this._disconnect();
          return;
        }
        throw e;
      }
    });
  }

  private async _pusher(req: Request) {
    if (!this._socket) {
      void this._connect(this._l);
    }

    const socket = await this._connectResolver.promise;

    const pushBody = (await req.json()) as PushBody;

    for (const m of pushBody.mutations) {
      if (m.id > this._lastMutationIDSent) {
        this._lastMutationIDSent = m.id;

        const msg: PushMessage = [
          'push',
          {...pushBody, mutations: [m], timestamp: performance.now()},
        ];

        this._pushTracker.push(performance.now());
        socket.send(JSON.stringify(msg));
      }
    }

    return {
      errorMessage: '',
      httpStatusCode: 200,
    };
  }

  // this will apply the current poke batch over time
  private async _processPokeBatch() {
    if (this._processingPokeBatch) {
      return;
    }

    this._processingPokeBatch = true;

    await sleep(POKE_BATCH_DEBOUNCE_MS);

    // ! overloading timestamp field to store the frame number
    this._batchStartFrame = this._pokeBatch[0].timestamp;
    this._processPokeBatchStartTime = performance.now();

    void this._applyPokeBatch();
  }

  private async _applyPokeBatch() {
    const dt = performance.now() - this._processPokeBatchStartTime;

    // wrongly assumes that successive frames are approx JIFFY_MS apart
    while (this._pokeBatch.length > 0) {
      const curPoke = this._pokeBatch[0];

      // ! overloading timestamp field to store the frame number
      const frame = curPoke.timestamp;

      const pokeApplyTime = (frame - this._batchStartFrame) * JIFFY_MS;

      if (pokeApplyTime > dt) {
        break;
      }

      await this._handlePoke(this._l, curPoke);
      this._pokeBatch.shift();
    }

    if (this._pokeBatch.length === 0) {
      this._processingPokeBatch = false;
      // console.log('exiting poke loop');
      return;
    }

    requestAnimationFrame(() => {
      void this._applyPokeBatch();
    });
  }

  private async _watchdog() {
    while (!this.closed) {
      const l = this._l.addContext('req', nanoid());
      l.debug?.('watchdog fired');
      if (this._state === ConnectionState.Connected) {
        await this._ping(l);
      } else {
        void this._connect(l);
      }
      await sleep(5000);
    }
  }

  private async _ping(l: LogContext) {
    l.debug?.('pinging');
    const {promise, resolve} = resolver();
    this._onPong = resolve;
    const pingMessage: PingMessage = ['ping', {}];
    const t0 = performance.now();
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    this._socket!.send(JSON.stringify(pingMessage));
    const connected = await Promise.race([
      promise.then(() => true),
      sleep(2000).then(() => false),
    ]);
    if (this._state !== ConnectionState.Connected) {
      return;
    }
    const delta = performance.now() - t0;
    if (connected) {
      l.debug?.('ping succeeded in', delta, 'ms');
    } else {
      l.info?.('ping failed in', delta, 'ms - disconnecting');
      this._disconnect();
    }
  }
}

// Total hack to get base cookie
async function getBaseCookie(rep: Replicache) {
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
  return await promise;
}

export function createSocket(
  socketOrigin: string,
  baseCookie: NullableVersion,
  clientID: string,
  roomID: string,
  auth: string,
  lmid: number,
  wsClass: typeof WebSocket,
): WebSocket {
  const url = new URL(socketOrigin);
  url.pathname = '/connect';
  const {searchParams} = url;
  searchParams.set('clientID', clientID);
  searchParams.set('roomID', roomID);
  searchParams.set('baseCookie', baseCookie === null ? '' : String(baseCookie));
  searchParams.set('ts', String(performance.now()));
  searchParams.set('lmid', String(lmid));
  // Pass auth to the server via the `Sec-WebSocket-Protocol` header by passing
  // it as a `protocol` to the `WebSocket` constructor.  The empty string is an
  // invalid `protocol`, and will result in an exception, so pass undefined
  // instead.  encodeURIComponent to ensure it only contains chars allowed
  // for a `protocol`.
  return new wsClass(
    url.toString(),
    auth === '' ? undefined : encodeURIComponent(auth),
  );
}
