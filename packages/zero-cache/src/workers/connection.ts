import {pipeline, Readable, Writable} from 'node:stream';
import type {LogContext, LogLevel} from '@rocicorp/logger';
import type {CloseEvent, Data, ErrorEvent} from 'ws';
import WebSocket, {createWebSocketStream} from 'ws';
import {assert} from '../../../shared/src/asserts.ts';
import * as valita from '../../../shared/src/valita.ts';
import type {ConnectedMessage} from '../../../zero-protocol/src/connect.ts';
import type {Downstream} from '../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import type {ErrorBody} from '../../../zero-protocol/src/error.ts';
import {
  isProtocolError,
  type ProtocolError,
} from '../../../zero-protocol/src/error.ts';
import {
  POKE_CHUNK_PROTOCOL_VERSION,
  type PokeChunk,
  type PokeEndMessage,
  type PokePartMessage,
} from '../../../zero-protocol/src/poke.ts';
import {
  MIN_SERVER_SUPPORTED_SYNC_PROTOCOL,
  PROTOCOL_VERSION,
} from '../../../zero-protocol/src/protocol-version.ts';
import {
  upstreamSchemaWithUnparsedAnalyzeQuery,
  type UpstreamWithUnparsedAnalyzeQuery,
} from '../../../zero-protocol/src/up.ts';
import {getOrCreateCounter} from '../observability/metrics.ts';
import type {ViewSyncerDownstream} from '../types/downstream.ts';
import {
  ProtocolErrorWithLevel,
  getLogLevel,
  wrapWithProtocolError,
} from '../types/error-with-level.ts';
import {PokeChunkEncoder} from '../types/poke-chunk.ts';
import type {Source} from '../types/streams.ts';
import type {ConnectParams} from './connect-params.ts';

export type HandlerResult =
  | {
      type: 'ok';
    }
  | {
      type: 'fatal';
      error: ErrorBody;
    }
  | {
      type: 'transient';
      errors: ErrorBody[];
    }
  | StreamResult;

export type StreamResult =
  | {
      type: 'stream';
      source: 'viewSyncer';
      stream: Source<ViewSyncerDownstream>;
    }
  | {
      type: 'stream';
      source: 'pusher';
      stream: Source<Downstream>;
    };

export interface MessageHandler {
  handleMessage(
    msg: UpstreamWithUnparsedAnalyzeQuery,
  ): Promise<HandlerResult[]>;
}

function hasOwn(value: unknown, property: string): boolean {
  return (
    typeof value === 'object' &&
    value !== null &&
    Object.hasOwn(value, property)
  );
}

function containsLegacyQuery(message: unknown): boolean {
  if (!Array.isArray(message)) {
    return false;
  }

  const body = message[1];
  if (typeof body !== 'object' || body === null) {
    return false;
  }

  if (
    message[0] !== 'initConnection' &&
    message[0] !== 'changeDesiredQueries'
  ) {
    return false;
  }

  const patch = body['desiredQueriesPatch'];
  return (
    Array.isArray(patch) && patch.some(operation => hasOwn(operation, 'ast'))
  );
}

const LEGACY_QUERIES_DISABLED_MESSAGE =
  'Legacy queries are disabled by this Zero server. This client must use custom queries instead of sending query ASTs directly. To temporarily restore compatibility, set ZERO_ALLOW_LEGACY_QUERIES=true on zero-cache.';

// Exported for testing purposes.
export function parseUpstreamMessage(
  value: unknown,
  allowLegacyQueries: boolean,
): UpstreamWithUnparsedAnalyzeQuery {
  if (!allowLegacyQueries && containsLegacyQuery(value)) {
    throw new Error(LEGACY_QUERIES_DISABLED_MESSAGE);
  }
  return valita.parse(value, upstreamSchemaWithUnparsedAnalyzeQuery);
}

// Ensures that a downstream message is sent at least every interval, sending a
// 'pong' if necessary. This is set to be slightly longer than the client-side
// PING_INTERVAL of 5 seconds, so that in the common case, 'pong's are sent in
// response to client-initiated 'ping's. However, if the inbound stream is
// backed up because a command is taking a long time to process, the pings
// will be stuck in the queue (i.e. back-pressured), in which case pongs will
// be manually sent to notify the client of server liveness.
//
// This is equivalent to what is done for Postgres keepalives on the
// replication stream (which can similarly be back-pressured):
// https://github.com/rocicorp/mono/blob/f98cb369a2dbb15650328859c732db358f187ef0/packages/zero-cache/src/services/change-source/pg/logical-replication/stream.ts#L21
const DOWNSTREAM_MSG_INTERVAL_MS = 6_000;
export const WEBSOCKET_SEND_TIMEOUT_MS = 10_000;
const PROTOCOL_VERSION_ATTRIBUTE = 'protocol.version';
const EVENT_TYPE_ATTRIBUTE = 'event.type';

/**
 * Represents a connection between the client and server.
 *
 * Handles incoming messages on the connection and dispatches
 * them to the correct service.
 *
 * Listens to the ViewSyncer and sends messages to the client.
 */
export class Connection {
  readonly #ws: WebSocket;
  readonly #wsID: string;
  readonly #protocolVersion: number;
  readonly #lc: LogContext;
  readonly #onClose: () => void;
  readonly #messageHandler: MessageHandler;
  readonly #allowLegacyQueries: boolean;
  readonly #downstreamSender: DownstreamSender;
  readonly #downstreamMsgTimer: NodeJS.Timeout | undefined;
  readonly #webSocketErrors = getOrCreateCounter(
    'sync',
    'websocket.errors',
    'Client WebSocket error events.',
  );

  #viewSyncerOutboundStream: Source<ViewSyncerDownstream> | undefined;
  #pusherOutboundStream: Source<Downstream> | undefined;
  #closed = false;

  constructor(
    lc: LogContext,
    connectParams: ConnectParams,
    ws: WebSocket,
    allowLegacyQueries: boolean,
    messageHandler: MessageHandler,
    onClose: () => void,
  ) {
    const {clientGroupID, clientID, wsID, protocolVersion} = connectParams;
    this.#messageHandler = messageHandler;
    this.#allowLegacyQueries = allowLegacyQueries;

    this.#ws = ws;
    this.#wsID = wsID;
    this.#protocolVersion = protocolVersion;

    this.#lc = lc
      .withContext('connection')
      .withContext('clientID', clientID)
      .withContext('clientGroupID', clientGroupID)
      .withContext('wsID', wsID);
    this.#downstreamSender = new DownstreamSender(
      this.#lc,
      ws,
      protocolVersion,
    );
    this.#lc.debug?.('new connection');
    this.#onClose = onClose;

    this.#ws.addEventListener('close', this.#handleClose);
    this.#ws.addEventListener('error', this.#handleError);

    this.#proxyInbound();
    this.#downstreamMsgTimer = setInterval(
      this.#maybeSendPong,
      DOWNSTREAM_MSG_INTERVAL_MS / 2,
    );
  }

  /**
   * Checks the protocol version and errors for unsupported protocols,
   * sending the initial `connected` response on success.
   *
   * This is early in the connection lifecycle because {@link #handleMessage}
   * will only parse messages with schema(s) of supported protocol versions.
   */
  init(): boolean {
    if (
      this.#protocolVersion > PROTOCOL_VERSION ||
      this.#protocolVersion < MIN_SERVER_SUPPORTED_SYNC_PROTOCOL
    ) {
      this.#closeWithError({
        kind: ErrorKind.VersionNotSupported,
        message: `server is at sync protocol v${PROTOCOL_VERSION} and does not support v${
          this.#protocolVersion
        }. The ${
          this.#protocolVersion > PROTOCOL_VERSION ? 'server' : 'client'
        } must be updated to a newer release.`,
        origin: ErrorOrigin.ZeroCache,
      });
    } else {
      const connectedMessage: ConnectedMessage = [
        'connected',
        {wsid: this.#wsID, timestamp: Date.now()},
      ];
      this.send(connectedMessage, 'ignore-backpressure');
      return true;
    }
    return false;
  }

  close(reason: string, ...args: unknown[]) {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#lc.info?.(`closing connection: ${reason}`, ...args);
    this.#ws.removeEventListener('close', this.#handleClose);
    this.#ws.removeEventListener('error', this.#handleError);
    this.#viewSyncerOutboundStream?.cancel();
    this.#viewSyncerOutboundStream = undefined;
    this.#pusherOutboundStream?.cancel();
    this.#pusherOutboundStream = undefined;
    this.#onClose();
    if (this.#ws.readyState !== this.#ws.CLOSED) {
      this.#ws.close();
    }
    clearTimeout(this.#downstreamMsgTimer);

    // spin down services if we have
    // no more client connections for the client group?
  }

  handleInitConnection(initConnectionMsg: string) {
    return this.#handleMessage({data: initConnectionMsg});
  }

  #handleMessage = async (event: {data: Data}) => {
    const data = event.data.toString();
    if (this.#closed) {
      this.#lc.debug?.('Ignoring message received after closed', data);
      return;
    }

    let msg;
    try {
      const value = JSON.parse(data);
      msg = parseUpstreamMessage(value, this.#allowLegacyQueries);
    } catch (e) {
      const errorBody = {
        kind: ErrorKind.InvalidMessage,
        message: String(e),
        origin: ErrorOrigin.ZeroCache,
      } as const;
      this.#closeWithError(
        errorBody,
        new ProtocolErrorWithLevel(errorBody, 'warn'),
      );
      return;
    }

    try {
      const msgType = msg[0];
      if (msgType === 'ping') {
        this.send(['pong', {}], 'ignore-backpressure');
        return;
      }

      const result = await this.#messageHandler.handleMessage(msg);
      for (const r of result) {
        this.#handleMessageResult(r);
      }
    } catch (e) {
      this.#closeWithThrown(e);
    }
  };

  #handleMessageResult(result: HandlerResult): void {
    switch (result.type) {
      case 'fatal':
        this.#closeWithError(result.error);
        break;
      case 'ok':
        break;
      case 'stream': {
        switch (result.source) {
          case 'viewSyncer':
            assert(
              this.#viewSyncerOutboundStream === undefined,
              'Outbound stream already set for this connection!',
            );
            this.#viewSyncerOutboundStream = result.stream;
            break;
          case 'pusher':
            assert(
              this.#pusherOutboundStream === undefined,
              'Outbound stream already set for this connection!',
            );
            this.#pusherOutboundStream = result.stream;
            break;
        }
        if (result.source === 'viewSyncer') {
          this.#proxyOutbound(result.stream, (downstream, callback) =>
            this.send(downstream.message, callback, downstream.serialized),
          );
        } else {
          this.#proxyOutbound(result.stream, (downstream, callback) =>
            this.send(downstream, callback),
          );
        }
        break;
      }
      case 'transient': {
        for (const error of result.errors) {
          this.sendError(error);
        }
      }
    }
  }

  #handleClose = (e: CloseEvent) => {
    const {code, reason, wasClean} = e;
    if (!wasClean) {
      this.#recordWebSocketError('unclean_close');
    }
    this.close('WebSocket close event', {code, reason, wasClean});
  };

  #handleError = (e: ErrorEvent) => {
    this.#recordWebSocketError('error_event');
    this.#lc.warn?.('WebSocket error event', e.message, e.error);
  };

  #recordWebSocketError(eventType: string) {
    this.#webSocketErrors.add(1, {
      [PROTOCOL_VERSION_ATTRIBUTE]: this.#protocolVersion,
      [EVENT_TYPE_ATTRIBUTE]: eventType,
    });
  }

  #proxyInbound() {
    pipeline(
      createWebSocketStream(this.#ws),
      new Writable({
        write: (data, _encoding, callback) => {
          this.#handleMessage({data}).then(() => callback(), callback);
        },
      }),
      // The done callback is not used, as #handleClose and #handleError,
      // configured on the underlying WebSocket, provide more complete
      // information.
      () => {},
    );
  }

  #proxyOutbound<T>(
    outboundStream: Source<T>,
    sendMessage: (
      downstream: T,
      callback: (err?: Error | null) => void,
    ) => void,
  ) {
    // Note: createWebSocketStream() is avoided here in order to control
    //       exception handling with #closeWithThrown(). If the Writable
    //       from createWebSocketStream() were instead used, exceptions
    //       from the outboundStream result in the Writable closing the
    //       the websocket before the error message can be sent.
    pipeline(
      Readable.from(outboundStream),
      new Writable({
        objectMode: true,
        write: (downstream: T, _encoding, callback) =>
          sendMessage(downstream, callback),
      }),
      e =>
        e
          ? this.#closeWithThrown(e)
          : this.close(`downstream closed by ViewSyncer`),
    );
  }

  #closeWithThrown(e: unknown) {
    const errorBody =
      findProtocolError(e)?.errorBody ?? wrapWithProtocolError(e).errorBody;

    this.#closeWithError(errorBody, e);
  }

  #closeWithError(errorBody: ErrorBody, thrown?: unknown) {
    this.sendError(errorBody, thrown);
    this.close(
      `${errorBody.kind} (${errorBody.origin}): ${errorBody.message}`,
      errorBody,
    );
  }

  #lastDownstreamMsgTime = Date.now();

  #maybeSendPong = () => {
    if (Date.now() - this.#lastDownstreamMsgTime > DOWNSTREAM_MSG_INTERVAL_MS) {
      this.#lc.debug?.('manually sending pong');
      this.send(['pong', {}], 'ignore-backpressure');
    }
  };

  send(
    data: Downstream,
    callback: ((err?: Error | null) => void) | 'ignore-backpressure',
    serialized?: string | undefined,
  ) {
    this.#lastDownstreamMsgTime = Date.now();
    this.#downstreamSender.send(data, callback, serialized);
  }

  sendError(errorBody: ErrorBody, thrown?: unknown) {
    sendError(this.#lc, this.#ws, errorBody, thrown);
  }
}

export type WebSocketLike = Pick<WebSocket, 'readyState'> & {
  send(data: string | PokeChunk, cb?: (err?: Error) => void): void;
};

// Exported for testing purposes.
export function send(
  lc: LogContext,
  ws: WebSocketLike,
  data: Downstream,
  callback: ((err?: Error | null) => void) | 'ignore-backpressure',
  serialized?: string | undefined,
) {
  if (ws.readyState === WebSocket.OPEN) {
    serialized ??= JSON.stringify(data);
    if (callback === 'ignore-backpressure') {
      ws.send(serialized);
      return;
    }

    let completed = false;
    const timer = setTimeout(() => {
      complete(webSocketSendTimeoutError());
    }, WEBSOCKET_SEND_TIMEOUT_MS);
    timer.unref();

    const complete = (error?: Error | null) => {
      if (completed) {
        return;
      }
      completed = true;
      clearTimeout(timer);
      callback(error);
    };

    ws.send(serialized, complete);
  } else {
    lc.debug?.(`Dropping outbound message on ws (state: ${ws.readyState})`, {
      dropped: data,
    });
    if (callback !== 'ignore-backpressure') {
      callback(
        new ProtocolErrorWithLevel(
          {
            kind: ErrorKind.Internal,
            message: 'WebSocket closed',
            origin: ErrorOrigin.ZeroCache,
          },
          'info',
        ),
      );
    }
  }
}

/**
 * Sends downstream messages in the format supported by a connection's sync
 * protocol version. Keep version forks contained here so the view-syncer can
 * continue producing one canonical poke representation.
 *
 * Exported for compatibility testing.
 */
export class DownstreamSender {
  readonly #lc: LogContext;
  readonly #ws: WebSocketLike;
  readonly #protocolVersion: number;
  #pokeChunkEncoder: PokeChunkEncoder | undefined;

  constructor(lc: LogContext, ws: WebSocketLike, protocolVersion: number) {
    this.#lc = lc;
    this.#ws = ws;
    this.#protocolVersion = protocolVersion;
  }

  send(
    data: Downstream,
    callback: ((err?: Error | null) => void) | 'ignore-backpressure',
    serialized?: string | undefined,
  ): void {
    if (this.#protocolVersion >= POKE_CHUNK_PROTOCOL_VERSION) {
      switch (data[0]) {
        case 'pokeStart':
          assert(
            this.#pokeChunkEncoder === undefined,
            'cannot start a poke while another poke is in progress',
          );
          this.#pokeChunkEncoder = new PokeChunkEncoder();
          break;
        case 'pokePart':
          this.#sendPokePart(data, callback, serialized);
          return;
        case 'pokeEnd':
          this.#sendPokeEnd(data, callback);
          return;
      }
    }
    send(this.#lc, this.#ws, data, callback, serialized);
  }

  #sendPokePart(
    pokePart: PokePartMessage,
    callback: ((err?: Error | null) => void) | 'ignore-backpressure',
    serialized?: string | undefined,
  ): void {
    const encoder = this.#pokeChunkEncoder;
    assert(encoder, 'pokePart received without a pokeStart');

    serialized ??= JSON.stringify(pokePart);
    assert(
      serialized.startsWith(POKE_PART_PREFIX) && serialized.endsWith(']'),
      'invalid serialized pokePart',
    );
    void encoder
      .addPatch(serialized.slice(POKE_PART_PREFIX.length, -1), chunk =>
        sendBinary(this.#lc, this.#ws, chunk),
      )
      .then(
        () => invokeCallback(callback),
        error => invokeCallback(callback, toError(error)),
      );
  }

  #sendPokeEnd(
    pokeEnd: PokeEndMessage,
    callback: ((err?: Error | null) => void) | 'ignore-backpressure',
  ): void {
    const encoder = this.#pokeChunkEncoder;
    assert(encoder, 'pokeEnd received without a pokeStart');
    this.#pokeChunkEncoder = undefined;

    if (pokeEnd[1].cancel) {
      encoder.cancel();
      send(this.#lc, this.#ws, pokeEnd, callback);
      return;
    }

    void encoder
      .finish(chunk => sendBinary(this.#lc, this.#ws, chunk))
      .then(() => sendAsync(this.#lc, this.#ws, pokeEnd))
      .then(
        () => invokeCallback(callback),
        error => invokeCallback(callback, toError(error)),
      );
  }
}

const POKE_PART_PREFIX = '["pokePart",';

function sendAsync(
  lc: LogContext,
  ws: WebSocketLike,
  data: Downstream,
): Promise<void> {
  return new Promise((resolve, reject) => {
    send(lc, ws, data, error => (error ? reject(error) : resolve()));
  });
}

function sendBinary(
  lc: LogContext,
  ws: WebSocketLike,
  chunk: PokeChunk,
): Promise<void> {
  return new Promise((resolve, reject) => {
    if (ws.readyState !== WebSocket.OPEN) {
      const error = webSocketClosedError();
      lc.debug?.(
        `Dropping outbound binary message on ws (state: ${ws.readyState})`,
      );
      reject(error);
      return;
    }
    let completed = false;
    const timer = setTimeout(
      () => complete(webSocketSendTimeoutError()),
      WEBSOCKET_SEND_TIMEOUT_MS,
    );
    timer.unref();

    const complete = (error?: Error | null) => {
      if (completed) {
        return;
      }
      completed = true;
      clearTimeout(timer);
      error ? reject(error) : resolve();
    };

    ws.send(chunk, complete);
  });
}

function invokeCallback(
  callback: ((err?: Error | null) => void) | 'ignore-backpressure',
  error?: Error,
): void {
  if (callback !== 'ignore-backpressure') {
    callback(error);
  }
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function webSocketClosedError(): ProtocolErrorWithLevel {
  return new ProtocolErrorWithLevel(
    {
      kind: ErrorKind.Internal,
      message: 'WebSocket closed',
      origin: ErrorOrigin.ZeroCache,
    },
    'info',
  );
}

function webSocketSendTimeoutError(): ProtocolErrorWithLevel {
  return new ProtocolErrorWithLevel(
    {
      kind: ErrorKind.Internal,
      message: `WebSocket send timed out after ${WEBSOCKET_SEND_TIMEOUT_MS} ms`,
      origin: ErrorOrigin.ZeroCache,
    },
    'info',
  );
}

export function sendError(
  lc: LogContext,
  ws: WebSocket,
  errorBody: ErrorBody,
  thrown?: unknown,
) {
  lc = lc.withContext('errorKind', errorBody.kind);

  let logLevel: LogLevel;

  // If the thrown error is a ProtocolErrorWithLevel, its explicit logLevel takes precedence
  if (thrown instanceof ProtocolErrorWithLevel) {
    logLevel = thrown.logLevel;
  }
  // Errors with errno or transient socket codes are low-level, transient I/O issues
  // (e.g., EPIPE, ECONNRESET) and should be warnings, not errors
  else if (
    hasErrno(thrown) ||
    hasTransientSocketCode(thrown) ||
    isTransientSocketMessage(errorBody.message)
  ) {
    logLevel = 'warn';
  }
  // Fallback: check errorBody.kind for errors that weren't thrown as ProtocolErrorWithLevel
  else if (
    errorBody.kind === ErrorKind.ClientNotFound ||
    errorBody.kind === ErrorKind.TransformFailed
  ) {
    logLevel = 'warn';
  } else {
    logLevel = thrown ? getLogLevel(thrown) : 'info';
  }

  lc[logLevel]?.('Sending error on WebSocket', errorBody, thrown ?? '');
  send(lc, ws, ['error', errorBody], 'ignore-backpressure');
}

export function findProtocolError(error: unknown): ProtocolError | undefined {
  if (isProtocolError(error)) {
    return error;
  }
  if (error instanceof Error && error.cause) {
    return findProtocolError(error.cause);
  }
  return undefined;
}

function hasErrno(error: unknown): boolean {
  return Boolean(
    error &&
    typeof error === 'object' &&
    'errno' in error &&
    typeof (error as {errno: unknown}).errno !== 'undefined',
  );
}

// System error codes that indicate transient socket conditions.
// These are checked via the `code` property on errors.
const TRANSIENT_SOCKET_ERROR_CODES = new Set([
  'EPIPE',
  'ECONNRESET',
  'ECANCELED',
]);

// Error messages that indicate transient socket conditions but don't have
// standard error codes (e.g., WebSocket library errors).
const TRANSIENT_SOCKET_MESSAGE_PATTERNS = [
  'socket was closed while data was being compressed',
];

function hasTransientSocketCode(error: unknown): boolean {
  if (!error || typeof error !== 'object') {
    return false;
  }
  const maybeCode =
    'code' in error ? String((error as {code?: unknown}).code) : undefined;
  return Boolean(
    maybeCode && TRANSIENT_SOCKET_ERROR_CODES.has(maybeCode.toUpperCase()),
  );
}

function isTransientSocketMessage(message: string | undefined): boolean {
  if (!message) {
    return false;
  }
  const lower = message.toLowerCase();
  return TRANSIENT_SOCKET_MESSAGE_PATTERNS.some(pattern =>
    lower.includes(pattern),
  );
}
