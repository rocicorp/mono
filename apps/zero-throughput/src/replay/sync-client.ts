import {performance} from 'node:perf_hooks';
import WebSocket from 'ws';
import type {ClientSchema} from '../../../../packages/zero-protocol/src/client-schema.ts';
import {
  encodeSecProtocols,
  type InitConnectionMessage,
} from '../../../../packages/zero-protocol/src/connect.ts';
import type {Row} from '../../../../packages/zero-protocol/src/data.ts';
import {
  POKE_CHUNK_MESSAGE_TYPE,
  type PokePartBody,
} from '../../../../packages/zero-protocol/src/poke.ts';
import type {UpQueriesPatchOp} from '../../../../packages/zero-protocol/src/queries-patch.ts';
import {hashOfNameAndArgs} from '../../../../packages/zero-protocol/src/query-hash.ts';
import type {WorkloadQuery} from './workload.ts';

export type QueryKind = 'session' | 'screen';

export type ReplayQuery = WorkloadQuery & {readonly kind: QueryKind};

/**
 * What a device keeps between sessions, like a real client's local store:
 * the cookie it last synced to and the queries the server has confirmed.
 * A session that starts with a cookie is a returning session; the server
 * only reports queries it has not confirmed before.
 */
export type DeviceState = {
  readonly clientGroupID: string;
  readonly clientID: string;
  readonly userID: string;
  cookie: string | null;
  readonly gotHashes: Set<string>;
};

export type SyncClientListener = {
  onConnected(info: {readonly connectMs: number}): void;
  /** The first complete poke: the group is caught up to the server. */
  onFirstPoke(info: {readonly ms: number}): void;
  onHydrated(info: {
    readonly name: string;
    readonly kind: QueryKind;
    readonly ms: number;
  }): void;
  onQueryError(info: {
    readonly name: string;
    readonly kind: QueryKind;
    readonly message: string;
  }): void;
  onPoke(info: {
    readonly rows: number;
    readonly bytes: number;
    readonly ms: number;
  }): void;
  onRow(tableName: string, row: Row): void;
  onPong(rttMs: number): void;
  onServerError(kind: string, message: string): void;
  onClose(info: {
    readonly code: number;
    readonly reason: string;
    readonly expected: boolean;
  }): void;
};

export type SyncClientOptions = {
  readonly cacheURL: string;
  readonly protocolVersion: number;
  readonly auth: string;
  readonly clientSchema: ClientSchema;
  readonly device: DeviceState;
  readonly pingIntervalMs: number;
  /** Like zero-client, a larger initConnection is sent as a message instead. */
  readonly maxHeaderLength: number;
  readonly listener: SyncClientListener;
};

type Pending = {
  readonly name: string;
  readonly kind: QueryKind;
  readonly sentAtMs: number;
};

type ReceivingPoke = {
  readonly pokeID: string;
  readonly startedAtMs: number;
  readonly parts: PokePartBody[];
  readonly chunks: Uint8Array[];
  bytes: number;
};

/**
 * A client group speaking the sync protocol directly, with no local store or
 * IVM: it registers named queries, times their hydration, and counts what
 * the server sends.
 */
export class SyncClient {
  readonly #options: SyncClientOptions;
  readonly #device: DeviceState;
  readonly #queries = new Map<string, ReplayQuery>();
  readonly #pending = new Map<string, Pending>();
  #socket: WebSocket | undefined;
  #startedAtMs = 0;
  #initSent = false;
  #connected = false;
  #firstPokeSeen = false;
  #closing = false;
  #receiving: ReceivingPoke | undefined;
  #pingTimer: NodeJS.Timeout | undefined;
  #pingSentAtMs: number | undefined;

  constructor(options: SyncClientOptions) {
    this.#options = options;
    this.#device = options.device;
  }

  get connected(): boolean {
    return this.#connected;
  }

  get pendingQueries(): number {
    return this.#pending.size;
  }

  get registeredQueries(): number {
    return this.#queries.size;
  }

  /** Opens the socket and registers `queries` in the initial connection. */
  connect(queries: readonly ReplayQuery[]): void {
    const {cacheURL, protocolVersion, auth, clientSchema, maxHeaderLength} =
      this.#options;
    const device = this.#device;
    this.#startedAtMs = performance.now();
    for (const q of queries) {
      this.#register(q, this.#startedAtMs);
    }

    const url = new URL(
      appendPath(toWebSocketURL(cacheURL), `/sync/v${protocolVersion}/connect`),
    );
    url.searchParams.set('clientID', device.clientID);
    url.searchParams.set('clientGroupID', device.clientGroupID);
    url.searchParams.set('userID', device.userID);
    url.searchParams.set('baseCookie', device.cookie ?? '');
    url.searchParams.set('ts', String(this.#startedAtMs));
    url.searchParams.set('lmid', '0');
    url.searchParams.set('wsid', randomID());
    url.searchParams.set('profileID', `p${device.clientGroupID}`);

    const init = this.#initConnectionMessage(
      device.cookie === null ? clientSchema : undefined,
    );
    let protocol = encodeSecProtocols(init, auth);
    if (protocol.length > maxHeaderLength) {
      protocol = encodeSecProtocols(undefined, auth);
    } else {
      this.#initSent = true;
    }

    const socket = new WebSocket(url, protocol);
    socket.binaryType = 'nodebuffer';
    this.#socket = socket;
    socket.on('message', (data: Buffer, isBinary: boolean) =>
      this.#onMessage(data, isBinary),
    );
    socket.on('close', (code: number, reason: Buffer) =>
      this.#onClose(code, reason.toString()),
    );
    socket.on('error', err => {
      this.#options.listener.onServerError('socket', String(err));
    });
  }

  /** Registers a query on the open connection. */
  addQuery(query: ReplayQuery): void {
    const hash = this.#register(query, performance.now());
    if (hash === undefined || !this.#initSent) {
      // An unsent initConnection will carry it.
      return;
    }
    this.#send([
      'changeDesiredQueries',
      {desiredQueriesPatch: [putOp(hash, query)]},
    ]);
  }

  /** Unregisters a query; the server keeps it for its TTL. */
  removeQuery(query: ReplayQuery): void {
    const hash = hashOfNameAndArgs(query.name, query.args);
    if (!this.#queries.delete(hash)) {
      return;
    }
    this.#pending.delete(hash);
    if (this.#initSent) {
      this.#send([
        'changeDesiredQueries',
        {desiredQueriesPatch: [{op: 'del', hash}]},
      ]);
    }
  }

  close(): void {
    this.#closing = true;
    this.#stopPinging();
    const socket = this.#socket;
    if (
      socket !== undefined &&
      (socket.readyState === WebSocket.OPEN ||
        socket.readyState === WebSocket.CONNECTING)
    ) {
      socket.close(1000, 'session ended');
    }
  }

  #register(query: ReplayQuery, sentAtMs: number): string | undefined {
    const hash = hashOfNameAndArgs(query.name, query.args);
    if (this.#queries.has(hash)) {
      return undefined;
    }
    this.#queries.set(hash, query);
    if (!this.#device.gotHashes.has(hash)) {
      this.#pending.set(hash, {name: query.name, kind: query.kind, sentAtMs});
    }
    return hash;
  }

  #initConnectionMessage(
    clientSchema: ClientSchema | undefined,
  ): InitConnectionMessage {
    return [
      'initConnection',
      {
        desiredQueriesPatch: Array.from(this.#queries, ([hash, q]) =>
          putOp(hash, q),
        ),
        ...(clientSchema === undefined ? {} : {clientSchema}),
        activeClients: [this.#device.clientID],
      },
    ];
  }

  #send(message: readonly unknown[]): void {
    const socket = this.#socket;
    if (socket?.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify(message));
    }
  }

  #onMessage(data: Buffer, isBinary: boolean): void {
    if (isBinary) {
      if (
        data[0] !== POKE_CHUNK_MESSAGE_TYPE ||
        this.#receiving === undefined
      ) {
        this.#options.listener.onServerError(
          'protocol',
          `unexpected binary message (type ${String(data[0])})`,
        );
        return;
      }
      this.#receiving.chunks.push(data.subarray(1));
      this.#receiving.bytes += data.length;
      return;
    }
    const text = data.toString('utf8');
    const message = JSON.parse(text) as [string, unknown];
    switch (message[0]) {
      case 'connected':
        return this.#onConnected();
      case 'pokeStart': {
        const body = message[1] as {pokeID: string};
        this.#receiving = {
          pokeID: body.pokeID,
          startedAtMs: performance.now(),
          parts: [],
          chunks: [],
          bytes: text.length,
        };
        return;
      }
      case 'pokePart': {
        const body = message[1] as PokePartBody;
        if (this.#receiving?.pokeID === body.pokeID) {
          this.#receiving.parts.push(body);
          this.#receiving.bytes += text.length;
        }
        return;
      }
      case 'pokeEnd':
        return this.#onPokeEnd(
          message[1] as {pokeID: string; cookie: string; cancel?: boolean},
        );
      case 'pong':
        if (this.#pingSentAtMs !== undefined) {
          this.#options.listener.onPong(performance.now() - this.#pingSentAtMs);
          this.#pingSentAtMs = undefined;
        }
        return;
      case 'transformError':
        return this.#onTransformError(
          message[1] as readonly {id: string; name: string; message?: string}[],
        );
      case 'error': {
        const body = message[1] as {kind?: string; message?: string};
        this.#options.listener.onServerError(
          body.kind ?? 'unknown',
          body.message ?? '',
        );
        return;
      }
      default:
        // pull, push and inspect responses and deleteClients are not used.
        return;
    }
  }

  #onConnected(): void {
    this.#connected = true;
    const now = performance.now();
    this.#options.listener.onConnected({connectMs: now - this.#startedAtMs});
    if (!this.#initSent) {
      this.#initSent = true;
      this.#send(
        this.#initConnectionMessage(
          this.#device.cookie === null ? this.#options.clientSchema : undefined,
        ),
      );
    }
    this.#startPinging();
  }

  #onPokeEnd(body: {pokeID: string; cookie: string; cancel?: boolean}): void {
    const receiving = this.#receiving;
    this.#receiving = undefined;
    if (receiving === undefined || receiving.pokeID !== body.pokeID) {
      return;
    }
    if (body.cancel) {
      return;
    }
    const parts =
      receiving.chunks.length > 0
        ? decodePokeChunks(receiving.chunks)
        : receiving.parts;
    const now = performance.now();
    const listener = this.#options.listener;
    let rows = 0;
    for (const part of parts) {
      for (const op of part.gotQueriesPatch ?? []) {
        if (op.op === 'put') {
          this.#device.gotHashes.add(op.hash);
          const pending = this.#pending.get(op.hash);
          if (pending !== undefined) {
            this.#pending.delete(op.hash);
            listener.onHydrated({
              name: pending.name,
              kind: pending.kind,
              ms: now - pending.sentAtMs,
            });
          }
        } else if (op.op === 'del') {
          this.#device.gotHashes.delete(op.hash);
        } else {
          this.#device.gotHashes.clear();
        }
      }
      for (const op of part.rowsPatch ?? []) {
        rows++;
        if (op.op === 'put') {
          listener.onRow(op.tableName, op.value);
        }
      }
    }
    this.#device.cookie = body.cookie;
    listener.onPoke({
      rows,
      bytes: receiving.bytes,
      ms: now - receiving.startedAtMs,
    });
    if (!this.#firstPokeSeen) {
      this.#firstPokeSeen = true;
      listener.onFirstPoke({ms: now - this.#startedAtMs});
    }
  }

  #onTransformError(
    errors: readonly {id: string; name: string; message?: string}[],
  ): void {
    for (const e of errors) {
      const pending = this.#pending.get(e.id);
      const query = this.#queries.get(e.id);
      this.#pending.delete(e.id);
      this.#options.listener.onQueryError({
        name: e.name,
        kind: pending?.kind ?? query?.kind ?? 'session',
        message: e.message ?? '',
      });
    }
  }

  #onClose(code: number, reason: string): void {
    this.#connected = false;
    this.#stopPinging();
    this.#options.listener.onClose({code, reason, expected: this.#closing});
  }

  #startPinging(): void {
    this.#stopPinging();
    this.#pingTimer = setInterval(() => {
      if (this.#pingSentAtMs === undefined) {
        this.#pingSentAtMs = performance.now();
        this.#send(['ping', {}]);
      }
    }, this.#options.pingIntervalMs);
  }

  #stopPinging(): void {
    if (this.#pingTimer !== undefined) {
      clearInterval(this.#pingTimer);
      this.#pingTimer = undefined;
    }
    this.#pingSentAtMs = undefined;
  }
}

function putOp(hash: string, q: ReplayQuery): UpQueriesPatchOp {
  return {op: 'put', hash, name: q.name, args: q.args, ttl: q.ttlMs};
}

/** Binary pokes are a JSON array of poke parts split into chunks. */
export function decodePokeChunks(
  chunks: readonly Uint8Array[],
): PokePartBody[] {
  const decoder = new TextDecoder('utf-8', {fatal: true});
  let text = '';
  for (const chunk of chunks) {
    text += decoder.decode(chunk, {stream: true});
  }
  text += decoder.decode();
  return JSON.parse(text) as PokePartBody[];
}

const HTTP_SCHEME = /^http(s?):\/\//;
const TRAILING_SLASHES = /\/+$/;

export function toWebSocketURL(url: string): string {
  return url.replace(HTTP_SCHEME, 'ws$1://');
}

export function appendPath(base: string, path: string): string {
  return base.replace(TRAILING_SLASHES, '') + path;
}

function randomID(): string {
  return Math.random().toString(36).slice(2, 12);
}
