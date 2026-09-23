import type {IncomingMessage} from 'node:http';
import websocket from '@fastify/websocket';
import type {LogContext} from '@rocicorp/logger';
import WebSocket from 'ws';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import type {IncomingMessageSubset} from '../../types/http.ts';
import {pgClient, type PostgresDB} from '../../types/pg.ts';
import {type Worker} from '../../types/processes.ts';
import {type ShardID} from '../../types/shards.ts';
import {
  readHead,
  streamInternal,
  streamInternalStringified,
  streamInternalWithSize,
  type Sized,
  type Source,
} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {URLParams} from '../../types/url-params.ts';
import {installWebSocketReceiver} from '../../types/websocket-handoff.ts';
import {closeWithError, PROTOCOL_ERROR} from '../../types/ws.ts';
import {HttpService, type Options as HttpOptions} from '../http-service.ts';
import {handleProfzRequest} from '../profz.ts';
import type {PreSerializedBatch} from './broadcast.ts';
import {
  PROTOCOL_VERSION,
  type ChangeStreamer,
  type ChangeStreamerService,
  type SubscriberContext,
} from './change-streamer.ts';
import {discoverChangeStreamerAddress} from './schema/tables.ts';
import {type SnapshotMessage} from './snapshot-message.ts';
import {
  subscribeDownstreamSchema,
  subscribeUpstreamSchema,
  type ReservedMessage,
  type SnapshotStatus,
  type SubscribeContext,
  type SubscribeDownstream,
  type SubscribeUpstream,
} from './subscribe.ts';

const MIN_SUPPORTED_PROTOCOL_VERSION = 4;

// The merged `/subscribe` protocol was introduced at v7.
const MIN_SUBSCRIBE_PROTOCOL_VERSION = 7;

const SNAPSHOT_PATH_PATTERN = '/replication/:version/snapshot';
const CHANGES_PATH_PATTERN = '/replication/:version/changes';
const SUBSCRIBE_PATH_PATTERN = '/replication/:version/subscribe';
const PATH_REGEX =
  /\/replication\/v(?<version>\d+)\/(changes|snapshot|subscribe)$/;

const SUBSCRIBE_PATH = `/replication/v${PROTOCOL_VERSION}/subscribe`;

type Options = HttpOptions & {
  startupDelayMs: number;
  config?: Pick<NormalizedZeroConfig, 'adminPassword'> | undefined;
  getProfileWorker?: (() => Promise<Worker>) | undefined;
};

export class ChangeStreamerHttpServer extends HttpService {
  readonly id = 'change-streamer-http-server';
  readonly #lc: LogContext;
  readonly #opts: Options;
  readonly #changeStreamer: ChangeStreamerService;

  constructor(
    lc: LogContext,
    opts: Options,
    parent: Worker,
    changeStreamer: ChangeStreamerService,
  ) {
    super('change-streamer-http-server', lc, opts, async fastify => {
      await fastify.register(websocket);

      fastify.get(CHANGES_PATH_PATTERN, {websocket: true}, this.#changes);
      fastify.get(
        SNAPSHOT_PATH_PATTERN,
        {websocket: true},
        this.#reserveSnapshot,
      );
      fastify.get(SUBSCRIBE_PATH_PATTERN, {websocket: true}, this.#subscribe);

      fastify.get('/profz', (req, res) =>
        handleProfzRequest(
          lc,
          opts.config ?? {adminPassword: undefined},
          req,
          res,
          opts.getProfileWorker,
          undefined,
          'change-streamer',
        ),
      );

      installWebSocketReceiver<'snapshot' | 'changes' | 'subscribe'>(
        lc,
        fastify.websocketServer,
        this.#receiveWebsocket,
        parent,
      );
    });

    this.#lc = lc;
    this.#opts = opts;
    this.#changeStreamer = changeStreamer;
  }

  // Called when receiving a web socket via the main dispatcher handoff.
  readonly #receiveWebsocket = (
    ws: WebSocket,
    action: 'changes' | 'snapshot' | 'subscribe',
    msg: IncomingMessageSubset,
  ) => {
    switch (action) {
      case 'snapshot':
        return this.#reserveSnapshot(ws, msg);
      case 'changes':
        return this.#changes(ws, msg);
      case 'subscribe':
        return this.#subscribe(ws, msg);
      default:
        closeWithError(
          this._lc,
          ws,
          `invalid action "${action}" received in handoff`,
        );
        return;
    }
  };

  readonly #reserveSnapshot = async (ws: WebSocket, req: RequestHeaders) => {
    this.#ensureChangeStreamerStarted('incoming snapshot reservation');
    try {
      const url = new URL(
        req.url ?? '',
        req.headers.origin ?? 'http://localhost',
      );
      checkProtocolVersion(url.pathname);
      const taskID = url.searchParams.get('taskID');
      if (!taskID) {
        throw new Error('Missing taskID in snapshot request');
      }
      const downstream =
        await this.#changeStreamer.startSnapshotReservation(taskID);
      // Send-only for now: the reservation carries no inbound application
      // messages (only transport acks, demuxed internally).
      void streamInternal(this._lc, ws, undefined, downstream);
    } catch (err) {
      closeWithError(this._lc, ws, err, PROTOCOL_ERROR);
    }
  };

  readonly #changes = async (ws: WebSocket, req: RequestHeaders) => {
    try {
      const ctx = getSubscriberContext(req);
      if (ctx.mode === 'serving') {
        this.#ensureChangeStreamerStarted('incoming subscription');
      }

      const downstream = await this.#changeStreamer.subscribe(ctx);
      // Send-only for now: the subscription carries no inbound application
      // messages (only transport acks, demuxed internally).
      void streamInternalStringified(this._lc, ws, undefined, downstream, {
        batched: ctx.wsBatched,
      });
    } catch (err) {
      closeWithError(this._lc, ws, err, PROTOCOL_ERROR);
    }
  };

  /**
   * The merged v7 `/subscribe` handler. A single bidirectional connection
   * carries, in sequence: an optional reservation phase (`reserve-snapshot` →
   * `reserved`) and then the subscription phase (`start-subscription` → change
   * stream). This ensures that the subscriber subscribes to the same task from
   * which it restored the backup.
   */
  readonly #subscribe = async (ws: WebSocket, req: RequestHeaders) => {
    let reservation: Source<SnapshotMessage> | undefined;
    const outbound = Subscription.create<string | PreSerializedBatch>();

    try {
      const url = new URL(
        req.url ?? '',
        req.headers.origin ?? 'http://localhost',
      );
      const protocolVersion = checkProtocolVersion(url.pathname);
      if (protocolVersion < MIN_SUBSCRIBE_PROTOCOL_VERSION) {
        throw new Error(
          `the /subscribe endpoint requires protocol v${MIN_SUBSCRIBE_PROTOCOL_VERSION} ` +
            `(client is at v${protocolVersion})`,
        );
      }

      const instream = await streamInternalStringified(
        this._lc,
        ws,
        subscribeUpstreamSchema,
        outbound,
        {batched: true},
      );

      // Keep reading upstream control messages for the life of the connection.
      // Do not manually exit loop, as that would close the socket. The loop
      // exits naturally when the connection closes.
      for await (const msg of instream) {
        if (msg[0] === 'reserve-snapshot') {
          const {taskID} = msg[1];
          this.#ensureChangeStreamerStarted('incoming snapshot reservation');
          reservation =
            await this.#changeStreamer.startSnapshotReservation(taskID);
          void this.#forwardReservation(reservation, outbound);
        } else if (msg[0] === 'start-subscription') {
          const ctx: SubscriberContext = {
            ...msg[1],
            protocolVersion,
            wsBatched: true,
          };
          if (ctx.mode === 'serving') {
            this.#ensureChangeStreamerStarted('incoming subscription');
          }
          // subscribe() registers the subscriber and then releases any
          // reservation for that task.
          reservation = undefined;
          await this.#changeStreamer.subscribe(ctx, outbound);
        }
      }
    } catch (err) {
      closeWithError(this._lc, ws, err, PROTOCOL_ERROR);
    } finally {
      // Covers a reservation-phase disconnect (instream ended before a
      // subscription started); idempotent with the close handler above.
      reservation?.cancel();
    }
  };

  /**
   * Forwards a snapshot reservation's single `['status', ...]` message onto
   * the merged connection's `outbound` sink as a `['reserved', ...]` message.
   * The loop then blocks on the held-open reservation until it is cancelled,
   * by `subscribe()` at the subscription handoff, or by the reservation-phase
   * teardown on disconnect.
   */
  async #forwardReservation(
    reservation: Source<SnapshotMessage>,
    outbound: Subscription<string | PreSerializedBatch>,
  ) {
    try {
      for await (const [, status] of reservation) {
        // Translate the /snapshot protocol's ['status', {tag: 'status', ...}]
        // message to the /subscribe protocol's ['reserved', {tag: 'snapshot', ...}].
        outbound.push(
          BigIntJSON.stringify([
            'reserved',
            {...status, tag: 'snapshot'},
          ] satisfies ReservedMessage),
        );
      }
    } catch (e) {
      this._lc.warn?.(`error forwarding snapshot reservation`, e);
      outbound.fail(e instanceof Error ? e : new Error(String(e)));
    }
  }

  #changeStreamerStarted = false;

  #ensureChangeStreamerStarted(reason?: string) {
    if (!this.#changeStreamerStarted && this._state.shouldRun()) {
      this.#lc.info?.(
        `starting ChangeStreamerService ${reason ? `(${reason})` : ''}`,
      );
      void this.#changeStreamer
        .run()
        .catch(e =>
          this.#lc.warn?.(`ChangeStreamerService ended with error`, e),
        )
        .finally(() => this.stop());

      this.#changeStreamerStarted = true;
    }
  }

  protected override _onStart(): void {
    const {startupDelayMs, readinessGate = promiseVoid} = this.#opts;
    if (startupDelayMs === 0) {
      // In RMv2, there is no need to delay starting the change streamer
      // because RM startup is non-disruptive.
      this.#ensureChangeStreamerStarted();
    } else {
      // In RMv1, starting the change-streamer forcibly shuts down the
      // previous change-streamer, causing view-syncers to reconnect.
      // If this replication-manager has just started, the routing layer may
      // not have registered it with DNS, as that only happens after it
      // confirms health checks. To minimize downtime, the takeover is
      // delayed for the configured startupDelayMs _after_ beginning to
      // advertise readiness.
      void readinessGate.then(() => {
        this.#lc.info?.(
          `waiting ${startupDelayMs}ms before taking over the change log`,
        );
        this._state.setTimeout(
          () =>
            this.#ensureChangeStreamerStarted(
              `startup delay elapsed (${startupDelayMs} ms)`,
            ),
          startupDelayMs,
        );
      });
    }
  }

  protected override async _onStop(): Promise<void> {
    if (this.#changeStreamerStarted) {
      await this.#changeStreamer.stop();
    }
  }
}

export interface SnapshotReserver {
  reserveSnapshot(taskID: string): Promise<{
    reserved: SnapshotStatus;
    followup: ReservationFollowup;
  }>;
}

export interface SubscriptionStarter {
  subscribe(
    ctx: SubscriberContext,
  ): Promise<Source<Sized<SubscribeDownstream>>>;
}

export interface ReservationFollowup extends SubscriptionStarter {
  /**
   * Abandons the reservation without subscribing, closing the connection that
   * was held open to pin the change-log floor. Used when the caller cannot
   * proceed to `subscribe()` (e.g. process shutdown while a restore is in
   * progress) and must not leave the reservation's connection open forever.
   */
  cancel(reason?: Error): void;
}

export class ChangeStreamerHttpClient
  implements ChangeStreamer, SnapshotReserver, SubscriptionStarter
{
  readonly #lc: LogContext;
  readonly #shardID: ShardID;
  readonly #changeDB: PostgresDB;
  readonly #changeStreamerURI: string | undefined;

  constructor(
    lc: LogContext,
    shardID: ShardID,
    changeDB: string,
    changeStreamerURI: string | undefined,
  ) {
    this.#lc = lc;
    this.#shardID = shardID;
    // Create a pg client with a single short-lived connection for the purpose
    // of change-streamer discovery (i.e. ChangeDB as DNS).
    this.#changeDB = pgClient(lc, changeDB, 'change-streamer-discovery', {
      max: 1,
      ['idle_timeout']: 15,
    });
    this.#changeStreamerURI = changeStreamerURI;
  }

  async #resolveChangeStreamer(path: string) {
    let baseURL = this.#changeStreamerURI;
    if (!baseURL) {
      const address = await discoverChangeStreamerAddress(
        this.#shardID,
        this.#changeDB,
      );
      if (!address) {
        throw new Error(`no change-streamer is running`);
      }
      baseURL = address.includes('://') ? `${address}/` : `ws://${address}/`;
    }
    const uri = new URL(path, baseURL);
    this.#lc.info?.(`connecting to change-streamer@${uri}`);
    return uri;
  }

  async reserveSnapshot(taskID: string): Promise<{
    reserved: SnapshotStatus;
    followup: ReservationFollowup;
  }> {
    const {instream, outbound} = await this.connect(taskID);
    outbound.push(['reserve-snapshot', {taskID}]);

    const {head, rest} = readHead(instream);
    const item = await head.next();
    if (!item.value) {
      throw new Error('stream closed without snapshot reservation');
    }
    const {data: msg} = item.value;
    if (msg[0] !== 'reserved') {
      throw new Error(`expected "reserved" message but got ${msg[0]}`);
    }
    const reserved: SnapshotStatus = msg[1];
    const followup: ReservationFollowup = {
      subscribe: ctx => {
        outbound.push(['start-subscription', toStartSubscriptionContext(ctx)]);
        return Promise.resolve(rest);
      },
      cancel: reason => outbound.cancel(reason),
    };

    return {reserved, followup};
  }

  async subscribe(
    ctx: SubscriberContext,
  ): Promise<Source<Sized<SubscribeDownstream>>> {
    const {instream, outbound} = await this.connect(ctx.taskID);
    outbound.push(['start-subscription', toStartSubscriptionContext(ctx)]);
    return instream;
  }

  /**
   * Opens a merged v7 `/subscribe` connection (see subscribe.ts). Returns the
   * inbound change/reservation stream and an `outbound` sink into which the
   * caller pushes the `reserve-snapshot` / `start-subscription` control
   * messages that drive the connection's phases. The single connection spans
   * the reservation, backup restore, and subscription, so the reserving
   * replication-manager is the one that serves the subscription.
   */
  // exported for testing
  async connect(taskID: string): Promise<{
    instream: Source<Sized<SubscribeDownstream>>;
    outbound: Subscription<SubscribeUpstream>;
  }> {
    const uri = await this.#resolveChangeStreamer(SUBSCRIBE_PATH);

    // taskID is carried in the query for observability/routing; the server
    // reads the authoritative taskID from the control messages themselves.
    const params = new URLSearchParams({taskID});
    const ws = new WebSocket(uri + `?${params.toString()}`);

    const outbound = Subscription.create<SubscribeUpstream>();
    const instream = await streamInternalWithSize(
      this.#lc,
      ws,
      subscribeDownstreamSchema,
      outbound,
    );
    return {instream, outbound};
  }
}

type RequestHeaders = Pick<IncomingMessage, 'url' | 'headers'>;

export function getSubscriberContext(req: RequestHeaders): SubscriberContext {
  const url = new URL(req.url ?? '', req.headers.origin ?? 'http://localhost');
  const protocolVersion = checkProtocolVersion(url.pathname);
  const params = new URLParams(url);

  return {
    protocolVersion,
    id: params.get('id', true),
    taskID: params.get('taskID', true),
    mode: params.get('mode', false) === 'backup' ? 'backup' : 'serving',
    replicaVersion: params.get('replicaVersion', true),
    watermark: params.get('watermark', true),
    // Absent for subscribers that predate the parameter, which is the safe
    // default: the barrier falls back to polling rather than waiting on an
    // ACK that would never be attributed to a writer.
    wsBatched: params.getBoolean('wsBatched'),
  };
}

function checkProtocolVersion(pathname: string): number {
  const match = PATH_REGEX.exec(pathname);
  if (!match) {
    throw new Error(`invalid path: ${pathname}`);
  }
  const v = Number(match.groups?.version);
  if (
    Number.isNaN(v) ||
    v > PROTOCOL_VERSION ||
    v < MIN_SUPPORTED_PROTOCOL_VERSION
  ) {
    throw new Error(
      `Cannot service client at protocol v${v}. ` +
        `Supported protocols: [v${MIN_SUPPORTED_PROTOCOL_VERSION} ... v${PROTOCOL_VERSION}]`,
    );
  }
  return v;
}

// Projects a client-side SubscriberContext onto the start-subscription
// message payload, dropping the fields that are implicit on the merged
// connection: protocolVersion (from the request path), wsBatched (always on).
function toStartSubscriptionContext(ctx: SubscriberContext): SubscribeContext {
  const {taskID, id, mode, replicaVersion, watermark} = ctx;
  return {taskID, id, mode, replicaVersion, watermark};
}
