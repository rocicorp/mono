import websocket from '@fastify/websocket';
import {LogContext} from '@rocicorp/logger';
import {IncomingMessage} from 'node:http';
import WebSocket from 'ws';
import {assert} from '../../../../shared/src/asserts.ts';
import {must} from '../../../../shared/src/must.ts';
import type {IncomingMessageSubset} from '../../types/http.ts';
import {pgClient, type PostgresDB} from '../../types/pg.ts';
import {type Worker} from '../../types/processes.ts';
import type {ShardID} from '../../types/shards.ts';
import {streamIn, streamOut, type Source} from '../../types/streams.ts';
import {URLParams} from '../../types/url-params.ts';
import {installWebSocketReceiver} from '../../types/websocket-handoff.ts';
import {closeWithError, PROTOCOL_ERROR} from '../../types/ws.ts';
import {HttpService, type Options} from '../http-service.ts';
import type {BackupMonitor} from './backup-monitor.ts';
import {
  downstreamSchema,
  PROTOCOL_VERSION,
  type ChangeStreamer,
  type Downstream,
  type SubscriberContext,
} from './change-streamer.ts';
import {discoverChangeStreamerAddress} from './schema/tables.ts';
import {snapshotMessageSchema, type SnapshotMessage} from './snapshot.ts';

const MIN_SUPPORTED_PROTOCOL_VERSION = 1;

const SNAPSHOT_PATH_PATTERN = '/replication/:version/snapshot';
const CHANGES_PATH_PATTERN = '/replication/:version/changes';
const PATH_REGEX = /\/replication\/v(?<version>\d+)\/(changes|snapshot)$/;

const SNAPSHOT_PATH = `/replication/v${PROTOCOL_VERSION}/snapshot`;
const CHANGES_PATH = `/replication/v${PROTOCOL_VERSION}/changes`;

export class ChangeStreamerHttpServer extends HttpService {
  readonly id = 'change-streamer-http-server';
  readonly #changeStreamer: ChangeStreamer;
  readonly #backupMonitor: BackupMonitor | null;

  constructor(
    lc: LogContext,
    opts: Options,
    parent: Worker,
    changeStreamer: ChangeStreamer,
    backupMonitor: BackupMonitor | null,
  ) {
    super('change-streamer-http-server', lc, opts, async fastify => {
      await fastify.register(websocket);

      fastify.get(CHANGES_PATH_PATTERN, {websocket: true}, this.#subscribe);
      fastify.get(
        SNAPSHOT_PATH_PATTERN,
        {websocket: true},
        this.#reserveSnapshot,
      );

      installWebSocketReceiver<'snapshot' | 'changes'>(
        lc,
        fastify.websocketServer,
        this.#receiveWebsocket,
        parent,
      );
    });

    this.#changeStreamer = changeStreamer;
    this.#backupMonitor = backupMonitor;
  }

  #getBackupMonitor() {
    return must(
      this.#backupMonitor,
      'replication-manager is not configured with a ZERO_LITESTREAM_BACKUP_URL',
    );
  }

  // Called when receiving a web socket via the main dispatcher handoff.
  readonly #receiveWebsocket = (
    ws: WebSocket,
    action: 'changes' | 'snapshot',
    msg: IncomingMessageSubset,
  ) => {
    switch (action) {
      case 'snapshot':
        return this.#reserveSnapshot(ws, msg);
      case 'changes':
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

  readonly #reserveSnapshot = (ws: WebSocket, req: RequestHeaders) => {
    try {
      const url = new URL(
        req.url ?? '',
        req.headers.origin ?? 'http://localhost',
      );
      const taskID = url.searchParams.get('taskID');
      if (!taskID) {
        throw new Error('Missing taskID in snapshot request');
      }
      const downstream =
        this.#getBackupMonitor().startSnapshotReservation(taskID);
      void streamOut(this._lc, downstream, ws);
    } catch (err) {
      closeWithError(this._lc, ws, err, PROTOCOL_ERROR);
    }
  };

  readonly #subscribe = async (ws: WebSocket, req: RequestHeaders) => {
    try {
      const ctx = getSubscriberContext(req);

      const downstream = await this.#changeStreamer.subscribe(ctx);
      if (ctx.initial && ctx.taskID && this.#backupMonitor) {
        // Now that the change-streamer knows about the subscriber and watermark,
        // end the reservation to safely resume scheduling cleanup.
        this.#backupMonitor.endReservation(ctx.taskID);
      }
      void streamOut(this._lc, downstream, ws);
    } catch (err) {
      closeWithError(this._lc, ws, err, PROTOCOL_ERROR);
    }
  };
}

export class ChangeStreamerHttpClient implements ChangeStreamer {
  readonly #lc: LogContext;
  readonly #shardID: ShardID;
  readonly #changeDB: PostgresDB;

  constructor(lc: LogContext, shardID: ShardID, changeDB: string) {
    this.#lc = lc;
    this.#shardID = shardID;
    // Create a pg client with a single short-lived connection for the purpose
    // of change-streamer discovery (i.e. ChangeDB as DNS).
    this.#changeDB = pgClient(lc, changeDB, {
      max: 1,
      ['idle_timeout']: 15,
      connection: {['application_name']: 'change-streamer-discovery'},
    });
  }

  async #resolveChangeStreamer(path: string) {
    const address = await discoverChangeStreamerAddress(
      this.#shardID,
      this.#changeDB,
    );
    if (!address) {
      throw new Error(`no change-streamer is running`);
    }
    const uri = new URL(
      path,
      address.includes('://') ? `${address}/` : `ws://${address}/`,
    );
    this.#lc.info?.(`connecting to change-streamer@${uri}`);
    return uri;
  }

  async reserveSnapshot(taskID: string): Promise<Source<SnapshotMessage>> {
    const uri = await this.#resolveChangeStreamer(SNAPSHOT_PATH);

    const params = new URLSearchParams({taskID});
    const ws = new WebSocket(uri + `?${params.toString()}`);

    return streamIn(this.#lc, ws, snapshotMessageSchema);
  }

  async subscribe(ctx: SubscriberContext): Promise<Source<Downstream>> {
    const uri = await this.#resolveChangeStreamer(CHANGES_PATH);

    const params = getParams(ctx);
    const ws = new WebSocket(uri + `?${params.toString()}`);

    return streamIn(this.#lc, ws, downstreamSchema);
  }
}

type RequestHeaders = Pick<IncomingMessage, 'url' | 'headers'>;

export function getSubscriberContext(req: RequestHeaders): SubscriberContext {
  const url = new URL(req.url ?? '', req.headers.origin ?? 'http://localhost');
  const protocolVersion = checkPath(url.pathname);
  const params = new URLParams(url);

  return {
    protocolVersion,
    id: params.get('id', true),
    taskID: params.get('taskID', false),
    mode: params.get('mode', false) === 'backup' ? 'backup' : 'serving',
    replicaVersion: params.get('replicaVersion', true),
    watermark: params.get('watermark', true),
    initial: params.getBoolean('initial'),
  };
}

function checkPath(pathname: string): number {
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

// This is called from the client-side (i.e. the replicator).
function getParams(ctx: SubscriberContext): URLSearchParams {
  // The protocolVersion is hard-coded into the CHANGES_PATH.
  const {protocolVersion, ...stringParams} = ctx;
  assert(
    protocolVersion === PROTOCOL_VERSION,
    `replicator should be setting protocolVersion to ${PROTOCOL_VERSION}`,
  );
  return new URLSearchParams({
    ...stringParams,
    taskID: ctx.taskID ? ctx.taskID : '',
    initial: ctx.initial ? 'true' : 'false',
  });
}
