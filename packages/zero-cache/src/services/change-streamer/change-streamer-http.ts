import websocket from '@fastify/websocket';
import type {LogContext} from '@rocicorp/logger';
import type {IncomingMessage} from 'node:http';
import WebSocket from 'ws';
import {assert} from '../../../../shared/src/asserts.ts';
import {must} from '../../../../shared/src/must.ts';
import type {ZeroConfig} from '../../config/zero-config.ts';
import type {IncomingMessageSubset} from '../../types/http.ts';
import {pgClient, type PostgresDB} from '../../types/pg.ts';
import {type Worker} from '../../types/processes.ts';
import {type ShardID} from '../../types/shards.ts';
import {streamIn, streamOut, type Source} from '../../types/streams.ts';
import {URLParams} from '../../types/url-params.ts';
import {installWebSocketReceiver} from '../../types/websocket-handoff.ts';
import {closeWithError, PROTOCOL_ERROR} from '../../types/ws.ts';
import {HttpService} from '../http-service.ts';
import type {Service} from '../service.ts';
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

type Options = {
  port: number;
  startupDelayMs: number;
};

export class ChangeStreamerHttpServer extends HttpService {
  readonly id = 'change-streamer-http-server';
  readonly #lc: LogContext;
  readonly #opts: Options;
  readonly #changeStreamer: ChangeStreamer & Service;
  readonly #backupMonitor: BackupMonitor | null;

  constructor(
    lc: LogContext,
    config: ZeroConfig,
    opts: Options,
    parent: Worker,
    changeStreamer: ChangeStreamer & Service,
    backupMonitor: BackupMonitor | null,
  ) {
    super('change-streamer-http-server', lc, opts, async fastify => {
      const websocketOptions: {perMessageDeflate?: boolean | object} = {};
      if (config.websocketCompression) {
        if (config.websocketCompressionOptions) {
          try {
            websocketOptions.perMessageDeflate = JSON.parse(
              config.websocketCompressionOptions,
            );
          } catch (e) {
            throw new Error(
              `Failed to parse ZERO_WEBSOCKET_COMPRESSION_OPTIONS: ${String(e)}. Expected valid JSON.`,
            );
          }
        } else {
          websocketOptions.perMessageDeflate = true;
        }
      }

      await fastify.register(websocket, {
        options: websocketOptions,
      });

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

    this.#lc = lc;
    this.#opts = opts;
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

  readonly #reserveSnapshot = async (ws: WebSocket, req: RequestHeaders) => {
    try {
      // Guard against WebSocket being closed during handoff from dispatcher.
      // This can happen due to network issues or client disconnection.
      if (ws.readyState === ws.CLOSED || ws.readyState === ws.CLOSING) {
        this._lc.warn?.('websocket closed before snapshot stream could start');
        return;
      }
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
        await this.#getBackupMonitor().startSnapshotReservation(taskID);
      void streamOut(this._lc, downstream, ws);
    } catch (err) {
      closeWithError(this._lc, ws, err, PROTOCOL_ERROR);
    }
  };

  readonly #subscribe = async (ws: WebSocket, req: RequestHeaders) => {
    try {
      // Guard against WebSocket being closed during handoff from dispatcher.
      // This can happen due to network issues or client disconnection.
      if (ws.readyState === ws.CLOSED || ws.readyState === ws.CLOSING) {
        this._lc.warn?.('websocket closed before subscribe stream could start');
        return;
      }
      const ctx = getSubscriberContext(req);
      if (ctx.mode === 'serving') {
        this.#ensureChangeStreamerStarted('incoming subscription');
      }

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

  #changeStreamerStarted = false;

  #ensureChangeStreamerStarted(reason: string) {
    if (!this.#changeStreamerStarted && this._state.shouldRun()) {
      this.#lc.info?.(`starting ChangeStreamerService: ${reason}`);
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
    const {startupDelayMs} = this.#opts;
    this._state.setTimeout(
      () =>
        this.#ensureChangeStreamerStarted(
          `startup delay elapsed (${startupDelayMs} ms)`,
        ),
      startupDelayMs,
    );
  }

  protected override async _onStop(): Promise<void> {
    if (this.#changeStreamerStarted) {
      await this.#changeStreamer.stop();
    }
  }
}

export class ChangeStreamerHttpClient implements ChangeStreamer {
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
    this.#changeDB = pgClient(lc, changeDB, {
      max: 1,
      ['idle_timeout']: 15,
      connection: {['application_name']: 'change-streamer-discovery'},
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
  const protocolVersion = checkProtocolVersion(url.pathname);
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
