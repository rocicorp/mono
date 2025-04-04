import websocket from '@fastify/websocket';
import {LogContext} from '@rocicorp/logger';
import {IncomingMessage} from 'node:http';
import WebSocket from 'ws';
import {assert} from '../../../../shared/src/asserts.ts';
import {type Worker} from '../../types/processes.ts';
import {streamIn, streamOut, type Source} from '../../types/streams.ts';
import {URLParams} from '../../types/url-params.ts';
import {closeWithError, PROTOCOL_ERROR} from '../../types/ws.ts';
import {installWebSocketReceiver} from '../dispatcher/websocket-handoff.ts';
import {HttpService, type Options} from '../http-service.ts';
import {
  downstreamSchema,
  PROTOCOL_VERSION,
  type ChangeStreamer,
  type Downstream,
  type SubscriberContext,
} from './change-streamer.ts';

const MIN_SUPPORTED_PROTOCOL_VERSION = 0;

const DIRECT_PATH_PATTERN = '/replication/:version/changes';
const TENANT_PATH_PATTERN = '/:tenant' + DIRECT_PATH_PATTERN;
const PATH_REGEX =
  /(?<tenant>[^/]+\/)?\/replication\/v(?<version>\d+)\/changes$/;

const CHANGES_PATH = `/replication/v${PROTOCOL_VERSION}/changes`;

export class ChangeStreamerHttpServer extends HttpService {
  readonly id = 'change-streamer-http-server';
  readonly #delegate: ChangeStreamer;

  constructor(
    lc: LogContext,
    delegate: ChangeStreamer,
    opts: Options,
    parent: Worker,
  ) {
    super('change-streamer-http-server', lc, opts, async fastify => {
      await fastify.register(websocket);

      // fastify does not support optional path components, so we just
      // register both patterns.
      fastify.get(DIRECT_PATH_PATTERN, {websocket: true}, this.#subscribe);
      fastify.get(TENANT_PATH_PATTERN, {websocket: true}, this.#subscribe);

      installWebSocketReceiver<SubscriberContext>(
        fastify.websocketServer,
        this.#handleSubscription,
        parent,
      );
    });

    this.#delegate = delegate;
  }

  readonly #subscribe = async (ws: WebSocket, req: RequestHeaders) => {
    let ctx: SubscriberContext;
    try {
      ctx = getSubscriberContext(req);
    } catch (err) {
      closeWithError(this._lc, ws, err, PROTOCOL_ERROR);
      return;
    }
    await this.#handleSubscription(ws, ctx);
  };

  readonly #handleSubscription = async (
    ws: WebSocket,
    ctx: SubscriberContext,
  ) => {
    const downstream = await this.#delegate.subscribe(ctx);
    await streamOut(this._lc, downstream, ws);
  };
}

export class ChangeStreamerHttpClient implements ChangeStreamer {
  readonly #lc: LogContext;
  readonly #uri: string;

  constructor(lc: LogContext, uri: string) {
    const url = new URL(uri);
    url.pathname += url.pathname.endsWith('/')
      ? CHANGES_PATH.substring(1)
      : CHANGES_PATH;
    uri = url.toString();
    this.#lc = lc;
    this.#uri = uri;
  }

  subscribe(ctx: SubscriberContext): Promise<Source<Downstream>> {
    this.#lc.info?.(`connecting to change-streamer@${this.#uri}`);
    const params = getParams(ctx);
    const ws = new WebSocket(this.#uri + `?${params.toString()}`);

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
    initial: ctx.initial ? 'true' : 'false',
  });
}
