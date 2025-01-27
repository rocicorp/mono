import websocket from '@fastify/websocket';
import {LogContext} from '@rocicorp/logger';
import {IncomingMessage} from 'node:http';
import WebSocket from 'ws';
import {type Worker} from '../../types/processes.ts';
import {streamIn, streamOut, type Source} from '../../types/streams.ts';
import {URLParams} from '../../types/url-params.ts';
import {closeWithProtocolError} from '../../types/ws.ts';
import {installWebSocketReceiver} from '../dispatcher/websocket-handoff.ts';
import {HttpService, type Options} from '../http-service.ts';
import {
  downstreamSchema,
  type ChangeStreamer,
  type Downstream,
  type SubscriberContext,
} from './change-streamer.ts';

const DIRECT_PATH_PATTERN = '/replication/:version/changes';
const TENANT_PATH_PATTERN = '/:tenant' + DIRECT_PATH_PATTERN;

const V0_CHANGES_PATH = '/replication/v0/changes';

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
      closeWithProtocolError(this._lc, ws, err);
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
      ? V0_CHANGES_PATH.substring(1)
      : V0_CHANGES_PATH;
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
  const params = new URLParams(url);

  return {
    id: params.get('id', true),
    mode: params.get('mode', false) === 'backup' ? 'backup' : 'serving',
    replicaVersion: params.get('replicaVersion', true),
    watermark: params.get('watermark', true),
    initial: params.getBoolean('initial'),
  };
}

function getParams(ctx: SubscriberContext): URLSearchParams {
  return new URLSearchParams({
    ...ctx,
    initial: ctx.initial ? 'true' : 'false',
  });
}
