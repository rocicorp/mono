import websocket from '@fastify/websocket';
import {LogContext} from '@rocicorp/logger';
import {type FastifyReply, type FastifyRequest} from 'fastify';
import WebSocket from 'ws';
import {streamIn, streamOut, type Source} from '../../types/streams.js';
import {URLParams} from '../../types/url-params.js';
import {HttpService, type Options} from '../http-service.js';
import {
  downstreamSchema,
  type ChangeStreamer,
  type Downstream,
  type SubscriberContext,
} from './change-streamer.js';

export const CHANGES_URL_PATTERN = '/api/replication/:version/changes';

export const DEFAULT_PORT = 4849;

export class ChangeStreamerHttpServer extends HttpService {
  readonly id = 'change-streamer-http-server';
  readonly #delegate: ChangeStreamer;

  constructor(
    lc: LogContext,
    delegate: ChangeStreamer,
    opts: Options = {port: DEFAULT_PORT},
  ) {
    super('change-streamer-http-server', lc, opts, async fastify => {
      await fastify.register(websocket);
      fastify.get('/', (_req, res) => res.send('OK'));
      fastify.addHook('preValidation', this.#checkParams);
      fastify.get(CHANGES_URL_PATTERN, {websocket: true}, this.#subscribe);
    });
    this.#delegate = delegate;
  }

  // Avoid upgrading to a websocket if the params are bad.
  readonly #checkParams = async (req: FastifyRequest, reply: FastifyReply) => {
    if (req.url === '/' || req.url.startsWith('/?')) {
      return; // Health check
    }
    try {
      getSubscriberContext(req);
    } catch (e) {
      this._lc.error?.('bad request', String(e));
      await reply.code(400).send(e instanceof Error ? e.message : String(e));
    }
  };

  readonly #subscribe = async (ws: WebSocket, req: FastifyRequest) => {
    const ctx = getSubscriberContext(req); // #checkSubscribe guarantees this.
    const downstream = await this.#delegate.subscribe(ctx);
    await streamOut(this._lc, downstream, ws);
  };
}

export class ChangeStreamerHttpClient implements ChangeStreamer {
  readonly #lc: LogContext;
  readonly #uri: string;

  constructor(lc: LogContext, uriOrPort: string | number = DEFAULT_PORT) {
    this.#lc = lc;
    this.#uri =
      (typeof uriOrPort === 'string'
        ? uriOrPort
        : `ws://localhost:${uriOrPort}`) +
      CHANGES_URL_PATTERN.replace(':version', 'v0');
  }

  subscribe(ctx: SubscriberContext): Promise<Source<Downstream>> {
    this.#lc.info?.(`connecting to change-streamer@${this.#uri}`);
    const params = getParams(ctx);
    const ws = new WebSocket(this.#uri + `?${params.toString()}`);

    return streamIn(this.#lc, ws, downstreamSchema);
  }
}

function getSubscriberContext(req: FastifyRequest): SubscriberContext {
  const url = new URL(req.url, req.headers.origin ?? 'http://localhost');
  const params = new URLParams(url);

  return {
    id: params.get('id', true),
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
