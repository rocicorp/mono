import websocket from '@fastify/websocket';
import {LogContext} from '@rocicorp/logger';
import Fastify, {
  type FastifyInstance,
  type FastifyReply,
  type FastifyRequest,
} from 'fastify';
import * as v from '../../../../shared/src/valita.js';
import WebSocket from 'ws';
import {jsonValueSchema} from '../../types/bigint-json.js';
import {type Source, streamIn, streamOut} from '../../types/streams.js';
import {URLParams} from '../../types/url-params.js';
import {RunningState} from '../running-state.js';
import type {Service} from '../service.js';
import type {
  ChangeStreamer,
  Downstream,
  SubscriberContext,
} from './change-streamer.js';

export const CHANGES_URL_PATTERN = '/api/replication/:version/changes';

export const DEFAULT_PORT = 4849;

export type Options = {
  port: number;
};

export class ChangeStreamerHttpServer implements Service {
  readonly id = 'change-streamer-http-server';
  readonly #lc: LogContext;
  readonly #delegate: ChangeStreamer;
  readonly #fastify: FastifyInstance;
  readonly #port: number;
  readonly #state = new RunningState(this.id);

  constructor(
    lc: LogContext,
    delegate: ChangeStreamer,
    opts: Partial<Options> = {},
  ) {
    const {port = DEFAULT_PORT} = opts;

    this.#lc = lc.withContext('component', this.id);
    this.#delegate = delegate;

    this.#fastify = Fastify();
    this.#port = port;
  }

  // start() is used in unit tests.
  // run() is the lifecycle method called by the ServiceRunner.
  async start(): Promise<void> {
    await this.#fastify.register(websocket);
    this.#fastify.get('/', (_req, res) => res.send('OK'));
    this.#fastify.addHook('preValidation', this.#checkParams);
    this.#fastify.get(CHANGES_URL_PATTERN, {websocket: true}, this.#subscribe);

    const address = await this.#fastify.listen({host: '::', port: this.#port});
    this.#lc.info?.(`Server listening at ${address}`);
  }

  async run(): Promise<void> {
    await this.start();
    await this.#state.stopped();
  }

  // Avoid upgrading to a websocket if the params are bad.
  readonly #checkParams = async (req: FastifyRequest, reply: FastifyReply) => {
    if (req.url === '/' || req.url.startsWith('/?')) {
      return; // Health check
    }
    try {
      getSubscriberContext(req);
    } catch (e) {
      this.#lc.error?.('bad request', String(e));
      await reply.code(400).send(e instanceof Error ? e.message : String(e));
    }
  };

  readonly #subscribe = async (ws: WebSocket, req: FastifyRequest) => {
    const ctx = getSubscriberContext(req); // #checkSubscribe guarantees this.
    const downstream = this.#delegate.subscribe(ctx);
    await streamOut(this.#lc, downstream, ws);
  };

  async stop(): Promise<void> {
    await this.#fastify.close();
    this.#state.stop(this.#lc);
  }
}

export class ChangeStreamerHttpClient implements ChangeStreamer {
  readonly #lc: LogContext;
  readonly #uri: string;

  constructor(lc: LogContext, uriOrPort: string | number = DEFAULT_PORT) {
    this.#lc = lc;
    this.#uri =
      typeof uriOrPort === 'string'
        ? uriOrPort
        : `ws://localhost:${uriOrPort}` +
          CHANGES_URL_PATTERN.replace(':version', 'v0');
  }

  subscribe(ctx: SubscriberContext): Source<Downstream> {
    const params = getParams(ctx);
    const ws = new WebSocket(this.#uri + `?${params.toString()}`);

    return streamIn(this.#lc, ws, downstreamSchema) as Source<Downstream>;
  }
}

// TODO: Define this more precisely.
const downstreamSchema = v.array(jsonValueSchema);

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
