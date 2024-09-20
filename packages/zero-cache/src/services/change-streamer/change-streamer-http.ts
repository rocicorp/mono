import websocket from '@fastify/websocket';
import {LogContext} from '@rocicorp/logger';
import Fastify, {FastifyInstance, FastifyReply, FastifyRequest} from 'fastify';
import * as v from 'shared/src/valita.js';
import WebSocket from 'ws';
import {jsonValueSchema} from 'zero-cache/src/types/bigint-json.js';
import {Source, streamIn, streamOut} from 'zero-cache/src/types/streams.js';
import {URLParams} from 'zero-cache/src/types/url-params.js';
import {RunningState} from '../running-state.js';
import {Service} from '../service.js';
import {
  ChangeStreamer,
  Downstream,
  SubscriberContext,
} from './change-streamer.js';

export const CHANGES_URL_PATTERN = '/api/replication/:version/changes';

export type Options = {
  port: number; // Defaults to 3001.
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
    const {port = 3001} = opts;

    this.#lc = lc.withContext('component', this.id);
    this.#delegate = delegate;

    this.#fastify = Fastify();
    this.#port = port;
  }

  // start() is used in unit tests.
  // run() is the lifecycle method called by the ServiceRunner.
  async start(): Promise<void> {
    await this.#fastify.register(websocket);
    this.#fastify.addHook('preValidation', this.#checkParams);
    this.#fastify.get(CHANGES_URL_PATTERN, {websocket: true}, this.#subscribe);

    const address = await this.#fastify.listen({port: this.#port});
    this.#lc.info?.(`Server listening at ${address}`);
  }

  async run(): Promise<void> {
    await this.start();
    await this.#state.stopped();
  }

  // Avoid upgrading to a websocket if the params are bad.
  readonly #checkParams = async (req: FastifyRequest, reply: FastifyReply) => {
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
  readonly #port: number;

  constructor(lc: LogContext, port = 3001) {
    this.#lc = lc;
    this.#port = port;
  }

  subscribe(ctx: SubscriberContext): Source<Downstream> {
    const params = getParams(ctx);
    const ws = new WebSocket(
      `ws://localhost:${this.#port}` +
        CHANGES_URL_PATTERN.replace(':version', 'v0') +
        `?${params.toString()}`,
    );

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
