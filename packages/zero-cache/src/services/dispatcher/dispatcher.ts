import {LogContext} from '@rocicorp/logger';
import Fastify, {FastifyInstance, FastifyReply, FastifyRequest} from 'fastify';
import {IncomingMessage} from 'http';
import {Worker} from 'worker_threads';
import {h32} from 'zero-cache/src/types/xxhash.js';
import {getStatusFromWorker} from '../../workers/replicator.js';
import {Service} from '../service.js';
import {getConnectParams} from './connect-params.js';
import {installWebSocketHandoff} from './websocket-handoff.js';

export const STATUS_URL_PATTERN = '/api/system/:version/status';
export const CONNECT_URL_PATTERN = '/api/sync/:version/connect';

export type Workers = {
  replicator: Worker;
  syncers: Worker[];
};

export class Dispatcher implements Service {
  readonly id = 'dispatcher';
  readonly #lc: LogContext;
  readonly #workersByHostname: (hostname: string) => Workers;
  readonly #fastify: FastifyInstance;

  constructor(
    lc: LogContext,
    workersByHostname: (hostname: string) => Workers,
  ) {
    this.#lc = lc;
    this.#workersByHostname = workersByHostname;
    this.#fastify = Fastify();
    this.#fastify.get(STATUS_URL_PATTERN, (req, res) => this.#status(req, res));
    this.#fastify.addHook('onRequest', (req, _, done) => {
      this.#lc?.debug?.(`received request`, req.hostname, req.url);
      done();
    });

    installWebSocketHandoff(this.#fastify.server, req => this.#handoff(req));
  }

  async #status(request: FastifyRequest, reply: FastifyReply) {
    const {replicator} = this.#workersByHostname(request.hostname);
    const status = await getStatusFromWorker(replicator);
    await reply.send(JSON.stringify(status));
  }

  #handoff(req: IncomingMessage) {
    const {headers, url} = req;
    const {params, error} = getConnectParams(
      new URL(url ?? '', 'http://unused/'),
    );
    if (error !== null) {
      throw new Error(error);
    }
    const {host} = headers;
    if (!host) {
      throw new Error('Missing Host field');
    }
    const {clientGroupID} = params;
    const {syncers} = this.#workersByHostname(host);
    const syncer = h32(clientGroupID) % syncers.length;

    this.#lc.debug?.(`connecting ${clientGroupID} to syncer ${syncer}`);
    return {payload: params, receiver: syncers[syncer]};
  }

  async run(): Promise<void> {
    const address = await this.#fastify.listen({port: 3000});
    this.#lc.info?.(`Server listening at ${address}`);
  }

  async stop(): Promise<void> {
    await this.#fastify.close();
  }
}
