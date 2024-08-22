import {LogContext} from '@rocicorp/logger';
import Fastify, {FastifyInstance, FastifyReply, FastifyRequest} from 'fastify';
import {Worker} from 'worker_threads';
import {getStatusFromWorker} from '../../workers/replicator.js';
import {Service} from '../service.js';

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
  }

  async #status(request: FastifyRequest, reply: FastifyReply) {
    const {replicator} = this.#workersByHostname(request.hostname);
    const status = await getStatusFromWorker(replicator);
    await reply.send(JSON.stringify(status));
  }

  async run(): Promise<void> {
    const address = await this.#fastify.listen({port: 3000});
    this.#lc.info?.(`Server listening at ${address}`);
  }

  async stop(): Promise<void> {
    await this.#fastify.close();
  }
}
