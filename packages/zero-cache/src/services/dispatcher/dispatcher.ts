import {LogContext} from '@rocicorp/logger';
import Fastify, {type FastifyInstance} from 'fastify';
import {IncomingMessage} from 'http';
import {h32} from 'shared/src/xxhash.js';
import type {Worker} from '../../types/processes.js';
import type {Service} from '../service.js';
import {getConnectParams} from './connect-params.js';
import {installWebSocketHandoff} from './websocket-handoff.js';

export const CONNECT_URL_PATTERN = '/api/sync/:version/connect';

export type Workers = {
  syncers: Worker[];
};

export const DEFAULT_PORT = 4848;

export type Options = {
  port: number;
};

export class Dispatcher implements Service {
  readonly id = 'dispatcher';
  readonly #lc: LogContext;
  readonly #workersByHostname: (hostname: string) => Workers;
  readonly #fastify: FastifyInstance;
  readonly #port: number;

  constructor(
    lc: LogContext,
    workersByHostname: (hostname: string) => Workers,
    opts: Partial<Options> = {},
  ) {
    const {port = DEFAULT_PORT} = opts;

    this.#lc = lc;
    this.#workersByHostname = workersByHostname;
    this.#fastify = Fastify();
    this.#fastify.get('/', (_req, res) => res.send('OK'));
    this.#fastify.addHook('onRequest', (req, _, done) => {
      this.#lc?.debug?.(`received request`, req.hostname, req.url);
      done();
    });
    this.#port = port;

    installWebSocketHandoff(this.#fastify.server, req => this.#handoff(req));
  }

  #handoff(req: IncomingMessage) {
    const {headers, url} = req;
    const {params, error} = getConnectParams(
      new URL(url ?? '', 'http://unused/'),
      headers,
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
    const address = await this.#fastify.listen({
      host: '::',
      port: this.#port,
    });
    this.#lc.info?.(`Server listening at ${address}`);
  }

  async stop(): Promise<void> {
    await this.#fastify.close();
  }
}
