import {LogContext} from '@rocicorp/logger';
import Fastify, {type FastifyInstance} from 'fastify';
import {RunningState} from './running-state.js';
import type {Service} from './service.js';

export type Options = {
  port: number;
};

export class HttpService implements Service {
  readonly id: string;
  protected readonly _lc: LogContext;
  readonly #fastify: FastifyInstance;
  readonly #port: number;
  readonly #state: RunningState;
  readonly #init: (fastify: FastifyInstance) => void | Promise<void>;

  constructor(
    id: string,
    lc: LogContext,
    opts: Options,
    init: (fastify: FastifyInstance) => void | Promise<void>,
  ) {
    this.id = id;
    this._lc = lc.withContext('component', this.id);
    this.#fastify = Fastify();
    this.#port = opts.port;
    this.#init = init;
    this.#state = new RunningState(id);
  }

  // start() is used in unit tests.
  // run() is the lifecycle method called by the ServiceRunner.
  async start(): Promise<void> {
    await this.#init(this.#fastify);
    const address = await this.#fastify.listen({
      host: '::',
      port: this.#port,
    });
    this._lc.info?.(`${this.id} listening at ${address}`);
  }

  async run(): Promise<void> {
    await this.start();
    await this.#state.stopped();
  }

  async stop(): Promise<void> {
    this._lc.info?.(`${this.id}: no longer accepting connections`);
    await this.#fastify.close();
    this.#state.stop(this._lc);
  }
}
