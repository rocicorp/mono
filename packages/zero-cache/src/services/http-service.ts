import {LogContext} from '@rocicorp/logger';
import Fastify, {type FastifyInstance} from 'fastify';
import {HeartbeatMonitor} from './life-cycle.ts';
import {RunningState} from './running-state.ts';
import type {Service} from './service.ts';
import {handleStatzRequest} from './statz.ts';
import {type ZeroConfig} from '../config/zero-config.ts';

export type Options = {
  port: number;
};

/**
 * Common functionality for all HttpServices. These include:
 * * Responding to health checks at "/"
 * * Tracking optional heartbeats at "/keepalive" and draining when they stop.
 */
export class HttpService implements Service {
  readonly id: string;
  protected readonly _lc: LogContext;
  readonly #config: ZeroConfig;
  readonly #fastify: FastifyInstance;
  readonly #port: number;
  readonly #state: RunningState;
  readonly #heartbeatMonitor: HeartbeatMonitor;
  readonly #init: (fastify: FastifyInstance) => void | Promise<void>;

  constructor(
    id: string,
    config: ZeroConfig,
    lc: LogContext,
    opts: Options,
    init: (fastify: FastifyInstance) => void | Promise<void>,
  ) {
    this.id = id;
    this._lc = lc.withContext('component', this.id);
    this.#config = config;
    this.#fastify = Fastify();
    this.#port = opts.port;
    this.#init = init;
    this.#state = new RunningState(id);
    this.#heartbeatMonitor = new HeartbeatMonitor(this._lc);
  }

  // start() is used in unit tests.
  // run() is the lifecycle method called by the ServiceRunner.
  async start(): Promise<string> {
    this.#fastify.get('/', (_req, res) => res.send('OK'));
    this.#fastify.get('/statz', (req, res) =>
      handleStatzRequest(this.#config, req, res),
    );
    this.#fastify.get('/keepalive', ({headers}, res) => {
      this.#heartbeatMonitor.onHeartbeat(headers);
      return res.send('OK');
    });
    await this.#init(this.#fastify);
    const address = await this.#fastify.listen({
      host: '::',
      port: this.#port,
    });
    this._lc.info?.(`${this.id} listening at ${address}`);
    return address;
  }

  async run(): Promise<void> {
    await this.start();
    await this.#state.stopped();
  }

  async stop(): Promise<void> {
    this._lc.info?.(`${this.id}: no longer accepting connections`);
    this.#heartbeatMonitor.stop();
    await this.#fastify.close();
    this.#state.stop(this._lc);
  }
}
