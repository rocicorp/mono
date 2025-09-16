/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {LogContext} from '@rocicorp/logger';
import Fastify, {type FastifyInstance} from 'fastify';
import {HeartbeatMonitor} from './life-cycle.ts';
import {RunningState} from './running-state.ts';
import type {Service} from './service.ts';

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
  readonly #fastify: FastifyInstance;
  readonly #port: number;
  readonly #state: RunningState;
  readonly #heartbeatMonitor: HeartbeatMonitor;
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
    this.#heartbeatMonitor = new HeartbeatMonitor(this._lc);
  }

  /** Override to delay responding to health checks on "/". */
  protected _respondToHealthCheck(): boolean {
    return true;
  }

  /** Override to delay responding to health checks on "/keepalive". */
  protected _respondToKeepalive(): boolean {
    return true;
  }

  // start() is used in unit tests.
  // run() is the lifecycle method called by the ServiceRunner.
  async start(): Promise<string> {
    this.#fastify.get('/', (_req, res) => res.send('OK'));
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
