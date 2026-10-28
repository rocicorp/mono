import type {LogContext} from '@rocicorp/logger';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import {handleHeapzRequest} from '../../services/heapz.ts';
import {HttpService, type Options} from '../../services/http-service.ts';
import {handlePlannerzRequest} from '../../services/plannerz.ts';
import {
  handleProfrmzRequest,
  handleProfzRequest,
} from '../../services/profz.ts';
import {handleStatzRequest} from '../../services/statz.ts';
import type {IncomingMessageSubset} from '../../types/http.ts';
import type {Worker} from '../../types/processes.ts';
import {
  installWebSocketHandoff,
  type HandoffSpec,
} from '../../types/websocket-handoff.ts';

export class ZeroDispatcher extends HttpService {
  readonly id = 'zero-dispatcher';
  readonly #getWorker: () => Promise<Worker>;
  readonly #onStart: (() => void) | undefined;
  readonly #shutdownDelayMs: number;
  #draining: Promise<void> | undefined;

  constructor(
    config: NormalizedZeroConfig,
    lc: LogContext,
    opts: Options,
    getWorker: () => Promise<Worker>,
    onStart?: () => void,
  ) {
    super(`zero-dispatcher`, lc, opts, fastify => {
      fastify.get('/statz', (req, res) =>
        handleStatzRequest(lc, config, req, res),
      );
      fastify.get('/plannerz', (req, res) =>
        handlePlannerzRequest(lc, config, req, res),
      );
      fastify.get('/heapz', (req, res) =>
        handleHeapzRequest(lc, config, req, res),
      );
      fastify.get('/profz', (req, res) =>
        handleProfzRequest(lc, config, req, res, this.#getWorker),
      );
      fastify.get('/profrmz', (req, res) =>
        handleProfrmzRequest(lc, config, req, res, this.#getWorker),
      );
      installWebSocketHandoff(lc, this.#handoff, fastify.server);
    });
    this.#getWorker = getWorker;
    this.#onStart = onStart;
    this.#shutdownDelayMs = config.shutdownDelayMs;
  }

  // Shared with ProcessManager so the listener and its workers remain available
  // until readiness withdrawal has propagated to the load balancer.
  drain(): Promise<void> {
    return (this.#draining ??= this.#prepareToDrain());
  }

  async #prepareToDrain(): Promise<void> {
    this.failReadinessChecks();
    if (this.#shutdownDelayMs > 0) {
      this._lc.info?.(`waiting ${this.#shutdownDelayMs} ms before draining`);
      await this._state.sleep(this.#shutdownDelayMs);
    }
  }

  protected override _onStart() {
    this.#onStart?.();
  }

  readonly #handoff = (
    _req: IncomingMessageSubset,
    dispatch: (h: HandoffSpec<string>) => void,
    onError: (error: unknown) => void,
  ) => {
    void this.#getWorker().then(
      sender => dispatch({payload: 'unused', sender}),
      onError,
    );
  };
}
