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
  type WebSocketHandoff,
} from '../../types/websocket-handoff.ts';
import {expiredAffinityCookieHeader, rehome} from '../../workers/rehome.ts';
import {parsePath} from '../worker-dispatcher.ts';

export class ZeroDispatcher extends HttpService {
  readonly id = 'zero-dispatcher';
  readonly #getWorker: () => Promise<Worker>;
  readonly #onStart: (() => void) | undefined;
  readonly #whenDrained: (() => Promise<void>) | undefined;
  readonly #drainingHeaders: string[];
  #draining = false;

  /**
   * @param whenDrained Resolves when the workers behind this dispatcher have
   *        drained (exited). The listener stays open until then, rehoming new
   *        sync connections, so that clients that are still routed here (e.g.
   *        by a sticky cookie) are not refused while the load balancer
   *        catches up.
   */
  constructor(
    config: NormalizedZeroConfig,
    lc: LogContext,
    opts: Options,
    getWorker: () => Promise<Worker>,
    onStart?: () => void,
    whenDrained?: () => Promise<void>,
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
    this.#whenDrained = whenDrained;
    const expiredCookie = expiredAffinityCookieHeader(config);
    this.#drainingHeaders = expiredCookie ? [expiredCookie] : [];
  }

  async drain() {
    this.#draining = true;
    this.failHealthChecks();
    await this.#whenDrained?.();
  }

  protected override _onStart() {
    this.#onStart?.();
  }

  readonly #handoff: WebSocketHandoff<string> = (
    req: IncomingMessageSubset,
    dispatch: (h: HandoffSpec<string>) => void,
    onError: (error: unknown) => void,
    accept,
  ) => {
    if (this.#draining && this.#isSync(req)) {
      // Respond here rather than handing off to a sync worker, which may
      // have already exited after draining its own clients.
      accept(ws => rehome(this._lc, ws), this.#drainingHeaders);
      return;
    }
    void this.#getWorker().then(
      sender => dispatch({payload: 'unused', sender}),
      onError,
    );
  };

  #isSync({url}: IncomingMessageSubset) {
    return parsePath(new URL(url ?? '', 'http://unused/'))?.worker === 'sync';
  }
}
