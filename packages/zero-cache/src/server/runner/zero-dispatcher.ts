import type {LogContext} from '@rocicorp/logger';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import {handleHeapzRequest} from '../../services/heapz.ts';
import {HttpService, type Options} from '../../services/http-service.ts';
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

  constructor(
    config: NormalizedZeroConfig,
    lc: LogContext,
    opts: Options,
    getWorker: () => Promise<Worker>,
  ) {
    super(`zero-dispatcher`, lc, opts, fastify => {
      fastify.get('/statz', (req, res) =>
        handleStatzRequest(lc, config, req, res),
      );
      fastify.get('/heapz', (req, res) =>
        handleHeapzRequest(lc, config, req, res),
      );
      installWebSocketHandoff(lc, this.#handoff, fastify.server);
    });
    this.#getWorker = getWorker;
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
