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
import {handleAnalyzeQueryRequest, setCors} from '../../services/analyze.ts';

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
      fastify.options('/analyze-queryz', (_req, res) =>
        setCors(res)
          .header('Access-Control-Max-Age', '86400')
          .status(204)
          .send(),
      );
      fastify.post('/analyze-queryz', (req, res) =>
        handleAnalyzeQueryRequest(lc, config, req, res),
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
