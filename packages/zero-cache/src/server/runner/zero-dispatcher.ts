/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
