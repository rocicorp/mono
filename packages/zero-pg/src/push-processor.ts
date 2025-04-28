import {LogContext, type LogLevel} from '@rocicorp/logger';
import {assert} from '../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import * as v from '../../shared/src/valita.ts';
import {MutationAlreadyProcessedError} from '../../zero-cache/src/services/mutagen/mutagen.ts';
import {
  pushBodySchema,
  pushParamsSchema,
  type Mutation,
  type MutationResponse,
  type PushBody,
  type PushResponse,
} from '../../zero-protocol/src/push.ts';
import {splitMutatorKey} from '../../zql/src/mutate/custom.ts';
import {createLogContext} from './logging.ts';
import type {CustomMutatorDefs} from './custom.ts';

export type Params = v.Infer<typeof pushParamsSchema>;

export interface TransactionProviderHooks {
  updateClientMutationID: () => Promise<{lastMutationID: number | bigint}>;
}

export interface TransactionProviderInput {
  upstreamSchema: string;
  clientGroupID: string;
  clientID: string;
  mutationID: number;
}

export interface DatabaseProvider<T> {
  transaction: <R>(
    callback: (tx: T, transactionHooks: TransactionProviderHooks) => Promise<R>,
    transactionInput: TransactionProviderInput,
  ) => Promise<R>;
}

type ExtractTransactionType<D> =
  D extends DatabaseProvider<infer T> ? T : never;

export class PushProcessor<
  D extends DatabaseProvider<ExtractTransactionType<D>>,
  MD extends CustomMutatorDefs<ExtractTransactionType<D>>,
> {
  readonly #dbProvider: D;
  readonly #lc: LogContext;

  constructor(dbProvider: D, logLevel: LogLevel = 'info') {
    this.#dbProvider = dbProvider;
    this.#lc = createLogContext(logLevel).withContext('PushProcessor');
  }

  /**
   * Processes a push request from zero-cache.
   * This function will parse the request, check the protocol version, and process each mutation in the request.
   * - If a mutation is out of order: processing will stop and an error will be returned. The zero client will retry the mutation.
   * - If a mutation has already been processed: it will be skipped and the processing will continue.
   * - If a mutation receives an application error: it will be skipped, the error will be returned to the client, and processing will continue.
   *
   * @param mutators the custom mutators for the application
   * @param queryString the query string from the request sent by zero-cache. This will include zero's postgres schema name and appID.
   * @param body the body of the request sent by zero-cache as a JSON object.
   */
  async process(
    mutators: MD,
    queryString: URLSearchParams | Record<string, string>,
    body: ReadonlyJSONValue,
  ): Promise<PushResponse>;

  /**
   * This override gets the query string and the body from a Request object.
   *
   * @param mutators the custom mutators for the application
   * @param request A `Request` object.
   */
  async process(mutators: MD, request: Request): Promise<PushResponse>;
  async process(
    mutators: MD,
    queryOrQueryString: Request | URLSearchParams | Record<string, string>,
    body?: ReadonlyJSONValue,
  ): Promise<PushResponse> {
    let queryString: URLSearchParams | Record<string, string>;
    if (queryOrQueryString instanceof Request) {
      const url = new URL(queryOrQueryString.url);
      queryString = url.searchParams;
      body = await queryOrQueryString.json();
    } else {
      queryString = queryOrQueryString;
    }
    const req = v.parse(body, pushBodySchema);
    if (queryString instanceof URLSearchParams) {
      queryString = Object.fromEntries(queryString);
    }
    const queryParams = v.parse(queryString, pushParamsSchema, 'passthrough');

    if (req.pushVersion !== 1) {
      this.#lc.error?.(
        `Unsupported push version ${req.pushVersion} for clientGroupID ${req.clientGroupID}`,
      );
      return {
        error: 'unsupportedPushVersion',
      };
    }

    const responses: MutationResponse[] = [];
    for (const m of req.mutations) {
      const res = await this.#processMutation(mutators, queryParams, req, m);
      responses.push(res);
      if ('error' in res.result) {
        break;
      }
    }

    return {
      mutations: responses,
    };
  }

  async #processMutation(
    mutators: MD,
    params: Params,
    req: PushBody,
    m: Mutation,
  ): Promise<MutationResponse> {
    try {
      return await this.#processMutationImpl(mutators, params, req, m, false);
    } catch (e) {
      if (e instanceof OutOfOrderMutation) {
        this.#lc.error?.(e);
        return {
          id: {
            clientID: m.clientID,
            id: m.id,
          },
          result: {
            error: 'oooMutation',
            details: e.message,
          },
        };
      }

      if (e instanceof MutationAlreadyProcessedError) {
        this.#lc.warn?.(e);
        return {
          id: {
            clientID: m.clientID,
            id: m.id,
          },
          result: {
            error: 'alreadyProcessed',
            details: e.message,
          },
        };
      }

      const ret = await this.#processMutationImpl(
        mutators,
        params,
        req,
        m,
        true,
      );
      if ('error' in ret.result) {
        this.#lc.error?.(
          `Error ${ret.result.error} processing mutation ${m.id} for client ${m.clientID}: ${ret.result.details}`,
        );
        return ret;
      }
      return {
        id: ret.id,
        result: {
          error: 'app',
          details:
            e instanceof Error
              ? e.message
              : 'exception was not of type `Error`',
        },
      };
    }
  }

  #processMutationImpl(
    mutators: MD,
    params: Params,
    req: PushBody,
    m: Mutation,
    errorMode: boolean,
  ): Promise<MutationResponse> {
    if (m.type === 'crud') {
      throw new Error(
        'crud mutators are deprecated in favor of custom mutators.',
      );
    }

    return this.#dbProvider.transaction(
      async (dbTx, transactionHooks): Promise<MutationResponse> => {
        await this.#checkAndIncrementLastMutationID(
          this.#lc,
          transactionHooks,
          m.clientID,
          m.id,
        );

        if (!errorMode) {
          await this.#dispatchMutation(dbTx, mutators, m);
        }

        return {
          id: {
            clientID: m.clientID,
            id: m.id,
          },
          result: {},
        };
      },
      {
        upstreamSchema: params.schema,
        clientGroupID: req.clientGroupID,
        clientID: m.clientID,
        mutationID: m.id,
      },
    );
  }

  #dispatchMutation(
    dbTx: ExtractTransactionType<D>,
    mutators: MD,
    m: Mutation,
  ): Promise<void> {
    const [namespace, name] = splitMutatorKey(m.name);
    if (name === undefined) {
      const mutator = mutators[namespace];
      assert(
        typeof mutator === 'function',
        () => `could not find mutator ${m.name}`,
      );
      return mutator(dbTx, m.args[0]);
    }

    const mutatorGroup = mutators[namespace];
    assert(
      typeof mutatorGroup === 'object',
      () => `could not find mutators for namespace ${namespace}`,
    );
    const mutator = mutatorGroup[name];
    assert(
      typeof mutator === 'function',
      () => `could not find mutator ${m.name}`,
    );
    return mutator(dbTx, m.args[0]);
  }

  async #checkAndIncrementLastMutationID(
    lc: LogContext,
    transactionHooks: TransactionProviderHooks,
    clientID: string,
    receivedMutationID: number,
  ) {
    lc.debug?.(`Incrementing LMID. Received: ${receivedMutationID}`);

    const {lastMutationID} = await transactionHooks.updateClientMutationID();

    if (receivedMutationID < lastMutationID) {
      throw new MutationAlreadyProcessedError(
        clientID,
        receivedMutationID,
        lastMutationID,
      );
    } else if (receivedMutationID > lastMutationID) {
      throw new OutOfOrderMutation(
        clientID,
        receivedMutationID,
        lastMutationID,
      );
    }
    lc.debug?.(
      `Incremented LMID. Received: ${receivedMutationID}. New: ${lastMutationID}`,
    );
  }
}

class OutOfOrderMutation extends Error {
  constructor(
    clientID: string,
    receivedMutationID: number,
    lastMutationID: number | bigint,
  ) {
    super(
      `Client ${clientID} sent mutation ID ${receivedMutationID} but expected ${lastMutationID}`,
    );
  }
}
