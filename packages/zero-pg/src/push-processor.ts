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
  after: (task: PostCommitTask) => void;
}

export interface DatabaseProvider<T> {
  transaction: <R>(
    callback: (tx: T, transactionHooks: TransactionProviderHooks) => Promise<R>,
    transactionInput: TransactionProviderInput,
  ) => Promise<R>;
}

type ExtractTransactionType<D> =
  D extends DatabaseProvider<infer T> ? T : never;

type PostCommitTask = () => Promise<void>;

export interface PushProcessorOptions {
  /**
   * The log level to use for messages about mutation processing.
   *
   * @default 'info'
   */
  logLevel?: LogLevel;

  /**
   * Whether to allow async background work. In async mode, `.process()` returns mutation responses
   * while tasks scheduled using `tx.after()` are still running in the background. The default of `false`
   * is a good option for serverless applications.
   *
   * If your server runs in a long-lived process like a container, setting this to `true`
   * may improve latency and throughput by responding to mutation requests
   * before post-commit tasks have completed.
   *
   * If using async mode, make sure to call `.close()` when your application shuts down
   * to ensure that all `tx.after()` tasks have a chance to complete.
   *
   * @default false
   */
  async?: boolean;
}

export class PushProcessor<
  D extends DatabaseProvider<ExtractTransactionType<D>>,
  MD extends CustomMutatorDefs<ExtractTransactionType<D>>,
> {
  readonly #dbProvider: D;
  readonly #lc: LogContext;
  readonly #async: boolean;
  readonly #pendingPostCommitTasks: Set<Promise<void>>;

  #closed: boolean;

  constructor(dbProvider: D, options: PushProcessorOptions = {}) {
    this.#dbProvider = dbProvider;
    this.#lc = createLogContext(options.logLevel ?? 'info').withContext(
      'PushProcessor',
    );
    this.#async = options.async ?? false;
    this.#pendingPostCommitTasks = new Set();
    this.#closed = false;
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
    if (this.#closed) {
      throw new Error(
        'PushProcessor has been closed and cannot process any more mutations',
      );
    }

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
    const postCommitTasks: PostCommitTask[] = [];
    for (const m of req.mutations) {
      const res = await this.#processMutation(
        mutators,
        queryParams,
        req,
        m,
        postCommitTasks,
      );
      responses.push(res);
      if ('error' in res.result) {
        break;
      }
    }

    await this.#executePostCommitTasks(postCommitTasks);

    return {
      mutations: responses,
    };
  }

  /**
   * Causes the processor to stop accepting new mutations. Returns a promise that resolves when all
   * tasks scheduled using `tx.after()` have completed.
   *
   * Mostly useful when `async` is set to `true` in the constructor options, for example in long-lived
   * environments like containers.
   *
   * Call and await this function in your application's shutdown sequence to ensure that all `tx.after()`
   * tasks have a chance to run before the application shuts down.
   */
  async close() {
    this.#closed = true;

    if (this.#pendingPostCommitTasks.size > 0) {
      await Promise.all(this.#pendingPostCommitTasks);
    }
  }

  async #processMutation(
    mutators: MD,
    params: Params,
    req: PushBody,
    m: Mutation,
    allPostCommitTasks: PostCommitTask[],
  ): Promise<MutationResponse> {
    try {
      const postCommitTasksForMutation: PostCommitTask[] = [];

      const res = await this.#processMutationImpl(
        mutators,
        params,
        req,
        m,
        false,
        postCommitTasksForMutation,
      );

      allPostCommitTasks.push(...postCommitTasksForMutation);

      return res;
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
    postCommitTasks?: PostCommitTask[],
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
        after: (task: PostCommitTask) => {
          postCommitTasks?.push(task);
        },
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

  async #executePostCommitTasks(postCommitTasks: PostCommitTask[]) {
    const postCommitTaskResults = postCommitTasks.map(task => task());

    const resultsSettled = Promise.allSettled(postCommitTaskResults).then(
      results => {
        for (const result of results) {
          if (result.status === 'rejected') {
            this.#lc.error?.(result.reason);
          }
        }

        this.#pendingPostCommitTasks.delete(resultsSettled);
      },
    );

    this.#pendingPostCommitTasks.add(resultsSettled);

    if (!this.#async) {
      await resultsSettled;
    }
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
