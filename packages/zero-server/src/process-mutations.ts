import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {
  pushBodySchema,
  pushParamsSchema,
  type CustomMutation,
  type Mutation,
  type MutationResponse,
  type PushBody,
  type PushResponse,
} from '../../zero-protocol/src/push.ts';
import * as v from '../../shared/src/valita.ts';
import {MutationAlreadyProcessedError} from '../../zero-cache/src/services/mutagen/mutagen.ts';
import {createLogContext} from './logging.ts';
import type {LogContext, LogLevel} from '@rocicorp/logger';
import {assert} from '../../shared/src/asserts.ts';
import type {CustomMutatorDefs, CustomMutatorImpl} from './custom.ts';

export interface TransactionProviderHooks {
  updateClientMutationID: () => Promise<{lastMutationID: number | bigint}>;
  writeMutationResult: (result: MutationResponse) => Promise<void>;
}

export interface TransactionProviderInput {
  upstreamSchema: string;
  clientGroupID: string;
  clientID: string;
  mutationID: number;
}

/**
 * Defines the abstract interface for a database that PushProcessor can execute
 * transactions against.
 */
export interface Database<T> {
  transaction: <R>(
    callback: (tx: T, transactionHooks: TransactionProviderHooks) => Promise<R>,
    transactionInput: TransactionProviderInput,
  ) => Promise<R>;
}

export type ExtractTransactionType<D> = D extends Database<infer T> ? T : never;
export type Params = v.Infer<typeof pushParamsSchema>;

export type TransactFn = <D extends Database<ExtractTransactionType<D>>>(
  dbProvider: D,
  cb: TransactFnCallback<D>,
) => Promise<MutationResponse>;

export type TransactFnCallback<D extends Database<ExtractTransactionType<D>>> =
  (
    tx: ExtractTransactionType<D>,
    mutatorName: string,
    mutatorArgs: ReadonlyJSONValue,
  ) => Promise<void>;

export type Parsed = {
  transact: TransactFn;
  mutations: CustomMutation[];
};

/**
 * Call `cb` for each mutation in the request.
 * The callback is called sequentially for each mutation.
 * If a mutation is out of order, the processing will stop and an error will be returned.
 * If a mutation has already been processed, it will be skipped and the processing will continue.
 * If a mutation receives an application error, it will be skipped, the error will be returned to the client, and processing will continue.
 */
export function handleMutationRequest(
  cb: (
    transact: TransactFn,
    mutation: CustomMutation,
  ) => Promise<MutationResponse>,
  queryString: URLSearchParams | Record<string, string>,
  body: ReadonlyJSONValue,
  logLevel?: LogLevel | undefined,
): Promise<PushResponse>;
export function handleMutationRequest(
  cb: (
    transact: TransactFn,
    mutation: CustomMutation,
  ) => Promise<MutationResponse>,
  request: Request,
  logLevel?: LogLevel | undefined,
): Promise<PushResponse>;
export async function handleMutationRequest(
  cb: (
    transact: TransactFn,
    mutation: CustomMutation,
  ) => Promise<MutationResponse>,
  queryOrQueryString: Request | URLSearchParams | Record<string, string>,
  body?: ReadonlyJSONValue | LogLevel,
  logLevel?: LogLevel | undefined,
): Promise<PushResponse> {
  if (logLevel === undefined) {
    if (queryOrQueryString instanceof Request && typeof body === 'string') {
      logLevel = body as LogLevel;
    } else {
      logLevel = 'info';
    }
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
    return {
      error: 'unsupportedPushVersion',
    };
  }

  const transactor = new Transactor(req, queryParams, logLevel);

  const responses: MutationResponse[] = [];
  for (const m of req.mutations) {
    assert(m.type === 'custom', 'Expected custom mutation');

    const res = await cb(
      (dbProvider, innerCb) => transactor.transact(dbProvider, m, innerCb),
      m,
    );
    responses.push(res);

    // We only stop processing if the mutation is out of order.
    // If the mutation has already been processed or if it returns an application error,
    // we continue processing the next mutation.
    if ('error' in res.result && res.result.error === 'oooMutation') {
      break;
    }
  }

  return {
    mutations: responses,
  };
}

class Transactor {
  readonly #req: PushBody;
  readonly #params: Params;
  readonly #lc: LogContext;

  constructor(req: PushBody, params: Params, logLevel: LogLevel) {
    this.#req = req;
    this.#params = params;
    this.#lc = createLogContext(logLevel).withContext('PushProcessor');
  }

  transact = async <D extends Database<ExtractTransactionType<D>>>(
    dbProvider: D,
    mutation: CustomMutation,
    cb: TransactFnCallback<D>,
  ): Promise<MutationResponse> => {
    let caughtError: unknown = undefined;
    for (;;) {
      try {
        const ret = await this.#transactImpl(
          dbProvider,
          mutation,
          cb,
          caughtError,
        );
        // The first time through we caught an error.
        // We want to report that error as it was an application
        // level error.
        if (caughtError !== undefined) {
          this.#lc.warn?.(
            `Mutation ${mutation.id} for client ${mutation.clientID} was retried after an error: ${caughtError}`,
          );
          return makeAppErrorResponse(mutation, caughtError);
        }

        return ret;
      } catch (e) {
        if (e instanceof OutOfOrderMutation) {
          this.#lc.error?.(e);
          return {
            id: {
              clientID: mutation.clientID,
              id: mutation.id,
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
              clientID: mutation.clientID,
              id: mutation.id,
            },
            result: {
              error: 'alreadyProcessed',
              details: e.message,
            },
          };
        }

        // We threw an error while running in error mode.
        // Re-throw the error and stop processing any further
        // mutations as all subsequent mutations will fail by being
        // out of order.
        if (caughtError !== undefined) {
          throw e;
        }

        caughtError = e;
        this.#lc.error?.(
          `Unexpected error processing mutation ${mutation.id} for client ${mutation.clientID}`,
          e,
        );
      }
    }
  };

  #transactImpl<D extends Database<ExtractTransactionType<D>>>(
    dbProvider: D,
    mutation: CustomMutation,
    cb: TransactFnCallback<D>,
    caughtError: unknown,
  ): Promise<MutationResponse> {
    return dbProvider.transaction(
      async (dbTx, transactionHooks) => {
        await this.#checkAndIncrementLastMutationID(
          transactionHooks,
          mutation.clientID,
          mutation.id,
        );

        if (caughtError === undefined) {
          await cb(dbTx, mutation.name, mutation.args[0]);
        } else {
          const appError = makeAppErrorResponse(mutation, caughtError);
          await transactionHooks.writeMutationResult(appError);
        }

        return {
          id: {
            clientID: mutation.clientID,
            id: mutation.id,
          },
          result: {},
        };
      },
      {
        upstreamSchema: this.#params.schema,
        clientGroupID: this.#req.clientGroupID,
        clientID: mutation.clientID,
        mutationID: mutation.id,
      },
    );
  }

  async #checkAndIncrementLastMutationID(
    transactionHooks: TransactionProviderHooks,
    clientID: string,
    receivedMutationID: number,
  ) {
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
  }
}

export class OutOfOrderMutation extends Error {
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

function makeAppErrorResponse(m: Mutation, e: unknown): MutationResponse {
  return {
    id: {
      clientID: m.clientID,
      id: m.id,
    },
    result: {
      error: 'app',
      details:
        e instanceof Error ? e.message : 'exception was not of type `Error`',
    },
  };
}

export function getMutation(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  mutators: CustomMutatorDefs<any>,
  name: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
): CustomMutatorImpl<any, any> {
  let path: string[];
  if (name.includes('|')) {
    path = name.split('|');
  } else {
    path = name.split('.');
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let mutator: any;
  if (path.length === 1) {
    mutator = mutators[path[0]];
  } else {
    const nextMap = mutators[path[0]];
    assert(
      typeof nextMap === 'object' && nextMap !== undefined,
      `could not find mutator map for ${name}`,
    );
    mutator = nextMap[path[1]];
  }

  assert(typeof mutator === 'function', () => `could not find mutator ${name}`);
  return mutator;
}
