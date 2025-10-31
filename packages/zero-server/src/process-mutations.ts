import type {LogContext, LogLevel} from '@rocicorp/logger';
import {assert} from '../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import * as v from '../../shared/src/valita.ts';
import {MutationAlreadyProcessedError} from '../../zero-cache/src/services/mutagen/error.ts';
import {
  ApplicationError,
  isApplicationError,
  wrapWithApplicationError,
} from '../../zero-protocol/src/application-error.ts';
import {ErrorKind} from '../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../zero-protocol/src/error-reason.ts';
import type {PushFailedBody} from '../../zero-protocol/src/error.ts';
import {
  pushBodySchema,
  pushParamsSchema,
  type CustomMutation,
  type Mutation,
  type MutationID,
  type MutationResponse,
  type PushBody,
  type PushResponse,
} from '../../zero-protocol/src/push.ts';
import type {CustomMutatorDefs, CustomMutatorImpl} from './custom.ts';
import {createLogContext} from './logging.ts';
import {getErrorDetails, getErrorMessage} from '../../shared/src/error.ts';

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
    transactionInput?: TransactionProviderInput,
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

const applicationErrorWrapper = async <T>(fn: () => Promise<T>): Promise<T> => {
  try {
    return await fn();
  } catch (error) {
    if (
      error instanceof DatabaseTransactionError ||
      error instanceof OutOfOrderMutation ||
      error instanceof MutationAlreadyProcessedError ||
      isApplicationError(error)
    ) {
      throw error;
    }

    throw wrapWithApplicationError(error);
  }
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
  logLevel?: LogLevel,
): Promise<PushResponse>;
export function handleMutationRequest(
  cb: (
    transact: TransactFn,
    mutation: CustomMutation,
  ) => Promise<MutationResponse>,
  request: Request,
  logLevel?: LogLevel,
): Promise<PushResponse>;
export async function handleMutationRequest(
  cb: (
    transact: TransactFn,
    mutation: CustomMutation,
  ) => Promise<MutationResponse>,
  queryOrQueryString: Request | URLSearchParams | Record<string, string>,
  body?: ReadonlyJSONValue | LogLevel,
  logLevel?: LogLevel,
): Promise<PushResponse> {
  if (logLevel === undefined) {
    if (queryOrQueryString instanceof Request && typeof body === 'string') {
      logLevel = body as LogLevel;
    } else {
      logLevel = 'info';
    }
  }

  const lc = createLogContext(logLevel).withContext('PushProcessor');

  let mutationIDs: MutationID[] = [];

  let req: PushBody;
  try {
    let rawBody: unknown;
    if (queryOrQueryString instanceof Request) {
      rawBody = await queryOrQueryString.json();
    } else {
      rawBody = body;
    }
    req = v.parse(rawBody, pushBodySchema);

    mutationIDs = req.mutations.map(m => ({
      id: m.id,
      clientID: m.clientID,
    }));
  } catch (error) {
    lc.error?.('Failed to parse push body', error);

    const message = `Failed to parse push body: ${getErrorMessage(error)}`;
    const details = getErrorDetails(error);

    return {
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Parse,
      message,
      mutationIDs,
      ...(details ? {details} : {}),
    } as const satisfies PushFailedBody;
  }

  let queryParams: Params;
  try {
    let queryString: URLSearchParams | Record<string, string>;

    if (queryOrQueryString instanceof Request) {
      const url = new URL(queryOrQueryString.url);
      queryString = url.searchParams;
    } else {
      queryString = queryOrQueryString;
    }

    if (queryString instanceof URLSearchParams) {
      queryString = Object.fromEntries(queryString);
    }

    queryParams = v.parse(queryString, pushParamsSchema, 'passthrough');
  } catch (error) {
    lc.error?.('Failed to parse push query parameters', error);

    const message = `Failed to parse push query parameters: ${getErrorMessage(error)}`;
    const details = getErrorDetails(error);

    return {
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Parse,
      message,
      mutationIDs,
      ...(details ? {details} : {}),
    } as const satisfies PushFailedBody;
  }

  if (req.pushVersion !== 1) {
    const response = {
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.UnsupportedPushVersion,
      mutationIDs,
      message: `Unsupported push version: ${req.pushVersion}`,
    } as const satisfies PushFailedBody;
    return response;
  }

  const responses: MutationResponse[] = [];
  let processedCount = 0;

  try {
    const transactor = new Transactor(req, queryParams, lc);

    for (const m of req.mutations) {
      assert(m.type === 'custom', 'Expected custom mutation');

      const transactProxy: TransactFn = (dbProvider, innerCb) =>
        transactor.transact(dbProvider, m, (tx, name, args) =>
          applicationErrorWrapper(() => innerCb(tx, name, args)),
        );

      try {
        const res = await applicationErrorWrapper(() => cb(transactProxy, m));
        responses.push(res);

        processedCount++;
      } catch (error) {
        if (!isApplicationError(error)) {
          throw error;
        }

        lc.warn?.(
          `Application error processing mutation ${m.id} for client ${m.clientID}`,
          error,
        );
        responses.push(makeAppErrorResponse(m, error));
        processedCount++;
      }
    }

    return {
      mutations: responses,
    };
  } catch (error) {
    lc.error?.('Failed to process push request', error);
    // only include mutationIDs for mutations that were not processed
    const unprocessedMutationIDs = mutationIDs.slice(processedCount);

    const message = getErrorMessage(error);
    const details = getErrorDetails(error);

    return {
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason:
        error instanceof OutOfOrderMutation
          ? ErrorReason.OutOfOrderMutation
          : error instanceof DatabaseTransactionError
            ? ErrorReason.Database
            : ErrorReason.Internal,
      message,
      mutationIDs: unprocessedMutationIDs,
      ...(details ? {details} : {}),
    };
  }
}

class Transactor {
  readonly #req: PushBody;
  readonly #params: Params;
  readonly #lc: LogContext;

  constructor(req: PushBody, params: Params, lc: LogContext) {
    this.#req = req;
    this.#params = params;
    this.#lc = lc;
  }

  transact = async <D extends Database<ExtractTransactionType<D>>>(
    dbProvider: D,
    mutation: CustomMutation,
    cb: TransactFnCallback<D>,
  ): Promise<MutationResponse> => {
    let appError: ApplicationError | undefined = undefined;
    for (;;) {
      try {
        const ret = await this.#transactImpl(
          dbProvider,
          mutation,
          cb,
          appError,
        );
        if (appError !== undefined) {
          this.#lc.warn?.(
            `Mutation ${mutation.id} for client ${mutation.clientID} was retried after an error`,
            appError,
          );
          return makeAppErrorResponse(mutation, appError);
        }

        return ret;
      } catch (error) {
        if (error instanceof OutOfOrderMutation) {
          this.#lc.error?.(error);
          throw error;
        }

        if (error instanceof MutationAlreadyProcessedError) {
          this.#lc.warn?.(error);
          return {
            id: {
              clientID: mutation.clientID,
              id: mutation.id,
            },
            result: {
              error: 'alreadyProcessed',
              details: error.message,
            },
          };
        }

        if (isApplicationError(error)) {
          appError = error;
          this.#lc.warn?.(
            `Application error processing mutation ${mutation.id} for client ${mutation.clientID}`,
            appError,
          );
          continue;
        }

        if (error instanceof DatabaseTransactionError) {
          this.#lc.error?.(
            `Database error processing mutation ${mutation.id} for client ${mutation.clientID}`,
            error,
          );
        } else {
          this.#lc.error?.(
            `Unexpected error processing mutation ${mutation.id} for client ${mutation.clientID}`,
            error,
          );
        }

        throw error;
      }
    }
  };

  #transactImpl<D extends Database<ExtractTransactionType<D>>>(
    dbProvider: D,
    mutation: CustomMutation,
    cb: TransactFnCallback<D>,
    appError: ApplicationError | undefined,
  ): Promise<MutationResponse> {
    let transactionPhase: DatabaseTransactionPhase = 'open';

    return dbProvider
      .transaction(
        async (dbTx, transactionHooks) => {
          // update the transaction phase to 'execute' after the transaction is opened
          transactionPhase = 'execute';

          await this.#checkAndIncrementLastMutationID(
            transactionHooks,
            mutation.clientID,
            mutation.id,
          );

          if (appError === undefined) {
            try {
              await cb(dbTx, mutation.name, mutation.args[0]);
            } catch (appError) {
              throw wrapWithApplicationError(appError);
            }
          } else {
            const mutationResult = makeAppErrorResponse(mutation, appError);
            await transactionHooks.writeMutationResult(mutationResult);
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
      )
      .catch(error => {
        if (
          isApplicationError(error) ||
          error instanceof OutOfOrderMutation ||
          error instanceof MutationAlreadyProcessedError
        ) {
          throw error;
        }

        throw new DatabaseTransactionError(transactionPhase, {cause: error});
      });
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

function makeAppErrorResponse(
  m: Mutation,
  error: ApplicationError<ReadonlyJSONValue | undefined>,
): MutationResponse {
  return {
    id: {
      clientID: m.clientID,
      id: m.id,
    },
    result: {
      error: 'app',
      message: error.message,
      ...(error.details ? {details: error.details} : {}),
    },
  };
}

export function getMutation(
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  mutators: CustomMutatorDefs<any>,
  name: string,
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
): CustomMutatorImpl<any, any> {
  let path: string[];
  if (name.includes('|')) {
    path = name.split('|');
  } else {
    path = name.split('.');
  }

  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
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

type DatabaseTransactionPhase = 'open' | 'execute';
class DatabaseTransactionError extends Error {
  constructor(phase: DatabaseTransactionPhase, options?: ErrorOptions) {
    super(
      phase === 'open'
        ? `Failed to open database transaction: ${getErrorMessage(options?.cause)}`
        : `Database transaction failed after opening: ${getErrorMessage(options?.cause)}`,
      options,
    );
    this.name = 'DatabaseTransactionError';
  }
}
