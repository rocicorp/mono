import type {LogContext, LogLevel} from '@rocicorp/logger';
import {assert} from '../../shared/src/asserts.ts';
import {getErrorDetails, getErrorMessage} from '../../shared/src/error.ts';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {promiseVoid} from '../../shared/src/resolved-promises.ts';
import type {MaybePromise} from '../../shared/src/types.ts';
import * as v from '../../shared/src/valita.ts';
import {MutationAlreadyProcessedError} from '../../zero-cache/src/services/mutagen/error.ts';
import type {ApplicationError} from '../../zero-protocol/src/application-error.ts';
import {
  isApplicationError,
  wrapWithApplicationError,
} from '../../zero-protocol/src/application-error.ts';
import {ErrorKind} from '../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../zero-protocol/src/error-reason.ts';
import type {PushFailedBody} from '../../zero-protocol/src/error.ts';
import {
  CLEANUP_RESULTS_MUTATION_NAME,
  cleanupResultsArgSchema,
  pushBodySchema,
  pushParamsSchema,
  type CleanupResultsArg,
  type CustomMutation,
  type Mutation,
  type MutationID,
  type MutationResponse,
  type PushBody,
  type PushResponse,
} from '../../zero-protocol/src/push.ts';
import type {AnyMutatorRegistry} from '../../zql/src/mutate/mutator-registry.ts';
import {isMutator} from '../../zql/src/mutate/mutator.ts';
import type {CustomMutatorDefs, CustomMutatorImpl} from './custom.ts';
import {createLogContext} from './logging.ts';

export interface TransactionProviderHooks {
  updateClientMutationID: () => Promise<{lastMutationID: number | bigint}>;
  writeMutationResult: (result: MutationResponse) => Promise<void>;
  deleteMutationResults: (
    clientGroupID: string,
    clientID: string,
    upToMutationID: number,
  ) => Promise<void>;
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
    callback: (
      tx: T,
      transactionHooks: TransactionProviderHooks,
    ) => MaybePromise<R>,
    transactionInput?: TransactionProviderInput,
  ) => Promise<R>;
}

export type ExtractTransactionType<D> = D extends Database<infer T> ? T : never;
export type Params = v.Infer<typeof pushParamsSchema>;

export type TransactFn<D extends Database<ExtractTransactionType<D>>> = (
  cb: TransactFnCallback<D>,
) => Promise<MutationResponse>;

export type TransactFnCallback<D extends Database<ExtractTransactionType<D>>> =
  (
    tx: ExtractTransactionType<D>,
    mutatorName: string,
    mutatorArgs: ReadonlyJSONValue | undefined,
  ) => Promise<void>;

export type Parsed<D extends Database<ExtractTransactionType<D>>> = {
  transact: TransactFn<D>;
  mutations: CustomMutation[];
};

type MutationPhase = 'preTransaction' | 'transactionPending' | 'postCommit';

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
 * @deprecated Use {@linkcode handleMutateRequest} instead.
 */
export const handleMutationRequest = handleMutateRequest;

export function handleMutateRequest<
  D extends Database<ExtractTransactionType<D>>,
>(
  dbProvider: D,
  cb: (
    transact: TransactFn<D>,
    mutation: CustomMutation,
  ) => Promise<MutationResponse>,
  queryString: URLSearchParams | Record<string, string>,
  body: ReadonlyJSONValue,
  logLevel?: LogLevel,
): Promise<PushResponse>;

export function handleMutateRequest<
  D extends Database<ExtractTransactionType<D>>,
>(
  dbProvider: D,
  cb: (
    transact: TransactFn<D>,
    mutation: CustomMutation,
  ) => Promise<MutationResponse>,
  request: Request,
  logLevel?: LogLevel,
): Promise<PushResponse>;

export async function handleMutateRequest<
  D extends Database<ExtractTransactionType<D>>,
>(
  dbProvider: D,
  cb: (
    transact: TransactFn<D>,
    mutation: CustomMutation,
  ) => Promise<MutationResponse>,
  queryStringOrRequest: Request | URLSearchParams | Record<string, string>,
  bodyOrLogLevel?: ReadonlyJSONValue | LogLevel,
  logLevel?: LogLevel,
): Promise<PushResponse> {
  // Parse overload arguments
  const isRequestOverload = queryStringOrRequest instanceof Request;

  let request: Request | undefined;
  let queryString: URLSearchParams | Record<string, string>;
  let jsonBody: unknown;

  let lc: LogContext;

  if (isRequestOverload) {
    request = queryStringOrRequest;
    const level = (bodyOrLogLevel as LogLevel | undefined) ?? 'info';

    // Create log context early, before extracting JSON from Request
    lc = createLogContext(level).withContext('PushProcessor');

    const url = new URL(request.url);
    queryString = url.searchParams;

    try {
      jsonBody = await request.json();
    } catch (error) {
      lc.error?.('Failed to parse push body', error);
      const message = `Failed to parse push body: ${getErrorMessage(error)}`;
      const details = getErrorDetails(error);
      return {
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.Server,
        reason: ErrorReason.Parse,
        message,
        mutationIDs: [],
        ...(details ? {details} : {}),
      } as const satisfies PushFailedBody;
    }
  } else {
    queryString = queryStringOrRequest;
    jsonBody = bodyOrLogLevel;
    const level = logLevel ?? 'info';
    lc = createLogContext(level).withContext('PushProcessor');
  }

  let mutationIDs: MutationID[] = [];

  let pushBody: PushBody;
  try {
    pushBody = v.parse(jsonBody, pushBodySchema);
    mutationIDs = pushBody.mutations.map(m => ({
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
    const queryStringObj =
      queryString instanceof URLSearchParams
        ? Object.fromEntries(queryString)
        : queryString;
    queryParams = v.parse(queryStringObj, pushParamsSchema, 'passthrough');
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

  if (pushBody.pushVersion !== 1) {
    const response = {
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.UnsupportedPushVersion,
      mutationIDs,
      message: `Unsupported push version: ${pushBody.pushVersion}`,
    } as const satisfies PushFailedBody;
    return response;
  }

  const responses: MutationResponse[] = [];
  let processedCount = 0;

  try {
    const transactor = new Transactor(dbProvider, pushBody, queryParams, lc);

    // Each mutation goes through three phases:
    //   1. Pre-transaction: user logic that runs before `transact` is called. If
    //      this throws we still advance LMID and persist the failure result.
    //   2. Transaction: the callback passed to `transact`, which can be retried
    //      if it fails with an ApplicationError.
    //   3. Post-commit: any logic that runs after `transact` resolves. Failures
    //      here are logged but the mutation remains committed.
    for (const m of pushBody.mutations) {
      // Handle internal mutations (like cleanup) directly without user dispatch
      if (m.type === 'custom' && m.name === CLEANUP_RESULTS_MUTATION_NAME) {
        lc.debug?.(
          `Processing internal mutation '${m.name}' (clientID=${m.clientID})`,
        );
        try {
          await processCleanupResultsMutation(dbProvider, m, queryParams, lc);
          // No response added - this is fire-and-forget
          processedCount++;
        } catch (error) {
          lc.warn?.(
            `Failed to process cleanup mutation for client ${m.clientID}`,
            error,
          );
          // Don't fail the whole push for cleanup errors
          processedCount++;
        }
        continue;
      }

      assert(m.type === 'custom', 'Expected custom mutation');
      lc.debug?.(
        `Processing mutation '${m.name}' (id=${m.id}, clientID=${m.clientID})`,
        m.args,
      );

      let mutationPhase: MutationPhase = 'preTransaction';

      const transactProxy: TransactFn<D> = async innerCb => {
        mutationPhase = 'transactionPending';
        const result = await transactor.transact(m, (tx, name, args) =>
          applicationErrorWrapper(() => innerCb(tx, name, args)),
        );
        mutationPhase = 'postCommit';
        return result;
      };

      try {
        const res = await applicationErrorWrapper(() => cb(transactProxy, m));
        responses.push(res);
        lc.debug?.(`Mutation '${m.name}' (id=${m.id}) completed successfully`);

        processedCount++;
      } catch (error) {
        if (!isApplicationError(error)) {
          throw error;
        }

        if (mutationPhase === 'preTransaction') {
          // Pre-transaction
          await transactor.persistPreTransactionFailure(m, error);
        } else if (mutationPhase === 'postCommit') {
          // Post-commit
          lc.error?.(
            `Post-commit mutation handler failed for mutation ${m.id} for client ${m.clientID}`,
            error,
          );
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

class Transactor<D extends Database<ExtractTransactionType<D>>> {
  readonly #dbProvider: D;
  readonly #req: PushBody;
  readonly #params: Params;
  readonly #lc: LogContext;

  constructor(dbProvider: D, req: PushBody, params: Params, lc: LogContext) {
    this.#dbProvider = dbProvider;
    this.#req = req;
    this.#params = params;
    this.#lc = lc;
  }

  transact = async (
    mutation: CustomMutation,
    cb: TransactFnCallback<D>,
  ): Promise<MutationResponse> => {
    let appError: ApplicationError | undefined = undefined;
    for (;;) {
      try {
        const ret = await this.#transactImpl(mutation, cb, appError);
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

        this.#lc.error?.(
          `Unexpected error processing mutation ${mutation.id} for client ${mutation.clientID}`,
          error,
        );

        throw error;
      }
    }
  };

  async persistPreTransactionFailure(
    mutation: CustomMutation,
    appError: ApplicationError<ReadonlyJSONValue | undefined>,
  ): Promise<MutationResponse> {
    // User-land code threw before calling `transact`. We still need to bump the
    // LMID for this mutation and persist the error so that the client knows it failed.
    const ret = await this.#transactImpl(
      mutation,
      // noop callback since there's no transaction to execute
      () => promiseVoid,
      appError,
    );
    return ret;
  }

  async #transactImpl(
    mutation: CustomMutation,
    cb: TransactFnCallback<D>,
    appError: ApplicationError | undefined,
  ): Promise<MutationResponse> {
    let transactionPhase: DatabaseTransactionPhase = 'open';

    try {
      const ret = await this.#dbProvider.transaction(
        async (dbTx, transactionHooks) => {
          // update the transaction phase to 'execute' after the transaction is opened
          transactionPhase = 'execute';

          await this.#checkAndIncrementLastMutationID(
            transactionHooks,
            mutation.clientID,
            mutation.id,
          );

          if (appError === undefined) {
            this.#lc.debug?.(
              `Executing mutator '${mutation.name}' (id=${mutation.id})`,
            );
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
        this.#getTransactionInput(mutation),
      );

      return ret;
    } catch (error) {
      if (
        isApplicationError(error) ||
        error instanceof OutOfOrderMutation ||
        error instanceof MutationAlreadyProcessedError
      ) {
        throw error;
      }

      throw new DatabaseTransactionError(transactionPhase, {cause: error});
    }
  }

  #getTransactionInput(mutation: CustomMutation): TransactionProviderInput {
    return {
      upstreamSchema: this.#params.schema,
      clientGroupID: this.#req.clientGroupID,
      clientID: mutation.clientID,
      mutationID: mutation.id,
    };
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

/** @deprecated Use getMutator instead */
export function getMutation(
  // oxlint-disable-next-line no-explicit-any
  mutators: AnyMutatorRegistry | CustomMutatorDefs<any>,
  name: string,
  // oxlint-disable-next-line no-explicit-any
): CustomMutatorImpl<any> {
  const path = name.split(/\.|\|/);
  const mutator = getObjectAtPath(mutators, path);
  assert(typeof mutator === 'function', `could not find mutator ${name}`);

  if (isMutator(mutator)) {
    // mutator needs to be called with {tx, args, ctx}
    // CustomMutatorImpl is called with (tx, args, ctx)
    return (tx, args, ctx) => mutator.fn({args, ctx, tx});
  }

  // oxlint-disable-next-line no-explicit-any
  return mutator as CustomMutatorImpl<any>;
}

function getObjectAtPath(
  obj: Record<string, unknown>,
  path: string[],
): unknown {
  let current: unknown = obj;
  for (const part of path) {
    if (typeof current !== 'object' || current === null || !(part in current)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[part];
  }
  return current;
}

/**
 * Processes internal cleanup mutation that deletes acknowledged mutation results
 * from the upstream database. This runs without LMID tracking since it's an
 * internal operation.
 */
async function processCleanupResultsMutation<
  D extends Database<ExtractTransactionType<D>>,
>(
  dbProvider: D,
  mutation: CustomMutation,
  queryParams: Params,
  lc: LogContext,
): Promise<void> {
  const parseResult = v.test(mutation.args[0], cleanupResultsArgSchema);
  if (!parseResult.ok) {
    lc.warn?.('Cleanup mutation has invalid args', parseResult.error);
    return;
  }
  const args: CleanupResultsArg = parseResult.value;

  // Run in a transaction, using the hook for DB-specific operation
  await dbProvider.transaction(
    async (_, hooks) => {
      await hooks.deleteMutationResults(
        args.clientGroupID,
        args.clientID,
        args.upToMutationID,
      );
    },
    // Provide transactionInput for upstreamSchema access in the hook
    {
      upstreamSchema: queryParams.schema,
      clientGroupID: args.clientGroupID,
      clientID: '',
      mutationID: 0,
    },
  );
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
