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
  mutateParamsSchema,
  type MutateResponse,
} from '../../zero-protocol/src/mutate-server.ts';
import type {MutationID} from '../../zero-protocol/src/mutation-id.ts';
import {
  CLEANUP_RESULTS_MUTATION_NAME,
  cleanupResultsArgSchema,
  type CleanupResultsArg,
  type CustomMutation,
  type Mutation,
} from '../../zero-protocol/src/mutation.ts';
import {
  pushBodySchema,
  type MutationResponse,
  type PushBody,
} from '../../zero-protocol/src/push.ts';
import type {AnyMutatorRegistry} from '../../zql/src/mutate/mutator-registry.ts';
import {isMutator} from '../../zql/src/mutate/mutator.ts';
import type {CustomMutatorDefs, CustomMutatorImpl} from './custom.ts';
import {createLogContext} from './logging.ts';
import {separatorRe} from './push-processor.ts';

export interface TransactionProviderHooks {
  updateClientMutationID: () => Promise<{lastMutationID: number | bigint}>;
  writeMutationResult: (result: MutationResponse) => Promise<void>;
  deleteMutationResults: (args: CleanupResultsArg) => Promise<void>;
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
export type Params = v.Infer<typeof mutateParamsSchema>;

export type TransactFn<D extends Database<ExtractTransactionType<D>>> = (
  cb: TransactFnCallback<D>,
) => Promise<MutationResponse>;

export type TransactFnCallback<D extends Database<ExtractTransactionType<D>>> =
  (
    tx: ExtractTransactionType<D>,
    mutatorName: string,
    mutatorArgs: ReadonlyJSONValue | undefined,
  ) => Promise<ReadonlyJSONValue | void>;

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

/**
 * Parsed query params accepted by {@linkcode handleMutateRequest} when the
 * incoming request URL has already been handled by your framework.
 */
export type MutateSearchParams = URLSearchParams | Record<string, string>;

/**
 * Runs once per custom mutation in a `/mutate` request.
 *
 * Call `transact` to execute database work for the mutation. Throwing before
 * `transact` persists an application error; throwing after `transact` resolves
 * only affects server-side logging because the mutation is already committed.
 */
export type MutateRequestHandler<
  D extends Database<ExtractTransactionType<D>>,
> = (
  transact: TransactFn<D>,
  mutation: CustomMutation,
) => Promise<MutationResponse>;

export type HandleMutateRequestArgs<
  D extends Database<ExtractTransactionType<D>>,
> = {
  /** Database used to run transactions and store mutation results. */
  dbProvider: D;
  handler: MutateRequestHandler<D>;
  /**
   * Authenticated user ID. Null or undefined means the user is logged out.
   */
  userID: string | null | undefined;
  /** Optional log level for request parsing and execution. */
  logLevel?: LogLevel | undefined;
} & (
  | {
      /** Fetch request containing both query params and the JSON body. */
      request: Request;
    }
  | {
      /** Parsed query params from the `/mutate` request URL. */
      query: MutateSearchParams;
      /** Parsed JSON body from the `/mutate` request. */
      body: ReadonlyJSONValue;
    }
);

type NormalizedMutateRequestArgs<
  D extends Database<ExtractTransactionType<D>>,
> = {
  readonly dbProvider: D;
  readonly handler: MutateRequestHandler<D>;
  // Note: semantics of undefined differ from HandleMutateRequestArgs.userID.
  // Here, undefined means the app didn't provide a user ID - we do not know if
  // the user is logged in or not. This is legacy behavior needed to support
  // deprecated signatures of handleMutateRequest which did not receive userID
  // from app.
  readonly userID: string | null | undefined;
  readonly logLevel: LogLevel;
} & (
  | {
      readonly type: 'request';
      readonly request: Request;
    }
  | {
      readonly type: 'body';
      readonly queryParams: Record<string, string>;
      readonly jsonBody: ReadonlyJSONValue;
    }
);

/**
 * Process a `/mutate` request from a Fetch `Request`.
 */
export function handleMutateRequest<
  D extends Database<ExtractTransactionType<D>>,
>(input: HandleMutateRequestArgs<D>): Promise<MutateResponse>;

/**
 * @deprecated Pass a single object instead:
 * `handleMutateRequest({dbProvider, handler, query, body, userID, logLevel})`.
 */
export function handleMutateRequest<
  D extends Database<ExtractTransactionType<D>>,
>(
  dbProvider: D,
  handler: MutateRequestHandler<D>,
  query: MutateSearchParams,
  body: ReadonlyJSONValue,
  logLevel?: LogLevel,
): Promise<MutateResponse>;

/**
 * @deprecated Pass a single object instead:
 * `handleMutateRequest({dbProvider, handler, request, userID, logLevel})`.
 */
export function handleMutateRequest<
  D extends Database<ExtractTransactionType<D>>,
>(
  dbProvider: D,
  handler: MutateRequestHandler<D>,
  request: Request,
  logLevel?: LogLevel,
): Promise<MutateResponse>;

export async function handleMutateRequest<
  D extends Database<ExtractTransactionType<D>>,
>(
  inputOrDbProvider: HandleMutateRequestArgs<D> | D,
  maybeHandler?: MutateRequestHandler<D> | undefined,
  requestOrQuery?: Request | MutateSearchParams | undefined,
  bodyOrLogLevel?: ReadonlyJSONValue | LogLevel | undefined,
  logLevel?: LogLevel | undefined,
): Promise<MutateResponse> {
  const normalized =
    typeof inputOrDbProvider === 'object' && 'handler' in inputOrDbProvider
      ? normalizeMutateRequestInput(inputOrDbProvider)
      : normalizeLegacyMutateRequestArgs(
          inputOrDbProvider,
          maybeHandler,
          requestOrQuery,
          bodyOrLogLevel,
          logLevel,
        );

  const lc = createLogContext(normalized.logLevel).withContext('PushProcessor');
  let jsonBody: unknown;

  if (normalized.type === 'request') {
    try {
      jsonBody = await normalized.request.json();
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
    jsonBody = normalized.jsonBody;
  }

  let mutationIDs: MutationID[] = [];

  let pushBody: PushBody;
  try {
    pushBody = v.parse(jsonBody, pushBodySchema, 'passthrough');
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

  let parsedQueryParams: Params;
  try {
    parsedQueryParams = v.parse(
      normalized.type === 'request'
        ? Object.fromEntries(new URL(normalized.request.url).searchParams)
        : normalized.queryParams,
      mutateParamsSchema,
      'passthrough',
    );
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
    const transactor = new Transactor(
      normalized.dbProvider,
      pushBody,
      parsedQueryParams,
      lc,
    );

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
          await processCleanupResultsMutation(
            normalized.dbProvider,
            m,
            parsedQueryParams,
            lc,
          );
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
      );

      let mutationPhase: MutationPhase = 'preTransaction';

      const transactProxy: TransactFn<D> = async innerCb => {
        mutationPhase = 'transactionPending';
        const result = await transactor.transact(m, innerCb);
        mutationPhase = 'postCommit';
        return result;
      };

      try {
        const res = await applicationErrorWrapper(() =>
          normalized.handler(transactProxy, m),
        );
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
      kind: 'MutateResponse',
      mutations: responses,
      ...(typeof normalized.userID !== 'undefined'
        ? {userID: normalized.userID}
        : {}),
    } as const satisfies MutateResponse;
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

function normalizeMutateRequestInput<
  D extends Database<ExtractTransactionType<D>>,
>(input: HandleMutateRequestArgs<D>): NormalizedMutateRequestArgs<D> {
  if ('request' in input) {
    return {
      type: 'request',
      dbProvider: input.dbProvider,
      handler: input.handler,
      request: input.request,
      userID: input.userID ?? null,
      logLevel: input.logLevel ?? 'info',
    };
  }

  return {
    type: 'body',
    dbProvider: input.dbProvider,
    handler: input.handler,
    jsonBody: input.body,
    userID: input.userID ?? null,
    queryParams:
      input.query instanceof URLSearchParams
        ? Object.fromEntries(input.query)
        : input.query,
    logLevel: input.logLevel ?? 'info',
  };
}

function normalizeLegacyMutateRequestArgs<
  D extends Database<ExtractTransactionType<D>>,
>(
  dbProvider: D,
  handler: MutateRequestHandler<D> | undefined,
  requestOrQuery: Request | MutateSearchParams | undefined,
  bodyOrLogLevel: ReadonlyJSONValue | LogLevel | undefined,
  logLevel: LogLevel | undefined,
): NormalizedMutateRequestArgs<D> {
  assert(typeof handler === 'function', 'Handler function is required');
  assert(
    typeof requestOrQuery !== 'undefined',
    'Request or query parameters are required',
  );

  if (requestOrQuery instanceof Request) {
    return {
      type: 'request',
      dbProvider,
      handler,
      request: requestOrQuery,
      userID: undefined,
      logLevel: (bodyOrLogLevel as LogLevel | undefined) ?? 'info',
    };
  }

  assert(
    typeof bodyOrLogLevel !== 'undefined',
    'JSON body cannot be undefined',
  );

  return {
    type: 'body',
    dbProvider,
    handler,
    jsonBody: bodyOrLogLevel,
    userID: undefined,
    queryParams:
      requestOrQuery instanceof URLSearchParams
        ? Object.fromEntries(requestOrQuery)
        : requestOrQuery,
    logLevel: logLevel ?? 'info',
  };
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

        if (appError !== undefined) {
          // Retry also failed → internal error, cannot skip mutation
          this.#lc.error?.(
            `Retry also failed for mutation ${mutation.id} for client ${mutation.clientID}`,
            error,
          );
          throw error;
        }

        // First attempt failed → store error and retry without mutator
        const originalError =
          error instanceof DatabaseTransactionError
            ? (error.cause ?? error)
            : error;
        appError = wrapWithApplicationError(originalError);
        this.#lc.warn?.(
          `Error processing mutation ${mutation.id} for client ${mutation.clientID}, retrying without mutator`,
          appError,
        );
        continue;
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

          let returnData: ReadonlyJSONValue | void = undefined;
          if (appError === undefined) {
            this.#lc.debug?.(
              `Executing mutator '${mutation.name}' (id=${mutation.id})`,
            );
            returnData = await cb(dbTx, mutation.name, mutation.args[0]);
          } else {
            const mutationResult = makeAppErrorResponse(mutation, appError);
            await transactionHooks.writeMutationResult(mutationResult);
          }

          return {
            id: {
              clientID: mutation.clientID,
              id: mutation.id,
            },
            result: {data: returnData as ReadonlyJSONValue | undefined},
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
  const path = name.split(separatorRe);
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

  // Determine clientID for transaction input based on cleanup type
  // Note: legacy format without type field is treated as single
  const clientID =
    'type' in args && args.type === 'bulk' ? args.clientIDs[0] : args.clientID;

  // Run in a transaction, using the hook for DB-specific operation.
  // Note: only upstreamSchema is used by deleteMutationResults; the other
  // fields are required by the interface but ignored for this operation.
  await dbProvider.transaction(
    async (_, hooks) => {
      await hooks.deleteMutationResults(args);
    },
    {
      upstreamSchema: queryParams.schema,
      clientGroupID: args.clientGroupID,
      clientID,
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
