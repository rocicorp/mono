import {type LogLevel} from '@rocicorp/logger';
import {assert} from '../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {must} from '../../shared/src/must.ts';
import {getValueAtPath} from '../../shared/src/object-traversal.ts';
import type {MutateResponse} from '../../zero-protocol/src/mutate-server.ts';
import type {CustomMutation} from '../../zero-protocol/src/mutation.ts';
import {type MutationResponse} from '../../zero-protocol/src/push.ts';
import {
  type Database,
  type ExtractTransactionType,
  handleMutateRequest,
  type MutateRequestHandler,
  type TransactFn,
} from '../../zero-server/src/process-mutations.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {Transaction} from '../../zql/src/mutate/custom.ts';
import type {AnyMutatorRegistry} from '../../zql/src/mutate/mutator-registry.ts';
import {isMutator} from '../../zql/src/mutate/mutator.ts';
import type {CustomMutatorDefs} from './custom.ts';

export const separatorRe = /[.|]/;

/** Options for {@linkcode PushProcessor}. */
export type PushProcessorOptions = {
  /** Log level for request parsing and execution. Defaults to `'info'`. */
  logLevel?: LogLevel | undefined;
  /**
   * Whether a rejection from a mutator should be answered by running that
   * mutator AGAIN, once, in a fresh transaction — rather than by the default
   * retry, which re-runs the transaction with the mutator skipped and returns
   * the error to the client.
   *
   * Return `true` only for errors the application knows are TRANSIENT, which in
   * practice means serialization failures and deadlocks reported by the
   * database. Zero cannot decide this itself: it does not parse driver errors,
   * and by the time a rejection reaches the `Transactor` an application's own
   * deliberate rejection and a driver rejection are wrapped identically in
   * `DatabaseTransactionError`.
   *
   * Left undefined, behaviour is exactly as before.
   */
  shouldRetryMutator?: ((error: unknown) => boolean) | undefined;
};

export class PushProcessor<
  _S extends Schema,
  D extends Database<ExtractTransactionType<D>>,
  MD extends AnyMutatorRegistry | CustomMutatorDefs<ExtractTransactionType<D>>,
  C = undefined,
> {
  readonly #dbProvider: D;
  readonly #logLevel: LogLevel;
  readonly #context: C;
  readonly #shouldRetryMutator: ((error: unknown) => boolean) | undefined;

  /**
   * @param logLevelOrOptions a `LogLevel`, or an options object. The bare
   * `LogLevel` form is kept so every existing call site compiles unchanged.
   */
  constructor(
    dbProvider: D,
    context?: C,
    logLevelOrOptions: LogLevel | PushProcessorOptions = 'info',
  ) {
    this.#dbProvider = dbProvider;
    this.#context = context as C;
    const options: PushProcessorOptions =
      typeof logLevelOrOptions === 'string'
        ? {logLevel: logLevelOrOptions}
        : logLevelOrOptions;
    this.#logLevel = options.logLevel ?? 'info';
    this.#shouldRetryMutator = options.shouldRetryMutator;
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
  process(
    mutators: MD,
    queryString: URLSearchParams | Record<string, string>,
    body: ReadonlyJSONValue,
  ): Promise<MutateResponse>;

  /**
   * This override gets the query string and the body from a Request object.
   *
   * @param mutators the custom mutators for the application
   * @param request A `Request` object.
   */
  process(mutators: MD, request: Request): Promise<MutateResponse>;
  process(
    mutators: MD,
    queryOrQueryString: Request | URLSearchParams | Record<string, string>,
    body?: ReadonlyJSONValue,
  ): Promise<MutateResponse> {
    const handler: MutateRequestHandler<D> = (transact, mutation) =>
      this.#processMutation(mutators, transact, mutation);

    // ⚠ THE POSITIONAL FORM IS KEPT WHEN THERE IS NOTHING EXTRA TO PASS, and
    // that is not stylistic. It normalizes `userID` to `undefined`, which
    // `handleMutateRequest` uses to OMIT `userID` from the response; the object
    // form coerces `undefined` to `null`, which would start emitting it. Until
    // `PushProcessor` has a `userID` of its own to pass, switching
    // unconditionally would be a response-shape change unrelated to this
    // feature.
    if (this.#shouldRetryMutator === undefined) {
      if (queryOrQueryString instanceof Request) {
        return handleMutateRequest(
          this.#dbProvider,
          handler,
          queryOrQueryString,
          this.#logLevel,
        );
      }
      return handleMutateRequest(
        this.#dbProvider,
        handler,
        queryOrQueryString,
        must(body, 'body is required when using query params directly'),
        this.#logLevel,
      );
    }

    if (queryOrQueryString instanceof Request) {
      return handleMutateRequest({
        dbProvider: this.#dbProvider,
        handler,
        request: queryOrQueryString,
        userID: undefined,
        logLevel: this.#logLevel,
        shouldRetryMutator: this.#shouldRetryMutator,
      });
    }
    return handleMutateRequest({
      dbProvider: this.#dbProvider,
      handler,
      query: queryOrQueryString,
      body: must(body, 'body is required when using query params directly'),
      userID: undefined,
      logLevel: this.#logLevel,
      shouldRetryMutator: this.#shouldRetryMutator,
    });
  }

  #processMutation(
    mutators: MD,
    transact: TransactFn<D>,
    _mutation: CustomMutation,
  ): Promise<MutationResponse> {
    return transact((tx, name, args) =>
      this.#dispatchMutation(mutators, tx, name, args),
    );
  }

  #dispatchMutation(
    mutators: MD,
    dbTx: ExtractTransactionType<D>,
    key: string,
    args: ReadonlyJSONValue | undefined,
  ): Promise<void> {
    // Legacy mutators used | as a separator, new mutators use .
    const mutator = getValueAtPath(mutators, key, separatorRe);
    assert(typeof mutator === 'function', `could not find mutator ${key}`);
    if (isMutator(mutator)) {
      return mutator.fn({
        args,
        ctx: this.#context,
        tx: dbTx as Transaction<Schema, unknown>,
      });
    }
    return mutator(dbTx, args);
  }
}
