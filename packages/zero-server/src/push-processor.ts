import {type LogLevel} from '@rocicorp/logger';
import {assert} from '../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {must} from '../../shared/src/must.ts';
import {getValueAtPath} from '../../shared/src/object-traversal.ts';
import {
  type CustomMutation,
  type MutationResponse,
  type PushResponse,
} from '../../zero-protocol/src/push.ts';
import {
  type Database,
  type ExtractTransactionType,
  handleMutateRequest,
  type TransactFn,
} from '../../zero-server/src/process-mutations.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {Transaction} from '../../zql/src/mutate/custom.ts';
import type {AnyMutatorRegistry} from '../../zql/src/mutate/mutator-registry.ts';
import {isMutator} from '../../zql/src/mutate/mutator.ts';
import type {CustomMutatorDefs} from './custom.ts';

export class PushProcessor<
  _S extends Schema,
  D extends Database<ExtractTransactionType<D>>,
  MD extends AnyMutatorRegistry | CustomMutatorDefs<ExtractTransactionType<D>>,
  C = undefined,
> {
  readonly #dbProvider: D;
  readonly #logLevel: LogLevel;
  readonly #context: C;

  constructor(dbProvider: D, context?: C, logLevel: LogLevel = 'info') {
    this.#dbProvider = dbProvider;
    this.#context = context as C;
    this.#logLevel = logLevel;
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
  ): Promise<PushResponse>;

  /**
   * This override gets the query string and the body from a Request object.
   *
   * @param mutators the custom mutators for the application
   * @param request A `Request` object.
   */
  process(mutators: MD, request: Request): Promise<PushResponse>;
  process(
    mutators: MD,
    queryOrQueryString: Request | URLSearchParams | Record<string, string>,
    body?: ReadonlyJSONValue,
  ): Promise<PushResponse> {
    if (queryOrQueryString instanceof Request) {
      return handleMutateRequest(
        this.#dbProvider,
        (transact, mutation) =>
          this.#processMutation(mutators, transact, mutation),
        queryOrQueryString,
        this.#logLevel,
      );
    }
    return handleMutateRequest(
      this.#dbProvider,
      (transact, mutation) =>
        this.#processMutation(mutators, transact, mutation),
      queryOrQueryString,
      must(body, 'body is required when using query params directly'),
      this.#logLevel,
    );
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
    const mutator = getValueAtPath(mutators, key, /\.|\|/);
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
