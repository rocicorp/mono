import {type LogLevel} from '@rocicorp/logger';
import {assert} from '../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {must} from '../../shared/src/must.ts';
import {isMutatorDefinition} from '../../zero-client/src/client/define-mutator.ts';
import type {MutatorDefinitions} from '../../zero-client/src/client/mutator-definitions.ts';
import {
  type CustomMutation,
  type MutationResponse,
  type PushResponse,
} from '../../zero-protocol/src/push.ts';
import {
  type Database,
  type ExtractTransactionType,
  handleMutationRequest,
  type TransactFn,
} from '../../zero-server/src/process-mutations.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import {splitMutatorKey} from '../../zql/src/mutate/custom.ts';
import type {CustomMutatorDefs} from './custom.ts';

export class PushProcessor<
  S extends Schema,
  D extends Database<ExtractTransactionType<D>>,
  MD extends
    | MutatorDefinitions<S, C>
    | CustomMutatorDefs<ExtractTransactionType<D>>,
  C,
> {
  readonly #dbProvider: D;
  readonly #logLevel;
  readonly #context: C;

  constructor(dbProvider: D, context: C, logLevel: LogLevel = 'info') {
    this.#dbProvider = dbProvider;
    this.#context = context;
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
      return handleMutationRequest(
        this.#dbProvider,
        (transact, mutation) =>
          this.#processMutation(mutators, transact, mutation),
        queryOrQueryString,
        this.#context,
        this.#logLevel,
      );
    }
    return handleMutationRequest(
      this.#dbProvider,
      (transact, mutation) =>
        this.#processMutation(mutators, transact, mutation),
      queryOrQueryString,
      must(body),
      this.#context,
      this.#logLevel,
    );
  }

  #processMutation(
    mutators: MD,
    transact: TransactFn<D, C>,
    _mutation: CustomMutation,
  ): Promise<MutationResponse> {
    return transact((tx, name, args, ctx) =>
      this.#dispatchMutation(mutators, tx, name, args, ctx),
    );
  }

  #dispatchMutation(
    mutators: MD,
    dbTx: ExtractTransactionType<D>,
    key: string,
    args: ReadonlyJSONValue | undefined,
    ctx: C,
  ): Promise<void> {
    // Legacy mutators used | as a separator, new mutators use .
    const parts = splitMutatorKey(key, /\.|\|/);
    const mutator = objectAtPath(mutators, parts);
    assert(typeof mutator === 'function', `could not find mutator ${key}`);
    const tx = dbTx;
    if (isMutatorDefinition(mutator)) {
      return mutator({tx, args, ctx});
    }
    return mutator(dbTx, args);
  }
}

function objectAtPath(obj: Record<string, unknown>, path: string[]): unknown {
  let current: unknown = obj;
  for (const part of path) {
    if (typeof current !== 'object' || current === null || !(part in current)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[part];
  }
  return current;
}
