import {type LogLevel} from '@rocicorp/logger';
import {assert} from '../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {
  type CustomMutation,
  type MutationResponse,
  type PushResponse,
} from '../../zero-protocol/src/push.ts';
import {splitMutatorKey} from '../../zql/src/mutate/custom.ts';
import type {CustomMutatorDefs} from './custom.ts';
import {
  type ExtractTransactionType,
  type Database,
  mapMutationRequest,
  type TransactFn,
} from '../../zero-server/src/process-mutations.ts';
import {must} from '../../shared/src/must.ts';

export class PushProcessor<
  D extends Database<ExtractTransactionType<D>>,
  MD extends CustomMutatorDefs<ExtractTransactionType<D>>,
> {
  readonly #dbProvider: D;
  readonly #logLevel;

  constructor(dbProvider: D, logLevel: LogLevel = 'info') {
    this.#dbProvider = dbProvider;
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
      return mapMutationRequest(
        (transact, mutations) =>
          this.#processMutation(mutators, transact, mutations),
        queryOrQueryString,
        this.#logLevel,
      );
    }
    return mapMutationRequest(
      (transact, mutation) =>
        this.#processMutation(mutators, transact, mutation),
      queryOrQueryString,
      must(body),
      this.#logLevel,
    );
  }

  #processMutation(
    mutators: MD,
    transact: TransactFn,
    _mutation: CustomMutation,
  ): Promise<MutationResponse> {
    return transact(this.#dbProvider, (tx, name, args) =>
      this.#dispatchMutation(mutators, tx, name, args),
    );
  }

  #dispatchMutation(
    mutators: MD,
    dbTx: ExtractTransactionType<D>,
    key: string,
    args: ReadonlyJSONValue,
  ): Promise<void> {
    const [namespace, name] = splitMutatorKey(key);
    if (name === undefined) {
      const mutator = mutators[namespace];
      assert(
        typeof mutator === 'function',
        () => `could not find mutator ${key}`,
      );
      return mutator(dbTx, args);
    }

    const mutatorGroup = mutators[namespace];
    assert(
      typeof mutatorGroup === 'object',
      () => `could not find mutators for namespace ${namespace}`,
    );
    const mutator = mutatorGroup[name];
    assert(
      typeof mutator === 'function',
      () => `could not find mutator ${key}`,
    );
    return mutator(dbTx, args);
  }
}
