import {resolver, type Resolver} from '@rocicorp/resolver';
import type {
  MutationError,
  MutationID,
  MutationOk,
  MutationResult,
  PushError,
  PushOk,
  PushResponse,
} from '../../../zero-protocol/src/push.ts';
import {assert} from '../../../shared/src/asserts.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {QueryManager} from './query-manager.ts';
import {must} from '../../../shared/src/must.ts';

const transientPushErrorTypes: PushError['error'][] = [
  'zero-pusher',
  'http',

  // These should never actually be received as they cause the websocket
  // connection to be closed.
  'unsupported-push-version',
  'unsupported-schema-version',
];

/**
 * Tracks what pushes are in-flight and resolves promises when they're acked.
 */
export class MutationTracker {
  readonly #outstandingMutations: Map<
    number,
    {
      removeQueries: () => void;
      resolver: Resolver<MutationResult>;
    }
  >;
  #queryManager: QueryManager | undefined;
  #clientID: string | undefined;

  constructor() {
    this.#outstandingMutations = new Map();
  }

  setQueryManager(queryManager: QueryManager) {
    this.#queryManager = queryManager;
  }

  set clientID(clientID: string) {
    this.#clientID = clientID;
  }

  trackMutation(
    id: number,
    // We also register the queries used by the mutation with the server
    // so the mutator does not lose the data it requires when rebasing.
    readQueries: readonly AST[],
  ): Promise<MutationResult> {
    assert(!this.#outstandingMutations.has(id));
    const mutationResolver = resolver<MutationResult>();

    const cleanupCallbacks: (() => void)[] = [];
    for (const query of readQueries) {
      cleanupCallbacks.push(
        must(
          this.#queryManager,
          'query manager was not set on the mutation-tracker',
        ).add(query, 0),
      );
    }
    this.#outstandingMutations.set(id, {
      removeQueries: () => cleanupCallbacks.forEach(cb => cb()),
      resolver: mutationResolver,
    });
    return mutationResolver.promise;
  }

  processPushResponse(response: PushResponse): void {
    if ('error' in response) {
      this.#processPushError(response);
    } else {
      this.#processPushOk(response);
    }
  }

  get size() {
    return this.#outstandingMutations.size;
  }

  #processPushError(error: PushError): void {
    // Mutations suffering from transient errors are not removed from the
    // outstanding mutations list. The client will retry.
    if (transientPushErrorTypes.includes(error.error)) {
      return;
    }

    const mids = error.mutationIDs;

    // TODO: remove this check once the server always sends mutationIDs
    if (!mids) {
      return;
    }

    for (const mid of mids) {
      this.#processMutationError(mid, error);
    }
  }

  #processPushOk(ok: PushOk): void {
    for (const mutation of ok.mutations) {
      if ('error' in mutation.result) {
        this.#processMutationError(mutation.id, mutation.result);
      } else {
        this.#processMutationOk(mutation.result, mutation.id);
      }
    }
  }

  #processMutationError(
    mid: MutationID,
    error: MutationError | Omit<PushError, 'mutationIDs'>,
  ): void {
    assert(
      mid.clientID === this.#clientID,
      'received mutation for the wrong client',
    );
    const entry = this.#outstandingMutations.get(mid.id);
    assert(entry);
    entry.removeQueries();
    entry.resolver.reject(error);
    this.#outstandingMutations.delete(mid.id);
  }

  #processMutationOk(result: MutationOk, mid: MutationID): void {
    assert(
      mid.clientID === this.#clientID,
      'received mutation for the wrong client',
    );
    const entry = this.#outstandingMutations.get(mid.id);
    assert(entry);
    entry.removeQueries();
    entry.resolver.resolve(result);
    this.#outstandingMutations.delete(mid.id);
  }
}
