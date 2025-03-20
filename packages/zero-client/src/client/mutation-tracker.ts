import {resolver, type Resolver} from '@rocicorp/resolver';
import type {
  MutationError,
  MutationID,
  MutationOk,
  PushError,
  PushOk,
  PushResponse,
} from '../../../zero-protocol/src/push.ts';
import {assert} from '../../../shared/src/asserts.ts';

type MutationResult = MutationOk | MutationError | PushError;

/**
 * Tracks what pushes are in-flight and resolves promises when they're acked.
 */
export class MutationTracker {
  readonly #outstandingMutations: Map<number, Resolver<MutationResult>>;
  readonly #clientID: string;

  constructor(clientID: string) {
    this.#outstandingMutations = new Map();
    this.#clientID = clientID;
  }

  trackMutation(id: number): Promise<MutationResult> {
    assert(!this.#outstandingMutations.has(id));
    const mutationResolver = resolver<MutationResult>();
    this.#outstandingMutations.set(id, mutationResolver);
    return mutationResolver.promise;
  }

  /**
   * Called if the mutation is never able to be sent to the server
   * and abandoned before persisted.
   * In that case, we have no `pushResponse` to process.
   */
  rejectMutation(id: number, e: Error) {
    const resolver = this.#outstandingMutations.get(id);
    assert(resolver);
    resolver.reject(e);
    this.#outstandingMutations.delete(id);
  }

  processPushResponse(response: PushResponse): void {
    if ('error' in response) {
      this.#processPushError(response);
    } else {
      this.#processPushOk(response);
    }
  }

  #processPushError(error: PushError): void {
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
    const resolver = this.#outstandingMutations.get(mid.id);
    assert(resolver);
    resolver.reject(error);
    this.#outstandingMutations.delete(mid.id);
  }

  #processMutationOk(result: MutationOk, mid: MutationID): void {
    assert(
      mid.clientID === this.#clientID,
      'received mutation for the wrong client',
    );
    const resolver = this.#outstandingMutations.get(mid.id);
    assert(resolver);
    resolver.resolve(result);
    this.#outstandingMutations.delete(mid.id);
  }
}
