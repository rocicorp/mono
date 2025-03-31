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
import {emptyObject} from '../../../shared/src/sentinels.ts';
import type {LogContext} from '@rocicorp/logger';

const transientPushErrorTypes: PushError['error'][] = [
  'zeroPusher',
  'http',

  // These should never actually be received as they cause the websocket
  // connection to be closed.
  'unsupportedPushVersion',
  'unsupportedSchemaVersion',
];

/**
 * Tracks what pushes are in-flight and resolves promises when they're acked.
 */
export class MutationTracker {
  readonly #outstandingMutations: Map<
    number,
    {
      resolver: Resolver<MutationResult>;
    }
  >;
  readonly #allMutationsConfirmedListeners: Set<() => void>;
  readonly #lc: LogContext;
  #clientID: string | undefined;

  constructor(lc: LogContext) {
    this.#lc = lc.withContext('MutationTracker');
    this.#outstandingMutations = new Map();
    this.#allMutationsConfirmedListeners = new Set();
  }

  set clientID(clientID: string) {
    this.#clientID = clientID;
  }

  trackMutation(id: number): Promise<MutationResult> {
    assert(!this.#outstandingMutations.has(id));
    const mutationResolver = resolver<MutationResult>();

    this.#outstandingMutations.set(id, {
      resolver: mutationResolver,
    });
    return mutationResolver.promise;
  }

  processPushResponse(response: PushResponse): void {
    if ('error' in response) {
      this.#lc.error?.(
        'Received an error response when pushing mutations',
        response,
      );
      this.#processPushError(response);
    } else {
      this.#processPushOk(response);
    }
  }

  /**
   * When we reconnect to zero-cache, we resolve all outstanding mutations
   * whose ID is less than or equal to the lastMutationID.
   *
   * The reason is that any responses the API server sent
   * to those mutations have been lost.
   *
   * An example case: the API server responds while the connection
   * is down. Those responses are dropped.
   *
   * Mutations whose LMID is > the lastMutationID are not resolved
   * since they will be retried by the client, giving us another chance
   * at getting a response.
   *
   * The only way to ensure that all API server responses are
   * received would be to have the API server write them
   * to the DB while writing the LMID.
   *
   * This would have the downside of not being able to provide responses to a
   * mutation with data gathered after the transaction.
   */
  onConnected(lastMutationID: number) {
    for (const [id, entry] of this.#outstandingMutations) {
      if (id <= lastMutationID) {
        this.#settleMutation(id, entry, 'resolve', emptyObject);
      } else {
        // the map is in insertion order which is in mutation ID order
        // so it is safe to break.
        break;
      }
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
    this.#lc.error?.(`Mutation ${mid.id} returned an error`, error);
    const entry = this.#outstandingMutations.get(mid.id);
    assert(entry);
    this.#settleMutation(mid.id, entry, 'reject', error);
  }

  #processMutationOk(result: MutationOk, mid: MutationID): void {
    assert(
      mid.clientID === this.#clientID,
      'received mutation for the wrong client',
    );
    const entry = this.#outstandingMutations.get(mid.id);
    assert(entry);
    this.#settleMutation(mid.id, entry, 'resolve', result);
  }

  #settleMutation(
    mutationID: number,
    entry: {
      resolver: Resolver<MutationResult>;
    },
    type: 'resolve' | 'reject',
    result: MutationOk | MutationError | Omit<PushError, 'mutationIDs'>,
  ): void {
    switch (type) {
      case 'resolve':
        assert(!('error' in result));
        entry.resolver.resolve(result);
        break;
      case 'reject':
        assert('error' in result);
        entry.resolver.reject(result);
        break;
    }

    const removed = this.#outstandingMutations.delete(mutationID);
    if (removed && this.#outstandingMutations.size === 0) {
      this.#notifyAllMutationsConfirmedListeners();
    }
  }

  onAllMutationsConfirmed(listener: () => void): void {
    this.#allMutationsConfirmedListeners.add(listener);
  }

  #notifyAllMutationsConfirmedListeners() {
    for (const listener of this.#allMutationsConfirmedListeners) {
      listener();
    }
  }
}
