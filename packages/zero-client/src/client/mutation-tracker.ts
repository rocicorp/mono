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
import {emptyObject} from '../../../shared/src/sentinels.ts';
import type {LogContext} from '@rocicorp/logger';
import type {
  EphemeralID,
  MutationTrackingData,
} from '../../../replicache/src/replicache-options.ts';
import type {MutationResult} from '../../../zero-protocol/src/push.ts';

const transientPushErrorTypes: PushError['error'][] = [
  'zeroPusher',
  'http',

  // These should never actually be received as they cause the websocket
  // connection to be closed.
  'unsupportedPushVersion',
  'unsupportedSchemaVersion',
];

let currentEphemeralID = 0;
function nextEphemeralID(): EphemeralID {
  return ++currentEphemeralID as EphemeralID;
}

type MutationResultOrPushError =
  | MutationResult
  | Omit<PushError, 'mutationIDs'>;

/**
 * Tracks what pushes are in-flight and resolves promises when they're acked.
 */
export class MutationTracker {
  readonly #outstandingMutations: Map<
    EphemeralID,
    {
      mutationID?: number | undefined;
      resolver: Resolver<MutationResultOrPushError>;
    }
  >;
  readonly #ephemeralIDsByMutationID: Map<number, EphemeralID>;
  readonly #allMutationsConfirmedListeners: Set<() => void>;
  readonly #lc: LogContext;
  #clientID: string | undefined;

  constructor(lc: LogContext) {
    this.#lc = lc.withContext('MutationTracker');
    this.#outstandingMutations = new Map();
    this.#ephemeralIDsByMutationID = new Map();
    this.#allMutationsConfirmedListeners = new Set();
  }

  set clientID(clientID: string) {
    this.#clientID = clientID;
  }

  trackMutation(): MutationTrackingData {
    const id = nextEphemeralID();
    const mutationResolver = resolver<MutationResultOrPushError>();

    this.#outstandingMutations.set(id, {
      resolver: mutationResolver,
    });
    return {ephemeralID: id, serverPromise: mutationResolver.promise};
  }

  mutationIDAssigned(id: EphemeralID, mutationID: number): void {
    const entry = this.#outstandingMutations.get(id);
    if (entry) {
      entry.mutationID = mutationID;
      this.#ephemeralIDsByMutationID.set(mutationID, id);
    }
  }

  /**
   * Reject the mutation due to an unhandled exception on the client.
   * The mutation must not have been persisted to the client store.
   */
  rejectMutation(id: EphemeralID, e: unknown): void {
    const entry = this.#outstandingMutations.get(id);
    if (entry) {
      this.#settleMutation(id, entry, 'reject', e);
    }
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
      if (!entry.mutationID) {
        continue;
      }

      if (entry.mutationID <= lastMutationID) {
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
    const ephemeralID = this.#ephemeralIDsByMutationID.get(mid.id);
    assert(
      ephemeralID,
      () =>
        'invalid state. An ephemeral id was never assigned to mutation ' +
        mid.id,
    );
    const entry = this.#outstandingMutations.get(ephemeralID);
    assert(entry && entry.mutationID === mid.id);
    // Resolving the promise with an error was an intentional API decision
    // so the user receives typed errors.
    this.#settleMutation(ephemeralID, entry, 'resolve', error);
  }

  #processMutationOk(result: MutationOk, mid: MutationID): void {
    assert(
      mid.clientID === this.#clientID,
      'received mutation for the wrong client',
    );
    const ephemeralID = this.#ephemeralIDsByMutationID.get(mid.id);
    assert(
      ephemeralID,
      () =>
        'invalid state. An ephemeral id was never assigned to mutation ' +
        mid.id,
    );
    const entry = this.#outstandingMutations.get(ephemeralID);
    assert(entry && entry.mutationID === mid.id);
    this.#settleMutation(ephemeralID, entry, 'resolve', result);
  }

  #settleMutation<Type extends 'resolve' | 'reject'>(
    ephemeralID: EphemeralID,
    entry: {
      mutationID?: number | undefined;
      resolver: Resolver<MutationResultOrPushError>;
    },
    type: Type,
    result: 'resolve' extends Type ? MutationResultOrPushError : unknown,
  ): void {
    switch (type) {
      case 'resolve':
        entry.resolver.resolve(result as MutationResultOrPushError);
        break;
      case 'reject':
        entry.resolver.reject(result);
        break;
    }

    const removed = this.#outstandingMutations.delete(ephemeralID);
    if (entry.mutationID) {
      this.#ephemeralIDsByMutationID.delete(entry.mutationID);
    }
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
