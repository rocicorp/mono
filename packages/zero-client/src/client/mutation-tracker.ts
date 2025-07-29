import {resolver, type Resolver} from '@rocicorp/resolver';
import type {
  EphemeralID,
  MutationTrackingData,
} from '../../../replicache/src/replicache-options.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {emptyObject} from '../../../shared/src/sentinels.ts';
import type {
  MutationError,
  MutationID,
  MutationOk,
  PushError,
  PushOk,
  PushResponse,
} from '../../../zero-protocol/src/push.ts';
import type {ZeroLogContext} from './zero-log-context.ts';
import type {MutationPatch} from '../../../zero-protocol/src/mutations-patch.ts';

type ErrorType =
  | MutationError
  | Omit<PushError, 'mutationIDs'>
  | Error
  | unknown;

const completeFailureTypes: PushError['error'][] = [
  // These should never actually be received as they cause the websocket
  // connection to be closed.
  'unsupportedPushVersion',
  'unsupportedSchemaVersion',
];

let currentEphemeralID = 0;
function nextEphemeralID(): EphemeralID {
  return ++currentEphemeralID as EphemeralID;
}

/**
 * Tracks what pushes are in-flight and resolves promises when they're acked.
 */
export class MutationTracker {
  readonly #outstandingMutations: Map<
    EphemeralID,
    {
      mutationID?: number | undefined;
      resolver: Resolver<MutationOk, ErrorType>;
    }
  >;
  readonly #ephemeralIDsByMutationID: Map<number, EphemeralID>;
  readonly #allMutationsAppliedListeners: Set<() => void>;
  readonly #lc: ZeroLogContext;
  readonly #limboMutations: Set<EphemeralID>;

  // This is only used in the new code path that processes
  // mutation responses that arrive via the `poke` protocol.
  // The old code path will be removed in the release after
  // the one containing mutation-responses-via-poke.
  readonly #ackMutations: (upTo: MutationID) => void;
  #clientID: string | undefined;
  #largestOutstandingMutationID: number;
  #currentMutationID: number;

  constructor(lc: ZeroLogContext, ackMutations: (upTo: MutationID) => void) {
    this.#lc = lc.withContext('MutationTracker');
    this.#outstandingMutations = new Map();
    this.#ephemeralIDsByMutationID = new Map();
    this.#allMutationsAppliedListeners = new Set();
    this.#limboMutations = new Set();
    this.#largestOutstandingMutationID = 0;
    this.#currentMutationID = 0;
    this.#ackMutations = ackMutations;
  }

  set clientID(clientID: string) {
    this.#clientID = clientID;
  }

  trackMutation(): MutationTrackingData {
    const id = nextEphemeralID();
    const mutationResolver = resolver<MutationOk, ErrorType>();

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
      this.#largestOutstandingMutationID = Math.max(
        this.#largestOutstandingMutationID,
        mutationID,
      );
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

  /**
   * Used when zero-cache pokes down mutation results.
   */
  processMutationResponses(patches: MutationPatch[]) {
    try {
      for (const patch of patches) {
        if (patch.mutation.id.clientID !== this.#clientID) {
          continue; // Mutation for a different client. We will not have its promise.
        }

        if ('error' in patch.mutation.result) {
          this.#processMutationError(patch.mutation.id, patch.mutation.result);
        } else {
          this.#processMutationOk(patch.mutation.id, patch.mutation.result);
        }
      }
    } finally {
      const last = patches[patches.length - 1];
      if (last) {
        // We only ack the last mutation in the batch.
        this.#ackMutations(last.mutation.id);
      }
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
   * is down. Those responses are lost.
   *
   * Mutations whose LMID is > the lastMutationID are not resolved
   * since they will be retried by the client, giving us another chance
   * at getting a response.
   *
   * The only way to ensure that all API server responses are
   * received would be to have the API server write them
   * to the DB while writing the LMID.
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

    for (const [id, entry] of this.#outstandingMutations) {
      if (entry.mutationID && entry.mutationID > lastMutationID) {
        // We don't know the state of these mutations.
        // They could have been applied by the server
        // or not since we sent them before the connection was lost.
        // Adding to `limbo` will cause them to be resolved
        // when the next lmid bump is received.
        this.#limboMutations.add(id);
      }
    }
    this.lmidAdvanced(lastMutationID);
  }

  /**
   * lmid advance will:
   * 1. notify "allMutationsApplied" listeners if the lastMutationID
   *    is greater than or equal to the largest outstanding mutation ID.
   * 2. resolve all limbo mutations whose mutation ID is less than or equal to
   *    the lastMutationID.
   *
   * We only resolve "limbo mutations" since we want to give the mutation
   * responses a chance to be received. `poke` and `pushResponse` are non transactional
   * so they race.
   *
   * E.g., a `push` may call the api server which:
   * writes to PG, replicates to zero-cache, then pokes the lmid down before the
   * push response is sent.
   *
   * The only fix for this would be to have mutation responses be written to the database
   * and sent down through the poke protocol.
   *
   * The artifact the user sees is that promise resolutions for mutations is not transactional
   * with the update to synced data.
   *
   * It was a mistake to not just write the mutation responses to the database
   * in the first place as this route ends up being more complicated
   * and less reliable.
   */
  lmidAdvanced(lastMutationID: number): void {
    assert(
      lastMutationID >= this.#currentMutationID,
      'lmid must be greater than or equal to current lmid',
    );
    if (lastMutationID === this.#currentMutationID) {
      return;
    }

    try {
      this.#currentMutationID = lastMutationID;
      this.#resolveLimboMutations(lastMutationID);
    } finally {
      if (lastMutationID >= this.#largestOutstandingMutationID) {
        // this is very important otherwise we hang query de-registration
        this.#notifyAllMutationsAppliedListeners();
      }
    }
  }

  get size() {
    return this.#outstandingMutations.size;
  }

  /**
   * Push errors fall into two categories:
   * - Those where we know the mutations were not applied
   * - Those where we do not know the state of the mutations.
   *
   * The first category includes errors like "unsupportedPushVersion"
   * and "unsupportedSchemaVersion". The mutations were never applied in those cases.
   *
   * The second category includes errors like "http" errors. E.g., a 500 on the user's
   * API server or a network error.
   *
   * The mutations may have been applied by the API server but we never
   * received the response.
   *
   * In this latter case, we must mark the mutations as being in limbo.
   * This allows us to resolve them when we receive the next
   * lmid bump if their lmids are lesser.
   */
  #processPushError(error: PushError): void {
    if (completeFailureTypes.includes(error.error)) {
      return;
    }

    const mids = error.mutationIDs;
    // TODO: remove this check once the server always sends mutationIDs
    if (!mids) {
      return;
    }

    // If the push request failed then we do not know the state of the mutations that were
    // included in that request.
    // Maybe they were applied. Whatever happened, we've lost the response.
    // Given that, we mark them as "in limbo."
    for (const mid of mids) {
      const ephemeralID = this.#ephemeralIDsByMutationID.get(mid.id);
      if (ephemeralID) {
        // if the lmid has already moved past the mutations we can settle them.
        if (mid.id <= this.#currentMutationID) {
          const entry = this.#outstandingMutations.get(ephemeralID);
          if (entry) {
            this.#settleMutation(ephemeralID, entry, 'resolve', emptyObject);
          }
          continue;
        }
        // otherwise put in limbo and wait for next lmid bump
        this.#limboMutations.add(ephemeralID);
      }
    }
  }

  #resolveLimboMutations(lastMutationID: number): void {
    for (const id of this.#limboMutations) {
      const entry = this.#outstandingMutations.get(id);
      if (!entry || !entry.mutationID) {
        this.#limboMutations.delete(id);
        continue;
      }
      if (entry.mutationID <= lastMutationID) {
        this.#limboMutations.delete(id);
        this.#settleMutation(id, entry, 'resolve', emptyObject);
      }
    }
  }

  #processPushOk(ok: PushOk): void {
    for (const mutation of ok.mutations) {
      if ('error' in mutation.result) {
        this.#processMutationError(mutation.id, mutation.result);
      } else {
        this.#processMutationOk(mutation.id, mutation.result);
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
    if (!ephemeralID && error.error === 'alreadyProcessed') {
      return;
    }

    // Each tab sends all mutations for the client group
    // and the server responds back to the individual client that actually
    // ran the mutation. This means that N clients can send the same
    // mutation concurrently. If that happens, the promise for the mutation tracked
    // by this class will try to be resolved N times.
    // Every time after the first, the ephemeral ID will not be
    // found in the map. These later times, however, should always have been
    // "mutation already processed" events which we ignore (above).
    assert(
      ephemeralID,
      `ephemeral ID is missing for mutation error: ${error.error}.`,
    );

    const entry = this.#outstandingMutations.get(ephemeralID);
    assert(entry && entry.mutationID === mid.id);
    // Resolving the promise with an error was an intentional API decision
    // so the user receives typed errors.
    this.#settleMutation(ephemeralID, entry, 'reject', error);
  }

  #processMutationOk(mid: MutationID, result: MutationOk): void {
    assert(
      mid.clientID === this.#clientID,
      'received mutation for the wrong client',
    );
    const ephemeralID = this.#ephemeralIDsByMutationID.get(mid.id);
    assert(
      ephemeralID,
      'ephemeral ID is missing. This can happen if a mutation response is received twice ' +
        'but it should be impossible to receive a success response twice for the same mutation.',
    );
    const entry = this.#outstandingMutations.get(ephemeralID);
    assert(entry && entry.mutationID === mid.id);
    this.#settleMutation(ephemeralID, entry, 'resolve', result);
  }

  #settleMutation<Type extends 'resolve' | 'reject'>(
    ephemeralID: EphemeralID,
    entry: {
      mutationID?: number | undefined;
      resolver: Resolver<MutationOk, ErrorType>;
    },
    type: Type,
    result: 'resolve' extends Type ? MutationOk : unknown,
  ): void {
    switch (type) {
      case 'resolve':
        entry.resolver.resolve(result as MutationOk);
        break;
      case 'reject':
        entry.resolver.reject(result);
        break;
    }

    this.#outstandingMutations.delete(ephemeralID);
    if (entry.mutationID) {
      this.#ephemeralIDsByMutationID.delete(entry.mutationID);
    }
    this.#limboMutations.delete(ephemeralID);
  }

  /**
   * Be notified when all mutations have been included in the server snapshot.
   *
   * The query manager will not de-register queries from the server until there
   * are no pending mutations.
   *
   * The reason is that a mutation may need to be rebased. We do not want
   * data that was available the first time it was run to not be available
   * on a rebase.
   */
  onAllMutationsApplied(listener: () => void): void {
    this.#allMutationsAppliedListeners.add(listener);
  }

  #notifyAllMutationsAppliedListeners() {
    for (const listener of this.#allMutationsAppliedListeners) {
      listener();
    }
  }
}
