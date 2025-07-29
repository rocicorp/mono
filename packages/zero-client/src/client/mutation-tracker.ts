import {resolver, type Resolver} from '@rocicorp/resolver';
import type {
  EphemeralID,
  MutationTrackingData,
} from '../../../replicache/src/replicache-options.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {emptyObject} from '../../../shared/src/sentinels.ts';
import {
  mutationResultSchema,
  type MutationError,
  type MutationID,
  type MutationOk,
  type PushError,
  type PushOk,
  type PushResponse,
} from '../../../zero-protocol/src/push.ts';
import type {ZeroLogContext} from './zero-log-context.ts';
import type {ReplicacheImpl} from '../../../replicache/src/impl.ts';
import {MUTATIONS_KEY_PREFIX} from './keys.ts';
import type {NoIndexDiff} from '../../../replicache/src/btree/node.ts';
import {must} from '../../../shared/src/must.ts';
import * as v from '../../../shared/src/valita.ts';

type ErrorType =
  | MutationError
  | Omit<PushError, 'mutationIDs'>
  | Error
  | unknown;

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

  readonly #ackMutations: (upTo: MutationID) => void;
  #clientID: string | undefined;
  #largestOutstandingMutationID: number;
  #currentMutationID: number;

  constructor(lc: ZeroLogContext, ackMutations: (upTo: MutationID) => void) {
    this.#lc = lc.withContext('MutationTracker');
    this.#outstandingMutations = new Map();
    this.#ephemeralIDsByMutationID = new Map();
    this.#allMutationsAppliedListeners = new Set();
    this.#largestOutstandingMutationID = 0;
    this.#currentMutationID = 0;
    this.#ackMutations = ackMutations;
  }

  setClientIDAndWatch(
    clientID: string,
    experimentalWatch: ReplicacheImpl['experimentalWatch'],
  ) {
    assert(this.#clientID === undefined, 'clientID already set');
    this.#clientID = clientID;
    experimentalWatch(
      diffs => {
        this.#processMutationResponses(diffs);
      },
      {
        prefix: MUTATIONS_KEY_PREFIX + clientID + '/',
        initialValuesInFirstDiff: true,
      },
    );
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
  #processMutationResponses(diffs: NoIndexDiff): void {
    const clientID = must(this.#clientID);
    let largestLmid = 0;
    for (const diff of diffs) {
      const mutationID = Number(
        diff.key.slice(MUTATIONS_KEY_PREFIX.length + clientID.length + 1),
      );
      assert(
        !isNaN(mutationID),
        `MutationTracker received a diff with an invalid mutation ID: ${diff.key}`,
      );
      largestLmid = Math.max(largestLmid, mutationID);
      switch (diff.op) {
        case 'add': {
          const result = v.parse(diff.newValue, mutationResultSchema);
          if ('error' in result) {
            this.#processMutationError(clientID, mutationID, result);
          } else {
            this.#processMutationOk(clientID, mutationID, result);
          }
          break;
        }
        case 'del':
          break;
        case 'change':
          throw new Error('MutationTracker does not expect change operations');
      }
    }

    if (largestLmid > 0) {
      this.#ackMutations({
        clientID: must(this.#clientID),
        id: largestLmid,
      });
    }
  }

  processPushResponse(response: PushResponse): void {
    if ('error' in response) {
      this.#lc.error?.(
        'Received an error response when pushing mutations',
        response,
      );
    } else {
      this.#processPushOk(response);
    }
  }

  /**
   * DEPRECATED: to be removed when we switch to fully driving
   * mutation resolution via poke.
   *
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

    this.lmidAdvanced(lastMutationID);
  }

  /**
   * lmid advance will:
   * 1. notify "allMutationsApplied" listeners if the lastMutationID
   *    is greater than or equal to the largest outstanding mutation ID.
   * 2. resolve all mutations whose mutation ID is less than or equal to
   *    the lastMutationID.
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
      this.#resolveMutations(lastMutationID);
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

  #resolveMutations(upTo: number): void {
    // We resolve all mutations whose mutation ID is less than or equal to
    // the upTo mutation ID.
    for (const [id, entry] of this.#outstandingMutations) {
      if (entry.mutationID && entry.mutationID <= upTo) {
        this.#settleMutation(id, entry, 'resolve', emptyObject);
      } else {
        break; // the map is in insertion order which is in mutation ID order
      }
    }
  }

  #processPushOk(ok: PushOk): void {
    for (const mutation of ok.mutations) {
      if ('error' in mutation.result) {
        this.#processMutationError(
          mutation.id.clientID,
          mutation.id.id,
          mutation.result,
        );
      } else {
        this.#processMutationOk(
          mutation.id.clientID,
          mutation.id.id,
          mutation.result,
        );
      }
    }
  }

  #processMutationError(
    clientID: string,
    mid: number,
    error: MutationError | Omit<PushError, 'mutationIDs'>,
  ): void {
    assert(
      clientID === this.#clientID,
      'received mutation for the wrong client',
    );
    this.#lc.error?.(`Mutation ${mid} returned an error`, error);

    const ephemeralID = this.#ephemeralIDsByMutationID.get(mid);
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
    assert(entry && entry.mutationID === mid);
    // Resolving the promise with an error was an intentional API decision
    // so the user receives typed errors.
    this.#settleMutation(ephemeralID, entry, 'reject', error);
  }

  #processMutationOk(clientID: string, mid: number, result: MutationOk): void {
    assert(
      clientID === this.#clientID,
      'received mutation for the wrong client',
    );

    const ephemeralID = this.#ephemeralIDsByMutationID.get(mid);
    assert(
      ephemeralID,
      'ephemeral ID is missing. This can happen if a mutation response is received twice ' +
        'but it should be impossible to receive a success response twice for the same mutation.',
    );
    const entry = this.#outstandingMutations.get(ephemeralID);
    assert(entry && entry.mutationID === mid);
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
