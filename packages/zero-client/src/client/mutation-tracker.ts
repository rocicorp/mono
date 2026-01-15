import type {LogContext} from '@rocicorp/logger';
import {resolver, type Resolver} from '@rocicorp/resolver';
import type {NoIndexDiff} from '../../../replicache/src/btree/node.ts';
import type {ReplicacheImpl} from '../../../replicache/src/impl.ts';
import type {
  EphemeralID,
  MutationTrackingData,
} from '../../../replicache/src/replicache-options.ts';
import {assert, unreachable} from '../../../shared/src/asserts.ts';
import {getErrorDetails} from '../../../shared/src/error.ts';
import {must} from '../../../shared/src/must.ts';
import {emptyObject} from '../../../shared/src/sentinels.ts';
import * as v from '../../../shared/src/valita.ts';
import {
  ApplicationError,
  isApplicationError,
  wrapWithApplicationError,
} from '../../../zero-protocol/src/application-error.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import {ProtocolError} from '../../../zero-protocol/src/error.ts';
import {
  mutationResultSchema,
  type MutationError,
  type MutationID,
  type MutationOk,
  type PushError,
  type PushOk,
  type PushResponseBody,
} from '../../../zero-protocol/src/push.ts';
import type {MutatorResultSuccessDetails} from './custom.ts';
import {isZeroError, type ZeroError} from './error.ts';
import {MUTATIONS_KEY_PREFIX} from './keys.ts';

type MutationSuccessType = MutatorResultSuccessDetails;
type MutationErrorType = ApplicationError | ZeroError;

let currentEphemeralID = 0;
function nextEphemeralID(): EphemeralID {
  return ++currentEphemeralID as EphemeralID;
}

const successResultDetails: MutationSuccessType = {type: 'success'};

/**
 * Tracks what pushes are in-flight and resolves promises when they're acked.
 */
export class MutationTracker {
  readonly #outstandingMutations: Map<
    EphemeralID,
    {
      mutationID?: number | undefined;
      resolver: Resolver<MutationSuccessType, MutationErrorType>;
    }
  >;
  readonly #ephemeralIDsByMutationID: Map<number, EphemeralID>;
  readonly #allMutationsAppliedListeners: Set<() => void>;
  readonly #lc: LogContext;

  readonly #ackMutations: (upTo: MutationID) => void;
  readonly #onFatalError: (error: ZeroError) => void;

  #clientID: string | undefined;
  #largestOutstandingMutationID: number;
  #currentMutationID: number;

  constructor(
    lc: LogContext,
    ackMutations: (upTo: MutationID) => void,
    onFatalError: (error: ZeroError) => void,
  ) {
    this.#lc = lc.withContext('MutationTracker');
    this.#outstandingMutations = new Map();
    this.#ephemeralIDsByMutationID = new Map();
    this.#allMutationsAppliedListeners = new Set();
    this.#largestOutstandingMutationID = 0;
    this.#currentMutationID = 0;
    this.#ackMutations = ackMutations;
    this.#onFatalError = onFatalError;
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

  trackMutation(): MutationTrackingData<MutationSuccessType> {
    const id = nextEphemeralID();
    const mutationResolver = resolver<MutationSuccessType, MutationErrorType>();

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
      this.#settleMutation(id, entry, wrapWithApplicationError(e));
    }
  }

  /**
   * Reject all outstanding mutations. Called when the client is in a state
   * that prevents mutations from being applied, such as offline or closed.
   */
  rejectAllOutstandingMutations(error: ZeroError): void {
    if (this.#outstandingMutations.size === 0) {
      return;
    }
    for (const [id, entry] of this.#outstandingMutations) {
      this.#settleMutation(id, entry, error);
    }
    this.#largestOutstandingMutationID = this.#currentMutationID;
    this.#notifyAllMutationsAppliedListeners();
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

  processPushResponse(response: PushResponseBody): void {
    if ('error' in response) {
      this.#lc.error?.(
        'Received an error response when pushing mutations',
        response,
      );
      const fatalError = this.#fatalErrorFromPushError(response);
      if (fatalError) {
        this.#onFatalError(fatalError);
      }
    } else {
      this.#processPushOk(response);
    }
  }

  #fatalErrorFromPushError(error: PushError): ZeroError | undefined {
    switch (error.error) {
      case 'unsupportedPushVersion':
        return new ProtocolError({
          kind: ErrorKind.PushFailed,
          origin: ErrorOrigin.ZeroCache,
          reason: ErrorReason.Internal,
          message: `Unsupported push version`,
          mutationIDs: [],
        });
      case 'unsupportedSchemaVersion':
        return new ProtocolError({
          kind: ErrorKind.PushFailed,
          origin: ErrorOrigin.ZeroCache,
          reason: ErrorReason.Internal,
          message: `Unsupported schema version`,
          mutationIDs: [],
        });
      case 'http':
        return new ProtocolError({
          kind: ErrorKind.PushFailed,
          origin: ErrorOrigin.ZeroCache,
          reason: ErrorReason.HTTP,
          status: error.status,
          message: `Fetch from API server returned non-OK status ${error.status}: ${error.details ?? 'unknown'}`,
          mutationIDs: [],
        });
      case 'zeroPusher':
        return new ProtocolError({
          kind: ErrorKind.PushFailed,
          origin: ErrorOrigin.ZeroCache,
          reason: ErrorReason.Internal,
          message: `ZeroPusher error: ${error.details ?? 'unknown'}`,
          mutationIDs: [],
        });
      default:
        unreachable(error);
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
        this.#settleMutation(id, entry, emptyObject);
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

    // Each tab sends all mutations for the client group
    // and the server responds back to the individual client that actually
    // ran the mutation. This means that N clients can send the same
    // mutation concurrently. If that happens, the promise for the mutation tracked
    // by this class will try to be resolved N times.
    // Every time after the first, the ephemeral ID will not be found.
    //
    // We also reject all outstanding mutations when the client is in a state
    // that prevents mutations from being applied, such as offline or closed.
    // In this case, the ephemeral ID will also not be found.
    const ephemeralID = this.#ephemeralIDsByMutationID.get(mid);
    if (!ephemeralID) {
      this.#lc.debug?.(
        'Mutation already resolved or rejected (e.g. due to disconnect); ignore late reject.',
      );
      return;
    }

    const entry = this.#outstandingMutations.get(ephemeralID);
    assert(
      entry && entry.mutationID === mid,
      `outstanding mutation not found for mutation ID ${mid} and ephemeral ID ${ephemeralID}`,
    );

    if (error.error === 'alreadyProcessed') {
      this.#settleMutation(ephemeralID, entry, emptyObject);
      return;
    }

    this.#settleMutation(
      ephemeralID,
      entry,
      error.error === 'app'
        ? new ApplicationError(
            error.message ?? `Unknown application error: ${error.error}`,
            error.details ? {details: error.details} : undefined,
          )
        : new ProtocolError({
            kind: ErrorKind.InvalidPush,
            origin: ErrorOrigin.Server,
            reason: ErrorReason.Internal,
            message:
              error.error === 'oooMutation'
                ? 'Server reported an out-of-order mutation'
                : `Unknown fallback error with mutation ID ${mid}: ${error.error}`,
            details: getErrorDetails(error),
          }),
    );

    // this is included for backwards compatibility with the per-mutation fatal error responses
    if (error.error === 'oooMutation') {
      this.#onFatalError(
        new ProtocolError({
          kind: ErrorKind.InvalidPush,
          origin: ErrorOrigin.Server,
          reason: ErrorReason.Internal,
          message: 'Server reported an out-of-order mutation',
          details: error.details,
        }),
      );
    }
  }

  #processMutationOk(clientID: string, mid: number, result: MutationOk): void {
    assert(
      clientID === this.#clientID,
      'received mutation for the wrong client',
    );

    // We reject all outstanding mutations when the client is in a state
    // that prevents mutations from being applied, such as offline or closed.
    // In this case, the ephemeral ID will not be found.
    const ephemeralID = this.#ephemeralIDsByMutationID.get(mid);
    if (!ephemeralID) {
      this.#lc.debug?.(
        'Mutation already resolved or rejected (e.g. due to disconnect); ignore late resolve.',
      );
      return;
    }

    const entry = this.#outstandingMutations.get(ephemeralID);
    assert(
      entry && entry.mutationID === mid,
      `outstanding mutation not found for mutation ID ${mid} and ephemeral ID ${ephemeralID}`,
    );
    this.#settleMutation(ephemeralID, entry, result);
  }

  #settleMutation<Result extends MutationOk | ApplicationError | ZeroError>(
    ephemeralID: EphemeralID,
    entry: {
      mutationID?: number | undefined;
      resolver: Resolver<MutationSuccessType, MutationErrorType>;
    },
    result: Result,
  ): void {
    if (isApplicationError(result) || isZeroError(result)) {
      // we reject here and catch in the mutator proxy
      // the mutator proxy catches both client and server errors
      entry.resolver.reject(result);
    } else {
      entry.resolver.resolve(successResultDetails);
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
