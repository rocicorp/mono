import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import type {
  PatchOperationInternal,
  PokeInternal,
} from '../../../replicache/src/impl.ts';
import type {PatchOperation} from '../../../replicache/src/patch-operation.ts';
import type {ClientID} from '../../../replicache/src/sync/ids.ts';
import {unreachable} from '../../../shared/src/asserts.ts';
import {getBrowserGlobalMethod} from '../../../shared/src/browser-env.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import type {MutationPatch} from '../../../zero-protocol/src/mutations-patch.ts';
import type {
  PokeEndBody,
  PokePartBody,
  PokeStartBody,
} from '../../../zero-protocol/src/poke.ts';
import type {QueriesPatchOp} from '../../../zero-protocol/src/queries-patch.ts';
import type {RowPatchOp} from '../../../zero-protocol/src/row-patch.ts';
import {
  serverToClient,
  type NameMapper,
} from '../../../zero-schema/src/name-mapper.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {
  toDesiredQueriesKey,
  toGotQueriesKey,
  toMutationResponseKey,
  toPrimaryKeyString,
} from './keys.ts';
import type {MutationTracker} from './mutation-tracker.ts';

type PokeAccumulator = {
  readonly pokeStart: PokeStartBody;
  readonly parts: PokePartBody[];
  readonly pokeEnd: PokeEndBody;
};

/**
 * Handles the multi-part format of zero pokes.
 * As an optimization it also debounces pokes, only poking Replicache with a
 * merged poke at most once per frame (as determined by requestAnimationFrame).
 * The client cannot control how fast the server sends pokes, and it can only
 * update the UI once per frame. This debouncing avoids wastefully
 * computing separate diffs and IVM updates for intermediate states that will
 * never been displayed to the UI.
 */
export class PokeHandler {
  readonly #replicachePoke: (poke: PokeInternal) => Promise<void>;
  readonly #onPokeError: () => void;
  readonly #clientID: ClientID;
  readonly #lc: LogContext;
  #receivingPoke: Omit<PokeAccumulator, 'pokeEnd'> | undefined = undefined;
  readonly #pokeBuffer: PokeAccumulator[] = [];
  #pokePlaybackLoopRunning = false;
  #lastRafPerfTimestamp = 0;
  // Serializes calls to this.#replicachePoke otherwise we can cause out of
  // order poke errors.
  readonly #pokeLock = new Lock();
  readonly #schema: Schema;
  readonly #serverToClient: NameMapper;
  readonly #mutationTracker: MutationTracker;

  readonly #raf =
    getBrowserGlobalMethod('requestAnimationFrame') ?? rafFallback;

  constructor(
    replicachePoke: (poke: PokeInternal) => Promise<void>,
    onPokeError: () => void,
    clientID: ClientID,
    schema: Schema,
    lc: LogContext,
    mutationTracker: MutationTracker,
  ) {
    this.#replicachePoke = replicachePoke;
    this.#onPokeError = onPokeError;
    this.#clientID = clientID;
    this.#schema = schema;
    this.#serverToClient = serverToClient(schema.tables);
    this.#lc = lc.withContext('PokeHandler');
    this.#mutationTracker = mutationTracker;
  }

  handlePokeStart(pokeStart: PokeStartBody) {
    if (this.#receivingPoke) {
      this.#handlePokeError(
        `pokeStart ${JSON.stringify(
          pokeStart,
        )} while still receiving  ${JSON.stringify(
          this.#receivingPoke.pokeStart,
        )} `,
      );
      return;
    }
    this.#receivingPoke = {
      pokeStart,
      parts: [],
    };
  }

  handlePokePart(pokePart: PokePartBody): number | undefined {
    if (pokePart.pokeID !== this.#receivingPoke?.pokeStart.pokeID) {
      this.#handlePokeError(
        `pokePart for ${pokePart.pokeID}, when receiving ${
          this.#receivingPoke?.pokeStart.pokeID
        }`,
      );
      return;
    }
    this.#receivingPoke.parts.push(pokePart);
    return pokePart.lastMutationIDChanges?.[this.#clientID];
  }

  handlePokeEnd(pokeEnd: PokeEndBody): void {
    if (pokeEnd.pokeID !== this.#receivingPoke?.pokeStart.pokeID) {
      this.#handlePokeError(
        `pokeEnd for ${pokeEnd.pokeID}, when receiving ${
          this.#receivingPoke?.pokeStart.pokeID
        }`,
      );
      return;
    }
    if (pokeEnd.cancel) {
      this.#receivingPoke = undefined;
      return;
    }
    this.#pokeBuffer.push({...this.#receivingPoke, pokeEnd});
    this.#receivingPoke = undefined;
    if (!this.#pokePlaybackLoopRunning) {
      this.#startPlaybackLoop();
    }
  }

  handleDisconnect(): void {
    this.#lc.debug?.('clearing due to disconnect');
    this.#clear();
  }

  #startPlaybackLoop() {
    this.#lc.debug?.('starting playback loop');
    this.#pokePlaybackLoopRunning = true;
    this.#raf(this.#rafCallback);
  }

  #rafCallback = async () => {
    const rafLC = this.#lc.withContext('rafAt', Math.floor(performance.now()));
    if (this.#pokeBuffer.length === 0) {
      rafLC.debug?.('stopping playback loop');
      this.#pokePlaybackLoopRunning = false;
      return;
    }
    this.#raf(this.#rafCallback);
    const start = performance.now();
    rafLC.debug?.(
      'raf fired, processing pokes.  Since last raf',
      start - this.#lastRafPerfTimestamp,
    );
    this.#lastRafPerfTimestamp = start;
    await this.#processPokesForFrame(rafLC);
    rafLC.debug?.('processing pokes took', performance.now() - start);
  };

  #processPokesForFrame(lc: LogContext): Promise<void> {
    return this.#pokeLock.withLock(async () => {
      const now = Date.now();
      lc.debug?.('got poke lock at', now);
      lc.debug?.('merging', this.#pokeBuffer.length);
      try {
        const merged = mergePokes(
          this.#pokeBuffer,
          this.#schema,
          this.#serverToClient,
        );
        this.#pokeBuffer.length = 0;
        if (merged === undefined) {
          lc.debug?.('frame is empty');
          return;
        }
        const start = performance.now();
        lc.debug?.('poking replicache');
        await this.#replicachePoke(merged);
        lc.debug?.('poking replicache took', performance.now() - start);

        if (!('error' in merged.pullResponse)) {
          const lmid =
            merged.pullResponse.lastMutationIDChanges[this.#clientID];
          if (lmid !== undefined) {
            this.#mutationTracker.lmidAdvanced(lmid);
          }
        }
      } catch (e) {
        this.#handlePokeError(e);
      }
    });
  }

  #handlePokeError(e: unknown) {
    if (String(e).includes('unexpected base cookie for poke')) {
      // This can happen if cookie changes due to refresh from idb due
      // to an update arriving to different tabs in the same
      // client group at very different times.  Unusual but possible.
      this.#lc.debug?.('clearing due to', e);
    } else {
      this.#lc.error?.('clearing due to unexpected poke error', e);
    }
    this.#clear();
    this.#onPokeError();
  }

  #clear() {
    this.#receivingPoke = undefined;
    this.#pokeBuffer.length = 0;
  }
}

export function mergePokes(
  pokeBuffer: PokeAccumulator[],
  schema: Schema,
  serverToClient: NameMapper,
):
  | (PokeInternal & {mutationResults?: MutationPatch[] | undefined})
  | undefined {
  if (pokeBuffer.length === 0) {
    return undefined;
  }
  const {baseCookie} = pokeBuffer[0].pokeStart;
  const lastPoke = pokeBuffer[pokeBuffer.length - 1];
  const {cookie} = lastPoke.pokeEnd;
  const mergedPatch: PatchOperationInternal[] = [];
  const mergedLastMutationIDChanges: Record<string, number> = {};
  const mutationResults: MutationPatch[] = [];

  let prevPokeEnd = undefined;
  for (const pokeAccumulator of pokeBuffer) {
    if (
      prevPokeEnd &&
      pokeAccumulator.pokeStart.baseCookie &&
      pokeAccumulator.pokeStart.baseCookie > prevPokeEnd.cookie
    ) {
      throw Error(
        `unexpected cookie gap ${JSON.stringify(prevPokeEnd)} ${JSON.stringify(
          pokeAccumulator.pokeStart,
        )}`,
      );
    }
    prevPokeEnd = pokeAccumulator.pokeEnd;
    for (const pokePart of pokeAccumulator.parts) {
      if (pokePart.lastMutationIDChanges) {
        for (const [clientID, lastMutationID] of Object.entries(
          pokePart.lastMutationIDChanges,
        )) {
          mergedLastMutationIDChanges[clientID] = lastMutationID;
        }
      }
      if (pokePart.desiredQueriesPatches) {
        for (const [clientID, queriesPatch] of Object.entries(
          pokePart.desiredQueriesPatches,
        )) {
          for (const op of queriesPatch) {
            mergedPatch.push(
              queryPatchOpToReplicachePatchOp(op, hash =>
                toDesiredQueriesKey(clientID, hash),
              ),
            );
          }
        }
      }
      if (pokePart.gotQueriesPatch) {
        for (const op of pokePart.gotQueriesPatch) {
          mergedPatch.push(
            queryPatchOpToReplicachePatchOp(op, toGotQueriesKey),
          );
        }
      }
      if (pokePart.rowsPatch) {
        for (const p of pokePart.rowsPatch) {
          mergedPatch.push(
            rowsPatchOpToReplicachePatchOp(p, schema, serverToClient),
          );
        }
      }
      if (pokePart.mutationsPatch) {
        for (const op of pokePart.mutationsPatch) {
          mergedPatch.push(mutationPatchOpToReplicachePatchOp(op));
        }
      }
    }
  }
  const ret: PokeInternal & {mutationResults?: MutationPatch[] | undefined} = {
    baseCookie,
    pullResponse: {
      lastMutationIDChanges: mergedLastMutationIDChanges,
      patch: mergedPatch,
      cookie,
    },
  };

  // For backwards compatibility. Because we're strict on our validation,
  // zero-client must be able to parse pokes with this field before we introduce it.
  // So users can update their clients and then start using custom mutators that write responses to the db.
  if (mutationResults.length > 0) {
    ret.mutationResults = mutationResults;
  }
  return ret;
}

function queryPatchOpToReplicachePatchOp(
  op: QueriesPatchOp,
  toKey: (hash: string) => string,
): PatchOperation {
  switch (op.op) {
    case 'clear':
      return op;
    case 'del':
      return {
        op: 'del',
        key: toKey(op.hash),
      };
    case 'put':
      return {
        op: 'put',
        key: toKey(op.hash),
        value: null,
      };
    default:
      unreachable(op);
  }
}

export function mutationPatchOpToReplicachePatchOp(
  op: MutationPatch,
): PatchOperationInternal {
  switch (op.op) {
    case 'put':
      return {
        op: 'put',
        key: toMutationResponseKey(op.mutation.id),
        value: op.mutation.result,
      };
    case 'del':
      return {
        op: 'del',
        key: toMutationResponseKey(op.id),
      };
  }
}

function rowsPatchOpToReplicachePatchOp(
  op: RowPatchOp,
  schema: Schema,
  serverToClient: NameMapper,
): PatchOperationInternal {
  if (op.op === 'clear') {
    return op;
  }
  const tableName = serverToClient.tableName(op.tableName, op as JSONValue);
  switch (op.op) {
    case 'del':
      return {
        op: 'del',
        key: toPrimaryKeyString(
          tableName,
          schema.tables[tableName].primaryKey,
          serverToClient.row(op.tableName, op.id),
        ),
      };
    case 'put':
      return {
        op: 'put',
        key: toPrimaryKeyString(
          tableName,
          schema.tables[tableName].primaryKey,
          serverToClient.row(op.tableName, op.value),
        ),
        value: serverToClient.row(op.tableName, op.value),
      };
    case 'update':
      return {
        op: 'update',
        key: toPrimaryKeyString(
          tableName,
          schema.tables[tableName].primaryKey,
          serverToClient.row(op.tableName, op.id),
        ),
        merge: op.merge
          ? serverToClient.row(op.tableName, op.merge)
          : undefined,
        constrain: serverToClient.columns(op.tableName, op.constrain),
      };
    default:
      unreachable(op);
  }
}

/**
 * Some environments we run in don't have `requestAnimationFrame` (such as
 * Node, Cloudflare Workers).
 */
function rafFallback(callback: () => void): void {
  setTimeout(callback, 0);
}
