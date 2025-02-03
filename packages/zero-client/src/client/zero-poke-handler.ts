import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import type {
  PatchOperationInternal,
  PokeInternal,
} from '../../../replicache/src/impl.ts';
import type {PatchOperation} from '../../../replicache/src/patch-operation.ts';
import type {ClientID} from '../../../replicache/src/sync/ids.ts';
import {getBrowserGlobalMethod} from '../../../shared/src/browser-env.ts';
import {toClientAST} from '../../../zero-protocol/src/ast.ts';
import type {ClientsPatchOp} from '../../../zero-protocol/src/clients-patch.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {
  PokeEndBody,
  PokePartBody,
  PokeStartBody,
} from '../../../zero-protocol/src/poke.ts';
import type {QueriesPatchOp} from '../../../zero-protocol/src/queries-patch.ts';
import type {RowPatchOp} from '../../../zero-protocol/src/row-patch.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  toClientsKey,
  toDesiredQueriesKey,
  toGotQueriesKey,
  toPrimaryKeyString,
} from './keys.ts';

type PokeAccumulator = {
  readonly pokeStart: PokeStartBody;
  readonly parts: PokePartBody[];
};

type ServerToClientColumns = {[serverName: string]: string};

type ClientNames = {
  tableName: string;
  columns: ServerToClientColumns | null;
};

export function makeClientNames(schema: Schema): Map<string, ClientNames> {
  return new Map(
    Object.entries(schema.tables).map(
      ([tableName, {serverName: serverTableName, columns}]) => {
        let allSame = true;
        const names: Record<string, string> = {};
        for (const [name, {serverName}] of Object.entries(columns)) {
          if (serverName && serverName !== name) {
            allSame = false;
          }
          names[serverName ?? name] = name;
        }
        return [
          serverTableName ?? tableName,
          {tableName, columns: allSame ? null : names},
        ];
      },
    ),
  );
}

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
  #receivingPoke: PokeAccumulator | undefined = undefined;
  readonly #pokeBuffer: PokeAccumulator[] = [];
  #pokePlaybackLoopRunning = false;
  #lastRafPerfTimestamp = 0;
  // Serializes calls to this.#replicachePoke otherwise we can cause out of
  // order poke errors.
  readonly #pokeLock = new Lock();
  readonly #schema: Schema;
  readonly #clientNames: Map<string, ClientNames>;

  readonly #raf =
    getBrowserGlobalMethod('requestAnimationFrame') ?? rafFallback;

  constructor(
    replicachePoke: (poke: PokeInternal) => Promise<void>,
    onPokeError: () => void,
    clientID: ClientID,
    schema: Schema,
    lc: LogContext,
  ) {
    this.#replicachePoke = replicachePoke;
    this.#onPokeError = onPokeError;
    this.#clientID = clientID;
    this.#schema = schema;
    this.#clientNames = makeClientNames(schema);
    this.#lc = lc.withContext('PokeHandler');
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
        `pokePart for ${pokePart.pokeID}, when receiving ${this.#receivingPoke
          ?.pokeStart.pokeID}`,
      );
      return;
    }
    this.#receivingPoke.parts.push(pokePart);
    return pokePart.lastMutationIDChanges?.[this.#clientID];
  }

  handlePokeEnd(pokeEnd: PokeEndBody): void {
    if (pokeEnd.pokeID !== this.#receivingPoke?.pokeStart.pokeID) {
      this.#handlePokeError(
        `pokeEnd for ${pokeEnd.pokeID}, when receiving ${this.#receivingPoke
          ?.pokeStart.pokeID}`,
      );
      return;
    }
    if (pokeEnd.cancel) {
      this.#receivingPoke = undefined;
      return;
    }
    this.#pokeBuffer.push(this.#receivingPoke);
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
          this.#clientNames,
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
  clientNames: Map<string, ClientNames>,
): PokeInternal | undefined {
  if (pokeBuffer.length === 0) {
    return undefined;
  }
  const {baseCookie} = pokeBuffer[0].pokeStart;
  const {cookie} = pokeBuffer[pokeBuffer.length - 1].pokeStart;
  const mergedPatch: PatchOperationInternal[] = [];
  const mergedLastMutationIDChanges: Record<string, number> = {};

  let prevPokeStart = undefined;
  for (const pokeAccumulator of pokeBuffer) {
    if (
      prevPokeStart &&
      pokeAccumulator.pokeStart.baseCookie &&
      pokeAccumulator.pokeStart.baseCookie > prevPokeStart.cookie
    ) {
      throw Error(
        `unexpected cookie gap ${JSON.stringify(
          prevPokeStart,
        )} ${JSON.stringify(pokeAccumulator.pokeStart)}`,
      );
    }
    prevPokeStart = pokeAccumulator.pokeStart;
    for (const pokePart of pokeAccumulator.parts) {
      if (pokePart.lastMutationIDChanges) {
        for (const [clientID, lastMutationID] of Object.entries(
          pokePart.lastMutationIDChanges,
        )) {
          mergedLastMutationIDChanges[clientID] = lastMutationID;
        }
      }
      if (pokePart.clientsPatch) {
        mergedPatch.push(
          ...pokePart.clientsPatch.map(clientsPatchOpToReplicachePatchOp),
        );
      }
      if (pokePart.desiredQueriesPatches) {
        for (const [clientID, queriesPatch] of Object.entries(
          pokePart.desiredQueriesPatches,
        )) {
          mergedPatch.push(
            ...queriesPatch.map(op =>
              queryPatchOpToReplicachePatchOp(
                op,
                hash => toDesiredQueriesKey(clientID, hash),
                schema,
              ),
            ),
          );
        }
      }
      if (pokePart.gotQueriesPatch) {
        mergedPatch.push(
          ...pokePart.gotQueriesPatch.map(op =>
            queryPatchOpToReplicachePatchOp(op, toGotQueriesKey, schema),
          ),
        );
      }
      if (pokePart.rowsPatch) {
        mergedPatch.push(
          ...pokePart.rowsPatch.map(p =>
            rowsPatchOpToReplicachePatchOp(p, schema, clientNames),
          ),
        );
      }
    }
  }
  return {
    baseCookie,
    pullResponse: {
      lastMutationIDChanges: mergedLastMutationIDChanges,
      patch: mergedPatch,
      cookie,
    },
  };
}

function clientsPatchOpToReplicachePatchOp(op: ClientsPatchOp): PatchOperation {
  switch (op.op) {
    case 'clear':
      return op;
    case 'del':
      return {
        op: 'del',
        key: toClientsKey(op.clientID),
      };
    case 'put':
    default:
      return {
        op: 'put',
        key: toClientsKey(op.clientID),
        value: true,
      };
  }
}

function queryPatchOpToReplicachePatchOp(
  op: QueriesPatchOp,
  toKey: (hash: string) => string,
  schema: Schema,
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
    default:
      return {
        op: 'put',
        key: toKey(op.hash),
        value: toClientAST(op.ast, schema.tables),
      };
  }
}

function rowsPatchOpToReplicachePatchOp(
  op: RowPatchOp,
  schema: Schema,
  clientNames: Map<string, ClientNames>,
): PatchOperationInternal {
  if (op.op === 'clear') {
    return op;
  }
  const names = clientNames.get(op.tableName);
  if (!names) {
    throw new Error(`unknown table name in ${JSON.stringify(op)}`);
  }
  const {tableName, columns} = names;
  switch (op.op) {
    case 'del':
      return {
        op: 'del',
        key: toPrimaryKeyString(
          tableName,
          schema.tables[tableName].primaryKey,
          toClientRow(op.id, columns),
        ),
      };
    case 'put':
      return {
        op: 'put',
        key: toPrimaryKeyString(
          tableName,
          schema.tables[tableName].primaryKey,
          toClientRow(op.value, columns),
        ),
        value: toClientRow(op.value, columns),
      };
    case 'update':
      return {
        op: 'update',
        key: toPrimaryKeyString(
          tableName,
          schema.tables[tableName].primaryKey,
          toClientRow(op.id, columns),
        ),
        merge: op.merge ? toClientRow(op.merge, columns) : undefined,
        constrain: toClientColumns(op.constrain, columns),
      };
    default:
      throw new Error('to be implemented');
  }
}

function toClientRow(row: Row, names: ServerToClientColumns | null) {
  if (names === null) {
    return row;
  }
  const clientRow: Record<string, Value> = {};
  for (const col in row) {
    // Note: Columns not defined in the client schema simply pass through.
    clientRow[names[col] ?? col] = row[col];
  }
  return clientRow;
}

function toClientColumns(
  columns: string[] | undefined,
  names: ServerToClientColumns | null,
): string[] | undefined {
  // Note: Columns not defined in the client schema simply pass through.
  return !names || !columns ? columns : columns.map(col => names[col] ?? col);
}

/**
 * Some environments we run in don't have `requestAnimationFrame` (such as
 * Node, Cloudflare Workers).
 */
function rafFallback(callback: () => void): void {
  setTimeout(callback, 0);
}
