import type {LogContext} from '@rocicorp/logger';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import type {JSONObject} from '../../../../shared/src/bigint-json.ts';
import {hasOwn} from '../../../../shared/src/has-own.ts';
import {
  assertJSONValue,
  type JSONObject as SafeJSONObject,
} from '../../../../shared/src/json.ts';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import * as v from '../../../../shared/src/valita.ts';
import type {Writable} from '../../../../shared/src/writable.ts';
import type {ErroredQuery} from '../../../../zero-protocol/src/custom-queries.ts';
import type {DeleteClientsBody} from '../../../../zero-protocol/src/delete-clients.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {
  ProtocolError,
  type TransformFailedBody,
} from '../../../../zero-protocol/src/error.ts';
import type {InspectDownBody} from '../../../../zero-protocol/src/inspect-down.ts';
import {mutationResultSchema} from '../../../../zero-protocol/src/mutation.ts';
import type {
  PokePartBody,
  PokePartMessage,
  PokeStartBody,
} from '../../../../zero-protocol/src/poke.ts';
import {primaryKeyValueRecordSchema} from '../../../../zero-protocol/src/primary-key.ts';
import type {RowPatchOp} from '../../../../zero-protocol/src/row-patch.ts';
import {
  getOrCreateCounter,
  getOrCreateLatencyHistogram,
} from '../../observability/metrics.ts';
import type {ViewSyncerDownstream} from '../../types/downstream.ts';
import {
  getLogLevel,
  wrapWithProtocolError,
} from '../../types/error-with-level.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import type {Subscription} from '../../types/subscription.ts';
import {
  cmpVersions,
  cookieToVersion,
  versionToCookie,
  versionToNullableCookie,
  type CVRVersion,
  type DelQueryPatch,
  type NullableCVRVersion,
  type PutQueryPatch,
  type RowID,
} from './schema/types.ts';

export type PutRowPatch = {
  type: 'row';
  op: 'put';
  id: RowID;
  contents: JSONObject;
};

export type DeleteRowPatch = {
  type: 'row';
  op: 'del';
  id: RowID;
};

export type RowPatch = PutRowPatch | DeleteRowPatch;
export type ConfigPatch = DelQueryPatch | PutQueryPatch;

export type Patch = ConfigPatch | RowPatch;

export type PatchToVersion = {
  patch: Patch;
  toVersion: CVRVersion;
};

export interface PokeHandler {
  addPatch(patch: PatchToVersion): Promise<void>;
  cancel(): Promise<void>;
  end(finalVersion: CVRVersion): Promise<void>;
}

/**
 * A put/del row patch and its JSON serialization. Computed once per client
 * group and shared by all of the group's clients.
 */
type SerializedRowPatch = {
  readonly rowPatch: RowPatchOp;
  readonly json: string;
};

/**
 * Lazily holds the {@link SerializedRowPatch} of one patch so that it is
 * computed at most once, by the first client that needs it.
 */
type SharedRowPatch = {serialized?: SerializedRowPatch | undefined};

/** A {@link PokeHandler} for a single client. */
export interface ClientPokeHandler extends PokeHandler {
  addPatch(patch: PatchToVersion, shared?: SharedRowPatch): Promise<void>;

  /**
   * Whether a patch has been sent to the client. Once one has, the poke can
   * only be ended with a `finalVersion` that is ahead of the client's base
   * version.
   */
  readonly patchesSent: boolean;
}

/**
 * A {@link PokeHandler} for multiple clients, as returned by
 * {@link startPoke}.
 */
export interface MultiPokeHandler extends PokeHandler {
  /** Whether a patch has been sent to any of the clients. */
  readonly patchesSent: boolean;
}

const NOOP: ClientPokeHandler = {
  patchesSent: false,
  addPatch: () => promiseVoid,
  cancel: () => promiseVoid,
  end: () => promiseVoid,
};

/** Wraps PokeHandlers for multiple clients in a single PokeHandler. */
export function startPoke(
  lc: LogContext,
  clients: ClientHandler[],
  tentativeVersion: CVRVersion,
): MultiPokeHandler {
  const pokers = clients.map(c => c.startPoke(tentativeVersion));

  // Promise.allSettled() ensures that a failed (e.g. disconnected) client
  // does not prevent other clients from receiving the pokes. However, the
  // rate (per client group) will be limited by the slowest connection.
  //
  // The individual pokers report their own failures (by failing the
  // connection), so a rejection here is not expected. Log it rather than
  // dropping it on the floor.
  const settle = async (op: string, results: Promise<void>[]) => {
    for (const result of await Promise.allSettled(results)) {
      if (result.status === 'rejected') {
        lc.error?.(`unhandled error poking client (${op})`, result.reason);
      }
    }
  };

  return {
    get patchesSent() {
      return pokers.some(poker => poker.patchesSent);
    },
    addPatch: patch => {
      const shared: SharedRowPatch = {};
      return settle(
        'addPatch',
        pokers.map(poker => poker.addPatch(patch, shared)),
      );
    },
    cancel: () =>
      settle(
        'cancel',
        pokers.map(poker => poker.cancel()),
      ),
    end: finalVersion =>
      settle(
        'end',
        pokers.map(poker => poker.end(finalVersion)),
      ),
  };
}

// Soft limit on the serialized row characters accumulated in a poke part.
// This bounds view-syncer memory and keeps legacy JSON messages reasonably
// sized. PokeChunkEncoder separately enforces the binary frame limit in bytes.
export const POKE_PART_FLUSH_THRESHOLD_CHARS = 1024 * 1024;
const PART_COUNT_FLUSH_THRESHOLD = 100;

/**
 * Handles a single `ViewSyncer` connection.
 */
export class ClientHandler {
  readonly #clientGroupID: string;
  readonly clientID: string;
  readonly wsID: string;
  readonly #zeroClientsTable: string;
  readonly #zeroMutationsTable: string;
  readonly #lc: LogContext;
  readonly #downstream: Subscription<ViewSyncerDownstream>;
  #baseVersion: NullableCVRVersion;
  // We will send a poke on connect even if the client is already caught up, so that it can learn its
  // got-queries state has been reconciled with the server. After that, we will only send a poke if
  // the client is behind.
  #everPoked = false;

  readonly #pokeTime = getOrCreateLatencyHistogram(
    'sync',
    'poke.time',
    'Time elapsed for each poke transaction. Canceled / noop pokes are excluded.',
  );

  readonly #pokeTransactions = getOrCreateCounter(
    'sync',
    'poke.transactions',
    'Count of poke transactions.',
  );

  readonly #pokedRows = getOrCreateCounter(
    'sync',
    'poke.rows',
    'Count of poked rows.',
  );

  constructor(
    lc: LogContext,
    clientGroupID: string,
    clientID: string,
    wsID: string,
    shard: ShardID,
    baseCookie: string | null,
    downstream: Subscription<ViewSyncerDownstream>,
  ) {
    lc.debug?.('new client handler');
    this.#clientGroupID = clientGroupID;
    this.clientID = clientID;
    this.wsID = wsID;
    this.#zeroClientsTable = `${upstreamSchema(shard)}.clients`;
    this.#zeroMutationsTable = `${upstreamSchema(shard)}.mutations`;
    this.#lc = lc;
    this.#downstream = downstream;
    this.#baseVersion = cookieToVersion(baseCookie);
  }

  version(): NullableCVRVersion {
    return this.#baseVersion;
  }

  async #push(msg: Downstream, serialized?: string | undefined): Promise<void> {
    const {result} = this.#downstream.push({message: msg, serialized});
    await result;
  }

  fail(e: unknown) {
    this.#lc[getLogLevel(e)]?.(
      `view-syncer closing connection with error: ${String(e)}`,
      e,
    );
    this.#downstream.fail(wrapWithProtocolError(e));
  }

  close(reason: string) {
    this.#lc.debug?.(`view-syncer closing connection: ${reason}`);
    this.#downstream.cancel();
  }

  startPoke(tentativeVersion: CVRVersion): ClientPokeHandler {
    const pokeID = versionToCookie(tentativeVersion);
    const lc = this.#lc.withContext('pokeID', pokeID);

    // Force an (empty) initial poke even when already caught up; see #everPoked.
    const forceInitialPoke = !this.#everPoked;
    const cmp = cmpVersions(this.#baseVersion, tentativeVersion);
    if (cmp > 0 || (cmp === 0 && !forceInitialPoke)) {
      lc.info?.(`already caught up, not sending poke.`);
      return NOOP;
    }

    const baseCookie = versionToNullableCookie(this.#baseVersion);
    const cookie = versionToCookie(tentativeVersion);
    lc.debug?.(`starting poke from ${baseCookie} to ${cookie}`);

    const start = performance.now();

    const pokeStart: PokeStartBody = {pokeID, baseCookie};

    let pokeStarted = false;
    let body: PokePartBody | undefined;
    // rowsPatch is kept out of `body` until it is flushed so that the poke
    // part can be serialized by concatenating the pre-serialized rows.
    let rowsPatch: RowPatchOp[] = [];
    let rowsJSON: string[] = [];
    let partCount = 0;
    let serializedRowsLength = 0;

    const ensureBody = async () => {
      if (!pokeStarted) {
        await this.#push(['pokeStart', pokeStart]);
        pokeStarted = true;
      }
      return (body ??= {pokeID});
    };

    const flushBody = async () => {
      if (body) {
        let message: PokePartMessage;
        let serialized: string;
        if (rowsPatch.length === 0) {
          message = ['pokePart', body];
          serialized = JSON.stringify(message);
        } else {
          // Equivalent to JSON.stringify(message): rowsPatch is the last
          // property of the body.
          const prefix = JSON.stringify(['pokePart', body]);
          message = ['pokePart', {...body, rowsPatch}];
          serialized = `${prefix.slice(0, -2)},"rowsPatch":[${rowsJSON.join(',')}]}]`;
          rowsPatch = [];
          rowsJSON = [];
        }
        await this.#push(message, serialized);
        body = undefined;
        partCount = 0;
        serializedRowsLength = 0;
      }
    };

    const addPatch = async (
      patchToVersion: PatchToVersion,
      shared: SharedRowPatch,
    ) => {
      const {patch, toVersion} = patchToVersion;
      if (cmpVersions(toVersion, this.#baseVersion) <= 0) {
        return;
      }
      const target = await ensureBody();

      const {type, op} = patch;
      switch (type) {
        case 'query': {
          const patches = patch.clientID
            ? ((target.desiredQueriesPatches ??= {})[patch.clientID] ??= [])
            : (target.gotQueriesPatch ??= []);
          patches.push({op, hash: patch.id});
          break;
        }
        case 'row':
          if (patch.id.table === this.#zeroClientsTable) {
            this.#updateLMIDs((target.lastMutationIDChanges ??= {}), patch);
          } else if (patch.id.table === this.#zeroMutationsTable) {
            const patches = (target.mutationsPatch ??= []);
            if (op === 'put') {
              const row = v.parse(
                ensureSafeJSON(patch.contents),
                mutationRowSchema,
                'passthrough',
              );
              patches.push({
                op: 'put',
                mutation: {
                  id: {
                    clientID: row.clientID,
                    id: row.mutationID,
                  },
                  result: row.result,
                },
              });
            } else {
              const {clientID, mutationID} = patch.id.rowKey;
              assert(
                typeof clientID === 'string',
                'client id must be a string',
              );
              const id = Number(mutationID);
              assert(
                !Number.isNaN(id) && Number.isFinite(id) && id >= 0,
                'mutation id must be a finite number',
              );
              patches.push({
                op: 'del',
                id: {
                  clientID,
                  id,
                },
              });
            }
          } else {
            const {rowPatch, json} = (shared.serialized ??=
              serializeRowPatch(patch));
            rowsPatch.push(rowPatch);
            rowsJSON.push(json);
            serializedRowsLength += json.length;
          }
          break;
        default:
          unreachable(patch);
      }

      partCount++;
      if (
        serializedRowsLength >= POKE_PART_FLUSH_THRESHOLD_CHARS ||
        partCount >= PART_COUNT_FLUSH_THRESHOLD
      ) {
        await flushBody();
      }
    };

    // A poke that was started must always be ended. If it isn't, the
    // connection is left mid-poke and the *next* pokeStart fails the
    // DownstreamSender's in-progress check, surfacing the error far from
    // its cause. Fail the connection instead; the client reconnects and
    // resyncs from its baseVersion.
    return {
      get patchesSent() {
        return pokeStarted;
      },
      addPatch: async (
        patchToVersion: PatchToVersion,
        shared: SharedRowPatch = {},
      ) => {
        try {
          await addPatch(patchToVersion, shared);
          if (patchToVersion.patch.type === 'row') {
            this.#pokedRows.add(1);
          }
        } catch (e) {
          this.fail(e);
        }
      },

      cancel: async () => {
        try {
          if (pokeStarted) {
            await this.#push(['pokeEnd', {pokeID, cookie: '', cancel: true}]);
          }
        } catch (e) {
          this.fail(e);
        }
      },

      end: async (finalVersion: CVRVersion) => {
        try {
          const cookie = versionToCookie(finalVersion);
          if (!pokeStarted) {
            if (
              cmpVersions(this.#baseVersion, finalVersion) === 0 &&
              !forceInitialPoke
            ) {
              return; // Nothing changed and nothing was sent.
            }
            await this.#push(['pokeStart', pokeStart]);
          } else if (cmpVersions(this.#baseVersion, finalVersion) >= 0) {
            // Sanity check: If the poke was started, the finalVersion
            // must be > #baseVersion.
            throw new Error(
              `Patches were sent but finalVersion ${cookie} is not ` +
                `greater than baseVersion ` +
                `${versionToNullableCookie(this.#baseVersion)}`,
            );
          }
          await flushBody();
          await this.#push(['pokeEnd', {pokeID, cookie}]);
          this.#baseVersion = finalVersion;
          this.#everPoked = true;

          const elapsed = performance.now() - start;
          this.#pokeTransactions.add(1);
          this.#pokeTime.recordMs(elapsed);
        } catch (e) {
          this.fail(e);
        }
      },
    };
  }

  async sendDeleteClients(
    lc: LogContext,
    deletedClientIDs: string[],
    deletedClientGroupIDs: string[],
  ) {
    const deleteClientsBody: Writable<DeleteClientsBody> = {};
    if (deletedClientIDs.length > 0) {
      deleteClientsBody.clientIDs = deletedClientIDs;
    }
    if (deletedClientGroupIDs.length > 0) {
      deleteClientsBody.clientGroupIDs = deletedClientGroupIDs;
    }
    lc.debug?.('sending deleteClients', deleteClientsBody);
    await this.#push(['deleteClients', deleteClientsBody]);
  }

  sendQueryTransformApplicationErrors(errors: ErroredQuery[]) {
    void this.#push(['transformError', errors]);
  }

  sendQueryTransformFailedError(error: TransformFailedBody) {
    this.fail(new ProtocolError(error));
  }

  sendInspectResponse(lc: LogContext, response: InspectDownBody): void {
    lc.debug?.('sending inspect response', response);
    this.#downstream.push({
      message: ['inspect', response],
      serialized: undefined,
    });
  }

  #updateLMIDs(lmids: Record<string, number>, patch: RowPatch) {
    if (patch.op === 'put') {
      const row = ensureSafeJSON(patch.contents);
      const {clientGroupID, clientID, lastMutationID} = v.parse(
        row,
        lmidRowSchema,
        'passthrough',
      );
      if (clientGroupID !== this.#clientGroupID) {
        this.#lc.error?.(
          `Received clients row for wrong clientGroupID. Ignoring.`,
          clientGroupID,
        );
      } else {
        lmids[clientID] = lastMutationID;
      }
    } else {
      // The 'constrain' and 'del' ops for clients can be ignored.
      patch.op satisfies 'constrain' | 'del';
    }
  }
}

// Note: The {APP_ID}_{SHARD_ID}.clients table is set up in replicator/initial-sync.ts.
const lmidRowSchema = v.object({
  clientGroupID: v.string(),
  clientID: v.string(),
  lastMutationID: v.number(), // Actually returned as a bigint, but converted by ensureSafeJSON().
});

const mutationRowSchema = v.object({
  clientGroupID: v.string(),
  clientID: v.string(),
  mutationID: v.number(),
  result: mutationResultSchema,
});

function serializeRowPatch(patch: RowPatch): SerializedRowPatch {
  const rowPatch = makeRowPatch(patch);
  return {rowPatch, json: JSON.stringify(rowPatch)};
}

function makeRowPatch(patch: RowPatch): RowPatchOp {
  const {
    op,
    id: {table: tableName, rowKey: id},
  } = patch;

  switch (op) {
    case 'put':
      // ensureSafeJSON() converts bigints and asserts (outside of prod) that
      // the values are JSON, which is all that rowSchema checks.
      return {
        op: 'put',
        tableName,
        value: ensureSafeJSON(patch.contents),
      };

    case 'del':
      return {
        op,
        tableName,
        id: v.parse(id, primaryKeyValueRecordSchema),
      };

    default:
      unreachable(op);
  }
}

/**
 * Column values of type INT8 are returned as the `bigint` from the
 * Postgres library. These are converted to `number` if they are within
 * the safe Number range, allowing the protocol to support numbers larger
 * than 32-bits. Values outside of the safe number range (e.g. > 2^53) will
 * result in an Error.
 */
export function ensureSafeJSON(row: JSONObject): SafeJSONObject {
  let modified: Record<string, unknown> | undefined;
  for (const k in row) {
    if (!hasOwn(row, k)) {
      continue;
    }
    const v = row[k];
    if (typeof v === 'bigint') {
      if (v < Number.MIN_SAFE_INTEGER || v > Number.MAX_SAFE_INTEGER) {
        throw new Error(`Value of "${k}" exceeds safe Number range (${v})`);
      }
      (modified ??= {...row})[k] = Number(v);
    } else if (typeof v === 'object') {
      assertJSONValue(v);
    }
  }
  return (modified ?? row) as SafeJSONObject;
}
