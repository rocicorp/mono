import type {LogContext} from '@rocicorp/logger';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import type {JSONObject} from '../../../../shared/src/bigint-json.ts';
import {
  assertJSONValue,
  type JSONObject as SafeJSONObject,
} from '../../../../shared/src/json.ts';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import * as v from '../../../../shared/src/valita.ts';
import type {Writable} from '../../../../shared/src/writable.ts';
import type {ErroredQuery} from '../../../../zero-protocol/src/custom-queries.ts';
import {rowSchema} from '../../../../zero-protocol/src/data.ts';
import type {DeleteClientsBody} from '../../../../zero-protocol/src/delete-clients.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {
  ProtocolError,
  type TransformFailedBody,
} from '../../../../zero-protocol/src/error.ts';
import type {InspectDownBody} from '../../../../zero-protocol/src/inspect-down.ts';
import {mutationResultSchema} from '../../../../zero-protocol/src/mutation.ts';
import type {MutationPatch} from '../../../../zero-protocol/src/mutations-patch.ts';
import type {
  PokePartBody,
  PokeStartBody,
} from '../../../../zero-protocol/src/poke.ts';
import {primaryKeyValueRecordSchema} from '../../../../zero-protocol/src/primary-key.ts';
import type {RowPatchOp} from '../../../../zero-protocol/src/row-patch.ts';
import {
  getOrCreateCounter,
  getOrCreateLatencyHistogram,
} from '../../observability/metrics.ts';
import {setSerializedDownstream} from '../../types/downstream.ts';
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

const NOOP: PokeHandler = {
  addPatch: () => promiseVoid,
  cancel: () => promiseVoid,
  end: () => promiseVoid,
};

/** Wraps PokeHandlers for multiple clients in a single PokeHandler. */
export function startPoke(
  clients: ClientHandler[],
  tentativeVersion: CVRVersion,
): PokeHandler {
  const pokers = clients.map(c => c.startPoke(tentativeVersion));

  // Promise.allSettled() ensures that a failed (e.g. disconnected) client
  // does not prevent other clients from receiving the pokes. However, the
  // rate (per client group) will be limited by the slowest connection.
  return {
    addPatch: async patch => {
      await Promise.allSettled(pokers.map(poker => poker.addPatch(patch)));
    },
    cancel: async () => {
      await Promise.allSettled(pokers.map(poker => poker.cancel()));
    },
    end: async finalVersion => {
      await Promise.allSettled(pokers.map(poker => poker.end(finalVersion)));
    },
  };
}

// Keep poke parts small enough that clients observe progress while syncing.
// A single patch can exceed this limit because rows are atomic at the protocol
// level, but it will be sent in a part by itself.
export const POKE_PART_FLUSH_THRESHOLD_CHARS = 1024 * 1024;

type QueryPatchOp = NonNullable<PokePartBody['gotQueriesPatch']>[number];

type SerializedRecordValue = {
  readonly key: string;
  value: string;
};

type SerializedDesiredQueries = {
  readonly key: string;
  readonly patches: string[];
};

class PokePartBuilder {
  readonly #body: PokePartBody;
  readonly #pokeID: string;
  readonly #lastMutationIDChanges = new Map<string, SerializedRecordValue>();
  readonly #desiredQueriesPatches = new Map<string, SerializedDesiredQueries>();
  readonly #gotQueriesPatch: string[] = [];
  readonly #rowsPatch: string[] = [];
  readonly #mutationsPatch: string[] = [];
  #length: number;
  #patchCount = 0;

  constructor(pokeID: string) {
    this.#body = {pokeID};
    this.#pokeID = JSON.stringify(pokeID);
    this.#length = `["pokePart",{"pokeID":${this.#pokeID}}]`.length;
  }

  get length(): number {
    return this.#length;
  }

  trySetLastMutationID(
    clientID: string,
    lastMutationID: number,
    maxChars: number,
  ): boolean {
    const value = JSON.stringify(lastMutationID);
    const existing = this.#lastMutationIDChanges.get(clientID);
    const key = existing?.key ?? JSON.stringify(clientID);
    const additionalChars = existing
      ? value.length - existing.value.length
      : this.#lastMutationIDChanges.size === 0
        ? `,"lastMutationIDChanges":{${key}:${value}}`.length
        : `,${key}:${value}`.length;
    return this.#tryAdd(additionalChars, maxChars, () => {
      this.#lastMutationIDChanges.set(clientID, {key, value});
      (this.#body.lastMutationIDChanges ??= {})[clientID] = lastMutationID;
    });
  }

  tryAddDesiredQueryPatch(
    clientID: string,
    patch: QueryPatchOp,
    serialized: string,
    maxChars: number,
  ): boolean {
    const existing = this.#desiredQueriesPatches.get(clientID);
    const key = existing?.key ?? JSON.stringify(clientID);
    const additionalChars = existing
      ? `,${serialized}`.length
      : this.#desiredQueriesPatches.size === 0
        ? `,"desiredQueriesPatches":{${key}:[${serialized}]}`.length
        : `,${key}:[${serialized}]`.length;
    return this.#tryAdd(additionalChars, maxChars, () => {
      if (existing) {
        existing.patches.push(serialized);
      } else {
        this.#desiredQueriesPatches.set(clientID, {
          key,
          patches: [serialized],
        });
      }
      ((this.#body.desiredQueriesPatches ??= {})[clientID] ??= []).push(patch);
    });
  }

  tryAddGotQueryPatch(
    patch: QueryPatchOp,
    serialized: string,
    maxChars: number,
  ): boolean {
    return this.#tryAddArrayItem(
      'gotQueriesPatch',
      this.#gotQueriesPatch,
      serialized,
      maxChars,
      () => (this.#body.gotQueriesPatch ??= []).push(patch),
    );
  }

  tryAddRowPatch(
    patch: RowPatchOp,
    serialized: string,
    maxChars: number,
  ): boolean {
    return this.#tryAddArrayItem(
      'rowsPatch',
      this.#rowsPatch,
      serialized,
      maxChars,
      () => (this.#body.rowsPatch ??= []).push(patch),
    );
  }

  tryAddMutationPatch(
    patch: MutationPatch,
    serialized: string,
    maxChars: number,
  ): boolean {
    return this.#tryAddArrayItem(
      'mutationsPatch',
      this.#mutationsPatch,
      serialized,
      maxChars,
      () => (this.#body.mutationsPatch ??= []).push(patch),
    );
  }

  message(): Downstream {
    const serialized = this.#serialize();
    assert(
      serialized.length === this.#length,
      'serialized poke part length did not match the measured length',
    );
    return setSerializedDownstream(['pokePart', this.#body], serialized);
  }

  #tryAddArrayItem(
    field: string,
    items: string[],
    serialized: string,
    maxChars: number,
    addToBody: () => void,
  ): boolean {
    const additionalChars =
      items.length === 0
        ? `,${JSON.stringify(field)}:[${serialized}]`.length
        : `,${serialized}`.length;
    return this.#tryAdd(additionalChars, maxChars, () => {
      items.push(serialized);
      addToBody();
    });
  }

  #tryAdd(additionalChars: number, maxChars: number, add: () => void): boolean {
    if (this.#patchCount > 0 && this.#length + additionalChars > maxChars) {
      return false;
    }
    add();
    this.#length += additionalChars;
    this.#patchCount++;
    return true;
  }

  #serialize(): string {
    const fields = [`"pokeID":${this.#pokeID}`];
    if (this.#lastMutationIDChanges.size > 0) {
      fields.push(
        `"lastMutationIDChanges":{${Array.from(
          this.#lastMutationIDChanges.values(),
          ({key, value}) => `${key}:${value}`,
        ).join(',')}}`,
      );
    }
    if (this.#desiredQueriesPatches.size > 0) {
      fields.push(
        `"desiredQueriesPatches":{${Array.from(
          this.#desiredQueriesPatches.values(),
          ({key, patches}) => `${key}:[${patches.join(',')}]`,
        ).join(',')}}`,
      );
    }
    this.#pushArrayField(fields, 'gotQueriesPatch', this.#gotQueriesPatch);
    this.#pushArrayField(fields, 'rowsPatch', this.#rowsPatch);
    this.#pushArrayField(fields, 'mutationsPatch', this.#mutationsPatch);
    return `["pokePart",{${fields.join(',')}}]`;
  }

  #pushArrayField(fields: string[], field: string, items: string[]): void {
    if (items.length > 0) {
      fields.push(`${JSON.stringify(field)}:[${items.join(',')}]`);
    }
  }
}

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
  readonly #downstream: Subscription<Downstream>;
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
    downstream: Subscription<Downstream>,
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

  async #push(msg: Downstream): Promise<void> {
    const {result} = this.#downstream.push(msg);
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

  startPoke(tentativeVersion: CVRVersion): PokeHandler {
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
    let part: PokePartBuilder | undefined;
    const ensurePart = async () => {
      if (!pokeStarted) {
        await this.#push(['pokeStart', pokeStart]);
        pokeStarted = true;
      }
      return (part ??= new PokePartBuilder(pokeID));
    };
    const flushPart = async () => {
      if (part) {
        await this.#push(part.message());
        part = undefined;
      }
    };

    const addToPart = async (add: (part: PokePartBuilder) => boolean) => {
      let target = await ensurePart();
      if (!add(target)) {
        await flushPart();
        target = await ensurePart();
        assert(add(target), 'failed to add patch to an empty poke part');
      }
      if (target.length >= POKE_PART_FLUSH_THRESHOLD_CHARS) {
        await flushPart();
      }
    };

    const addPatch = async (patchToVersion: PatchToVersion) => {
      const {patch, toVersion} = patchToVersion;
      if (cmpVersions(toVersion, this.#baseVersion) <= 0) {
        return;
      }
      const {type, op} = patch;
      switch (type) {
        case 'query': {
          const queryPatch = {op, hash: patch.id};
          const serialized = JSON.stringify(queryPatch);
          await addToPart(part =>
            patch.clientID
              ? part.tryAddDesiredQueryPatch(
                  patch.clientID,
                  queryPatch,
                  serialized,
                  POKE_PART_FLUSH_THRESHOLD_CHARS,
                )
              : part.tryAddGotQueryPatch(
                  queryPatch,
                  serialized,
                  POKE_PART_FLUSH_THRESHOLD_CHARS,
                ),
          );
          break;
        }
        case 'row':
          if (patch.id.table === this.#zeroClientsTable) {
            const lmidChanges: Record<string, number> = {};
            this.#updateLMIDs(lmidChanges, patch);
            for (const [clientID, lastMutationID] of Object.entries(
              lmidChanges,
            )) {
              await addToPart(part =>
                part.trySetLastMutationID(
                  clientID,
                  lastMutationID,
                  POKE_PART_FLUSH_THRESHOLD_CHARS,
                ),
              );
            }
          } else if (patch.id.table === this.#zeroMutationsTable) {
            let mutationPatch: MutationPatch;
            if (op === 'put') {
              const row = v.parse(
                ensureSafeJSON(patch.contents),
                mutationRowSchema,
                'passthrough',
              );
              mutationPatch = {
                op: 'put',
                mutation: {
                  id: {
                    clientID: row.clientID,
                    id: row.mutationID,
                  },
                  result: row.result,
                },
              };
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
              mutationPatch = {
                op: 'del',
                id: {
                  clientID,
                  id,
                },
              };
            }
            const serialized = JSON.stringify(mutationPatch);
            await addToPart(part =>
              part.tryAddMutationPatch(
                mutationPatch,
                serialized,
                POKE_PART_FLUSH_THRESHOLD_CHARS,
              ),
            );
          } else {
            const rowPatch = makeRowPatch(patch);
            const serialized = JSON.stringify(rowPatch);
            await addToPart(part =>
              part.tryAddRowPatch(
                rowPatch,
                serialized,
                POKE_PART_FLUSH_THRESHOLD_CHARS,
              ),
            );
          }
          break;
        default:
          unreachable(patch);
      }
    };

    return {
      addPatch: async (patchToVersion: PatchToVersion) => {
        try {
          await addPatch(patchToVersion);
          if (patchToVersion.patch.type === 'row') {
            this.#pokedRows.add(1);
          }
        } catch (e) {
          this.#downstream.fail(wrapWithProtocolError(e));
        }
      },

      cancel: async () => {
        if (pokeStarted) {
          await this.#push(['pokeEnd', {pokeID, cookie: '', cancel: true}]);
        }
      },

      end: async (finalVersion: CVRVersion) => {
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
            `Patches were sent but finalVersion ${finalVersion} is ` +
              `not greater than baseVersion ${this.#baseVersion}`,
          );
        }
        await flushPart();
        await this.#push(['pokeEnd', {pokeID, cookie}]);
        this.#baseVersion = finalVersion;
        this.#everPoked = true;

        const elapsed = performance.now() - start;
        this.#pokeTransactions.add(1);
        this.#pokeTime.recordMs(elapsed);
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
    this.#downstream.push(['inspect', response]);
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

function makeRowPatch(patch: RowPatch): RowPatchOp {
  const {
    op,
    id: {table: tableName, rowKey: id},
  } = patch;

  switch (op) {
    case 'put':
      return {
        op: 'put',
        tableName,
        value: v.parse(ensureSafeJSON(patch.contents), rowSchema),
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
  const modified = Object.entries(row)
    .filter(([k, v]) => {
      if (typeof v === 'bigint') {
        if (v >= Number.MIN_SAFE_INTEGER && v <= Number.MAX_SAFE_INTEGER) {
          return true; // send this entry onto the next map() step.
        }
        throw new Error(`Value of "${k}" exceeds safe Number range (${v})`);
      } else if (typeof v === 'object') {
        assertJSONValue(v);
      }
      return false;
    })
    .map(([k, v]) => [k, Number(v)]);

  return modified.length
    ? {...row, ...Object.fromEntries(modified)}
    : (row as SafeJSONObject);
}
