import type {LogContext} from '@rocicorp/logger';
import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {unreachable} from 'shared/src/asserts.js';
import {
  assertJSONValue,
  JSONObject as SafeJSONObject,
} from 'shared/src/json.js';
import * as v from 'shared/src/valita.js';
import type {Downstream, EntitiesPatchOp, PokePartBody} from 'zero-protocol';
import type {JSONObject, JSONValue} from '../../types/bigint-json.js';
import type {Subscription} from '../../types/subscription.js';
import {
  ClientPatch,
  cmpVersions,
  cookieToVersion,
  DelQueryPatch,
  NullableCVRVersion,
  PutQueryPatch,
  RowID,
  versionToCookie,
  versionToNullableCookie,
  type CVRVersion,
} from './schema/types.js';

export type PutRowPatch = {
  type: 'row';
  op: 'put';
  id: RowID;
  contents: JSONObject;
};

export type MergeRowPatch = {
  type: 'row';
  op: 'merge';
  id: RowID;
  contents: JSONObject;
};

export type ConstrainRowPatch = {
  type: 'row';
  op: 'constrain';
  id: RowID;
  columns: string[];
};

export type DeleteRowPatch = {
  type: 'row';
  op: 'del';
  id: RowID;
};

export type RowPatch =
  | PutRowPatch
  | MergeRowPatch
  | ConstrainRowPatch
  | DeleteRowPatch;
export type ConfigPatch =
  | ClientPatch
  | DelQueryPatch
  | (PutQueryPatch & {ast: AST});

export type Patch = ConfigPatch | RowPatch;

export type PatchToVersion = {
  patch: Patch;
  toVersion: CVRVersion;
};

export interface PokeHandler {
  addPatch(patch: PatchToVersion): void;
  end(): void;
}

const NOOP: PokeHandler = {
  addPatch: () => {},
  end: () => {},
};

// Semi-arbitrary threshold at which poke body parts are flushed.
// When row size is being computed, that should be used as a threshold instead.
const PART_COUNT_FLUSH_THRESHOLD = 100;

/**
 * Handles a single {@link ViewSyncer.sync()} connection.
 */
export class ClientHandler {
  readonly #clientGroupID: string;
  readonly clientID: string;
  readonly wsID: string;
  readonly #lc: LogContext;
  readonly #pokes: Subscription<Downstream>;
  #baseVersion: NullableCVRVersion;

  constructor(
    lc: LogContext,
    clientGroupID: string,
    clientID: string,
    wsID: string,
    baseCookie: string | null,
    pokes: Subscription<Downstream>,
  ) {
    this.#clientGroupID = clientGroupID;
    this.clientID = clientID;
    this.wsID = wsID;
    this.#lc = lc.withContext('clientID', clientID);
    this.#pokes = pokes;
    this.#baseVersion = cookieToVersion(baseCookie);
  }

  version(): NullableCVRVersion {
    return this.#baseVersion;
  }

  fail(e: unknown) {
    this.#pokes.fail(e instanceof Error ? e : new Error(String(e)));
  }

  close() {
    this.#pokes.cancel();
  }

  startPoke(finalVersion: CVRVersion): PokeHandler {
    const pokeID = versionToCookie(finalVersion);
    const lc = this.#lc.withContext('pokeID', pokeID);

    if (cmpVersions(this.#baseVersion, finalVersion) >= 0) {
      lc.info?.(`already caught up, not sending poke.`);
      return NOOP;
    }

    const baseCookie = versionToNullableCookie(this.#baseVersion);
    const cookie = versionToCookie(finalVersion);
    lc.info?.(`starting poke from ${baseCookie} to ${cookie}`);

    this.#pokes.push(['pokeStart', {pokeID, baseCookie, cookie}]);

    let body: PokePartBody | undefined;
    let partCount = 0;
    const ensureBody = () => (body ??= {pokeID});
    const flushBody = () => {
      if (body) {
        this.#pokes.push(['pokePart', body]);
        body = undefined;
        partCount = 0;
      }
    };

    const addPatch = (patchToVersion: PatchToVersion) => {
      const {patch, toVersion} = patchToVersion;
      if (cmpVersions(toVersion, this.#baseVersion) <= 0) {
        return;
      }
      const body = ensureBody();

      const {type, op} = patch;
      switch (type) {
        case 'client':
          (body.clientsPatch ??= []).push({op, clientID: patch.id});
          break;
        case 'query': {
          const patches = patch.clientID
            ? ((body.desiredQueriesPatches ??= {})[patch.clientID] ??= [])
            : (body.gotQueriesPatch ??= []);
          if (op === 'put') {
            const {ast} = patch;
            patches.push({op, hash: patch.id, ast});
          } else {
            patches.push({op, hash: patch.id});
          }
          break;
        }
        case 'row':
          if (patch.id.schema === 'zero' && patch.id.table === 'clients') {
            this.#updateLMIDs((body.lastMutationIDChanges ??= {}), patch);
          } else {
            (body.entitiesPatch ??= []).push(makeEntityPatch(patch));
          }
          break;
        default:
          patch satisfies never;
      }

      if (++partCount >= PART_COUNT_FLUSH_THRESHOLD) {
        flushBody();
      }
    };

    return {
      addPatch: (patchToVersion: PatchToVersion) => {
        try {
          addPatch(patchToVersion);
        } catch (e) {
          this.#pokes.fail(e instanceof Error ? e : new Error(String(e)));
        }
      },

      end: () => {
        flushBody();
        this.#pokes.push(['pokeEnd', {pokeID}]);
        this.#baseVersion = finalVersion;
      },
    };
  }

  #updateLMIDs(lmids: Record<string, number>, patch: RowPatch) {
    if (patch.op === 'put' || patch.op === 'merge') {
      const row = ensureSafeJSON(patch.contents);
      const {clientGroupID, clientID, lastMutationID} = v.parse(
        row,
        lmidRowSchema,
      );
      if (clientGroupID !== this.#clientGroupID) {
        this.#lc.error?.(
          `Received zero.clients row for wrong clientGroupID. Ignoring.`,
          clientGroupID,
        );
      } else {
        lmids[clientID] = lastMutationID;
      }
    } else {
      // The 'constrain' and 'del' ops for zero.clients can be ignored.
      patch.op satisfies 'constrain' | 'del';
    }
  }
}

// Note: The zero.clients table is set up in replicator/initial-sync.ts.
const lmidRowSchema = v.object({
  clientGroupID: v.string(),
  clientID: v.string(),
  lastMutationID: v.number(), // Actually returned as a bigint, but converted by ensureSafeJSON().
  userID: v.string().optional(),
});

function makeEntityPatch(patch: RowPatch): EntitiesPatchOp {
  const {
    op,
    id: {table: entityType, rowKey: entityID},
  } = patch;

  assertStringValues(entityID); // TODO: Enforce this ZQL constraint at sync time.
  const entity = {entityType, entityID};

  switch (op) {
    case 'constrain':
      return {...entity, op: 'update', constrain: patch.columns};
    case 'put': {
      const {contents} = patch;
      return {...entity, op: 'put', value: ensureSafeJSON(contents)};
    }
    case 'merge': {
      const {contents} = patch;
      return {...entity, op: 'update', merge: ensureSafeJSON(contents)};
    }
    case 'del':
      return {...entity, op};
    default:
      patch satisfies never;
      unreachable();
  }
}

function assertStringValues(
  rowKey: Record<string, JSONValue>,
): asserts rowKey is Record<string, string> {
  for (const value of Object.values(rowKey)) {
    if (typeof value !== 'string') {
      throw new Error(`invalid row key type ${typeof value}`);
    }
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

  return modified.length ? {...row, ...Object.fromEntries(modified)} : row;
}
