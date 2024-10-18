import type {LogContext} from '@rocicorp/logger';
import {unreachable} from '../../../../shared/src/asserts.js';
import {
  assertJSONValue,
  type JSONObject as SafeJSONObject,
} from '../../../../shared/src/json.js';
import * as v from '../../../../shared/src/valita.js';
import type {AST} from '../../../../zero-protocol/src/ast.js';
import {rowSchema} from '../../../../zero-protocol/src/data.js';
import type {
  Downstream,
  PokePartBody,
  RowPatchOp,
} from '../../../../zero-protocol/src/mod.js';
import {primaryKeyValueRecordSchema} from '../../../../zero-protocol/src/primary-key.js';
import type {JSONObject} from '../../types/bigint-json.js';
import {
  getErrorForClientIfSchemaVersionNotSupported,
  type SchemaVersions,
} from '../../types/schema-versions.js';
import type {Subscription} from '../../types/subscription.js';
import {
  type ClientPatch,
  cmpVersions,
  cookieToVersion,
  type CVRVersion,
  type DelQueryPatch,
  type NullableCVRVersion,
  type PutQueryPatch,
  type RowID,
  versionToCookie,
  versionToNullableCookie,
} from './schema/types.js';

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
  cancel(): void;
  end(): void;
}

const NOOP: PokeHandler = {
  addPatch: () => {},
  cancel: () => {},
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
  readonly #schemaVersion: number;

  constructor(
    lc: LogContext,
    clientGroupID: string,
    clientID: string,
    wsID: string,
    baseCookie: string | null,
    schemaVersion: number,
    pokes: Subscription<Downstream>,
  ) {
    this.#clientGroupID = clientGroupID;
    this.clientID = clientID;
    this.wsID = wsID;
    this.#lc = lc.withContext('clientID', clientID);
    this.#pokes = pokes;
    this.#baseVersion = cookieToVersion(baseCookie);
    this.#schemaVersion = schemaVersion;
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

  startPoke(
    finalVersion: CVRVersion,
    schemaVersions: SchemaVersions,
  ): PokeHandler {
    const pokeID = versionToCookie(finalVersion);
    const lc = this.#lc.withContext('pokeID', pokeID);

    const schemaVersionError = getErrorForClientIfSchemaVersionNotSupported(
      this.#schemaVersion,
      schemaVersions,
    );

    if (schemaVersionError) {
      this.fail(schemaVersionError);
      return NOOP;
    }

    if (cmpVersions(this.#baseVersion, finalVersion) >= 0) {
      lc.info?.(`already caught up, not sending poke.`);
      return NOOP;
    }

    const baseCookie = versionToNullableCookie(this.#baseVersion);
    const cookie = versionToCookie(finalVersion);
    lc.info?.(`starting poke from ${baseCookie} to ${cookie}`);

    this.#pokes.push([
      'pokeStart',
      {pokeID, baseCookie, cookie, schemaVersions},
    ]);

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
          if (patch.id.table === 'zero.clients') {
            this.#updateLMIDs((body.lastMutationIDChanges ??= {}), patch);
          } else {
            (body.rowsPatch ??= []).push(makeRowPatch(patch));
          }
          break;
        default:
          unreachable(patch);
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

      cancel: () => {
        this.#pokes.push(['pokeEnd', {pokeID, cancel: true}]);
      },

      end: () => {
        flushBody();
        this.#pokes.push(['pokeEnd', {pokeID}]);
        this.#baseVersion = finalVersion;
      },
    };
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

  return modified.length ? {...row, ...Object.fromEntries(modified)} : row;
}
