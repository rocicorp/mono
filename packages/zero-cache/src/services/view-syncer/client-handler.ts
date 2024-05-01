import type {LogContext} from '@rocicorp/logger';
import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {unreachable} from 'shared/src/asserts.js';
import {assertJSONValue} from 'shared/src/json.js';
import type {Downstream, EntitiesPatchOp, PokePartBody} from 'zero-protocol';
import type {JSONObject, JSONValue} from '../../types/bigint-json.js';
import type {Subscription} from '../../types/subscription.js';
import {
  ClientPatch,
  DelQueryPatch,
  NullableCVRVersion,
  PutQueryPatch,
  RowID,
  cmpVersions,
  cookieToVersion,
  versionToCookie,
  versionToNullableCookie,
  type CVRVersion,
} from './schema/types.js';

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

export type RowPatch = MergeRowPatch | ConstrainRowPatch | DeleteRowPatch;
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

/**
 * Handles a single {@link ViewSyncer.sync()} connection.
 */
export class ClientHandler {
  readonly #lc: LogContext;
  readonly #pokes: Subscription<Downstream>;
  #baseVersion: NullableCVRVersion;

  constructor(
    lc: LogContext,
    clientID: string,
    baseCookie: string | null,
    pokes: Subscription<Downstream>,
  ) {
    this.#lc = lc.withContext('clientID', clientID);
    this.#pokes = pokes;
    this.#baseVersion = cookieToVersion(baseCookie);
  }

  version(): NullableCVRVersion {
    return this.#baseVersion;
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
    const ensureBody = () => (body ??= {pokeID});
    const flushBody = () => {
      if (body) {
        this.#pokes.push(['pokePart', body]);
        body = undefined;
      }
    };

    return {
      addPatch: (patchToVersion: PatchToVersion) => {
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
            (body.entitiesPatch ??= []).push(makeEntityPatch(patch));
            break;
          default:
            patch satisfies never;
        }

        // TODO: Add logic to flush body at certain simple thresholds.
      },

      end: () => {
        flushBody();
        this.#pokes.push(['pokeEnd', {pokeID}]);
        this.#baseVersion = finalVersion;
      },
    };
  }
}

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
    case 'merge': {
      const {contents} = patch;
      assertJSONValue(contents); // Asserts on unsafe integers, which BigIntJSON deserializes to bigints.
      return {...entity, op: 'update', merge: contents};
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
