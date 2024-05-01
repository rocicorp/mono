import type {LogContext} from '@rocicorp/logger';
import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {unreachable} from 'shared/src/asserts.js';
import type {Downstream, PokePartBody} from 'zero-protocol';
import type {Subscription} from '../../types/subscription.js';
import {
  ClientPatch,
  DelQueryPatch,
  NullableCVRVersion,
  PutQueryPatch,
  cmpVersions,
  cookieToVersion,
  versionToCookie,
  versionToNullableCookie,
  type CVRVersion,
} from './schema/types.js';

export type Patch = ClientPatch | DelQueryPatch | (PutQueryPatch & {ast: AST});

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

        switch (patch.type) {
          case 'client':
            (body.clientsPatch ??= []).push({op: patch.op, clientID: patch.id});
            break;
          case 'query': {
            const patches = patch.clientID
              ? ((body.desiredQueriesPatches ??= {})[patch.clientID] ??= [])
              : (body.gotQueriesPatch ??= []);
            if (patch.op === 'put') {
              const {ast} = patch;
              patches.push({op: 'put', hash: patch.id, ast});
            } else {
              patches.push({op: 'del', hash: patch.id});
            }
            break;
          }
          default:
            unreachable();
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
