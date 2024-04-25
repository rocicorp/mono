import type {LogContext} from '@rocicorp/logger';
import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {assert, unreachable} from 'shared/src/asserts.js';
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

export interface PokeHandler {
  // TODO: RowPatch w/contents
  addPatch(
    toVersion: CVRVersion,
    patch: DelQueryPatch | ClientPatch,
  ): Promise<void>;

  addPatch(
    toVersion: CVRVersion,
    patch: PutQueryPatch,
    ast: AST,
  ): Promise<void>;

  end(): Promise<void>;
}

const NOOP: PokeHandler = {
  addPatch: () => Promise.resolve(),
  end: () => Promise.resolve(),
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
      // eslint-disable-next-line require-await
      addPatch: async (toVersion, patch, ast?: AST) => {
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
              assert(ast);
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

      // eslint-disable-next-line require-await
      end: async () => {
        flushBody();
        this.#pokes.push(['pokeEnd', {pokeID}]);
        this.#baseVersion = finalVersion;
      },
    };
  }
}
