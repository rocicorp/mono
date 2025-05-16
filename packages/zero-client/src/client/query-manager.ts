import type {ReplicacheImpl} from '../../../replicache/src/replicache-impl.ts';
import type {ClientID} from '../../../replicache/src/sync/ids.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {hashOfAST} from '../../../zero-protocol/src/ast-hash.ts';
import {
  mapAST,
  normalizeAST,
  type AST,
} from '../../../zero-protocol/src/ast.ts';
import type {ChangeDesiredQueriesMessage} from '../../../zero-protocol/src/change-desired-queries.ts';
import type {UpQueriesPatchOp} from '../../../zero-protocol/src/queries-patch.ts';
import {
  clientToServer,
  type NameMapper,
} from '../../../zero-schema/src/name-mapper.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {GotCallback} from '../../../zql/src/query/query-impl.ts';
import {compareTTL, parseTTL, type TTL} from '../../../zql/src/query/ttl.ts';
import {desiredQueriesPrefixForClient, GOT_QUERIES_KEY_PREFIX} from './keys.ts';
import type {MutationTracker} from './mutation-tracker.ts';
import type {ReadTransaction} from './replicache-types.ts';

type QueryHash = string;

type Entry = {
  normalized: AST;
  count: number;
  gotCallbacks: GotCallback[];
  ttl: TTL;
};

/**
 * Tracks what queries the client is currently subscribed to on the server.
 * Sends `changeDesiredQueries` message to server when this changes.
 * Deduplicates requests so that we only listen to a given unique query once.
 */
export class QueryManager {
  readonly #clientID: ClientID;
  readonly #clientToServer: NameMapper;
  readonly #send: (change: ChangeDesiredQueriesMessage) => void;
  readonly #queries: Map<QueryHash, Entry> = new Map();
  readonly #recentQueriesMaxSize: number;
  readonly #recentQueries: Set<string> = new Set();
  readonly #gotQueries: Set<string> = new Set();
  readonly #mutationTracker: MutationTracker;
  #pendingRemovals: Array<() => void> = [];

  constructor(
    mutationTracker: MutationTracker,
    clientID: ClientID,
    tables: Record<string, TableSchema>,
    send: (change: ChangeDesiredQueriesMessage) => void,
    experimentalWatch: ReplicacheImpl['experimentalWatch'],
    recentQueriesMaxSize: number,
  ) {
    this.#clientID = clientID;
    this.#clientToServer = clientToServer(tables);
    this.#recentQueriesMaxSize = recentQueriesMaxSize;
    this.#send = send;
    this.#mutationTracker = mutationTracker;

    this.#mutationTracker.onAllMutationsConfirmed(() => {
      if (this.#pendingRemovals.length === 0) {
        return;
      }
      const pendingRemovals = this.#pendingRemovals;
      this.#pendingRemovals = [];
      for (const removal of pendingRemovals) {
        removal();
      }
    });

    experimentalWatch(
      diff => {
        for (const diffOp of diff) {
          const queryHash = diffOp.key.substring(GOT_QUERIES_KEY_PREFIX.length);
          switch (diffOp.op) {
            case 'add':
              this.#gotQueries.add(queryHash);
              this.#fireGotCallbacks(queryHash, true);
              break;
            case 'del':
              this.#gotQueries.delete(queryHash);
              this.#fireGotCallbacks(queryHash, false);
              break;
          }
        }
      },
      {
        prefix: GOT_QUERIES_KEY_PREFIX,
        initialValuesInFirstDiff: true,
      },
    );
  }

  #fireGotCallbacks(queryHash: string, got: boolean) {
    const gotCallbacks = this.#queries.get(queryHash)?.gotCallbacks ?? [];
    for (const gotCallback of gotCallbacks) {
      gotCallback(got);
    }
  }

  /**
   * Get the queries that need to be registered with the server.
   *
   * An optional `lastPatch` can be provided. This is the last patch that was
   * sent to the server and may not yet have been acked. If `lastPatch` is provided,
   * this method will return a patch that does not include any events sent in `lastPatch`.
   *
   * This diffing of last patch and current patch is needed since we send
   * a set of queries to the server when we first connect inside of the `sec-protocol` as
   * the `initConnectionMessage`.
   *
   * While we're waiting for the `connected` response to come back from the server,
   * the client may have registered more queries. We need to diff the `initConnectionMessage`
   * queries with the current set of queries to understand what those were.
   */
  async getQueriesPatch(
    tx: ReadTransaction,
    lastPatch?: Map<string, UpQueriesPatchOp> | undefined,
  ): Promise<Map<string, UpQueriesPatchOp>> {
    const existingQueryHashes = new Set<string>();
    const prefix = desiredQueriesPrefixForClient(this.#clientID);
    for await (const key of tx.scan({prefix}).keys()) {
      existingQueryHashes.add(key.substring(prefix.length, key.length));
    }
    const patch: Map<string, UpQueriesPatchOp> = new Map();
    for (const hash of existingQueryHashes) {
      if (!this.#queries.has(hash)) {
        patch.set(hash, {op: 'del', hash});
      }
    }
    for (const [hash, {normalized, ttl}] of this.#queries) {
      if (!existingQueryHashes.has(hash)) {
        patch.set(hash, {op: 'put', hash, ast: normalized, ttl: parseTTL(ttl)});
      }
    }

    if (lastPatch) {
      // if there are any `puts` in `lastPatch` that are not in `patch` then we need to
      // send a `del` event in `patch`.
      for (const [hash, {op}] of lastPatch) {
        if (op === 'put' && !patch.has(hash)) {
          patch.set(hash, {op: 'del', hash});
        }
      }
      // Remove everything from `patch` that was already sent in `lastPatch`.
      for (const [hash, {op}] of patch) {
        const lastPatchOp = lastPatch.get(hash);
        if (lastPatchOp && lastPatchOp.op === op) {
          patch.delete(hash);
        }
      }
    }

    return patch;
  }

  add(ast: AST, ttl: TTL, gotCallback?: GotCallback | undefined): () => void {
    const normalized = normalizeAST(ast);
    const astHash = hashOfAST(normalized);
    let entry = this.#queries.get(astHash);
    this.#recentQueries.delete(astHash);
    if (!entry) {
      const serverAST = mapAST(normalized, this.#clientToServer);
      entry = {
        normalized: serverAST,
        count: 1,
        gotCallbacks: gotCallback ? [gotCallback] : [],
        ttl,
      };
      this.#queries.set(astHash, entry);
      this.#send([
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {op: 'put', hash: astHash, ast: serverAST, ttl: parseTTL(ttl)},
          ],
        },
      ]);
    } else {
      ++entry.count;
      this.#updateEntry(entry, astHash, ttl);

      if (gotCallback) {
        entry.gotCallbacks.push(gotCallback);
      }
    }

    if (gotCallback) {
      gotCallback(this.#gotQueries.has(astHash));
    }

    let removed = false;
    return () => {
      if (removed) {
        return;
      }
      removed = true;

      // We cannot remove queries while mutations are pending
      // as that could take data out of scope that is needed in a rebase
      if (this.#mutationTracker.size > 0) {
        this.#pendingRemovals.push(() =>
          this.#remove(entry, astHash, gotCallback),
        );
        return;
      }

      this.#remove(entry, astHash, gotCallback);
    };
  }

  update(ast: AST, ttl: TTL) {
    const normalized = normalizeAST(ast);
    const astHash = hashOfAST(normalized);
    const entry = must(this.#queries.get(astHash));
    this.#updateEntry(entry, astHash, ttl);
  }

  #updateEntry(entry: Entry, hash: string, ttl: TTL): void {
    // If the query already exists and the new ttl is larger than the old one
    // we send a changeDesiredQueries message to the server to update the ttl.
    if (compareTTL(ttl, entry.ttl) > 0) {
      entry.ttl = ttl;
      this.#send([
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {
              op: 'put',
              hash,
              ast: entry.normalized,
              ttl: parseTTL(ttl),
            },
          ],
        },
      ]);
    }
  }

  #remove(entry: Entry, astHash: string, gotCallback: GotCallback | undefined) {
    if (gotCallback) {
      const index = entry.gotCallbacks.indexOf(gotCallback);
      entry.gotCallbacks.splice(index, 1);
    }
    --entry.count;
    if (entry.count === 0) {
      this.#recentQueries.add(astHash);
      if (this.#recentQueries.size > this.#recentQueriesMaxSize) {
        const lruAstHash = this.#recentQueries.values().next().value;
        assert(lruAstHash);
        this.#queries.delete(lruAstHash);
        this.#recentQueries.delete(lruAstHash);
        this.#send([
          'changeDesiredQueries',
          {
            desiredQueriesPatch: [{op: 'del', hash: lruAstHash}],
          },
        ]);
      }
    }
  }
}
