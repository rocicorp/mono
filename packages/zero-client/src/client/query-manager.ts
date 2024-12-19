import type {ClientID} from '../../../replicache/src/mod.js';
import type {ReplicacheImpl} from '../../../replicache/src/replicache-impl.js';
import {assert} from '../../../shared/src/asserts.js';
import {must} from '../../../shared/src/must.js';
import {hashOfAST} from '../../../zero-protocol/src/ast-hash.js';
import {normalizeAST, type AST} from '../../../zero-protocol/src/ast.js';
import type {
  ChangeDesiredQueriesMessage,
  QueriesPatchOp,
} from '../../../zero-protocol/src/mod.js';
import type {GotCallback} from '../../../zql/src/query/query-impl.js';
import type {ReadTransaction} from '../mod.js';
import {desiredQueriesPrefixForClient, GOT_QUERIES_KEY_PREFIX} from './keys.js';
import {findCover} from '../../../zql/src/cover/find-cover.js';

type QueryHash = string;
type QueryEntry = {
  normalized: AST;
  count: number;
  gotCallbacks: GotCallback[];
  coveredBy: QueryHash | undefined;
};

/**
 * Tracks what queries the client is currently subscribed to on the server.
 * Sends `changeDesiredQueries` message to server when this changes.
 * Deduplicates requests so that we only listen to a given unique query once.
 */
export class QueryManager {
  readonly #clientID: ClientID;
  readonly #send: (change: ChangeDesiredQueriesMessage) => void;
  readonly #queries: Map<QueryHash, QueryEntry> = new Map();
  readonly #recentQueriesMaxSize: number;
  readonly #recentQueries: Set<QueryHash> = new Set();
  readonly #gotQueries: Set<QueryHash> = new Set();

  // Queries that may cover other queries because they are registered with
  // the server.
  readonly #queriesThatCanCover: Map<string, Map<QueryHash, QueryEntry>> =
    new Map();
  // Queries that are covered by other queries.
  readonly #coverToCovered: Map<QueryHash, Set<QueryHash>> = new Map();
  readonly #coveredToCover: Map<QueryHash, QueryHash> = new Map();

  constructor(
    clientID: ClientID,
    send: (change: ChangeDesiredQueriesMessage) => void,
    experimentalWatch: InstanceType<typeof ReplicacheImpl>['experimentalWatch'],
    recentQueriesMaxSize: number,
  ) {
    this.#clientID = clientID;
    this.#recentQueriesMaxSize = recentQueriesMaxSize;
    this.#send = send;
    experimentalWatch(
      diff => {
        for (const diffOp of diff) {
          const queryHash = diffOp.key.substring(GOT_QUERIES_KEY_PREFIX.length);
          switch (diffOp.op) {
            case 'add':
              this.#gotQueries.add(queryHash);
              this.#updateCoverGots(queryHash, true);
              this.#fireGotCallbacks(queryHash, true);
              break;
            case 'del':
              this.#gotQueries.delete(queryHash);
              this.#updateCoverGots(queryHash, true);
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

  /**
   * If a cover becomes got (or not), then all queries that are covered by that query
   * should also be considered got (or not).
   */
  #updateCoverGots(queryHash: string, got: boolean) {
    const covered = this.#coverToCovered.get(queryHash);
    if (covered) {
      for (const hash of covered) {
        if (got) {
          this.#gotQueries.add(hash);
        } else {
          this.#gotQueries.delete(hash);
        }
      }
    }

    if (!got && covered) {
      // remove the query from the coveredToCover map
      for (const hash of covered) {
        this.#coveredToCover.delete(hash);
      }
      this.#coverToCovered.delete(queryHash);
    }
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
    lastPatch?: Map<string, QueriesPatchOp> | undefined,
  ): Promise<Map<string, QueriesPatchOp>> {
    // TODO: this needs to handle covering.

    const existingQueryHashes = new Set<string>();
    const prefix = desiredQueriesPrefixForClient(this.#clientID);
    for await (const key of tx.scan({prefix}).keys()) {
      existingQueryHashes.add(key.substring(prefix.length, key.length));
    }
    const patch: Map<string, QueriesPatchOp> = new Map();
    for (const hash of existingQueryHashes) {
      if (!this.#queries.has(hash)) {
        patch.set(hash, {op: 'del', hash});
      }
    }
    for (const [hash, {normalized}] of this.#queries) {
      if (!existingQueryHashes.has(hash)) {
        patch.set(hash, {op: 'put', hash, ast: normalized});
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

  add(ast: AST, gotCallback?: GotCallback | undefined): () => void {
    const normalized = normalizeAST(ast);
    const astHash = hashOfAST(normalized);
    let entry = this.#queries.get(astHash);
    this.#recentQueries.delete(astHash);

    if (!entry) {
      // TODO: would be nice if we could constrain the first arg to preload queries only
      const covering = findCover(this.#queriesThatCanCover, normalized);

      entry = {
        normalized,
        count: 1,
        gotCallbacks: gotCallback === undefined ? [] : [gotCallback],
        coveredBy: covering?.hash,
      };
      this.#set(astHash, entry);

      if (covering !== undefined) {
        if (gotCallback && this.#gotQueries.has(covering.hash)) {
          this.#gotQueries.add(astHash);
        }
      } else {
        this.#send([
          'changeDesiredQueries',
          {
            desiredQueriesPatch: [{op: 'put', hash: astHash, ast: normalized}],
          },
        ]);
      }
    } else {
      ++entry.count;
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
      this.#remove(astHash, gotCallback);
    };
  }

  #set(astHash: string, entry: QueryEntry) {
    this.#queries.set(astHash, entry);

    if (entry.coveredBy === undefined) {
      let existing = this.#queriesThatCanCover.get(entry.normalized.table);
      if (!existing) {
        existing = new Map();
        this.#queriesThatCanCover.set(entry.normalized.table, existing);
      }
      existing.set(astHash, entry);
    } else {
      let existing = this.#coverToCovered.get(entry.coveredBy);
      if (!existing) {
        existing = new Set();
        this.#coverToCovered.set(entry.coveredBy, existing);
      }
      existing.add(astHash);
      this.#coveredToCover.set(astHash, entry.coveredBy);
    }
  }

  #remove(astHash: string, gotCallback: GotCallback | undefined) {
    const entry = must(this.#queries.get(astHash));
    if (gotCallback) {
      const index = entry.gotCallbacks.indexOf(gotCallback);
      entry.gotCallbacks.splice(index, 1);
    }
    --entry.count;

    if (entry.count === 0) {
      if (entry.coveredBy !== undefined) {
        const covering = must(this.#coverToCovered.get(entry.coveredBy));
        covering.delete(astHash);
        this.#coveredToCover.delete(astHash);
        if (covering.size === 0) {
          this.#coverToCovered.delete(entry.coveredBy);
          const coverEntry = must(this.#queries.get(entry.coveredBy));
          if (coverEntry.count === 0) {
            this.#remove(entry.coveredBy, undefined);
          }
        }
      }
    }

    if (entry.count === 0) {
      // If we're covering a query, don't remove this query.
      if (entry.coveredBy === undefined) {
        const covering = this.#coverToCovered.get(astHash);
        if (covering && covering.size > 0) {
          return;
        }
      }

      this.#recentQueries.add(astHash);
      if (this.#recentQueries.size > this.#recentQueriesMaxSize) {
        const lruAstHash = this.#recentQueries.values().next().value;
        assert(lruAstHash);
        this.#queries.delete(lruAstHash);
        this.#queriesThatCanCover
          .get(entry.normalized.table)
          ?.delete(lruAstHash);
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
