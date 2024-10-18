import type {ClientID} from '../../../replicache/src/mod.js';
import type {ReplicacheImpl} from '../../../replicache/src/replicache-impl.js';
import {must} from '../../../shared/src/must.js';
import {h64} from '../../../shared/src/xxhash.js';
import {normalizeAST, type AST} from '../../../zero-protocol/src/ast.js';
import type {
  ChangeDesiredQueriesMessage,
  QueriesPatchOp,
} from '../../../zero-protocol/src/mod.js';
import type {GotCallback} from '../../../zql/src/zql/query/query-impl.js';
import type {ReadTransaction} from '../mod.js';
import {desiredQueriesPrefixForClient, GOT_QUERIES_KEY_PREFIX} from './keys.js';

/**
 * Tracks what queries the client is currently subscribed to on the server.
 * Sends `changeDesiredQueries` message to server when this changes.
 * Deduplicates requests so that we only listen to a given unique query once.
 */
export class QueryManager {
  readonly #clientID: ClientID;
  readonly #send: (change: ChangeDesiredQueriesMessage) => void;
  readonly #queries: Map<
    QueryHash,
    {normalized: AST; count: number; gotCallbacks: GotCallback[]}
  > = new Map();
  readonly #gotQueries: Set<string> = new Set();

  constructor(
    clientID: ClientID,
    send: (change: ChangeDesiredQueriesMessage) => void,
    experimentalWatch: InstanceType<typeof ReplicacheImpl>['experimentalWatch'],
  ) {
    this.#clientID = clientID;
    this.#send = send;
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
    lastPatch?: Map<string, QueriesPatchOp> | undefined,
  ): Promise<Map<string, QueriesPatchOp>> {
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
    const astHash = hash(normalized);
    let entry = this.#queries.get(astHash);
    if (!entry) {
      entry = {
        normalized,
        count: 1,
        gotCallbacks: gotCallback === undefined ? [] : [gotCallback],
      };
      this.#queries.set(astHash, entry);

      this.#send([
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'put', hash: astHash, ast: normalized}],
        },
      ]);
    } else {
      ++entry.count;
      if (gotCallback) {
        entry.gotCallbacks.push(gotCallback);
      }
    }

    if (gotCallback) {
      queueMicrotask(() => {
        gotCallback(this.#gotQueries.has(astHash));
      });
    }

    let removed = false;
    return () => {
      if (removed) {
        return;
      }
      removed = true;
      this.#remove(astHash);
    };
  }

  #remove(astHash: string) {
    const entry = must(this.#queries.get(astHash));
    --entry.count;
    if (entry.count === 0) {
      this.#queries.delete(astHash);
      this.#send([
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: astHash}],
        },
      ]);
    }
    return true;
  }
}

type QueryHash = string;

function hash(normalized: AST): QueryHash {
  return h64(JSON.stringify(normalized)).toString(36);
}
