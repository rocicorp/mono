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
import type {QueriesPatchOp} from '../../../zero-protocol/src/queries-patch.ts';
import {
  clientToServer,
  type NameMapper,
} from '../../../zero-schema/src/name-mapper.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {GotCallback} from '../../../zql/src/query/query-impl.ts';
import {desiredQueriesPrefixForClient, GOT_QUERIES_KEY_PREFIX} from './keys.ts';
import type {ReadTransaction} from './replicache-types.ts';

type QueryHash = string;

/**
 * Tracks what queries the client is currently subscribed to on the server.
 * Sends `changeDesiredQueries` message to server when this changes.
 * Deduplicates requests so that we only listen to a given unique query once.
 */
export class QueryManager {
  readonly #clientID: ClientID;
  readonly #clientToServer: NameMapper;
  readonly #send: (change: ChangeDesiredQueriesMessage) => void;
  readonly #queries: Map<
    QueryHash,
    {normalized: AST; count: number; gotCallbacks: GotCallback[]}
  > = new Map();
  readonly #recentQueriesMaxSize: number;
  readonly #recentQueries: Set<string> = new Set();
  readonly #gotQueries: Set<string> = new Set();

  constructor(
    clientID: ClientID,
    tables: Record<string, TableSchema>,
    send: (change: ChangeDesiredQueriesMessage) => void,
    experimentalWatch: InstanceType<typeof ReplicacheImpl>['experimentalWatch'],
    recentQueriesMaxSize: number,
  ) {
    this.#clientID = clientID;
    this.#clientToServer = clientToServer(tables);
    this.#recentQueriesMaxSize = recentQueriesMaxSize;
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
    const astHash = hashOfAST(normalized);
    let entry = this.#queries.get(astHash);
    this.#recentQueries.delete(astHash);
    if (!entry) {
      const serverAST = mapAST(normalized, this.#clientToServer);
      entry = {
        normalized: serverAST,
        count: 1,
        gotCallbacks: gotCallback === undefined ? [] : [gotCallback],
      };
      this.#queries.set(astHash, entry);
      this.#send([
        'changeDesiredQueries',
        {desiredQueriesPatch: [{op: 'put', hash: astHash, ast: serverAST}]},
      ]);
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

  #remove(astHash: string, gotCallback: GotCallback | undefined) {
    const entry = must(this.#queries.get(astHash));
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
