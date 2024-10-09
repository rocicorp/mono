import type {ClientID} from '../../../replicache/src/mod.js';
import type {ReplicacheImpl} from '../../../replicache/src/replicache-impl.js';
import {must} from '../../../shared/src/must.js';
import {h64} from '../../../shared/src/xxhash.js';
import type {
  ChangeDesiredQueriesMessage,
  QueriesPatch,
} from '../../../zero-protocol/src/mod.js';
import {normalizeAST, type AST} from '../../../zql/src/zql/ast/ast.js';
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

  async getQueriesPatch(tx: ReadTransaction): Promise<QueriesPatch> {
    const existingQueryHashes = new Set<string>();
    const prefix = desiredQueriesPrefixForClient(this.#clientID);
    for await (const key of tx.scan({prefix}).keys()) {
      existingQueryHashes.add(key.substring(prefix.length, key.length));
    }
    const patch: QueriesPatch = [];
    for (const hash of existingQueryHashes) {
      if (!this.#queries.has(hash)) {
        patch.push({op: 'del', hash});
      }
    }
    for (const [hash, {normalized}] of this.#queries) {
      if (!existingQueryHashes.has(hash)) {
        patch.push({op: 'put', hash, ast: normalized});
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
