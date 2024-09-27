import type {ClientID} from 'replicache';
import {must} from 'shared/src/must.js';
import {h64} from 'shared/src/xxhash.js';
import type {ChangeDesiredQueriesMessage, QueriesPatch} from 'zero-protocol';
import {normalizeAST, type AST} from 'zql/src/zql/ast/ast.js';
import type {ReadTransaction} from '../mod.js';
import {desiredQueriesPrefixForClient} from './keys.js';

/**
 * Tracks what queries the client is currently subscribed to on the server.
 * Sends `changeDesiredQueries` message to server when this changes.
 * Deduplicates requests so that we only listen to a given unique query once.
 */
export class QueryManager {
  readonly #clientID: ClientID;
  readonly #send: (change: ChangeDesiredQueriesMessage) => void;
  readonly #queries: Map<QueryHash, {normalized: AST; count: number}> =
    new Map();

  constructor(
    clientID: ClientID,
    send: (change: ChangeDesiredQueriesMessage) => void,
  ) {
    this.#clientID = clientID;
    this.#send = send;
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

  add(ast: AST): () => void {
    const normalized = normalizeAST(ast);
    const astHash = hash(normalized);
    let entry = this.#queries.get(astHash);
    if (!entry) {
      entry = {normalized, count: 1};
      this.#queries.set(astHash, entry);
      this.#send([
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'put', hash: astHash, ast: normalized}],
        },
      ]);
    } else {
      ++entry.count;
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
